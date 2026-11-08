/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator
 *
 * 1.0 class StreamingJobGraphGenerator（StreamingJobGraphGenerator.java:78）
 * —— 把 StreamGraph（逻辑拓扑）编译成 JobGraph（JobManager 接受的物理拓扑）。
 * **算子链的切分就在这里**（isChainable），它产出的 StreamConfig 正是
 * OperatorChain / OneInputStreamTask / TwoInputStreamTask 在运行时读的那份。
 *
 * 常量与字段（:86-99）：
 *   private static final long DEFAULT_RESTART_DELAY = 10000L;
 *       1.0 原注释：「Restart delay used for the FixedDelayRestartStrategy in
 *       case checkpointing was enabled but no restart strategy has been
 *       specified.」
 *   private StreamGraph streamGraph;
 *   private Map<Integer, JobVertex> jobVertices;
 *   private JobGraph jobGraph;
 *   private Collection<Integer> builtVertices;
 *   private List<StreamEdge> physicalEdgesInOrder;
 *   private Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;
 *   private Map<Integer, StreamConfig> vertexConfigs;
 *   private Map<Integer, String> chainedNames;
 *
 * ━━━━━━━━━━━━━ createJobGraph()（:114）的六个步骤 ━━━━━━━━━━━━━
 *   jobGraph = new JobGraph(streamGraph.getJobName());
 *   jobGraph.setScheduleMode(ScheduleMode.ALL);   // "make sure that all
 *                                                //  vertices start immediately"
 *   init();
 *   // "Generate deterministic hashes for the nodes in order to identify them
 *   //  across submission iff they didn't change."
 *   Map<Integer, byte[]> hashes = traverseStreamGraphAndGenerateHashes();
 *   setChaining(hashes);
 *   setPhysicalEdges();
 *   setSlotSharing();
 *   configureCheckpointing();
 *   configureRestartStrategy();
 *   InstantiationUtil.writeObjectToConfig(executionConfig, jobConfiguration, ...)
 *   return jobGraph;
 *
 * ━━━━━━━━━━━━━━━ createChain（:181）—— 整个文件的核心 ━━━━━━━━━━━━━━━
 *
 * 1.0 setChaining 的注释：「Sets up task chains from the source StreamNode
 * instances. This will recursively create all JobVertex instances.」
 *
 *   createChain(startNodeId, currentNodeId, hashes):
 *     if builtVertices 已含 startNodeId -> 返回空表
 *     把 current 的出边按 isChainable 分成 chainableOutputs / nonChainableOutputs
 *     对每条 chainable：   transitiveOutEdges += createChain(startNodeId,
 *                                                target, hashes)
 *                         —— startNodeId **不变**，说明还在同一条链里
 *     对每条 nonChainable：transitiveOutEdges += 这条边本身
 *                         createChain(target, target, hashes)
 *                         —— startNodeId **换成 target**，开一条新链
 *     chainedNames[current] = createChainedName(current, chainableOutputs)
 *     config = (current == start) ? createJobVertex(start, hashes)
 *                                 : new StreamConfig(new Configuration())
 *     setVertexConfig(current, config, chainableOutputs, nonChainableOutputs)
 *     if (current == start):           // 链头
 *         config.setChainStart();
 *         config.setOutEdgesInOrder(transitiveOutEdges);
 *         config.setOutEdges(current 的全部出边);
 *         for (edge : transitiveOutEdges) connect(startNodeId, edge);
 *         config.setTransitiveChainedTaskConfigs(chainedConfigs[startNodeId]);
 *     else:                            // 链内
 *         chainedConfigs[startNodeId][currentNodeId] = config;
 *     return transitiveOutEdges;
 *
 *   —— transitiveOutEdges 是「整条链**作为一个顶点**向外的出边」，所以它的
 *   顺序就是 config.setOutEdgesInOrder 的顺序，也就是运行时
 *   Environment.getWriter(i) 的 i。这与 OperatorChain.createStreamOutput 那边
 *   的下标约定是同一件事的两端。
 *
 * ━━━━━━━━━━━━━━━━━ isChainable（:401）—— 链切分判定 ━━━━━━━━━━━━━━━━━
 *   return downStreamVertex.getInEdges().size() == 1
 *       && outOperator  != null
 *       && headOperator != null
 *       && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
 *       && outOperator.getChainingStrategy()  == ChainingStrategy.ALWAYS
 *       && (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
 *           headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
 *       && (edge.getPartitioner() instanceof ForwardPartitioner)
 *       && upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
 *       && streamGraph.isChainingEnabled();
 *   八个条件缺一不可，逐条照搬。
 *
 * ━━━━━━━━━━━━━━━━━━━━ 确定性 hash（:520-745）━━━━━━━━━━━━━━━━━━━━
 *
 * 1.0 traverseStreamGraphAndGenerateHashes 的注释：
 *   「Returns a map with a hash for each StreamNode of the StreamGraph. The
 *    hash is used as the JobVertexID in order to identify nodes across job
 *    submissions if they didn't change. ... The generated hash is
 *    deterministic with respect to: node-local properties (like parallelism,
 *    UDF, node ID), chained output nodes, and input nodes hashes」
 *
 *   - 「We need to make the source order deterministic. The source IDs are not
 *      returned in the same order, which means that submitting the same
 *      program twice might result in different traversal, which breaks the
 *      deterministic hash assignment.」-> Collections.sort(sources)
 *   - BFS（ArrayDeque 当队列）：generateNodeHash 返回 false 表示「入边的 hash
 *     还没齐」，此时把自己从 visited 里摘掉（"We will revisit this later."）
 *   - generateNodeLocalHash(node, hasher, id)：
 *       hasher.putInt(id);                 // id = hashes.size()，不是 node.id
 *                                          // "because it is incremented in a
 *                                          //  static counter. Therefore, the
 *                                          //  IDs for identical jobs will
 *                                          //  otherwise be different."
 *       hasher.putInt(node.getParallelism());
 *       若算子是 AbstractUdfStreamOperator -> putString(UDF 类名, UTF-8)
 *   - generateDeterministicHash：节点本身 + 所有 chainable 下游（都用同一个
 *     hashes.size() 当 id），再对每条入边做
 *       hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
 *   - 用户指定 uid 的分支：先确认本节点不是链的中间节点（否则
 *     UnsupportedOperationException "Cannot assign user-specified hash to
 *     intermediate node in chain. ..."），再 putString(uid)，并对已有 hash
 *     查重（IllegalArgumentException "Hash collision on user-specified ID. ..."）
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— hash 函数】1.0 用 Guava 的 Hashing.murmur3_128(0)。这里自带一个
 * 标准 MurmurHash3_x64_128（seed 0）的增量实现（见 Murmur3_128Hasher），
 * putInt 按小端写 4 字节、putString 按 UTF-8 写原始字节 —— 与 Guava 的
 * Hasher 契约一致。但**不保证与 Guava 逐字节相同**：Guava 的 tail 处理有自己
 * 的缓冲实现。语义要求只是「同一程序两次提交得到同一 hash、不同程序大概率
 * 不同」，本实现满足。
 *
 * 【偏差 2 —— InputFormatVertex 不落地】1.0 createJobVertex 里
 * `if (streamNode.getInputFormat() != null)` 那一支建 InputFormatVertex 并写
 * TaskConfig.setStubWrapper；inputFormat 字段本身就没移植（StreamNode.h
 * 偏差 3），所以只保留 else 分支。
 *
 * 【偏差 3 —— TimeCharacteristic 的来源】1.0 setVertexConfig 里
 * `config.setTimeCharacteristic(streamGraph.getEnvironment()
 * .getStreamTimeCharacteristic())`。StreamGraph 不持 env（StreamGraph.h
 * 偏差 1），所以本实现由生成器持一个 timeCharacteristic_ 字段（构造时传入，
 * 默认 ProcessingTime —— 与 1.0 StreamExecutionEnvironment 的默认值一致）。
 *
 * （connect 的三个分支逐条照搬，包括 1.0 里 ForwardPartitioner 与
 *  RescalePartitioner 两支函数体完全相同这一点。）
 *
 * 【偏差 4 —— 重启策略】configureCheckpointing 末尾那段
 * 「if (getRestartStrategy() == null) setRestartStrategy(fixedDelayRestart(
 *   Integer.MAX_VALUE, DEFAULT_RESTART_DELAY))」与 configureRestartStrategy()
 * 都依赖 RestartStrategies 体系（未移植，见 JobGraph.h 偏差 2）。常量
 * DEFAULT_RESTART_DELAY 保留以备后续接入。
 *
 * 【偏差 5 —— ExecutionConfig 不写进 jobConfiguration】createJobGraph 末尾的
 * InstantiationUtil.writeObjectToConfig 需要序列化层，见 JobGraph.h 偏差 3。
 * 1.0 包的那句 RuntimeException("Config object could not be written to Job
 * Configuration: ") 一并不出现。
 *
 * 【偏差 6 —— setVertexConfig 里的死代码】1.0:355-356 建了一个
 * `List<StreamEdge> allOutputs = chainableOutputs + nonChainableOutputs`
 * 之后**完全没用**就返回了。原样省掉。
 *
 * 【偏差 7 —— 日志省掉】LOG.debug 若干处（含 "CONNECTED: {} - {} -> {}"
 * 与 "Generated hash '...' for node"）。
 */
#ifndef FLINK_STREAMING_API_GRAPH_STREAMINGJOBGRAPHGENERATOR_H
#define FLINK_STREAMING_API_GRAPH_STREAMINGJOBGRAPHGENERATOR_H

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "flink/runtime/jobgraph/JobGraph.h"
#include "flink/streaming/api/TimeCharacteristic.h"
#include "flink/streaming/api/graph/StreamGraph.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

/*
 * 【偏差 1】Guava Hashing.murmur3_128(0) 的替代：标准
 * MurmurHash3_x64_128（seed 0）的增量版本，接口对齐 Guava 的 Hasher：
 *   putInt(int)                  —— 小端 4 字节
 *   putString(std::string)       —— UTF-8 原始字节
 *   hash()                       —— 16 字节
 */
class Murmur3_128Hasher {
 public:
  Murmur3_128Hasher();

  void putInt(int32_t value);
  void putString(const std::string& value);
  std::vector<unsigned char> hash() const;

 private:
  void putByte(unsigned char byte);
  void processBlock(const unsigned char* block);

  uint64_t h1_;
  uint64_t h2_;
  unsigned char buffer_[16];
  int bufferLength_;
  size_t totalLength_;
};

class StreamingJobGraphGenerator {
 public:
  typedef flink::runtime::jobgraph::JobGraph JobGraph;
  typedef flink::runtime::jobgraph::JobVertex JobVertex;
  typedef flink::runtime::jobgraph::JobVertexID JobVertexID;
  typedef std::shared_ptr<JobVertex> JobVertexPtr;
  typedef StreamGraph::StreamEdgePtr StreamEdgePtr;
  typedef std::vector<StreamEdgePtr> StreamEdgeList;
  typedef std::vector<unsigned char> NodeHash;
  typedef std::map<int, NodeHash> HashMap;

  // 1.0 的构造只收 StreamGraph。【偏差 3】额外收 timeCharacteristic
  explicit StreamingJobGraphGenerator(
      StreamGraph* streamGraph,
      TimeCharacteristic timeCharacteristic =
          TimeCharacteristic::ProcessingTime)
      : streamGraph_(streamGraph), timeCharacteristic_(timeCharacteristic) {}

  // 1.0 createJobGraph()（:114）
  std::shared_ptr<JobGraph> createJobGraph();

  // 1.0 private isChainable(StreamEdge)（:401）—— 测试要直接验证这八个条件，
  // 所以这里放成 public（1.0 的 StreamingJobGraphGeneratorTest 走的是
  // 作业级断言，本仓库没有 DataStream API，见 graph_test.cpp 的说明）
  bool isChainable(const StreamEdgePtr& edge) const;

  // 1.0 private traverseStreamGraphAndGenerateHashes()（:520）
  HashMap traverseStreamGraphAndGenerateHashes();

 private:
  void init();

  // 1.0 private setPhysicalEdges()（:145）
  void setPhysicalEdges();
  // 1.0 private setChaining(hashes)（:175）
  void setChaining(const HashMap& hashes);
  // 1.0 private createChain(startNodeId, currentNodeId, hashes)（:181）
  StreamEdgeList createChain(int startNodeId, int currentNodeId,
                             const HashMap& hashes);
  // 1.0 private createChainedName(vertexID, chainedOutputs)（:247）
  std::string createChainedName(int vertexID,
                                const StreamEdgeList& chainedOutputs) const;
  // 1.0 private createJobVertex(streamNodeId, hashes)（:262）
  StreamConfig* createJobVertex(int streamNodeId, const HashMap& hashes);
  // 1.0 private setVertexConfig(...)（:310）
  void setVertexConfig(int vertexID, StreamConfig* config,
                       const StreamEdgeList& chainableOutputs,
                       const StreamEdgeList& nonChainableOutputs);
  // 1.0 private connect(headOfChain, edge)（:361）
  void connect(int headOfChain, const StreamEdgePtr& edge);
  // 1.0 private setSlotSharing()（:420）
  void setSlotSharing();
  // 1.0 private configureCheckpointing()（:451）
  void configureCheckpointing();

  // 1.0 private generateNodeHash(node, hashFunction, hashes)（:587）
  bool generateNodeHash(StreamNode* node, HashMap& hashes);
  // 1.0 private generateDeterministicHash(node, hasher, hashes)（:665）
  NodeHash generateDeterministicHash(StreamNode* node,
                                     Murmur3_128Hasher& hasher,
                                     const HashMap& hashes);
  // 1.0 private generateUserSpecifiedHash(node, hasher)（:655）
  NodeHash generateUserSpecifiedHash(StreamNode* node,
                                     Murmur3_128Hasher& hasher) const;
  // 1.0 private generateNodeLocalHash(node, hasher, id)（:731）
  void generateNodeLocalHash(StreamNode* node, Murmur3_128Hasher& hasher,
                             int id) const;

  // 1.0: "Restart delay used for the FixedDelayRestartStrategy in case
  //       checkpointing was enabled but no restart strategy has been
  //       specified."（【偏差 4】暂未接入）
  static const int64_t DEFAULT_RESTART_DELAY = 10000LL;

  StreamGraph* streamGraph_;
  TimeCharacteristic timeCharacteristic_;

  std::map<int, JobVertexPtr> jobVertices_;
  std::shared_ptr<JobGraph> jobGraph_;
  std::set<int> builtVertices_;

  StreamEdgeList physicalEdgesInOrder_;

  // 1.0 Map<Integer, Map<Integer, StreamConfig>>；内层正好是 StreamConfig 的
  // ChainedConfigMap（map<int, shared_ptr<StreamConfig>>），直接复用
  std::map<int, StreamConfig::ChainedConfigMap> chainedConfigs_;
  // 1.0 的 vertexConfigs 存的是 StreamConfig 对象（实质是对 Configuration 的
  // 视图，多份视图改的是同一个底层 map）。本仓库 StreamConfig 是独立结构体，
  // 所以这里存**指针**，指向真正的那一份（链头在 JobVertex 里、链内在
  // chainedConfigs_ 里），从而 setPhysicalEdges 的回填能生效
  std::map<int, StreamConfig*> vertexConfigs_;
  std::map<int, std::string> chainedNames_;

  // 1.0 的 SlotSharingGroup 由局部 map 持有，但 JobVertex 只存裸指针，
  // 所以这里必须让生成器保管它们的生命周期
  std::vector<std::shared_ptr<flink::runtime::jobgraph::SlotSharingGroup> >
      slotSharingGroupPool_;
  std::vector<std::shared_ptr<flink::runtime::jobgraph::CoLocationGroup> >
      coLocationGroupPool_;
};

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_GRAPH_STREAMINGJOBGRAPHGENERATOR_H
