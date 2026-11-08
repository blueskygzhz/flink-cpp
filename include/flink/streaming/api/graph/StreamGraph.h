/*
 * flink-streaming-java : org.apache.flink.streaming.api.graph.StreamGraph
 *
 * 1.0 class StreamGraph extends StreamingPlan（StreamGraph.java:71）
 *
 * 1.0 类注释：「Class representing the streaming topology. It contains all the
 * information necessary to build the jobgraph for the execution.」
 *
 * 字段（:75-92）：
 *   private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;
 *   private final StreamExecutionEnvironment environment;
 *   private final ExecutionConfig executionConfig;
 *   private final CheckpointConfig checkpointConfig;
 *   private boolean chaining;
 *   private Map<Integer, StreamNode> streamNodes;
 *   private Set<Integer> sources;
 *   private Set<Integer> sinks;
 *   private Map<Integer, Tuple2<Integer, List<String>>> virtualSelectNodes;
 *   private Map<Integer, Tuple2<Integer, StreamPartitioner<?>>> virtuaPartitionNodes;
 *       （1.0 的字段名就少一个 l，拼写错误原样保留在本实现的注释里，
 *         成员名按 C++ 规范写作 virtualPartitionNodes_）
 *   protected Map<Integer, String> vertexIDtoBrokerID;
 *   protected Map<Integer, Long> vertexIDtoLoopTimeout;
 *   private AbstractStateBackend stateBackend;
 *   private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;
 *
 * ━━━━━━━━━━━━ 本文件的三处核心机制 ━━━━━━━━━━━━
 *
 * ① 虚拟节点（virtual select / virtual partition），:283-312
 *   DataStream 上的 .select(...) 与 .shuffle()/.rebalance()/.keyBy() 这类
 *   「只改连接方式、不产生算子」的转换**不建真实顶点**，而是登记一个虚拟 id：
 *     virtualSelectNodes[virtualId]    = (originalId, selectedNames)
 *     virtuaPartitionNodes[virtualId]  = (originalId, partitioner)
 *   1.0 原注释（:273-277）：「Adds a new virtual node that is used to connect
 *   a downstream vertex to only the outputs with the selected names. When
 *   adding an edge from the virtual node to a downstream node the connection
 *   will be made to the original node, only with the selected names given
 *   here.」
 *   随后 addEdgeInternal **递归**把虚拟 id 解析回真实 id，并沿途把
 *   selectedNames / partitioner 带下来（:339-387）。注意两条优先级规则：
 *     - select：`if (outputNames.isEmpty())` 才采用虚拟节点上的名字 ——
 *       1.0 注释「selections that happen downstream override earlier
 *       selections」（:350）
 *     - partition：`if (partitioner == null)` 才采用虚拟节点上的
 *   这两条合起来决定了 StreamEdge 最终的 selectedNames 与 partitioner，也就是
 *   DirectedOutput 的路由依据与 RecordWriter 的通道选择依据。
 *
 * ② partitioner 的缺省推导（:365-380）
 *   1.0 原注释：「If no partitioner was specified and the parallelism of
 *   upstream and downstream operator matches use forward partitioning, use
 *   rebalance otherwise.」
 *   并且 ForwardPartitioner 被显式校验：上下游并行度不等就
 *   UnsupportedOperationException("Forward partitioning does not allow change
 *   of parallelism. ... You must use another partitioning strategy, such as
 *   broadcast, rebalance, shuffle or global.")
 *
 * ③ 按算子类型挑 StreamTask 子类（addOperator，:190-196）
 *   StoppableStreamSource -> StoppableSourceStreamTask
 *   StreamSource          -> SourceStreamTask
 *   其它（单输入）        -> OneInputStreamTask
 *   addCoOperator         -> TwoInputStreamTask（:230）
 *   createIterationSourceAndSink -> StreamIterationHead / StreamIterationTail
 *   —— 这正是 StreamNode.jobVertexClass 的来源。
 *
 * 其余方法（逐一复刻）：
 *   clear() :107          「Remove all registered nodes etc.」
 *   addSource / addSink   = addOperator + 记入 sources / sinks
 *   addNode :249          重复 id 则 RuntimeException("Duplicate vertexID " + id)
 *   addVirtualSelectNode :283    重复则 IllegalStateException(
 *                                "Already has virtual select node with id " + id)
 *   addVirtualPartitionNode :304 重复则 IllegalStateException(
 *                                "Already has virtual partition node with id " + id)
 *   getSlotSharingGroup(id) :317 「Determines the slot sharing group of an
 *                                operation across virtual nodes.」—— 沿虚拟
 *                                节点递归回真实顶点再取组名
 *   addOutputSelector :389       同样要穿透虚拟节点（先 partition 再 select）
 *   setParallelism / setBufferTimeout :404/:423  顶点不存在时**静默返回**
 *                                （1.0 的 `if (getStreamNode(id) != null)`）
 *   setOneInputStateKey / setTwoInputStateKey :410/:416
 *   setSerializers :429 / setSerializersFrom :436
 *       注意 setSerializersFrom 的第二行：
 *         toVertex.setSerializerOut(fromVertex.getTypeSerializerIn1())
 *       取的是 **In1**（不是 Out）—— 这是 1.0 为 iteration 的 head/tail 对
 *       准备的「反向」接法，原样照搬。
 *   setOutType / setOperator / setTransformationId
 *   getStreamNode / getVertexIDs / getStreamNodes / getOperators
 *   getStreamEdges(sourceId, targetId) :471  找不到则 RuntimeException(
 *       "No such edge in stream graph: " + sourceId + " -> " + targetId)
 *   getSourceIDs / getSinkIDs
 *   isIterative() :158  = !vertexIDtoLoopTimeout.isEmpty()
 *   createIterationSourceAndSink :517
 *   getBrokerID / getLoopTimeout / getIterationSourceSinkPairs
 *   enum ResourceStrategy { DEFAULT, ISOLATE, NEWGROUP } :608
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 不持 StreamExecutionEnvironment】同 StreamNode.h 偏差 1：
 * StreamExecutionEnvironment 还没落地，而且让图反向依赖 env 会把头文件绕成环。
 * 1.0 用 env 做三件事：(a) 取 executionConfig / checkpointConfig —— 这里改为
 * StreamGraph **自己持有**这两个对象（env 落地后把它们交给图即可）；
 * (b) 给 StreamNode 提供 parallelism / bufferTimeout 的缺省值 —— 这里由
 * StreamGraph 持 defaultParallelism_ / defaultBufferTimeout_ 并在 addNode 时
 * 注入；(c) getEnvironment() 供 JSONGenerator 用 —— 见偏差 5。
 *
 * 【偏差 2 —— TypeInformation 换成 serializer】1.0 的 addOperator /
 * addCoOperator / setOutType 收 TypeInformation<T>，内部再
 * `typeInfo.createSerializer(executionConfig)`，并对 MissingTypeInfo 判空。
 * 本实现没有 TypeInformation 体系（类型由模板参数在编译期定，serializer 从
 * TypeExtractor<T> 取），所以这些入口**直接收 AnySerializer**，`null` 判定
 * 退化成空 shared_ptr 判定。相应地，1.0 在这两个方法里顺带做的
 * OutputTypeConfigurable.setOutputType / InputTypeConfigurable.setInputType
 * 回填也不存在（见 AbstractUdfStreamOperator.h 的同名偏差）。
 *
 * 【偏差 3 —— jobVertexClass 用字符串】见 StreamNode.h 偏差 2。于是
 * addOperator 里的 `instanceof` 链变成：dynamic_cast<SourceOperator*> 成功
 * -> "SourceStreamTask"，否则 -> "OneInputStreamTask"。
 * StoppableStreamSource 这一支没有对应实现（1.0 的 stoppable source 整条线
 * 未移植），判定省掉；常量名 "StoppableSourceStreamTask" 一并不出现。
 *
 * 【偏差 4 —— getJobGraph() 不落地】1.0 的 getJobGraph()（:569）先做
 * 「迭代作业默认禁 checkpoint」的校验，再 new StreamingJobGraphGenerator(this)
 * .createJobGraph()。JobGraph 与 StreamingJobGraphGenerator 都还没移植，所以
 * 这里只把那条校验单独留成 checkJobGraphPreconditions()（文案逐字照搬），
 * 等生成器落地后再由它调用。
 *
 * 【偏差 5 —— JSON 输出不落地】getStreamingPlanAsJSON() /
 * dumpStreamingPlanAsJSON(File)（:584/:594）依赖 JSONGenerator 与 Jackson，
 * 且只服务于 Web UI 的执行计划展示，不影响运行时语义，省掉。
 * StreamingPlan 基类同理（本类不继承任何东西）。
 *
 * 【偏差 6 —— removeEdge / removeVertex 省掉】1.0 这两个方法是 private 且
 * **全仓库没有调用者**（grep 确认，只有 removeVertex 内部调 removeEdge），
 * 是遗留的死代码。
 *
 * 【偏差 7 —— setInputFormat 省掉】见 StreamNode.h 偏差 3（inputFormat 字段
 * 本身就没落地）。
 *
 * 【偏差 8 —— 容器与类型擦除】Map<Integer,...> -> std::map<int,...>，
 * Set<Integer> -> std::set<int>（1.0 用 HashSet，这里用有序 set 让
 * getSourceIDs/getSinkIDs/getVertexIDs 的遍历顺序确定 —— 1.0 的 HashMap
 * 遍历顺序本就不保证，StreamingJobGraphGenerator 也不依赖它）。
 * StreamOperator<?> / KeySelector<?,?> / TypeSerializer<?> / OutputSelector<?>
 * 的擦除手法沿用 StreamNode.h 偏差 4。
 *
 * 【偏差 9 —— iteration 顶点保留但对应 task 未落地】
 * createIterationSourceAndSink / vertexIDtoBrokerID / vertexIDtoLoopTimeout /
 * iterationSourceSinkPairs / isIterative() 全部原样复刻（jobVertexClass 已是
 * 字符串，不需要 StreamIterationHead/Tail 真实存在）。注意 1.0 这里给
 * addNode 传的 operator 是 **null**，所以 iteration 顶点的 getOperator() 为空。
 */
#ifndef FLINK_STREAMING_API_GRAPH_STREAMGRAPH_H
#define FLINK_STREAMING_API_GRAPH_STREAMGRAPH_H

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "flink/api/common/ExecutionConfig.h"
#include "flink/api/java/tuple/Tuple2.h"
#include "flink/runtime/state/AbstractStateBackend.h"
#include "flink/streaming/api/environment/CheckpointConfig.h"
#include "flink/streaming/api/graph/StreamEdge.h"
#include "flink/streaming/api/graph/StreamNode.h"
#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

class StreamGraph {
 public:
  typedef flink::streaming::api::operators::StreamOperator StreamOperator;
  typedef StreamNode::StreamOperatorPtr StreamOperatorPtr;
  typedef StreamNode::SerializerPtr SerializerPtr;
  typedef StreamNode::KeySelectorPtr KeySelectorPtr;
  typedef StreamNode::OutputSelectorPtr OutputSelectorPtr;
  typedef std::shared_ptr<StreamNode> StreamNodePtr;
  typedef std::shared_ptr<StreamEdge> StreamEdgePtr;
  typedef StreamEdge::PartitionerPtr PartitionerPtr;
  typedef std::shared_ptr<flink::runtime::state::AbstractStateBackend>
      StateBackendPtr;
  typedef flink::api::common::ExecutionConfig ExecutionConfig;
  typedef flink::streaming::api::environment::CheckpointConfig CheckpointConfig;

  // 1.0 StreamExecutionEnvironment.DEFAULT_JOB_NAME
  static const char* defaultJobName() { return "Flink Streaming Job"; }

  // 1.0 enum ResourceStrategy（:608）
  enum class ResourceStrategy { DEFAULT, ISOLATE, NEWGROUP };

  // 1.0 的 jobVertexClass 常量，这里是类名字符串（偏差 3）
  static const char* SOURCE_STREAM_TASK;
  static const char* ONE_INPUT_STREAM_TASK;
  static const char* TWO_INPUT_STREAM_TASK;
  static const char* STREAM_ITERATION_HEAD;
  static const char* STREAM_ITERATION_TAIL;

  // 1.0 StreamGraph(StreamExecutionEnvironment)：从 env 取 executionConfig /
  // checkpointConfig，然后 clear()。【偏差 1】改为直接收缺省并行度与
  // bufferTimeout（env 为 StreamNode 提供的那两个缺省值）
  StreamGraph(int defaultParallelism, int64_t defaultBufferTimeout);

  // 1.0 clear()：「Remove all registered nodes etc.」
  void clear();

  ExecutionConfig& getExecutionConfig() { return executionConfig_; }
  const ExecutionConfig& getExecutionConfig() const { return executionConfig_; }
  CheckpointConfig& getCheckpointConfig() { return checkpointConfig_; }
  const CheckpointConfig& getCheckpointConfig() const {
    return checkpointConfig_;
  }

  const std::string& getJobName() const { return jobName_; }
  void setJobName(const std::string& jobName) { jobName_ = jobName; }

  void setChaining(bool chaining) { chaining_ = chaining; }
  bool isChainingEnabled() const { return chaining_; }

  void setStateBackend(const StateBackendPtr& backend) {
    stateBackend_ = backend;
  }
  const StateBackendPtr& getStateBackend() const { return stateBackend_; }

  // 1.0 isIterative() = !vertexIDtoLoopTimeout.isEmpty()
  bool isIterative() const { return !vertexIDtoLoopTimeout_.empty(); }

  // ------------------------------- 建顶点 -------------------------------

  // 1.0 addSource(...) = addOperator(...) + sources.add(vertexID)
  void addSource(int vertexID, const std::string& slotSharingGroup,
                 const StreamOperatorPtr& operatorObject,
                 const SerializerPtr& inSerializer,
                 const SerializerPtr& outSerializer,
                 const std::string& operatorName);

  // 1.0 addSink(...) = addOperator(...) + sinks.add(vertexID)
  void addSink(int vertexID, const std::string& slotSharingGroup,
               const StreamOperatorPtr& operatorObject,
               const SerializerPtr& inSerializer,
               const SerializerPtr& outSerializer,
               const std::string& operatorName);

  // 1.0 addOperator(...)（:182）—— 按算子类型挑 StreamTask 子类
  void addOperator(int vertexID, const std::string& slotSharingGroup,
                   const StreamOperatorPtr& operatorObject,
                   const SerializerPtr& inSerializer,
                   const SerializerPtr& outSerializer,
                   const std::string& operatorName);

  // 1.0 addCoOperator(...)（:221）—— 一律 TwoInputStreamTask
  void addCoOperator(int vertexID, const std::string& slotSharingGroup,
                     const StreamOperatorPtr& taskOperatorObject,
                     const SerializerPtr& in1Serializer,
                     const SerializerPtr& in2Serializer,
                     const SerializerPtr& outSerializer,
                     const std::string& operatorName);

  // ------------------------------ 虚拟节点 ------------------------------

  // 1.0 addVirtualSelectNode(originalId, virtualId, selectedNames)（:283）
  void addVirtualSelectNode(int originalId, int virtualId,
                            const std::vector<std::string>& selectedNames);

  // 1.0 addVirtualPartitionNode(originalId, virtualId, partitioner)（:304）
  void addVirtualPartitionNode(int originalId, int virtualId,
                               const PartitionerPtr& partitioner);

  // 1.0 getSlotSharingGroup(id)（:317）：「Determines the slot sharing group
  // of an operation across virtual nodes.」
  std::string getSlotSharingGroup(int id) const;

  // -------------------------------- 建边 --------------------------------

  // 1.0 addEdge(up, down, typeNumber)（:330）
  void addEdge(int upStreamVertexID, int downStreamVertexID, int typeNumber);

  // ------------------------------ 顶点属性 ------------------------------

  // 1.0 addOutputSelector(vertexID, outputSelector)（:389）：穿透虚拟节点
  void addOutputSelector(int vertexID, const OutputSelectorPtr& outputSelector);

  // 1.0 setParallelism / setBufferTimeout：顶点不存在时静默返回
  void setParallelism(int vertexID, int parallelism);
  void setBufferTimeout(int vertexID, int64_t bufferTimeout);

  // 1.0 setOneInputStateKey / setTwoInputStateKey（:410/:416）
  void setOneInputStateKey(int vertexID, const KeySelectorPtr& keySelector,
                           const SerializerPtr& keySerializer);
  void setTwoInputStateKey(int vertexID, const KeySelectorPtr& keySelector1,
                           const KeySelectorPtr& keySelector2,
                           const SerializerPtr& keySerializer);

  // 1.0 setSerializers(vertexID, in1, in2, out)（:429）
  void setSerializers(int vertexID, const SerializerPtr& in1,
                      const SerializerPtr& in2, const SerializerPtr& out);

  // 1.0 setSerializersFrom(from, to)（:436）—— 注意第二行取的是 In1，见头注释
  void setSerializersFrom(int from, int to);

  // 1.0 setOutType(vertexID, TypeInformation<OUT>)（:444）—— 偏差 2
  void setOutType(int vertexID, const SerializerPtr& outSerializer);

  // 1.0 setOperator(vertexID, operatorObject)（:448）
  void setOperator(int vertexID, const StreamOperatorPtr& operatorObject);

  // 1.0 setTransformationId(nodeId, transformationId)（:456）：
  // 包可见，顶点不存在时静默返回
  void setTransformationId(int nodeId, const std::string& transformationId);

  // -------------------------------- 查询 --------------------------------

  // 1.0 getStreamNode(vertexID)：不存在返回 null
  StreamNode* getStreamNode(int vertexID) const;

  // 1.0 getVertexIDs()（protected）
  std::vector<int> getVertexIDs() const;

  // 1.0 getStreamEdges(sourceId, targetId)（:471）
  std::vector<StreamEdgePtr> getStreamEdges(int sourceId, int targetId) const;

  const std::set<int>& getSourceIDs() const { return sources_; }
  const std::set<int>& getSinkIDs() const { return sinks_; }

  // 1.0 getStreamNodes()：返回 streamNodes.values()
  std::vector<StreamNode*> getStreamNodes() const;

  // 1.0 getOperators()：Set<Tuple2<Integer, StreamOperator<?>>>
  std::vector<flink::api::java::tuple::Tuple2<int, StreamOperatorPtr> >
  getOperators() const;

  // ------------------------------- iteration -------------------------------

  // 1.0 createIterationSourceAndSink(loopId, sourceId, sinkId, timeout,
  //                                  parallelism)（:517）
  flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*>
  createIterationSourceAndSink(int loopId, int sourceId, int sinkId,
                               int64_t timeout, int parallelism);

  const std::vector<
      flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*> >&
  getIterationSourceSinkPairs() const {
    return iterationSourceSinkPairs_;
  }

  // 1.0 getBrokerID(vertexID) / getLoopTimeout(vertexID)
  std::string getBrokerID(int vertexID) const;
  int64_t getLoopTimeout(int vertexID) const;

  // 【偏差 4】1.0 getJobGraph() 开头那段校验，单独留出来
  void checkJobGraphPreconditions() const;

 protected:
  // 1.0 protected addNode(vertexID, slotSharingGroup, vertexClass,
  //                       operatorObject, operatorName)（:249）
  StreamNode* addNode(int vertexID, const std::string& slotSharingGroup,
                      const std::string& vertexClassName,
                      const StreamOperatorPtr& operatorObject,
                      const std::string& operatorName);

 private:
  // 1.0 private addEdgeInternal(up, down, typeNumber, partitioner,
  //                             outputNames)（:339）—— 递归解析虚拟节点
  void addEdgeInternal(int upStreamVertexID, int downStreamVertexID,
                       int typeNumber, PartitionerPtr partitioner,
                       std::vector<std::string> outputNames);

  std::string jobName_;
  ExecutionConfig executionConfig_;
  CheckpointConfig checkpointConfig_;
  bool chaining_;

  std::map<int, StreamNodePtr> streamNodes_;
  std::set<int> sources_;
  std::set<int> sinks_;

  // 1.0 virtualSelectNodes: virtualId -> (originalId, selectedNames)
  std::map<int, flink::api::java::tuple::Tuple2<
                    int, std::vector<std::string> > >
      virtualSelectNodes_;
  // 1.0 virtuaPartitionNodes（原文拼写少个 l）:
  //     virtualId -> (originalId, partitioner)
  std::map<int, flink::api::java::tuple::Tuple2<int, PartitionerPtr> >
      virtualPartitionNodes_;

  std::map<int, std::string> vertexIDtoBrokerID_;
  std::map<int, int64_t> vertexIDtoLoopTimeout_;
  StateBackendPtr stateBackend_;
  // 1.0 是 Set<Tuple2<StreamNode,StreamNode>>；顶点指针唯一，用 vector 足够
  std::vector<flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*> >
      iterationSourceSinkPairs_;

  // 【偏差 1】1.0 由 env 提供
  const int defaultParallelism_;
  const int64_t defaultBufferTimeout_;
};

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_GRAPH_STREAMGRAPH_H
