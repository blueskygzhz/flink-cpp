// 作业图构建层测试
//
// 覆盖 1.0 的这几条路径：
//   StreamGraph.addOperator / addSource / addSink / addCoOperator
//                                       按算子类型挑 StreamTask 子类
//   StreamGraph.addNode                 重复 vertexID 的检查
//   StreamGraph.addVirtualSelectNode    split/select 的虚拟节点
//   StreamGraph.addVirtualPartitionNode shuffle/rebalance/... 的虚拟节点
//   StreamGraph.addEdgeInternal         递归解析虚拟节点、缺省 partitioner
//                                       推导、ForwardPartitioner 的并行度校验
//   StreamGraph.getSlotSharingGroup     穿透虚拟节点
//   StreamGraph.addOutputSelector       穿透虚拟节点
//   StreamGraph.createIterationSourceAndSink / isIterative
//   CheckpointConfig                    参数校验与默认值
//   StreamingJobGraphGenerator.isChainable          算子链的八条切分条件
//   StreamingJobGraphGenerator.createChain          链 -> JobVertex
//   StreamingJobGraphGenerator.createChainedName    "A -> B" / "A -> (B, C)"
//   StreamingJobGraphGenerator.connect              POINTWISE / ALL_TO_ALL
//   StreamingJobGraphGenerator.setPhysicalEdges     inPhysicalEdges 回填
//   StreamingJobGraphGenerator.setSlotSharing       槽位共享组与 co-location
//   StreamingJobGraphGenerator.configureCheckpointing  JobSnapshottingSettings
//   StreamingJobGraphGenerator.traverseStreamGraphAndGenerateHashes
//   JobGraph.getVerticesSortedTopologicallyFromSources
//
// 1.0 对应的测试是 StreamGraphGeneratorTest（api/graph/），但它整套都是
// **DataStream 级**的（env.fromElements(...).rebalance().map(...).split(...)），
// 依赖还没移植的 StreamExecutionEnvironment / DataStream / StreamGraphGenerator。
// 这里把同一批场景下沉到 StreamGraph 层：手工按 StreamGraphGenerator 会做的
// 动作调 addVirtualPartitionNode / addVirtualSelectNode / addEdge，断言的
// 期望值（partitioner 类型、selectedNames、outputSelectors）逐条照抄 1.0。

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "flink/api/common/typeutils/TypedAnySerializer.h"
#include "flink/runtime/state/memory/MemoryStateBackend.h"
#include "flink/streaming/api/collector/selector/TypedAnyOutputSelector.h"
#include "flink/streaming/api/graph/StreamGraph.h"
#include "flink/streaming/api/graph/StreamingJobGraphGenerator.h"
#include "flink/streaming/api/operators/StreamFilter.h"
#include "flink/streaming/api/operators/StreamMap.h"
#include "flink/streaming/api/operators/StreamSource.h"
#include "flink/streaming/api/operators/co/CoStreamMap.h"
#include "flink/streaming/runtime/partitioner/BroadcastPartitioner.h"
#include "flink/streaming/runtime/partitioner/ForwardPartitioner.h"
#include "flink/streaming/runtime/partitioner/GlobalPartitioner.h"
#include "flink/streaming/runtime/partitioner/HashPartitioner.h"
#include "flink/streaming/runtime/partitioner/RebalancePartitioner.h"
#include "flink/streaming/runtime/partitioner/ShufflePartitioner.h"

using flink::api::common::functions::MapFunction;
using flink::api::common::typeutils::TypedAnySerializer;
using flink::streaming::api::graph::StreamGraph;
using flink::streaming::api::graph::StreamNode;
using flink::streaming::api::operators::StreamMap;
using flink::streaming::runtime::partitioner::BroadcastPartitioner;
using flink::streaming::runtime::partitioner::ForwardPartitioner;
using flink::streaming::runtime::partitioner::GlobalPartitioner;
using flink::streaming::runtime::partitioner::RebalancePartitioner;
using flink::streaming::runtime::partitioner::ShufflePartitioner;
using flink::streaming::api::graph::StreamingJobGraphGenerator;
using flink::runtime::jobgraph::DistributionPattern;
using flink::runtime::jobgraph::JobVertex;
using flink::streaming::api::graph::StreamConfig;
using flink::streaming::runtime::partitioner::HashPartitioner;

namespace {

int failures = 0;

void check(bool condition, const char* what) {
  std::printf(condition ? "ok    %s\n" : "FAIL  %s\n", what);
  if (!condition) ++failures;
}

// ---------------------------------------------------------------------- UDFs

// 1.0 StreamGraphGeneratorTest.NoOpIntMap（:272）
class NoOpIntMap : public MapFunction<int32_t, int32_t> {
 public:
  int32_t map(const int32_t& value) override { return value; }
};

// 1.0 EvenOddOutputSelector（flink-streaming-java test utils）：
//   return value % 2 == 0 ? Arrays.asList("even") : Arrays.asList("odd");
class EvenOddOutputSelector
    : public flink::streaming::api::collector::selector::OutputSelector<
          int32_t> {
 public:
  std::vector<std::string> select(const int32_t& value) override {
    return value % 2 == 0 ? std::vector<std::string>({"even"})
                          : std::vector<std::string>({"odd"});
  }
};

// 一个最小的 SourceFunction，仅用来让 addOperator 走到
// 「dynamic_cast<SourceOperator*> 成功 -> SourceStreamTask」那一支
class NoOpSource
    : public flink::streaming::api::functions::source::SourceFunction<int32_t> {
 public:
  void run(flink::streaming::api::functions::source::SourceContext<int32_t>&)
      override {}
  void cancel() override {}
};

// ------------------------------------------------------------------- helpers

StreamGraph::StreamOperatorPtr makeMapOperator() {
  return std::make_shared<StreamMap<int32_t, int32_t> >(
      std::make_shared<NoOpIntMap>());
}

StreamGraph::StreamOperatorPtr makeSourceOperator() {
  typedef flink::streaming::api::operators::StreamSource<int32_t, NoOpSource>
      Source;
  return std::make_shared<Source>(std::make_shared<NoOpSource>());
}

StreamGraph::StreamOperatorPtr makeCoMapOperator() {
  class IdentityCoMap
      : public flink::streaming::api::functions::co::CoMapFunction<
            int32_t, int32_t, int32_t> {
   public:
    int32_t map1(const int32_t& value) override { return value; }
    int32_t map2(const int32_t& value) override { return value; }
  };
  return std::make_shared<flink::streaming::api::operators::co::CoStreamMap<
      int32_t, int32_t, int32_t> >(std::make_shared<IdentityCoMap>());
}

StreamGraph::SerializerPtr intSerializer() {
  return TypedAnySerializer<int32_t>::create();
}

// 新建一张图：缺省并行度 1、缺省 bufferTimeout 100（1.0
// StreamConfig.DEFAULT_TIMEOUT）
StreamGraph newGraph(int defaultParallelism = 1) {
  return StreamGraph(defaultParallelism, 100);
}

// 把一个单输入算子顶点加进图（1.0 StreamGraphGenerator.transformOneInputTransform
// 会做的事）
void addMap(StreamGraph& graph, int id, const std::string& name = "Map") {
  graph.addOperator(id, "", makeMapOperator(), intSerializer(),
                    intSerializer(), name);
}

template <class T>
bool isPartitioner(const StreamGraph::StreamEdgePtr& edge) {
  return dynamic_cast<T*>(edge->getPartitioner().get()) != NULL;
}

// 1.0 的 `node.getOutputSelectors().contains(selector)`
bool containsSelector(StreamNode* node,
                      const StreamGraph::OutputSelectorPtr& selector) {
  for (const auto& candidate : node->getOutputSelectors()) {
    if (candidate == selector) return true;
  }
  return false;
}

// ---------------------------------------------------------------------- tests

// 1.0 addOperator 的 instanceof 链（StreamGraph.java:190-196）+ addCoOperator
// （:230）：顶点的 jobVertexClass 由算子类型决定
void testVertexClassSelection() {
  StreamGraph graph = newGraph();

  graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                  intSerializer(), "Source");
  addMap(graph, 2);
  graph.addCoOperator(3, "", makeCoMapOperator(), intSerializer(),
                      intSerializer(), intSerializer(), "Co-Map");
  graph.addSink(4, "", makeMapOperator(), intSerializer(),
                StreamGraph::SerializerPtr(), "Sink");

  check(graph.getStreamNode(1)->getJobVertexClassName() == "SourceStreamTask",
        "StreamSource 的顶点挑 SourceStreamTask");
  check(graph.getStreamNode(2)->getJobVertexClassName() == "OneInputStreamTask",
        "普通单输入算子挑 OneInputStreamTask");
  check(graph.getStreamNode(3)->getJobVertexClassName() == "TwoInputStreamTask",
        "addCoOperator 一律挑 TwoInputStreamTask");
  check(graph.getSourceIDs() == std::set<int>({1}) &&
            graph.getSinkIDs() == std::set<int>({4}),
        "addSource / addSink 额外记进 sources / sinks");
  check(graph.getStreamNodes().size() == 4 &&
            graph.getVertexIDs() == std::vector<int>({1, 2, 3, 4}),
        "四个顶点都在 streamNodes 里");

  // 1.0 addOperator 内部 setSerializers(vertexID, in, null, out)
  check(graph.getStreamNode(2)->getTypeSerializerIn1() &&
            !graph.getStreamNode(2)->getTypeSerializerIn2() &&
            graph.getStreamNode(2)->getTypeSerializerOut(),
        "addOperator 的 in2 恒为 null（单输入）");
  check(graph.getStreamNode(3)->getTypeSerializerIn1() &&
            graph.getStreamNode(3)->getTypeSerializerIn2(),
        "addCoOperator 把两条输入的 serializer 都装上");

  // 1.0 addNode: throw new RuntimeException("Duplicate vertexID " + vertexID)
  bool threwDuplicate = false;
  std::string message;
  try {
    addMap(graph, 2);
  } catch (const std::exception& e) {
    threwDuplicate = true;
    message = e.what();
  }
  check(threwDuplicate && message == "Duplicate vertexID 2",
        "重复的 vertexID 被拒（1.0: \"Duplicate vertexID \" + vertexID）");

  check(graph.getStreamNode(99) == NULL,
        "getStreamNode 对不存在的 id 返回 null");
  check(graph.getJobName() == "Flink Streaming Job",
        "jobName 默认取 StreamExecutionEnvironment.DEFAULT_JOB_NAME");
}

// 1.0 addEdgeInternal 的缺省 partitioner 推导（StreamGraph.java:365-371）：
// 「If no partitioner was specified and the parallelism of upstream and
//   downstream operator matches use forward partitioning, use rebalance
//   otherwise.」
void testDefaultPartitioner() {
  StreamGraph graph = newGraph();
  addMap(graph, 1);
  addMap(graph, 2);
  addMap(graph, 3);
  graph.setParallelism(3, 4);  // 与上游并行度不同

  graph.addEdge(1, 2, 0);
  graph.addEdge(1, 3, 0);

  auto sameParallelism = graph.getStreamEdges(1, 2);
  auto differentParallelism = graph.getStreamEdges(1, 3);

  check(sameParallelism.size() == 1 &&
            isPartitioner<ForwardPartitioner>(sameParallelism[0]),
        "上下游并行度相同 -> ForwardPartitioner");
  check(differentParallelism.size() == 1 &&
            isPartitioner<RebalancePartitioner>(differentParallelism[0]),
        "并行度不同 -> RebalancePartitioner");

  // 边双向挂好了：源点的 outEdges、目标点的 inEdges
  check(graph.getStreamNode(1)->getOutEdges().size() == 2 &&
            graph.getStreamNode(2)->getInEdges().size() == 1 &&
            graph.getStreamNode(3)->getInEdges().size() == 1,
        "addEdgeInternal 同时挂 outEdge 与 inEdge");
  check(graph.getStreamNode(1)->getOutEdgeIndices() ==
            std::vector<int>({2, 3}),
        "getOutEdgeIndices 返回对端 id");

  // 1.0 getStreamEdges: RuntimeException("No such edge in stream graph: ...")
  bool threw = false;
  std::string message;
  try {
    graph.getStreamEdges(2, 3);
  } catch (const std::exception& e) {
    threw = true;
    message = e.what();
  }
  check(threw && message == "No such edge in stream graph: 2 -> 3",
        "getStreamEdges 找不到边时抛 1.0 的原文案");
}

// 1.0 addEdgeInternal 的 ForwardPartitioner 校验（StreamGraph.java:373-380）
void testForwardPartitionerParallelismCheck() {
  StreamGraph graph = newGraph();
  addMap(graph, 1);
  addMap(graph, 2);
  graph.setParallelism(2, 4);

  // 显式用 ForwardPartitioner（1.0 是 .forward() 建的虚拟 partition 节点）
  graph.addVirtualPartitionNode(1, 100,
                                std::make_shared<ForwardPartitioner>());

  bool threw = false;
  std::string message;
  try {
    graph.addEdge(100, 2, 0);
  } catch (const std::exception& e) {
    threw = true;
    message = e.what();
  }
  check(threw, "forward + 并行度变化被拒");
  check(message.find("Forward partitioning does not allow change of "
                     "parallelism.") == 0 &&
            message.find("You must use another partitioning strategy, such as "
                         "broadcast, rebalance, shuffle or global.") !=
                std::string::npos,
        "错误信息与 1.0 逐字一致（含两端并行度）");
  check(message.find("parallelism: 1") != std::string::npos &&
            message.find("parallelism: 4") != std::string::npos,
        "错误信息里带上了上下游各自的并行度");
}

// 1.0 StreamGraphGeneratorTest.testVirtualTransformations（:65-139）下沉到
// StreamGraph 层。
//
// 1.0 的程序（:69-116）与这里的对应：
//   source(1) -> rebalance() -> map(rebalanceMap=2)
//   rebalanceMap -> forward().global().broadcast() -> map(broadcastMap=3)
//   rebalanceMap -> map(map1Operator=4) -> broadcast().split(s1).select("even")
//   rebalanceMap -> map(map2Operator=5) -> split(s2).select("odd").global()
//   rebalanceMap -> map(map3Operator=6) -> global().split(s3).select("even")
//                                          .shuffle()
//   三者 union 后 -> map(unionedMap=7)
// 每个虚拟转换按 StreamGraphGenerator 的做法分配一个新 id（这里用 1xx）。
void testVirtualTransformations() {
  StreamGraph graph = newGraph();

  graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                  intSerializer(), "Source");
  addMap(graph, 2, "rebalanceMap");
  addMap(graph, 3, "broadcastMap");
  addMap(graph, 4, "map1Operator");
  addMap(graph, 5, "map2Operator");
  addMap(graph, 6, "map3Operator");
  addMap(graph, 7, "unionedMap");

  // source.rebalance().map(...)
  graph.addVirtualPartitionNode(1, 101,
                                std::make_shared<RebalancePartitioner>());
  graph.addEdge(101, 2, 0);

  // rebalanceMap.forward().global().broadcast().map(...)
  // 1.0:73 "verify that only the partitioning that was set last is used"
  graph.addVirtualPartitionNode(2, 102, std::make_shared<ForwardPartitioner>());
  graph.addVirtualPartitionNode(102, 103,
                                std::make_shared<GlobalPartitioner>());
  graph.addVirtualPartitionNode(103, 104,
                                std::make_shared<BroadcastPartitioner>());
  graph.addEdge(104, 3, 0);

  // 三条分支各自 map
  graph.addEdge(2, 4, 0);
  graph.addEdge(2, 5, 0);
  graph.addEdge(2, 6, 0);

  auto selector1 = std::make_shared<
      flink::streaming::api::collector::selector::TypedAnyOutputSelector<
          int32_t> >(std::make_shared<EvenOddOutputSelector>());
  auto selector2 = std::make_shared<
      flink::streaming::api::collector::selector::TypedAnyOutputSelector<
          int32_t> >(std::make_shared<EvenOddOutputSelector>());
  auto selector3 = std::make_shared<
      flink::streaming::api::collector::selector::TypedAnyOutputSelector<
          int32_t> >(std::make_shared<EvenOddOutputSelector>());

  // map1Operator.broadcast().split(selector1).select("even")
  graph.addVirtualPartitionNode(4, 110,
                                std::make_shared<BroadcastPartitioner>());
  graph.addOutputSelector(110, selector1);  // split 作用在虚拟节点上
  graph.addVirtualSelectNode(110, 111, std::vector<std::string>({"even"}));

  // map2Operator.split(selector2).select("odd").global()
  graph.addOutputSelector(5, selector2);
  graph.addVirtualSelectNode(5, 120, std::vector<std::string>({"odd"}));
  graph.addVirtualPartitionNode(120, 121,
                                std::make_shared<GlobalPartitioner>());

  // map3Operator.global().split(selector3).select("even").shuffle()
  graph.addVirtualPartitionNode(6, 130, std::make_shared<GlobalPartitioner>());
  graph.addOutputSelector(130, selector3);
  graph.addVirtualSelectNode(130, 131, std::vector<std::string>({"even"}));
  graph.addVirtualPartitionNode(131, 132,
                                std::make_shared<ShufflePartitioner>());

  // map1.union(map2).union(map3).map(unionedMap)
  graph.addEdge(111, 7, 0);
  graph.addEdge(121, 7, 0);
  graph.addEdge(132, 7, 0);

  // 1.0:121
  check(isPartitioner<RebalancePartitioner>(
            graph.getStreamNode(2)->getInEdges()[0]),
        "rebalance() 的 partitioner 落到 rebalanceMap 的入边上");

  // 1.0:124-125 "verify that only last partitioning takes precedence"
  check(isPartitioner<BroadcastPartitioner>(
            graph.getStreamNode(3)->getInEdges()[0]),
        "forward().global().broadcast() 只有最后那个 broadcast 生效");
  check(graph.getStreamNode(3)->getInEdges()[0]->getSourceVertex()->getId() ==
            2,
        "虚拟 partition 节点链被解析回真实上游顶点 rebalanceMap");

  // 1.0:127-138 "verify that partitioning in unions is preserved and that it
  //              works across split/select"
  auto map1Out = graph.getStreamNode(4)->getOutEdges()[0];
  check(isPartitioner<BroadcastPartitioner>(map1Out),
        "broadcast 跨 split/select 保留到最终边上（map1）");
  check(map1Out->getSelectedNames() == std::vector<std::string>({"even"}),
        "select(\"even\") 落到最终边的 selectedNames");
  check(containsSelector(graph.getStreamNode(4), selector1),
        "split 的 OutputSelector 穿透虚拟节点挂到真实顶点（map1）");

  auto map2Out = graph.getStreamNode(5)->getOutEdges()[0];
  check(isPartitioner<GlobalPartitioner>(map2Out),
        "select 之后再 global()：partitioner 取 global（map2）");
  check(map2Out->getSelectedNames() == std::vector<std::string>({"odd"}),
        "select(\"odd\") 落到最终边（map2）");
  check(containsSelector(graph.getStreamNode(5), selector2),
        "OutputSelector 挂到 map2Operator");

  auto map3Out = graph.getStreamNode(6)->getOutEdges()[0];
  check(isPartitioner<ShufflePartitioner>(map3Out),
        "global().split().select().shuffle()：最后的 shuffle 胜出（map3）");
  check(map3Out->getSelectedNames() == std::vector<std::string>({"even"}),
        "select(\"even\") 落到最终边（map3）");
  check(containsSelector(graph.getStreamNode(6), selector3),
        "OutputSelector 挂到 map3Operator");

  check(graph.getStreamNode(7)->getInEdges().size() == 3,
        "union 的三条分支都连到同一个下游顶点");
}

// 1.0 addEdgeInternal:349-352 「selections that happen downstream override
// earlier selections」—— 下游的 select 覆盖上游的 select
void testDownstreamSelectionOverridesEarlier() {
  StreamGraph graph = newGraph();
  addMap(graph, 1);
  addMap(graph, 2);

  // .split(s).select("even").select("odd")：第二次 select 在更下游
  graph.addVirtualSelectNode(1, 100, std::vector<std::string>({"even"}));
  graph.addVirtualSelectNode(100, 101, std::vector<std::string>({"odd"}));
  graph.addEdge(101, 2, 0);

  auto edge = graph.getStreamEdges(1, 2)[0];
  check(edge->getSelectedNames() == std::vector<std::string>({"odd"}),
        "更靠下游的 select 覆盖更早的（outputNames 非空就不再采用上游的）");
}

// 1.0 getSlotSharingGroup(id)（:317）：「Determines the slot sharing group of
// an operation across virtual nodes.」
void testSlotSharingGroupAcrossVirtualNodes() {
  StreamGraph graph = newGraph();
  graph.addOperator(1, "group-a", makeMapOperator(), intSerializer(),
                    intSerializer(), "Map");

  graph.addVirtualPartitionNode(1, 100,
                                std::make_shared<BroadcastPartitioner>());
  graph.addVirtualSelectNode(100, 101, std::vector<std::string>({"even"}));

  check(graph.getSlotSharingGroup(1) == "group-a", "真实顶点直接取组名");
  check(graph.getSlotSharingGroup(100) == "group-a",
        "虚拟 partition 节点递归回真实顶点取组名");
  check(graph.getSlotSharingGroup(101) == "group-a",
        "虚拟 select 节点同样递归（嵌套两层也对）");

  // 1.0 的重复虚拟节点检查
  bool threwSelect = false;
  bool threwPartition = false;
  try {
    graph.addVirtualSelectNode(100, 101, std::vector<std::string>({"odd"}));
  } catch (const std::exception&) {
    threwSelect = true;
  }
  try {
    graph.addVirtualPartitionNode(1, 100,
                                  std::make_shared<GlobalPartitioner>());
  } catch (const std::exception&) {
    threwPartition = true;
  }
  check(threwSelect && threwPartition,
        "虚拟节点 id 重复被拒（1.0: \"Already has virtual ... node with id\"）");
}

// 1.0 setSerializersFrom(from, to)（:436）—— 注意第二行取的是 In1
void testSerializersFromAndStateKey() {
  StreamGraph graph = newGraph();
  addMap(graph, 1);
  addMap(graph, 2);

  auto in1 = intSerializer();
  auto out = intSerializer();
  graph.setSerializers(1, in1, StreamGraph::SerializerPtr(), out);
  graph.setSerializersFrom(1, 2);

  check(graph.getStreamNode(2)->getTypeSerializerIn1() == out,
        "setSerializersFrom：to.in1 = from.out");
  check(graph.getStreamNode(2)->getTypeSerializerOut() == in1,
        "setSerializersFrom：to.out = from.**in1**（1.0 的反向接法，照搬）");

  auto keySerializer = intSerializer();
  graph.setOneInputStateKey(1, StreamGraph::KeySelectorPtr(), keySerializer);
  check(graph.getStreamNode(1)->getStateKeySerializer() == keySerializer,
        "setOneInputStateKey 装上 keyed state 的 key serializer");

  graph.setTwoInputStateKey(2, StreamGraph::KeySelectorPtr(),
                            StreamGraph::KeySelectorPtr(), keySerializer);
  check(graph.getStreamNode(2)->getStateKeySerializer() == keySerializer,
        "setTwoInputStateKey 同样装上（两个 partitioner 各一条输入）");

  // 1.0 setParallelism / setBufferTimeout 对不存在的顶点静默返回
  graph.setParallelism(99, 8);
  graph.setBufferTimeout(99, 8);
  graph.setTransformationId(99, "nope");
  check(true, "setParallelism/setBufferTimeout/setTransformationId 对不存在的顶点静默返回");

  check(graph.getStreamNode(1)->getBufferTimeout() == 100,
        "未显式设过 bufferTimeout 时回落到缺省值（1.0 来自 env）");
  graph.setBufferTimeout(1, 0);
  check(graph.getStreamNode(1)->getBufferTimeout() == 0,
        "显式设成 0 也要生效（1.0 的 Long != null 判定，不是 > 0）");
}

// 1.0 createIterationSourceAndSink（:517）/ isIterative（:158）
void testIterationSourceAndSink() {
  StreamGraph graph = newGraph();
  addMap(graph, 1);

  check(!graph.isIterative(), "没有迭代顶点时 isIterative() 为 false");

  auto pair = graph.createIterationSourceAndSink(0, -1, -2, 1000, 2);

  check(pair.f0->getOperatorName() == "IterationSource-0" &&
            pair.f1->getOperatorName() == "IterationSink-0",
        "迭代顶点的名字是 IterationSource-{loopId} / IterationSink-{loopId}");
  check(pair.f0->getJobVertexClassName() == "StreamIterationHead" &&
            pair.f1->getJobVertexClassName() == "StreamIterationTail",
        "迭代顶点挑 StreamIterationHead / StreamIterationTail");
  check(!pair.f0->getOperator() && !pair.f1->getOperator(),
        "1.0 给 addNode 传的 operator 是 null，迭代顶点不带算子");
  check(pair.f0->getParallelism() == 2 && pair.f1->getParallelism() == 2,
        "迭代 source/sink 用传入的并行度");
  check(graph.getSourceIDs().count(-1) == 1 &&
            graph.getSinkIDs().count(-2) == 1,
        "迭代 source 记进 sources、sink 记进 sinks");
  check(graph.getBrokerID(-1) == "broker-0" &&
            graph.getBrokerID(-2) == "broker-0",
        "两端共用同一个 brokerID（broker-{loopId}）");
  check(graph.getLoopTimeout(-1) == 1000 && graph.getLoopTimeout(-2) == 1000,
        "两端共用同一个 loop timeout");
  check(graph.isIterative(),
        "有了 loopTimeout 之后 isIterative() 为 true（= !map.isEmpty()）");
  check(graph.getIterationSourceSinkPairs().size() == 1,
        "source/sink 成对登记");

  // 【偏差 4】1.0 getJobGraph() 开头的校验
  graph.getCheckpointConfig().setCheckpointInterval(1000);
  bool threw = false;
  std::string message;
  try {
    graph.checkJobGraphPreconditions();
  } catch (const std::exception& e) {
    threw = true;
    message = e.what();
  }
  check(threw &&
            message.find("Checkpointing is currently not supported by default "
                         "for iterative jobs") == 0,
        "迭代作业开 checkpoint 被拒（1.0 getJobGraph 的那段校验）");

  graph.getCheckpointConfig().setForceCheckpointing(true);
  bool threwAgain = false;
  try {
    graph.checkJobGraphPreconditions();
  } catch (const std::exception&) {
    threwAgain = true;
  }
  check(!threwAgain,
        "setForceCheckpointing(true) 之后放行（1.0 的 reduced guarantees）");
}

// 1.0 clear()（:107）/ setChaining / setStateBackend / getOperators
void testGraphMiscellaneous() {
  StreamGraph graph = newGraph();
  addMap(graph, 1);
  addMap(graph, 2);
  graph.addEdge(1, 2, 0);
  graph.setJobName("my job");
  graph.setChaining(true);
  graph.setStateBackend(
      std::make_shared<flink::runtime::state::memory::MemoryStateBackend>());

  check(graph.isChainingEnabled(), "setChaining 生效");
  check(graph.getStateBackend() != NULL, "setStateBackend 生效");

  auto operators = graph.getOperators();
  check(operators.size() == 2 && operators[0].f0 == 1 && operators[0].f1,
        "getOperators 返回 (vertexId, operator) 对");

  graph.clear();
  check(graph.getStreamNodes().empty() && graph.getSourceIDs().empty() &&
            graph.getSinkIDs().empty() && !graph.isIterative(),
        "clear() 清空所有顶点与登记表（1.0: \"Remove all registered nodes etc.\"）");
  check(graph.getJobName() == "my job" && graph.isChainingEnabled(),
        "clear() 不动 jobName / chaining / stateBackend（1.0 原样）");
}

// 1.0 CheckpointConfig（CheckpointConfig.java:32）
void testCheckpointConfig() {
  StreamGraph::CheckpointConfig config;

  check(!config.isCheckpointingEnabled(),
        "checkpointInterval 默认 -1（disabled），isCheckpointingEnabled 为 false");
  check(config.getCheckpointingMode() ==
            flink::streaming::api::CheckpointingMode::EXACTLY_ONCE,
        "默认模式 EXACTLY_ONCE（1.0 DEFAULT_MODE）");
  check(config.getCheckpointTimeout() == 10 * 60 * 1000,
        "默认超时 10 分钟（1.0 DEFAULT_TIMEOUT）");
  check(config.getMinPauseBetweenCheckpoints() == 0 &&
            config.getMaxConcurrentCheckpoints() == 1,
        "默认 minPause 0、maxConcurrent 1");

  config.setCheckpointInterval(500);
  check(config.isCheckpointingEnabled() &&
            config.getCheckpointInterval() == 500,
        "setCheckpointInterval 之后 isCheckpointingEnabled 为 true");

  int rejected = 0;
  try {
    config.setCheckpointInterval(0);
  } catch (const std::exception&) {
    ++rejected;
  }
  try {
    config.setCheckpointTimeout(0);
  } catch (const std::exception&) {
    ++rejected;
  }
  try {
    config.setMaxConcurrentCheckpoints(0);
  } catch (const std::exception&) {
    ++rejected;
  }
  check(rejected == 3,
        "interval/timeout 必须 > 0、maxConcurrent 必须 >= 1（1.0 的三条校验）");
}

// ══════════════════ StreamingJobGraphGenerator ══════════════════
//
// 1.0 对应的 StreamingJobGraphGeneratorTest 同样是 DataStream 级的
// （env.fromElements(...).map(...).keyBy(0).map(...).addSink(...)）。这里把
// testParallelismOneNotChained（StreamingJobGraphGeneratorTest.java:96-137）
// 的拓扑按 StreamGraphGenerator 会做的动作搭在 StreamGraph 层，期望值
// （顶点数 2、两个顶点并行度都是 1）逐条照抄。其余几组是对 1.0 的
// isChainable 八条件 / createChainedName 三分支 / connect 两种
// DistributionPattern 的直接验证 —— 这些在 1.0 里没有单元测试，只被
// ITCase 间接覆盖。

// 一条用来建「不可链接」出边的 filter 算子：ChainingStrategy 同样是 ALWAYS，
// 用它是为了让链名能出现两个不同的算子名
class NoOpIntFilter : public flink::api::common::functions::FilterFunction<
                          int32_t> {
 public:
  bool filter(const int32_t&) override { return true; }
};

StreamGraph::StreamOperatorPtr makeFilterOperator() {
  return std::make_shared<flink::streaming::api::operators::StreamFilter<
      int32_t> >(std::make_shared<NoOpIntFilter>());
}

// 1.0 StreamGraphGenerator 建完图后一定会 setChaining(env.isChainingEnabled())
StreamGraph newChainingGraph(int defaultParallelism = 1) {
  StreamGraph graph(defaultParallelism, 100);
  graph.setChaining(true);
  return graph;
}

// 1.0 isChainable（StreamingJobGraphGenerator.java:401）的八个条件
void testIsChainable() {
  // ① 基准：source(HEAD) -> map(ALWAYS)，forward、同并行度、同组、单入边
  {
    StreamGraph graph = newChainingGraph();
    graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                    intSerializer(), "Source");
    addMap(graph, 2);
    graph.addEdge(1, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "基准情形可链接（HEAD -> ALWAYS、forward、并行度相同、单入边）");
  }

  // ② streamGraph.isChainingEnabled() == false
  {
    StreamGraph graph = newGraph();  // chaining 默认 false
    addMap(graph, 1);
    addMap(graph, 2);
    graph.addEdge(1, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "chaining 全局关掉就一律不可链接");
  }

  // ③ 下游入边数 != 1
  {
    StreamGraph graph = newChainingGraph();
    addMap(graph, 1);
    addMap(graph, 2);
    addMap(graph, 3);
    graph.addEdge(1, 3, 0);
    graph.addEdge(2, 3, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 3)[0]),
          "下游有两条入边就不可链接（1.0: getInEdges().size() == 1）");
  }

  // ④ 下游算子的 ChainingStrategy 不是 ALWAYS（StreamSource 是 HEAD）
  {
    StreamGraph graph = newChainingGraph();
    addMap(graph, 1);
    graph.addOperator(2, "", makeSourceOperator(), intSerializer(),
                      intSerializer(), "Source");
    graph.addEdge(1, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "下游算子是 HEAD（而非 ALWAYS）就不可链接");
  }

  // ⑤ 上游算子的 ChainingStrategy 是 NEVER
  {
    StreamGraph graph = newChainingGraph();
    auto neverOperator = makeMapOperator();
    neverOperator->setChainingStrategy(
        flink::streaming::api::operators::ChainingStrategy::NEVER);
    graph.addOperator(1, "", neverOperator, intSerializer(), intSerializer(),
                      "Map");
    addMap(graph, 2);
    graph.addEdge(1, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "上游算子是 NEVER 就不可链接（1.0 只认 HEAD 与 ALWAYS）");
  }

  // ⑥ partitioner 不是 ForwardPartitioner
  {
    StreamGraph graph = newChainingGraph();
    addMap(graph, 1);
    addMap(graph, 2);
    graph.addVirtualPartitionNode(1, 100,
                                  std::make_shared<ShufflePartitioner>());
    graph.addEdge(100, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "非 forward 的边不可链接（shuffle 必须过网络）");
  }

  // ⑦ 槽位共享组不同
  {
    StreamGraph graph = newChainingGraph();
    graph.addOperator(1, "group-a", makeMapOperator(), intSerializer(),
                      intSerializer(), "Map");
    graph.addOperator(2, "group-b", makeMapOperator(), intSerializer(),
                      intSerializer(), "Map");
    graph.addEdge(1, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "槽位共享组不同就不可链接（isSameSlotSharingGroup）");
  }

  // ⑧ 算子为 null（iteration 顶点就是这种）
  {
    StreamGraph graph = newChainingGraph();
    addMap(graph, 1);
    graph.addOperator(2, "", StreamGraph::StreamOperatorPtr(),
                      intSerializer(), intSerializer(), "NoOperator");
    graph.addEdge(1, 2, 0);
    StreamingJobGraphGenerator generator(&graph);
    check(!generator.isChainable(graph.getStreamEdges(1, 2)[0]),
          "下游没有算子就不可链接（1.0: outOperator != null）");
  }
}

// 1.0 StreamingJobGraphGeneratorTest.testParallelismOneNotChained（:96-137）
// 的拓扑：source -> map -> keyBy -> map -> sink，并行度全 1。
// keyBy 那一步是 hash 分区，所以链在那里断开 -> 2 个 JobVertex。
void testParallelismOneNotChained() {
  StreamGraph graph = newChainingGraph(1);
  graph.setJobName("test job");

  graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                  intSerializer(), "Source");
  addMap(graph, 2, "Map");
  addMap(graph, 3, "Map");
  graph.addSink(4, "", makeMapOperator(), intSerializer(),
                StreamGraph::SerializerPtr(), "Sink");

  graph.addEdge(1, 2, 0);
  // keyBy(0)：虚拟 partition 节点 + HashPartitioner
  graph.addVirtualPartitionNode(
      2, 100, std::make_shared<HashPartitioner>(StreamGraph::KeySelectorPtr()));
  graph.addEdge(100, 3, 0);
  graph.addEdge(3, 4, 0);

  StreamingJobGraphGenerator generator(&graph);
  auto jobGraph = generator.createJobGraph();

  // 1.0:134-136
  check(jobGraph->getNumberOfVertices() == 2,
        "1.0 testParallelismOneNotChained：keyBy 断链 -> 2 个 JobVertex");
  check(jobGraph->getVertices()[0]->getParallelism() == 1 &&
            jobGraph->getVertices()[1]->getParallelism() == 1,
        "两个顶点的并行度都是 1");
  check(jobGraph->getName() == "test job",
        "JobGraph 的名字取自 streamGraph.getJobName()");
  check(jobGraph->getScheduleMode() ==
            flink::runtime::jobgraph::ScheduleMode::ALL,
        "1.0: \"make sure that all vertices start immediately\" -> ScheduleMode.ALL");

  // 顶点的**插入序**：createChain 是深度优先的 —— 碰到不可链接的出边时先
  // createChain(target, target) 递归到底，再回来给自己 createJobVertex。
  // 所以下游那条链的顶点**先**进 JobGraph。1.0 的 taskVertices 是
  // LinkedHashMap，顺序完全一样（1.0 的测试只断言并行度，没有暴露这一点）。
  auto vertices = jobGraph->getVertices();
  check(vertices[0]->getName() == "Map -> Sink" &&
            vertices[1]->getName() == "Source -> Map",
        "createChain 深度优先：下游链的 JobVertex 先建（1.0 LinkedHashMap 同序）");

  // 1.0 createChainedName 的单下游分支：operatorName + " -> " + 下游链名
  JobVertex* head = vertices[1];  // Source -> Map
  JobVertex* down = vertices[0];  // Map -> Sink
  check(head->getName() == "Source -> Map" && down->getName() == "Map -> Sink",
        "createChainedName 的单下游分支：\"A -> B\"");
  check(head->getInvokableClassName() == "SourceStreamTask" &&
            down->getInvokableClassName() == "OneInputStreamTask",
        "invokableClass 取自链头顶点的 jobVertexClass");
  check(head->isInputVertex() && !down->isInputVertex(),
        "只有 source 那条链是 inputVertex（configureCheckpointing 的触发点判定）");

  // connect：hash 分区 -> ALL_TO_ALL
  auto inputs = down->getInputs();
  check(inputs.size() == 1 &&
            inputs[0]->getDistributionPattern() ==
                DistributionPattern::ALL_TO_ALL,
        "非 forward/rescale 的边 -> DistributionPattern.ALL_TO_ALL");
  check(inputs.size() == 1 && inputs[0]->getSource()->getProducer() == head,
        "数据集挂在上游顶点上，边挂在下游的 inputs 里");
  check(inputs.size() == 1 &&
            flink::runtime::io::network::partition::isPipelined(
                inputs[0]->getSource()->getResultType()),
        "流式作业一律 ResultPartitionType.PIPELINED");
  check(inputs.size() == 1 && inputs[0]->getSource()->getEagerlyDeployConsumers(),
        "1.0 connect 三个分支都传 eagerlyDeployConsumers = true");
  check(head->getNumberOfProducedIntermediateDataSets() == 1,
        "connectNewDataSetAsInput 在上游顶点上建了一个 IntermediateDataSet");

  // 链头的 StreamConfig：1.0 createChain 的 if 分支
  const StreamConfig& headConfig = head->getConfiguration();
  check(headConfig.isChainStart(), "链头 config.setChainStart()");
  check(headConfig.getVertexID() == 1, "链头 config 的 vertexID 是链头顶点 id");
  check(headConfig.getChainedOutputs().size() == 1 &&
            headConfig.getNonChainedOutputs().empty(),
        "Source 的唯一出边是链内的（chainedOutputs），没有网络出边");
  check(headConfig.getOutEdgesInOrder().size() == 1 &&
            headConfig.getOutEdgesInOrder()[0]->getTargetId() == 3,
        "transitiveOutEdges 是整条链向外的出边（Map(2) -> Map(3)）");
  check(headConfig.getTransitiveChainedTaskConfigs().size() == 1,
        "链内算子的 config 登记进 transitiveChainedTaskConfigs");

  // 链内算子的 StreamConfig：1.0 createChain 的 else 分支
  const auto& chainedConfigs = headConfig.getTransitiveChainedTaskConfigs();
  auto chainedIt = chainedConfigs.find(2);
  check(chainedIt != chainedConfigs.end() &&
            !chainedIt->second->isChainStart(),
        "链内算子不是 chainStart");
  check(chainedIt != chainedConfigs.end() &&
            chainedIt->second->getNumberOfOutputs() == 1,
        "链内 Map 有 1 条非链接出边（通向下一条链）");

  // setPhysicalEdges：第二条链的 inPhysicalEdges
  const StreamConfig& downConfig = down->getConfiguration();
  check(downConfig.getInPhysicalEdges().size() == 1 &&
            downConfig.getInPhysicalEdges()[0]->getSourceId() == 2,
        "setPhysicalEdges 把物理入边回填进下游链头的 config");
  check(downConfig.getNumberOfInputs() == 1,
        "connect 里 setNumberOfInputs(+1) 累加到 1");
  // 没开 checkpoint 时的那条注释：
  // "the 'at-least-once' input handler is slightly cheaper ..."
  check(downConfig.getCheckpointMode() ==
            flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE,
        "没开 checkpoint 时 checkpointMode 退成 AT_LEAST_ONCE");
}

// 1.0 createChainedName（:247）的三个分支 + createChain 的分叉处理
void testChainedNameAndFork() {
  // 一个算子链出两个下游：createChainedName 走 size() > 1 分支
  StreamGraph graph = newChainingGraph(1);
  graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                  intSerializer(), "Source");
  addMap(graph, 2, "MapA");
  graph.addOperator(3, "", makeFilterOperator(), intSerializer(),
                    intSerializer(), "FilterB");
  graph.addEdge(1, 2, 0);
  graph.addEdge(1, 3, 0);

  StreamingJobGraphGenerator generator(&graph);
  auto jobGraph = generator.createJobGraph();

  check(jobGraph->getNumberOfVertices() == 1,
        "一个 source 链出两个 forward 下游 -> 仍是一个 JobVertex");
  // 1.0: operatorName + " -> (" + join(names, ", ") + ")"
  check(jobGraph->getVertices()[0]->getName() == "Source -> (MapA, FilterB)",
        "createChainedName 的多下游分支：\"A -> (B, C)\"");

  const StreamConfig& config = jobGraph->getVertices()[0]->getConfiguration();
  check(config.getChainedOutputs().size() == 2,
        "两条出边都在链内（chainedOutputs）");
  check(config.getOutEdgesInOrder().empty(),
        "整条链没有通向网络的出边，transitiveOutEdges 为空");
  check(config.getTransitiveChainedTaskConfigs().size() == 2,
        "两个链内算子都登记进 transitiveChainedTaskConfigs");
}

// 1.0 connect（:375-380）：ForwardPartitioner -> POINTWISE
void testForwardConnectIsPointwise() {
  // chaining 关掉，强制 forward 边变成物理边
  StreamGraph graph = newGraph(1);
  graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                  intSerializer(), "Source");
  addMap(graph, 2, "Map");
  graph.addEdge(1, 2, 0);

  StreamingJobGraphGenerator generator(&graph);
  auto jobGraph = generator.createJobGraph();

  check(jobGraph->getNumberOfVertices() == 2,
        "chaining 关掉后 forward 边也要过网络 -> 2 个 JobVertex");

  // 同前：深度优先，下游顶点先建
  auto vertices = jobGraph->getVertices();
  auto inputs = vertices[0]->getInputs();
  check(inputs.size() == 1 && inputs[0]->getDistributionPattern() ==
                                  DistributionPattern::POINTWISE,
        "ForwardPartitioner -> DistributionPattern.POINTWISE");

  // 1.0 createChainedName 的 size() == 0 分支
  check(vertices[0]->getName() == "Map" && vertices[1]->getName() == "Source",
        "没有链内下游时链名就是算子名本身");
}

// 1.0 setSlotSharing（:420）与 configureCheckpointing（:451）
void testSlotSharingAndCheckpointing() {
  StreamGraph graph = newGraph(2);
  graph.addSource(1, "default", makeSourceOperator(),
                  StreamGraph::SerializerPtr(), intSerializer(), "Source");
  graph.addOperator(2, "default", makeMapOperator(), intSerializer(),
                    intSerializer(), "Map");
  graph.addOperator(3, "other", makeMapOperator(), intSerializer(),
                    intSerializer(), "Map2");
  graph.addEdge(1, 2, 0);
  graph.addEdge(2, 3, 0);

  graph.getCheckpointConfig().setCheckpointInterval(1000);
  graph.getCheckpointConfig().setCheckpointTimeout(5000);

  StreamingJobGraphGenerator generator(&graph);
  auto jobGraph = generator.createJobGraph();

  auto vertices = jobGraph->getVertices();
  check(vertices.size() == 3, "三个顶点（chaining 关掉）");

  JobVertex* source = NULL;
  JobVertex* map = NULL;
  JobVertex* map2 = NULL;
  for (JobVertex* vertex : vertices) {
    if (vertex->getName() == "Source") source = vertex;
    if (vertex->getName() == "Map") map = vertex;
    if (vertex->getName() == "Map2") map2 = vertex;
  }
  check(source != NULL && map != NULL && map2 != NULL,
        "三个顶点的名字各是对应算子名（没有链内下游）");
  check(source->getSlotSharingGroup() == map->getSlotSharingGroup(),
        "同一个 slotSharingGroup 名字 -> 同一个 SlotSharingGroup 对象");
  check(source->getSlotSharingGroup() != map2->getSlotSharingGroup(),
        "不同名字 -> 不同对象");
  check(source->getSlotSharingGroup()->getJobVertexIds().size() == 2 &&
            map2->getSlotSharingGroup()->getJobVertexIds().size() == 1,
        "setSlotSharingGroup 把顶点 id 登记进组里");

  // 1.0 configureCheckpointing
  auto settings = jobGraph->getSnapshotSettings();
  check(settings != NULL, "开了 checkpoint 就装上 JobSnapshottingSettings");
  check(settings->getVerticesToTrigger().size() == 1,
        "1.0: \"these are all the sources\" —— 只有无入边的顶点触发");
  check(settings->getVerticesToAcknowledge().size() == 3 &&
            settings->getVerticesToConfirm().size() == 3,
        "1.0: ack 与 commit 是**全部**顶点");
  check(settings->getCheckpointInterval() == 1000 &&
            settings->getCheckpointTimeout() == 5000 &&
            settings->getMinPauseBetweenCheckpoints() == 0 &&
            settings->getMaxConcurrentCheckpoints() == 1,
        "四个参数从 CheckpointConfig 原样搬过来");

  // 开了 checkpoint 时 setVertexConfig 用的是配置里的模式
  check(source->getConfiguration().getCheckpointMode() ==
            flink::streaming::api::CheckpointingMode::EXACTLY_ONCE,
        "开了 checkpoint 时 checkpointMode 取 CheckpointConfig 的值");
  check(source->getConfiguration().getBufferTimeout() == 100,
        "bufferTimeout 从 StreamNode 搬进 StreamConfig");
  check(settings->getVerticesToTrigger()[0] == source->getID(),
        "触发点正是那个 source 顶点");

  // 没开 checkpoint 时不装 settings
  StreamGraph plain = newGraph(1);
  addMap(plain, 1);
  StreamingJobGraphGenerator plainGenerator(&plain);
  check(plainGenerator.createJobGraph()->getSnapshotSettings() == NULL,
        "没开 checkpoint 就不装 JobSnapshottingSettings");
}

// 1.0 setSlotSharing 后半段（:436-447）：迭代 source/sink 进同一个 CoLocationGroup
void testIterationCoLocation() {
  StreamGraph graph = newGraph(1);
  addMap(graph, 1);
  graph.createIterationSourceAndSink(0, -1, -2, 1000, 1);
  graph.addEdge(-1, 1, 0);
  graph.addEdge(1, -2, 0);

  StreamingJobGraphGenerator generator(&graph);
  auto jobGraph = generator.createJobGraph();

  JobVertex* head = NULL;
  JobVertex* tail = NULL;
  for (JobVertex* vertex : jobGraph->getVertices()) {
    if (vertex->getInvokableClassName() == "StreamIterationHead") head = vertex;
    if (vertex->getInvokableClassName() == "StreamIterationTail") tail = vertex;
  }
  check(head != NULL && tail != NULL,
        "迭代 head/tail 顶点都建出来了");
  check(head != NULL && tail != NULL &&
            head->getCoLocationGroup() == tail->getCoLocationGroup() &&
            head->getCoLocationGroup() != NULL,
        "迭代 source/sink 进同一个 CoLocationGroup");
  // 1.0 setVertexConfig 的 iteration 分支
  check(head != NULL && head->getConfiguration().getIterationId() ==
                            "broker-0" &&
            head->getConfiguration().getIterationWaitTime() == 1000,
        "iteration 顶点的 config 带上 iterationId 与 iterationWaitTime");
}

// 1.0 traverseStreamGraphAndGenerateHashes（:520）
void testDeterministicHashes() {
  // 同一个程序建两次，hash 必须一致
  auto buildGraph = []() {
    StreamGraph graph = newChainingGraph(1);
    graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                    intSerializer(), "Source");
    addMap(graph, 2, "Map");
    graph.addVirtualPartitionNode(2, 100,
                                  std::make_shared<RebalancePartitioner>());
    addMap(graph, 3, "Map2");
    graph.addEdge(1, 2, 0);
    graph.addEdge(100, 3, 0);
    return graph;
  };

  StreamGraph graphA = buildGraph();
  StreamGraph graphB = buildGraph();
  StreamingJobGraphGenerator generatorA(&graphA);
  StreamingJobGraphGenerator generatorB(&graphB);
  auto jobGraphA = generatorA.createJobGraph();
  auto jobGraphB = generatorB.createJobGraph();

  auto verticesA = jobGraphA->getVertices();
  auto verticesB = jobGraphB->getVertices();
  check(verticesA.size() == 2 && verticesB.size() == 2,
        "同一程序两次编译得到同样的顶点数");
  check(verticesA[0]->getID() == verticesB[0]->getID() &&
            verticesA[1]->getID() == verticesB[1]->getID(),
        "1.0: \"identify them across submission iff they didn't change\" "
        "—— 两次编译的 JobVertexID 一致");
  check(!(verticesA[0]->getID() == verticesA[1]->getID()),
        "同一张图里不同顶点的 hash 不同");

  // 并行度变了 hash 就要变（generateNodeLocalHash 把 parallelism 混进去了）。
  // 这里只改 Map2(3) 的并行度：它是第二条链的链头，所以**只有**那条链的
  // JobVertexID 该变，Source 那条链必须保持原值 —— 这正是 1.0 想要的
  // 「didn't change 的节点 id 不变」语义。
  StreamGraph changed = buildGraph();
  changed.setParallelism(3, 4);
  StreamingJobGraphGenerator changedGenerator(&changed);
  auto changedVertices = changedGenerator.createJobGraph()->getVertices();
  // 深度优先：[0] 是 Map2 那条链，[1] 是 Source 那条链
  check(changedVertices.size() == 2 &&
            !(changedVertices[0]->getID() == verticesA[0]->getID()),
        "改了并行度，该顶点的 hash 变了（parallelism 混进 generateNodeLocalHash）");
  check(changedVertices.size() == 2 &&
            changedVertices[1]->getID() == verticesA[1]->getID(),
        "没动过的那条链 hash 不变（1.0 的 \"iff they didn't change\"）");

  // 用户指定 uid 的分支。注意必须有 source：setChaining 与
  // traverseStreamGraphAndGenerateHashes 都是从 getSourceIDs() 出发的
  StreamGraph withUid = newGraph(1);
  withUid.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                    intSerializer(), "Source");
  addMap(withUid, 2, "Map2");
  withUid.addEdge(1, 2, 0);
  withUid.setTransformationId(1, "my-uid");
  withUid.setTransformationId(2, "my-uid");  // 故意撞车
  StreamingJobGraphGenerator uidGenerator(&withUid);
  bool threwCollision = false;
  std::string message;
  try {
    uidGenerator.createJobGraph();
  } catch (const std::exception& e) {
    threwCollision = true;
    message = e.what();
  }
  check(threwCollision &&
            message.find("Hash collision on user-specified ID.") == 0,
        "两个顶点用同一个 uid 被拒（1.0 的原文案）");

  // uid 加在链的中间节点上 -> UnsupportedOperationException
  StreamGraph uidInChain = newChainingGraph(1);
  uidInChain.addSource(1, "", makeSourceOperator(),
                       StreamGraph::SerializerPtr(), intSerializer(),
                       "Source");
  addMap(uidInChain, 2, "Map");
  uidInChain.addEdge(1, 2, 0);
  uidInChain.setTransformationId(2, "mid-uid");
  StreamingJobGraphGenerator chainUidGenerator(&uidInChain);
  bool threwIntermediate = false;
  std::string chainMessage;
  try {
    chainUidGenerator.createJobGraph();
  } catch (const std::exception& e) {
    threwIntermediate = true;
    chainMessage = e.what();
  }
  check(threwIntermediate &&
            chainMessage.find(
                "Cannot assign user-specified hash to intermediate node in "
                "chain.") == 0,
        "uid 加在链中间节点上被拒（1.0 的原文案，含 \"start new chain at task\"）");
  check(chainMessage.find("start new chain at task Map.") !=
            std::string::npos,
        "错误信息末尾带上了那个算子的名字");
}

// 1.0 JobGraph.getVerticesSortedTopologicallyFromSources（JobGraph.java:360）
void testTopologicalSort() {
  // A -> B -> D，A -> C -> D（菱形），chaining 关掉让每个算子一个顶点
  StreamGraph graph = newGraph(1);
  graph.addSource(1, "", makeSourceOperator(), StreamGraph::SerializerPtr(),
                  intSerializer(), "A");
  addMap(graph, 2, "B");
  addMap(graph, 3, "C");
  addMap(graph, 4, "D");
  graph.addEdge(1, 2, 0);
  graph.addEdge(1, 3, 0);
  graph.addEdge(2, 4, 0);
  graph.addEdge(3, 4, 0);

  StreamingJobGraphGenerator generator(&graph);
  auto jobGraph = generator.createJobGraph();

  auto sorted = jobGraph->getVerticesSortedTopologicallyFromSources();
  check(sorted.size() == 4, "四个顶点都排进去了");
  check(sorted[0]->getName() == "A",
        "无入边的顶点排最前（1.0: hasNoConnectedInputs）");
  check(sorted[3]->getName() == "D",
        "D 要等 B 与 C 都排好才进（addNodesThatHaveNoNewPredecessors "
        "的 hasNewPredecessors 判定）");

  // 空图
  flink::runtime::jobgraph::JobGraph empty("empty");
  check(empty.getVerticesSortedTopologicallyFromSources().empty(),
        "1.0: \"early out on empty lists\"");
}

}  // namespace

int main() {
  testVertexClassSelection();
  testDefaultPartitioner();
  testForwardPartitionerParallelismCheck();
  testVirtualTransformations();
  testDownstreamSelectionOverridesEarlier();
  testSlotSharingGroupAcrossVirtualNodes();
  testSerializersFromAndStateKey();
  testIterationSourceAndSink();
  testGraphMiscellaneous();
  testCheckpointConfig();
  testIsChainable();
  testParallelismOneNotChained();
  testChainedNameAndFork();
  testForwardConnectIsPointwise();
  testSlotSharingAndCheckpointing();
  testIterationCoLocation();
  testDeterministicHashes();
  testTopologicalSort();
  std::printf(failures == 0 ? "\nALL PASSED\n" : "\n%d FAILED\n", failures);
  return failures == 0 ? 0 : 1;
}
