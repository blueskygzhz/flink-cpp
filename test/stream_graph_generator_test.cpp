// StreamTransformation 树 -> StreamGraph 的翻译层测试
//
// 1.0 对应的测试：
//   org.apache.flink.streaming.api.graph.StreamGraphGeneratorTest
//   org.apache.flink.streaming.api.graph.SlotAllocationTest
//
// 这两个测试都是 **DataStream 级**的写法
// （env.fromElements(...).rebalance().map(...).split(...).select(...)），而
// StreamExecutionEnvironment / DataStream 这一层还没移植。所以这里把拓扑
// 「下沉」到 transformations 层**手工搭建** —— 完全照着 1.0 的 DataStream
// 实现会建出的那棵树来搭：
//   env.fromElements(1, 10)  -> SourceTransformation("Collection Source", op,
//                                  type, 1)（StreamExecutionEnvironment:743）
//   x.rebalance()/.forward()/
//     .global()/.broadcast()/
//     .shuffle()             -> PartitionTransformation（DataStream:1048）
//   x.map(fn)                -> OneInputTransformation(x, "Map", StreamMap(fn),
//                                  outType, 并行度)（DataStream:508 -> :1025）
//   x.filter(fn)             -> OneInputTransformation(..., "Filter", ...)
//                                  （DataStream:551）
//   a.connect(b).map(coMap)  -> TwoInputTransformation(..., "Co-Map", ...)
//   x.split(sel)             -> SplitTransformation（SplitStream:39）
//   x.select(name)           -> SelectTransformation（SplitStream:61）
//   a.union(b)               -> UnionTransformation([a, b])（DataStream:199）
//   x.addSink(fn)            -> SinkTransformation(x, "Unnamed",
//                                  StreamSink(fn), 并行度)（DataStreamSink:39）
//   env.addOperator(t)       -> 追加进 transformations 列表（每个 transform()
//                               与 addSink() 都会做）
// 断言的期望值（partitioner 类型、selectedNames、outputSelectors、槽位共享组
// 的相等/不相等关系）逐条照抄 1.0，一字未改。
//
// 1.0 StreamGraphGeneratorTest 另外两个用例
// （testOutputTypeConfigurationWith{One,Two}InputTransformation）测的是
// OutputTypeConfigurable 拿到的 TypeInformation 是不是
// BasicTypeInfo.INT_TYPE_INFO —— 本仓库没有 TypeInformation 体系
// （见 StreamTransformation.h 偏差 1），这条路径不存在，故不移植。
//
// 【测试侧注意点】UnionTransformation 的类型一致性校验在本仓库退化成了
// 「SerializerPtr 指针相等」（见 UnionTransformation.h 偏差 1），所以 intSer()
// 返回的是**同一个**共享实例 —— 这正对应 1.0 里 BasicTypeInfo.INT_TYPE_INFO
// 是静态单例、equals 是值比较的事实。

#include <cstdio>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "flink/api/common/typeutils/TypedAnySerializer.h"
#include "flink/streaming/api/collector/selector/TypedAnyOutputSelector.h"
#include "flink/streaming/api/graph/StreamGraphGenerator.h"
#include "flink/streaming/api/operators/StreamFilter.h"
#include "flink/streaming/api/operators/StreamMap.h"
#include "flink/streaming/api/operators/StreamSink.h"
#include "flink/streaming/api/operators/StreamSource.h"
#include "flink/streaming/api/operators/co/CoStreamMap.h"
#include "flink/streaming/runtime/partitioner/BroadcastPartitioner.h"
#include "flink/streaming/runtime/partitioner/ForwardPartitioner.h"
#include "flink/streaming/runtime/partitioner/GlobalPartitioner.h"
#include "flink/streaming/runtime/partitioner/RebalancePartitioner.h"
#include "flink/streaming/runtime/partitioner/ShufflePartitioner.h"

using flink::api::common::functions::FilterFunction;
using flink::api::common::functions::MapFunction;
using flink::api::common::typeutils::TypedAnySerializer;
using flink::streaming::api::graph::StreamGraph;
using flink::streaming::api::graph::StreamGraphGenerator;
using flink::streaming::api::graph::StreamNode;
using flink::streaming::api::operators::StreamFilter;
using flink::streaming::api::operators::StreamMap;
using flink::streaming::api::operators::StreamSink;
using flink::streaming::runtime::partitioner::BroadcastPartitioner;
using flink::streaming::runtime::partitioner::ForwardPartitioner;
using flink::streaming::runtime::partitioner::GlobalPartitioner;
using flink::streaming::runtime::partitioner::RebalancePartitioner;
using flink::streaming::runtime::partitioner::ShufflePartitioner;

namespace tr = flink::streaming::api::transformations;

typedef tr::StreamTransformation Transformation;
typedef std::shared_ptr<Transformation> TransformationPtr;
typedef StreamGraph::StreamOperatorPtr StreamOperatorPtr;
typedef StreamGraph::SerializerPtr SerializerPtr;
typedef StreamGraph::OutputSelectorPtr OutputSelectorPtr;
typedef StreamGraph::PartitionerPtr PartitionerPtr;

namespace {

int failures = 0;

void check(bool condition, const std::string& what) {
  std::printf(condition ? "ok    %s\n" : "FAIL  %s\n", what.c_str());
  if (!condition) ++failures;
}

// ---------------------------------------------------------------------- UDFs

// 1.0 org.apache.flink.streaming.util.NoOpIntMap
class NoOpIntMap : public MapFunction<int32_t, int32_t> {
 public:
  int32_t map(const int32_t& value) override { return value; }
};

// 1.0 org.apache.flink.streaming.util.NoOpSink
class NoOpSink
    : public flink::streaming::api::functions::sink::SinkFunction<int32_t> {
 public:
  void invoke(const int32_t&) override {}
};

// 1.0 SlotAllocationTest 的 dummyFilter：`return false;`
class DummyFilter : public FilterFunction<int32_t> {
 public:
  bool filter(const int32_t&) override { return false; }
};

// 1.0 SlotAllocationTest 的 dummyCoMap
class DummyCoMap : public flink::streaming::api::functions::co::CoMapFunction<
                       int32_t, int32_t, int32_t> {
 public:
  int32_t map1(const int32_t& value) override { return value; }
  int32_t map2(const int32_t& value) override { return value; }
};

// 1.0 EvenOddOutputSelector：
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

class NoOpSource
    : public flink::streaming::api::functions::source::SourceFunction<int32_t> {
 public:
  void run(flink::streaming::api::functions::source::SourceContext<int32_t>&)
      override {}
  void cancel() override {}
};

// ------------------------------------------------------------------- helpers

// 见文件头「测试侧注意点」：全局共享**一个**实例
SerializerPtr intSer() {
  static SerializerPtr instance = TypedAnySerializer<int32_t>::create();
  return instance;
}

StreamOperatorPtr mapOp() {
  return std::make_shared<StreamMap<int32_t, int32_t> >(
      std::make_shared<NoOpIntMap>());
}
StreamOperatorPtr filterOp() {
  return std::make_shared<StreamFilter<int32_t> >(
      std::make_shared<DummyFilter>());
}
StreamOperatorPtr sinkOp() {
  return std::make_shared<StreamSink<int32_t> >(std::make_shared<NoOpSink>());
}
StreamOperatorPtr sourceOp() {
  typedef flink::streaming::api::operators::StreamSource<int32_t, NoOpSource>
      Source;
  return std::make_shared<Source>(std::make_shared<NoOpSource>());
}
StreamOperatorPtr coMapOp() {
  return std::make_shared<flink::streaming::api::operators::co::CoStreamMap<
      int32_t, int32_t, int32_t> >(std::make_shared<DummyCoMap>());
}

OutputSelectorPtr eraseSelector(
    const std::shared_ptr<EvenOddOutputSelector>& selector) {
  return flink::streaming::api::collector::selector::eraseOutputSelector<
      int32_t>(selector);
}

// --- 「DataStream API」的手工等价物（出处见文件头）---

TransformationPtr source(const std::string& name) {
  return std::make_shared<tr::SourceTransformation>(name, sourceOp(), intSer(),
                                                    1);
}
TransformationPtr partition(const TransformationPtr& input,
                            const PartitionerPtr& partitioner) {
  return std::make_shared<tr::PartitionTransformation>(input, partitioner);
}
TransformationPtr map(const TransformationPtr& input) {
  return std::make_shared<tr::OneInputTransformation>(input, "Map", mapOp(),
                                                      intSer(), 1);
}
TransformationPtr filter(const TransformationPtr& input) {
  return std::make_shared<tr::OneInputTransformation>(input, "Filter",
                                                      filterOp(), intSer(), 1);
}
TransformationPtr coMap(const TransformationPtr& in1,
                        const TransformationPtr& in2) {
  return std::make_shared<tr::TwoInputTransformation>(in1, in2, "Co-Map",
                                                      coMapOp(), intSer(), 1);
}
TransformationPtr split(const TransformationPtr& input,
                        const OutputSelectorPtr& selector) {
  return std::make_shared<tr::SplitTransformation>(input, selector);
}
TransformationPtr select(const TransformationPtr& input,
                         const std::string& name) {
  return std::make_shared<tr::SelectTransformation>(
      input, std::vector<std::string>(1, name));
}
TransformationPtr unionOf(const TransformationPtr& a,
                          const TransformationPtr& b) {
  std::vector<TransformationPtr> inputs;
  inputs.push_back(a);
  inputs.push_back(b);
  return std::make_shared<tr::UnionTransformation>(inputs);
}
TransformationPtr addSink(const TransformationPtr& input) {
  return std::make_shared<tr::SinkTransformation>(input, "Unnamed", sinkOp(),
                                                  1);
}

// env.getStreamGraph()：缺省并行度 1、缺省 bufferTimeout 100
// （1.0 StreamConfig.DEFAULT_TIMEOUT）、chaining 开启、stateBackend 为空
std::shared_ptr<StreamGraph> generate(
    const std::vector<TransformationPtr>& transformations) {
  return StreamGraphGenerator::generate(
      1, 100, true, StreamGraph::StateBackendPtr(), transformations);
}

void resetIds() {
  Transformation::resetIdCounter();
  StreamGraphGenerator::resetIterationIdCounter();
}

template <class T>
bool isPartitioner(const StreamGraph::StreamEdgePtr& edge) {
  return dynamic_cast<T*>(edge->getPartitioner().get()) != NULL;
}

// 1.0 的 `node.getOutputSelectors().contains(selector)`
bool containsSelector(StreamNode* node, const OutputSelectorPtr& selector) {
  if (node == NULL) return false;
  for (std::size_t i = 0; i < node->getOutputSelectors().size(); ++i) {
    if (node->getOutputSelectors()[i] == selector) return true;
  }
  return false;
}

std::string groupOf(const std::shared_ptr<StreamGraph>& graph,
                    const TransformationPtr& t) {
  StreamNode* node = graph->getStreamNode(t->getId());
  return node == NULL ? std::string("<missing>") : node->getSlotSharingGroup();
}

// ---------------------------------------------------------------------- tests

// 1.0 StreamGraphGeneratorTest.testVirtualTransformations（:65）
// 「This tests whether virtual Transformations behave correctly. Verifies that
//  partitioning, output selector, selected names are correctly set in the
//  StreamGraph when they are intermixed.」
void testVirtualTransformations() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  // DataStream<Integer> source = env.fromElements(1, 10);
  TransformationPtr src = source("Collection Source");

  // DataStream<Integer> rebalanceMap = source.rebalance().map(new NoOpIntMap());
  TransformationPtr rebalanceMap =
      map(partition(src, std::make_shared<RebalancePartitioner>()));
  transformations.push_back(rebalanceMap);

  // 1.0 注释：「verify that only the partitioning that was set last is used」
  // rebalanceMap.forward().global().broadcast().map(new NoOpIntMap());
  TransformationPtr broadcastMap = map(partition(
      partition(partition(rebalanceMap, std::make_shared<ForwardPartitioner>()),
                std::make_shared<GlobalPartitioner>()),
      std::make_shared<BroadcastPartitioner>()));
  transformations.push_back(broadcastMap);

  // broadcastMap.addSink(new NoOpSink<Integer>());
  transformations.push_back(addSink(broadcastMap));

  // 1.0 注释：「verify that partitioning is preserved across union and
  // split/select」
  OutputSelectorPtr selector1 =
      eraseSelector(std::make_shared<EvenOddOutputSelector>());
  OutputSelectorPtr selector2 =
      eraseSelector(std::make_shared<EvenOddOutputSelector>());
  OutputSelectorPtr selector3 =
      eraseSelector(std::make_shared<EvenOddOutputSelector>());

  // map1Operator = rebalanceMap.map(new NoOpIntMap());
  // map1 = map1Operator.broadcast().split(selector1).select("even");
  TransformationPtr map1Operator = map(rebalanceMap);
  transformations.push_back(map1Operator);
  TransformationPtr map1 = select(
      split(partition(map1Operator, std::make_shared<BroadcastPartitioner>()),
            selector1),
      "even");

  // map2Operator = rebalanceMap.map(new NoOpIntMap());
  // map2 = map2Operator.split(selector2).select("odd").global();
  TransformationPtr map2Operator = map(rebalanceMap);
  transformations.push_back(map2Operator);
  TransformationPtr map2 =
      partition(select(split(map2Operator, selector2), "odd"),
                std::make_shared<GlobalPartitioner>());

  // map3Operator = rebalanceMap.map(new NoOpIntMap());
  // map3 = map3Operator.global().split(selector3).select("even").shuffle();
  TransformationPtr map3Operator = map(rebalanceMap);
  transformations.push_back(map3Operator);
  TransformationPtr map3 = partition(
      select(split(partition(map3Operator, std::make_shared<GlobalPartitioner>()),
                   selector3),
             "even"),
      std::make_shared<ShufflePartitioner>());

  // unionedMap = map1.union(map2).union(map3).map(new NoOpIntMap());
  TransformationPtr unionedMap = map(unionOf(unionOf(map1, map2), map3));
  transformations.push_back(unionedMap);

  // unionedMap.addSink(new NoOpSink<Integer>());
  transformations.push_back(addSink(unionedMap));

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  // ---- 以下断言逐条照抄 1.0（:121-138）----

  // rebalanceMap
  StreamNode* rebalanceMapNode = graph->getStreamNode(rebalanceMap->getId());
  const bool rebalanceOk =
      rebalanceMapNode != NULL && rebalanceMapNode->getInEdges().size() == 1;
  check(rebalanceOk &&
            isPartitioner<RebalancePartitioner>(
                rebalanceMapNode->getInEdges()[0]),
        "rebalanceMap 的入边 partitioner 是 Rebalance");

  // 1.0 注释：「verify that only last partitioning takes precedence」
  StreamNode* broadcastMapNode = graph->getStreamNode(broadcastMap->getId());
  const bool broadcastOk =
      broadcastMapNode != NULL && broadcastMapNode->getInEdges().size() == 1;
  check(broadcastOk && isPartitioner<BroadcastPartitioner>(
                           broadcastMapNode->getInEdges()[0]),
        "forward().global().broadcast() 只有最后设的 broadcast 生效");
  check(broadcastOk &&
            broadcastMapNode->getInEdges()[0]->getSourceVertex()->getId() ==
                rebalanceMap->getId(),
        "三层虚拟 partition 节点被解析回 rebalanceMap 这个真实顶点");

  // 1.0 注释：「verify that partitioning in unions is preserved and that it
  // works across split/select」
  StreamNode* map1Node = graph->getStreamNode(map1Operator->getId());
  const bool map1Ok = map1Node != NULL && map1Node->getOutEdges().size() == 1;
  check(map1Ok && isPartitioner<BroadcastPartitioner>(map1Node->getOutEdges()[0]),
        "map1 的出边 partitioner 是 Broadcast（穿过 split/select/union）");
  check(map1Ok && map1Node->getOutEdges()[0]->getSelectedNames() ==
                      std::vector<std::string>(1, "even"),
        "map1 的出边 selectedNames[0] == \"even\"");
  check(containsSelector(map1Node, selector1),
        "map1 的 outputSelectors 含 selector1");

  StreamNode* map2Node = graph->getStreamNode(map2Operator->getId());
  const bool map2Ok = map2Node != NULL && map2Node->getOutEdges().size() == 1;
  check(map2Ok && isPartitioner<GlobalPartitioner>(map2Node->getOutEdges()[0]),
        "map2 的出边 partitioner 是 Global");
  check(map2Ok && map2Node->getOutEdges()[0]->getSelectedNames() ==
                      std::vector<std::string>(1, "odd"),
        "map2 的出边 selectedNames[0] == \"odd\"");
  check(containsSelector(map2Node, selector2),
        "map2 的 outputSelectors 含 selector2");

  StreamNode* map3Node = graph->getStreamNode(map3Operator->getId());
  const bool map3Ok = map3Node != NULL && map3Node->getOutEdges().size() == 1;
  check(map3Ok && isPartitioner<ShufflePartitioner>(map3Node->getOutEdges()[0]),
        "map3 的出边 partitioner 是 Shuffle（global 被后面的 shuffle 盖掉）");
  check(map3Ok && map3Node->getOutEdges()[0]->getSelectedNames() ==
                      std::vector<std::string>(1, "even"),
        "map3 的出边 selectedNames[0] == \"even\"");
  check(containsSelector(map3Node, selector3),
        "map3 的 outputSelectors 含 selector3");

  // ---- 1.0 没断言，但顺手验证 generator 自己的命名约定
  //      （transformSource:427 / transformSink:450 的前缀）----
  StreamNode* srcNode = graph->getStreamNode(src->getId());
  check(srcNode != NULL &&
            srcNode->getOperatorName() == "Source: Collection Source",
        "transformSource 给顶点名加 \"Source: \" 前缀");
  check(graph->getSourceIDs() == std::set<int>({src->getId()}),
        "source 只被转换一次（alreadyTransformed 命中）");
  check(graph->getSinkIDs().size() == 2, "两个 addSink 各建一个 sink 顶点");
  bool sinkNamesOk = true;
  for (std::set<int>::const_iterator it = graph->getSinkIDs().begin();
       it != graph->getSinkIDs().end(); ++it) {
    if (graph->getStreamNode(*it)->getOperatorName() != "Sink: Unnamed") {
      sinkNamesOk = false;
    }
  }
  check(sinkNamesOk, "transformSink 给顶点名加 \"Sink: \" 前缀");
}

// 1.0 StreamGraphGeneratorTest.testVirtualTransformations2（:147）
// 「Checks whether output selector, partitioning works correctly when applied
//  on a union.」
void testVirtualTransformations2() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  TransformationPtr src = source("Collection Source");

  TransformationPtr rebalanceMap =
      map(partition(src, std::make_shared<RebalancePartitioner>()));
  transformations.push_back(rebalanceMap);

  TransformationPtr map1 = map(rebalanceMap);
  transformations.push_back(map1);
  TransformationPtr map2 = map(rebalanceMap);
  transformations.push_back(map2);
  TransformationPtr map3 = map(rebalanceMap);
  transformations.push_back(map3);

  OutputSelectorPtr selector =
      eraseSelector(std::make_shared<EvenOddOutputSelector>());

  // map1.union(map2).union(map3).broadcast().split(selector).select("foo")
  //     .map(new NoOpIntMap());
  TransformationPtr unionedMap = map(select(
      split(partition(unionOf(unionOf(map1, map2), map3),
                      std::make_shared<BroadcastPartitioner>()),
            selector),
      "foo"));
  transformations.push_back(unionedMap);
  transformations.push_back(addSink(unionedMap));

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  // 1.0 注释：「verify that the properties are correctly set on all input
  // operators」（:177-187）
  const TransformationPtr maps[3] = {map1, map2, map3};
  const char* names[3] = {"map1", "map2", "map3"};
  for (int i = 0; i < 3; ++i) {
    StreamNode* node = graph->getStreamNode(maps[i]->getId());
    const bool ok = node != NULL && node->getOutEdges().size() == 1;
    const std::string who(names[i]);

    check(ok && isPartitioner<BroadcastPartitioner>(node->getOutEdges()[0]),
          who + " 的出边 partitioner 是 Broadcast");
    check(ok && node->getOutEdges()[0]->getSelectedNames() ==
                    std::vector<std::string>(1, "foo"),
          who + " 的出边 selectedNames[0] == \"foo\"");
    check(containsSelector(node, selector), who + " 的 outputSelectors 含 selector");
  }
}

// 1.0 SlotAllocationTest.testUnion（:86）
// determineSlotSharingGroup 的结果就落在 StreamNode.slotSharingGroup 上
// （1.0 再由 StreamingJobGraphGenerator.setSlotSharing 转成 JobVertex 的组，
//  那一段已由 graph_test 覆盖），所以这里直接在 StreamNode 上断言。
void testUnionSlotSharing() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  // DataStream<Long> src1 = env.generateSequence(1, 10);
  TransformationPtr src1 = source("Sequence Source");
  // DataStream<Long> src2 = ....slotSharingGroup("src-1");
  TransformationPtr src2 = source("Sequence Source");
  src2->setSlotSharingGroup("src-1");

  // 1.0 注释：「this should not inherit group "src-1"」
  TransformationPtr filter1 = filter(unionOf(src1, src2));
  transformations.push_back(filter1);

  TransformationPtr src3 = source("Sequence Source");
  src3->setSlotSharingGroup("group-1");
  TransformationPtr src4 = source("Sequence Source");
  src4->setSlotSharingGroup("group-1");

  // 1.0 注释：「this should inherit "group-1" now」
  TransformationPtr filter2 = filter(unionOf(src3, src4));
  transformations.push_back(filter2);

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  const std::string g1 = groupOf(graph, src1);
  const std::string g2 = groupOf(graph, src2);
  const std::string gf1 = groupOf(graph, filter1);
  const std::string g3 = groupOf(graph, src3);
  const std::string g4 = groupOf(graph, src4);
  const std::string gf2 = groupOf(graph, filter2);

  check(g1 == "default", "没指定组的 source 落在 \"default\"");
  check(g2 == "src-1", "指定了 \"src-1\" 的 source 原样保留");

  // 1.0 first pipeline（:111-113）：
  //   assertEquals(v0, v4); assertNotEquals(v0, v1); assertNotEquals(v1, v4)
  check(gf1 == g1, "union 两输入组不一致 -> filter 回落 \"default\"（= src1 的组）");
  check(g1 != g2, "src1 与 src2 不同组");
  check(gf1 != g2, "filter 没有继承 \"src-1\"");

  // 1.0 second pipeline（:116-118）
  check(g3 == g4, "src3 与 src4 同组");
  check(gf2 == g3, "两输入组一致 -> filter 继承上游的组");
  check(gf2 == "group-1", "继承到的组名就是 \"group-1\"");
}

// 1.0 SlotAllocationTest.testInheritOverride（:122）
// 「verify that we can explicitly disable inheritance of the input slot
//  sharing groups」
void testInheritOverride() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  TransformationPtr src1 = source("Sequence Source");
  src1->setSlotSharingGroup("group-1");
  TransformationPtr src2 = source("Sequence Source");
  src2->setSlotSharingGroup("group-1");

  // 1.0 注释：「this should not inherit group but be in "default"」
  // src1.union(src2).filter(dummyFilter).slotSharingGroup("default");
  TransformationPtr filter1 = filter(unionOf(src1, src2));
  filter1->setSlotSharingGroup("default");
  transformations.push_back(filter1);

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  const std::string g1 = groupOf(graph, src1);
  const std::string g2 = groupOf(graph, src2);
  const std::string gf = groupOf(graph, filter1);

  // 1.0（:141-143）
  check(g1 == g2, "两个 source 同在 \"group-1\"");
  check(g1 != gf, "显式 slotSharingGroup(\"default\") 压过了继承");
  check(gf == "default", "filter 的组是 \"default\"");
}

// 1.0 SlotAllocationTest.testCoOperation（:148）
void testCoOperationSlotSharing() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  TransformationPtr src1 = source("Sequence Source");
  TransformationPtr src2 = source("Sequence Source");
  src2->setSlotSharingGroup("src-1");

  // 1.0 注释：「this should not inherit group "src-1"」
  TransformationPtr co1 = coMap(src1, src2);
  transformations.push_back(co1);

  TransformationPtr src3 = source("Sequence Source");
  src3->setSlotSharingGroup("group-1");
  TransformationPtr src4 = source("Sequence Source");
  src4->setSlotSharingGroup("group-1");

  // 1.0 注释：「this should inherit "group-1" now」
  TransformationPtr co2 = coMap(src3, src4);
  transformations.push_back(co2);

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  // 1.0 first pipeline（:180-182）
  check(groupOf(graph, co1) == groupOf(graph, src1),
        "两输入组不一致 -> Co-Map 回落 \"default\"（= src1 的组）");
  check(groupOf(graph, src1) != groupOf(graph, src2), "src1 与 src2 不同组");
  check(groupOf(graph, src2) != groupOf(graph, co1),
        "Co-Map 没有继承 \"src-1\"");

  // 1.0 second pipeline（:185-187）
  check(groupOf(graph, src3) == groupOf(graph, src4), "src3 与 src4 同组");
  check(groupOf(graph, src3) == groupOf(graph, co2),
        "两输入组一致 -> Co-Map 继承 \"group-1\"");
  check(groupOf(graph, co2) == "group-1", "继承到的组名就是 \"group-1\"");

  // 1.0 transformTwoInputTransform：input1 的边 typeNumber = 1、input2 = 2
  StreamNode* coNode = graph->getStreamNode(co1->getId());
  check(coNode != NULL && coNode->getInEdges().size() == 2 &&
            coNode->getInEdges()[0]->getTypeNumber() == 1 &&
            coNode->getInEdges()[1]->getTypeNumber() == 2,
        "Co-Map 的两条入边 typeNumber 分别是 1 和 2");
  check(coNode != NULL && coNode->getJobVertexClassName() ==
                              "TwoInputStreamTask",
        "transformTwoInputTransform 走 addCoOperator -> TwoInputStreamTask");
}

// transform() 的通用行为：uid / bufferTimeout 的回填，以及 sink 返回空 id 表
// （1.0 transform:181-186 与 transformSink:467）
void testUidAndBufferTimeout() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  TransformationPtr src = source("Collection Source");
  TransformationPtr m = map(src);
  m->setUid("my-uid");
  m->setBufferTimeout(42);
  transformations.push_back(m);
  TransformationPtr sink = addSink(m);
  transformations.push_back(sink);

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  StreamNode* mapNode = graph->getStreamNode(m->getId());
  check(mapNode != NULL && mapNode->getTransformationId() == "my-uid",
        "uid 非空时回填到 StreamNode.transformationId");
  check(mapNode != NULL && mapNode->getBufferTimeout() == 42,
        "bufferTimeout > 0 时回填到 StreamNode");

  StreamNode* srcNode = graph->getStreamNode(src->getId());
  check(srcNode != NULL && srcNode->getTransformationId().empty(),
        "没设 uid 的顶点 transformationId 保持为空");

  // 1.0 transformSink 返回 Collections.emptyList()：sink 没有下游，
  // 所以 sink 顶点没有出边
  StreamNode* sinkNode = graph->getStreamNode(sink->getId());
  check(sinkNode != NULL && sinkNode->getOutEdges().empty(),
        "sink 顶点没有出边（transformSink 返回空 id 表）");
  check(sinkNode != NULL && sinkNode->getTypeSerializerIn1() &&
            !sinkNode->getTypeSerializerOut(),
        "addSink 的 in 取 input 的输出类型、out 传 null");
}

// 1.0 transformFeedback（:292-353）。1.0 对应的测试是 DataStream 级的
// IterateTest，这里下沉手工搭：
//   src -> iterate(FeedbackTransformation) -> map -> 作为反馈边接回 iterate
void testFeedback() {
  resetIds();
  std::vector<TransformationPtr> transformations;

  TransformationPtr src = source("Collection Source");

  std::shared_ptr<tr::FeedbackTransformation> iterate =
      std::make_shared<tr::FeedbackTransformation>(src, 7);

  // 1.0：feedback 边的并行度必须与迭代头一致
  bool threwParallelism = false;
  std::string parallelismMessage;
  try {
    TransformationPtr wrong = map(iterate);
    wrong->setParallelism(3);
    iterate->addFeedbackEdge(wrong);
  } catch (const std::exception& e) {
    threwParallelism = true;
    parallelismMessage = e.what();
  }
  check(threwParallelism &&
            parallelismMessage ==
                "Parallelism of the feedback stream must match the parallelism "
                "of the original stream. Parallelism of original stream: 1; "
                "parallelism of feedback stream: 3",
        "addFeedbackEdge 的并行度不一致异常文案与 1.0 一致");

  TransformationPtr feedbackMap = map(iterate);
  iterate->addFeedbackEdge(feedbackMap);
  transformations.push_back(feedbackMap);

  TransformationPtr tail = map(iterate);
  transformations.push_back(tail);
  transformations.push_back(addSink(tail));

  std::shared_ptr<StreamGraph> graph = generate(transformations);

  check(graph->isIterative(), "建了迭代顶点之后 isIterative() 为真");
  check(graph->getIterationSourceSinkPairs().size() == 1,
        "一对迭代 source/sink 顶点");
  StreamNode* itSource = graph->getIterationSourceSinkPairs()[0].f0;
  StreamNode* itSink = graph->getIterationSourceSinkPairs()[0].f1;
  // 1.0 getNewIterationNodeId()：**先减再返回**，所以第一个是 -1
  check(itSource->getId() == -1 && itSink->getId() == -2,
        "迭代顶点 id 从 -1 开始递减");
  check(graph->getLoopTimeout(itSource->getId()) == 7,
        "waitTime 写进 vertexIDtoLoopTimeout");
  // 1.0: setSerializers(itSource, null, null, out) / (itSink, out, null, null)
  check(!itSource->getTypeSerializerIn1() && itSource->getTypeSerializerOut(),
        "迭代 source 只设 out serializer");
  check(itSink->getTypeSerializerIn1() && !itSink->getTypeSerializerOut(),
        "迭代 sink 只设 in1 serializer");
  // 反馈边 feedbackMap -> itSink
  check(itSink->getInEdges().size() == 1 &&
            itSink->getInEdges()[0]->getSourceId() == feedbackMap->getId(),
        "反馈边接到迭代 sink 上");
  // 下游同时接 src 与 itSource（transformFeedback 返回 inputIds + itSource）
  StreamNode* tailNode = graph->getStreamNode(tail->getId());
  check(tailNode != NULL && tailNode->getInEdges().size() == 2,
        "迭代下游的输入是「原输入 + 迭代 source」两条");

  // 1.0: "Iteration " + iterate + " does not have any feedback edges."
  resetIds();
  TransformationPtr lonelySrc = source("Collection Source");
  std::shared_ptr<tr::FeedbackTransformation> empty =
      std::make_shared<tr::FeedbackTransformation>(lonelySrc, 0);
  std::vector<TransformationPtr> bad;
  bad.push_back(map(empty));
  bool threwNoFeedback = false;
  std::string message;
  try {
    generate(bad);
  } catch (const std::exception& e) {
    threwNoFeedback = true;
    message = e.what();
  }
  check(threwNoFeedback &&
            message.find("does not have any feedback edges.") !=
                std::string::npos,
        "没有反馈边的 iteration 抛「does not have any feedback edges.」");
}

// transformations 包里那几条「虚拟变换不准设 chaining 策略」的异常文案，
// 包含 1.0 原文的三处复制粘贴笔误（见各自头文件的说明）
void testChainingStrategyRefusals() {
  resetIds();
  typedef flink::streaming::api::operators::ChainingStrategy Strategy;
  TransformationPtr src = source("Collection Source");

  struct Case {
    TransformationPtr t;
    const char* expected;
    const char* what;
  };

  std::vector<Case> cases;
  Case c;

  c.t = partition(src, std::make_shared<ForwardPartitioner>());
  c.expected = "Cannot set chaining strategy on Union Transformation.";
  c.what = "PartitionTransformation 的文案（1.0 原文笔误写成 Union）";
  cases.push_back(c);

  c.t = unionOf(src, src);
  c.expected = "Cannot set chaining strategy on Union Transformation.";
  c.what = "UnionTransformation 的文案";
  cases.push_back(c);

  c.t = split(src, eraseSelector(std::make_shared<EvenOddOutputSelector>()));
  c.expected = "Cannot set chaining strategy on Split Transformation.";
  c.what = "SplitTransformation 的文案";
  cases.push_back(c);

  c.t = select(src, "even");
  c.expected = "Cannot set chaining strategy on Select Transformation.";
  c.what = "SelectTransformation 的文案";
  cases.push_back(c);

  c.t = std::make_shared<tr::FeedbackTransformation>(src, 0);
  c.expected = "Cannot set chaining strategy on Split Transformation.";
  c.what = "FeedbackTransformation 的文案（1.0 原文笔误写成 Split）";
  cases.push_back(c);

  c.t = std::make_shared<tr::CoFeedbackTransformation>(1, intSer(), 0);
  c.expected = "Cannot set chaining strategy on Split Transformation.";
  c.what = "CoFeedbackTransformation 的文案（1.0 原文笔误写成 Split）";
  cases.push_back(c);

  for (std::size_t i = 0; i < cases.size(); ++i) {
    bool threw = false;
    std::string message;
    try {
      cases[i].t->setChainingStrategy(Strategy::ALWAYS);
    } catch (const std::exception& e) {
      threw = true;
      message = e.what();
    }
    check(threw && message == cases[i].expected, cases[i].what);
  }
}

// StreamTransformation 自己的几条规则（StreamTransformation.java:100-105 /
// setParallelism / setOutputType 的 typeUsed 守卫）与
// getTransitivePredecessors 的形状
void testTransformationBasics() {
  resetIds();
  TransformationPtr src = source("Collection Source");
  // 1.0: idCounter++ 之后再返回 -> 第一个 id 是 1
  check(src->getId() == 1, "第一个 transformation 的 id 是 1（先加再返回）");

  TransformationPtr m = map(src);
  check(m->getId() == 2, "id 按创建顺序递增");

  // 1.0: checkArgument(parallelism > 0, "Parallelism must be bigger than zero.")
  bool threw = false;
  std::string message;
  try {
    m->setParallelism(0);
  } catch (const std::exception& e) {
    threw = true;
    message = e.what();
  }
  check(threw && message == "Parallelism must be bigger than zero.",
        "setParallelism(0) 的异常文案与 1.0 一致");

  // 1.0 setOutputType：typeUsed 之后不准再改
  bool threwTypeUsed = false;
  std::string typeMessage;
  try {
    m->getOutputType();  // typeUsed = true
    m->setOutputType(intSer());
  } catch (const std::exception& e) {
    threwTypeUsed = true;
    typeMessage = e.what();
  }
  check(threwTypeUsed &&
            typeMessage ==
                "TypeInformation cannot be filled in for the type after it has "
                "been used. Please make sure that the type info hints are the "
                "first call after the transformation function, before any "
                "access to types or semantic properties, etc.",
        "setOutputType 在 typeUsed 之后抛 1.0 的原文案");

  // getTransitivePredecessors
  check(src->getTransitivePredecessors().size() == 1 &&
            src->getTransitivePredecessors()[0] == src.get(),
        "SourceTransformation 的传递前驱只有自己");

  TransformationPtr p = partition(m, std::make_shared<GlobalPartitioner>());
  TransformationPtr m2 = map(p);
  // m2 -> p -> m -> src
  std::vector<Transformation*> preds = m2->getTransitivePredecessors();
  check(preds.size() == 4 && preds[0] == m2.get() && preds[1] == p.get() &&
            preds[2] == m.get() && preds[3] == src.get(),
        "链式 transformation 的传递前驱按「自己 + 输入的递归结果」排列");

  TransformationPtr two = coMap(m, m2);
  std::vector<Transformation*> twoPreds = two->getTransitivePredecessors();
  // this + input1 的 2 个 + input2 的 4 个
  check(twoPreds.size() == 7 && twoPreds[0] == two.get(),
        "TwoInputTransformation 的传递前驱 = 自己 + 两条输入各自的递归结果");

  // CoFeedbackTransformation.getTransitivePredecessors() 只有自己
  std::shared_ptr<tr::CoFeedbackTransformation> coFeedback =
      std::make_shared<tr::CoFeedbackTransformation>(1, intSer(), 0);
  check(coFeedback->getTransitivePredecessors().size() == 1,
        "CoFeedbackTransformation 的传递前驱只有自己（不含 input）");

  // UnionTransformation 的类型一致性校验
  bool threwMismatch = false;
  try {
    std::vector<TransformationPtr> inputs;
    inputs.push_back(src);
    TransformationPtr other = std::make_shared<tr::SourceTransformation>(
        "Other", sourceOp(), SerializerPtr(), 1);
    inputs.push_back(other);
    std::make_shared<tr::UnionTransformation>(inputs);
  } catch (const std::exception&) {
    threwMismatch = true;
  }
  check(threwMismatch, "UnionTransformation 的输入类型不一致时抛异常");
}

}  // namespace

int main() {
  testVirtualTransformations();
  testVirtualTransformations2();
  testUnionSlotSharing();
  testInheritOverride();
  testCoOperationSlotSharing();
  testUidAndBufferTimeout();
  testFeedback();
  testChainingStrategyRefusals();
  testTransformationBasics();

  if (failures == 0) {
    std::printf("\nstream_graph_generator_test: all checks passed\n");
    return 0;
  }
  std::printf("\nstream_graph_generator_test: %d check(s) FAILED\n", failures);
  return 1;
}
