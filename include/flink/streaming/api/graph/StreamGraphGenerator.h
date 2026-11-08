/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.graph.StreamGraphGenerator
 *
 * 1.0 class StreamGraphGenerator（StreamGraphGenerator.java:76，全文 593 行）
 *
 * 1.0 类注释（全文）：
 *   「A generator that generates a StreamGraph from a graph of
 *    StreamTransformations.
 *    This traverses the tree of StreamTransformations starting from the sinks.
 *    At each transformation we recursively transform the inputs, then create a
 *    node in the StreamGraph and add edges from the input Nodes to our newly
 *    created node. The transformation methods return the IDs of the nodes in
 *    the StreamGraph that represent the input transformation. Several IDs can
 *    be returned to be able to deal with feedback transformations and unions.
 *    Partitioning, split/select and union don't create actual nodes in the
 *    StreamGraph. For these, we create a virtual node in the StreamGraph that
 *    holds the specific property, i.e. partitioning, selector and so on. When
 *    an edge is created from a virtual node to a downstream node the
 *    StreamGraph resolved the id of the original node and creates an edge
 *    in the graph with the desired property. For example, if you have this
 *    graph:
 *        Map-1 -> HashPartition-2 -> Map-3
 *    where the numbers represent transformation IDs. We first recurse all the
 *    way down. Map-1 is transformed, i.e. we create a StreamNode with ID 1.
 *    Then we transform the HashPartition, for this, we create virtual node of
 *    ID 4 that holds the property HashPartition. This transformation returns
 *    the ID 4. Then we transform the Map-3. We add the edge 4 -> 3. The
 *    StreamGraph resolved the actual node with ID 1 and creates and edge 1 -> 3
 *    with the property HashPartition.」
 *
 * ──────────────── 1.0 的结构 ────────────────
 *
 * 字段（:78-94）：
 *   private StreamGraph streamGraph;              // 「this is initialized at
 *                                                 //  the beginning」
 *   private final StreamExecutionEnvironment env;
 *   protected static Integer iterationIdCounter = 0;
 *   public static int getNewIterationNodeId() {
 *       iterationIdCounter--; return iterationIdCounter; }
 *       —— 1.0 原注释：「This is used to assign a unique ID to iteration
 *       source/sink」。注意是**先减再返回**，所以第一个 id 是 -1 —— 迭代顶点用
 *       负数 id，与 StreamTransformation 的正数 id 永不冲突。
 *   private Map<StreamTransformation<?>, Collection<Integer>> alreadyTransformed;
 *       —— 1.0 原注释：「Keep track of which Transforms we have already
 *       transformed, this is necessary because we have loops, i.e. feedback
 *       edges.」
 *
 * 构造（:100-106，private）：
 *   streamGraph = new StreamGraph(env);
 *   streamGraph.setChaining(env.isChainingEnabled());
 *   streamGraph.setStateBackend(env.getStateBackend());
 *   1.0 注释：「Private constructor. The generator should only be invoked using
 *   generate().」
 *
 * generate(env, transformations)（:118-120，static）->
 *   new StreamGraphGenerator(env).generateInternal(transformations)
 * generateInternal（:125-130）：对每个 transformation 调 transform()，返回
 *   streamGraph。
 *
 * transform(transform)（:139-189）—— 核心分派：
 *   ① alreadyTransformed 命中则**直接返回**
 *   ② LOG.debug("Transforming " + transform)
 *   ③ transform.getOutputType();
 *      1.0 原注释：「call at least once to trigger exceptions about
 *      MissingTypeInfo」
 *   ④ instanceof 链，**顺序很重要**（照搬）：
 *        OneInputTransformation / TwoInputTransformation / SourceTransformation
 *        / SinkTransformation / UnionTransformation / SplitTransformation
 *        / SelectTransformation / FeedbackTransformation
 *        / CoFeedbackTransformation / PartitionTransformation
 *      都不匹配 -> IllegalStateException("Unknown transformation: " + transform)
 *   ⑤ 1.0 原注释：「need this check because the iterate transformation adds
 *      itself before transforming the feedback edges」
 *        if (!alreadyTransformed.containsKey(transform))
 *            alreadyTransformed.put(transform, transformedIds);
 *   ⑥ bufferTimeout > 0 -> streamGraph.setBufferTimeout(id, timeout)
 *      uid != null      -> streamGraph.setTransformationId(id, uid)
 *
 * transformUnion（:198-207）：只递归各输入并把 id **全部拼起来**返回，
 *   「so that downstream operations can connect to all upstream nodes」。
 *   union 自己在 StreamGraph 里没有节点。
 *
 * transformPartition（:216-228）：对每个上游 id 都建一个虚拟 partition 节点
 *   （虚拟 id 来自 StreamTransformation.getNewNodeId()），返回虚拟 id 们。
 *
 * transformSplit（:236-252）：先递归 input 拿 resultIds，然后
 *   「the recursive transform call might have transformed this already」
 *   再对每个 inputId 调 streamGraph.addOutputSelector(inputId, selector)，
 *   **原样返回 resultIds**（split 不建虚拟节点）。
 *
 * transformSelect（:261-279）：先递归 input，命中 alreadyTransformed 则返回；
 *   否则对每个 inputId 建虚拟 select 节点，返回虚拟 id 们。
 *
 * transformFeedback（:292-353）：
 *   - feedbackEdges 为空 -> IllegalStateException(
 *       "Iteration " + iterate + " does not have any feedback edges.")
 *   - 先 transform(input)，resultIds = inputIds
 *   - 「the recursive transform might have already transformed this」
 *   - createIterationSourceAndSink(iterate.getId(), getNewIterationNodeId(),
 *       getNewIterationNodeId(), iterate.getWaitTime(),
 *       iterate.getParallelism())
 *   - setSerializers(itSource, null, null, out) / (itSink, out, null, null)
 *   - resultIds.add(itSource.getId())
 *     「also add the feedback source ID to the result IDs, so that downstream
 *      operators will add both as input」
 *   - alreadyTransformed.put(iterate, resultIds)  —— **提前**放进去，
 *     「so that we can transform the feedback edges and let them stop when
 *      encountering the iterate node」
 *   - 遍历 feedbackEdges：transform 之后 addEdge(feedbackId, itSink, 0)
 *   - slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds)
 *     并同时设给 itSink 与 itSource
 *
 * transformCoFeedback（:367-415）：与上面几乎一样，区别是
 *   「For Co-Iteration we don't need to transform the input and wire the input
 *    to the head operator by returning the input IDs, the input is directly
 *    wired to the left input of the co-operation. This transform only needs to
 *    return the ids of the feedback edges, since they need to be wired to the
 *    second input of the co-operation.」
 *   所以返回的是 singleton(itSource.getId())，且**没有**空 feedbackEdges 校验。
 *
 * transformSource（:420-434）：
 *   slotSharingGroup = determineSlotSharingGroup(source.getSlotSharingGroup(),
 *                                                emptyList)
 *   streamGraph.addSource(id, group, operator, null, outputType,
 *                         "Source: " + source.getName())
 *   —— 注意名字**加了 "Source: " 前缀**
 *   if (operator.getUserFunction() instanceof FileSourceFunction)
 *       streamGraph.setInputFormat(id, fs.getFormat());
 *   setParallelism(id, source.getParallelism())
 *   return singleton(source.getId())
 *
 * transformSink（:439-468）：
 *   inputIds = transform(sink.getInput())
 *   group = determineSlotSharingGroup(sink.getSlotSharingGroup(), inputIds)
 *   streamGraph.addSink(id, group, operator, sink.getInput().getOutputType(),
 *                       null, "Sink: " + sink.getName())
 *   —— 注意名字**加了 "Sink: " 前缀**；in 用的是 input 的输出类型，out 传 null
 *   setParallelism；对每个 inputId addEdge(inputId, id, 0)
 *   stateKeySelector != null -> setOneInputStateKey(...)
 *   return emptyList()   —— **空表**，sink 没有下游
 *
 * transformOnInputTransform（:477-507）（1.0 的方法名就少个 e）：
 *   inputIds = transform(transform.getInput())
 *   「the recursive call might have already transformed this」
 *   group = determineSlotSharingGroup(getSlotSharingGroup(), inputIds)
 *   addOperator(id, group, operator, getInputType(), getOutputType(), getName())
 *   stateKeySelector != null -> setOneInputStateKey
 *   setParallelism; 每个 inputId addEdge(inputId, id, 0)
 *   return singleton(id)
 *
 * transformTwoInputTransform（:516-564）：
 *   inputIds1 = transform(getInput1()); inputIds2 = transform(getInput2());
 *   「the recursive call might have already transformed this」
 *   allInputIds = inputIds1 ++ inputIds2
 *   group = determineSlotSharingGroup(getSlotSharingGroup(), allInputIds)
 *   addCoOperator(id, group, operator, inType1, inType2, outType, name)
 *   stateKeySelector1 != null -> setTwoInputStateKey(id, sel1, sel2, keySer)
 *   setParallelism;
 *   inputIds1 的边 typeNumber = **1**，inputIds2 的边 typeNumber = **2**
 *   return singleton(id)
 *
 * determineSlotSharingGroup(specifiedGroup, inputIds)（:577-592）：
 *   1.0 注释：「If the user specifies a group name, this is taken as is. If
 *   nothing is specified and the input operations all have the same group name
 *   then this name is taken. Otherwise the default group is choosen.」
 *     if (specifiedGroup != null) return specifiedGroup;
 *     String inputGroup = null;
 *     for (int id: inputIds) {
 *         String cand = streamGraph.getSlotSharingGroup(id);
 *         if (inputGroup == null) inputGroup = cand;
 *         else if (!inputGroup.equals(cand)) return "default";
 *     }
 *     return inputGroup == null ? "default" : inputGroup;
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有 StreamExecutionEnvironment】那一层还没移植。1.0 的 generator
 * 只从 env 取四样东西：
 *   - new StreamGraph(env)       -> 本仓库的 StreamGraph 构造要
 *                                   (defaultParallelism, defaultBufferTimeout)
 *                                   （见 StreamGraph.h 偏差 1）
 *   - env.isChainingEnabled()    -> 构造参数 chainingEnabled
 *   - env.getStateBackend()      -> 构造参数 stateBackend
 *   - env.getConfig()            -> 只用于 createSerializer，见偏差 2
 * 所以 generate() 的签名变成
 *   generate(defaultParallelism, defaultBufferTimeout, chainingEnabled,
 *            stateBackend, transformations)
 * 等 StreamExecutionEnvironment 移植好之后，由它来填这四个实参。
 *
 * 【偏差 2 —— 没有 TypeInformation，所以没有 createSerializer】1.0 的
 * `xxx.getOutputType().createSerializer(env.getConfig())` 与
 * `getStateKeyType().createSerializer(env.getConfig())` 在本仓库里退化为直接取
 * SerializerPtr —— 因为 transformation 的 outputType / stateKeyType 本身就已经是
 * AnySerializer（见 StreamTransformation.h 偏差 1）。这也是上面偏差 1 里
 * env.getConfig() 唯一的用处，去掉之后 env 就只剩三个标量了。
 *
 * 【偏差 3 —— instanceof 分派换成 dynamic_cast】顺序与 1.0 的 if-else 链
 * **完全一致**（1.0 这些类之间没有继承关系，所以顺序其实不影响结果，但照搬）。
 *
 * 【偏差 4 —— FileSourceFunction / setInputFormat 省掉】transformSource 里
 * 1.0 那段 `instanceof FileSourceFunction -> setInputFormat` 需要 InputFormat
 * 体系（flink-core 的 api.common.io），本仓库只做 streaming 骨架，整条
 * readFile/readTextFile 的线还没到。StreamGraph 也相应没有 setInputFormat
 * （见 StreamGraph.h 偏差 8 同类处理）。不影响其它任何路径。
 *
 * 【偏差 5 —— alreadyTransformed 用裸指针做 key】1.0 是
 * Map<StreamTransformation<?>, Collection<Integer>>，依赖的是对象同一性
 * （理由见 StreamTransformation.h 偏差 3）。这里用 std::map<StreamTransformation*,
 * std::vector<int> >。顺带一提：这个 map 只在一次 generate 期间存活，而
 * transformations 由调用方用 shared_ptr 持有，所以裸指针不会悬空。
 *
 * 【偏差 6 —— uid 的「未设置」是空串】1.0 的 `transform.getUid() != null` 变成
 * `!transform->getUid().empty()`；determineSlotSharingGroup 的
 * `specifiedGroup != null` 变成 `!specifiedGroup.empty()`
 * （见 StreamTransformation.h 偏差 2）。
 *
 * 【偏差 7 —— 没有 LOG】1.0 的 LOG.debug("Transforming " + transform) 省掉
 * （本仓库至今没有引入日志门面）。
 *
 * 【偏差 9 —— 迭代顶点 id 要显式按序求值】1.0 写的是
 *   createIterationSourceAndSink(id, getNewIterationNodeId(),
 *                                getNewIterationNodeId(), ...)
 * Java 的实参求值是**从左到右**的，所以 sourceId = -1、sinkId = -2。C++ 的实参
 * 求值顺序未定义（gcc 实际从右往左），直接照抄会让两个 id 对调。实现里先把两个
 * id 按顺序取出来再传。—— 这是「照搬 1.0 写法」会踩的语言级坑，特此记一条。
 *
 * 【偏差 8 —— Collection<Integer> 一律用 std::vector<int>】1.0 混用了
 * ArrayList / Collections.singleton / Collections.emptyList，但所有消费点都只做
 * for-each 与 addAll，所以 vector 完全等价（顺序也与 1.0 一致：1.0 的
 * ArrayList 与 singleton 都是有序的）。
 */
#ifndef FLINK_STREAMING_API_GRAPH_STREAMGRAPHGENERATOR_H
#define FLINK_STREAMING_API_GRAPH_STREAMGRAPHGENERATOR_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "flink/streaming/api/graph/StreamGraph.h"
#include "flink/streaming/api/transformations/CoFeedbackTransformation.h"
#include "flink/streaming/api/transformations/FeedbackTransformation.h"
#include "flink/streaming/api/transformations/OneInputTransformation.h"
#include "flink/streaming/api/transformations/PartitionTransformation.h"
#include "flink/streaming/api/transformations/SelectTransformation.h"
#include "flink/streaming/api/transformations/SinkTransformation.h"
#include "flink/streaming/api/transformations/SourceTransformation.h"
#include "flink/streaming/api/transformations/SplitTransformation.h"
#include "flink/streaming/api/transformations/StreamTransformation.h"
#include "flink/streaming/api/transformations/TwoInputTransformation.h"
#include "flink/streaming/api/transformations/UnionTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

class StreamGraphGenerator {
 public:
  typedef flink::streaming::api::transformations::StreamTransformation
      StreamTransformation;
  typedef std::shared_ptr<StreamTransformation> TransformationPtr;
  typedef StreamGraph::StateBackendPtr StateBackendPtr;

  // 1.0: protected static Integer iterationIdCounter = 0;
  //      public static int getNewIterationNodeId() {
  //          iterationIdCounter--; return iterationIdCounter; }
  static int getNewIterationNodeId() {
    --iterationIdCounter_;
    return iterationIdCounter_;
  }
  // 1.0 没有这个方法（同 StreamTransformation::resetIdCounter，偏差见那边）
  static void resetIterationIdCounter() { iterationIdCounter_ = 0; }

  // 1.0 static StreamGraph generate(StreamExecutionEnvironment env,
  //                                 List<StreamTransformation<?>> transformations)
  // 【偏差 1】env 换成它实际提供的三个值
  static std::shared_ptr<StreamGraph> generate(
      int defaultParallelism, int64_t defaultBufferTimeout,
      bool chainingEnabled, const StateBackendPtr& stateBackend,
      const std::vector<TransformationPtr>& transformations);

 private:
  // 1.0: private StreamGraphGenerator(StreamExecutionEnvironment env)
  // 「The generator should only be invoked using generate().」
  StreamGraphGenerator(int defaultParallelism, int64_t defaultBufferTimeout,
                       bool chainingEnabled,
                       const StateBackendPtr& stateBackend);

  // 1.0 generateInternal：「This starts the actual transformation, beginning
  // from the sinks.」
  std::shared_ptr<StreamGraph> generateInternal(
      const std::vector<TransformationPtr>& transformations);

  // 1.0 transform(StreamTransformation<?>)：
  // 「This checks whether we already transformed it and exits early in that
  //  case. If not it delegates to one of the transformation specific methods.」
  std::vector<int> transform(StreamTransformation* transform);

  std::vector<int> transformUnion(
      flink::streaming::api::transformations::UnionTransformation* unionTr);
  std::vector<int> transformPartition(
      flink::streaming::api::transformations::PartitionTransformation*
          partition);
  std::vector<int> transformSplit(
      flink::streaming::api::transformations::SplitTransformation* split);
  std::vector<int> transformSelect(
      flink::streaming::api::transformations::SelectTransformation* select);
  std::vector<int> transformFeedback(
      flink::streaming::api::transformations::FeedbackTransformation* iterate);
  std::vector<int> transformCoFeedback(
      flink::streaming::api::transformations::CoFeedbackTransformation*
          coIterate);
  std::vector<int> transformSource(
      flink::streaming::api::transformations::SourceTransformation* source);
  std::vector<int> transformSink(
      flink::streaming::api::transformations::SinkTransformation* sink);
  // 1.0 的方法名就是 transformOnInputTransform（少个 e），照搬
  std::vector<int> transformOnInputTransform(
      flink::streaming::api::transformations::OneInputTransformation*
          transform);
  std::vector<int> transformTwoInputTransform(
      flink::streaming::api::transformations::TwoInputTransformation*
          transform);

  // 1.0 determineSlotSharingGroup(String specifiedGroup,
  //                               Collection<Integer> inputIds)
  std::string determineSlotSharingGroup(const std::string& specifiedGroup,
                                        const std::vector<int>& inputIds) const;

  // 1.0「The StreamGraph that is being built, this is initialized at the
  // beginning.」
  std::shared_ptr<StreamGraph> streamGraph_;

  // 1.0「Keep track of which Transforms we have already transformed, this is
  // necessary because we have loops, i.e. feedback edges.」
  // 【偏差 5】裸指针做 key
  std::map<StreamTransformation*, std::vector<int> > alreadyTransformed_;

  static int iterationIdCounter_;
};

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_GRAPH_STREAMGRAPHGENERATOR_H
