/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.graph.StreamGraphGenerator
 *
 * 实现体。逐段对照 StreamGraphGenerator.java，偏差清单见
 * include/flink/streaming/api/graph/StreamGraphGenerator.h 的头注释。
 */
#include "flink/streaming/api/graph/StreamGraphGenerator.h"

#include <stdexcept>

namespace flink {
namespace streaming {
namespace api {
namespace graph {

namespace tr = flink::streaming::api::transformations;

// 1.0: protected static Integer iterationIdCounter = 0;
int StreamGraphGenerator::iterationIdCounter_ = 0;

// 1.0 private StreamGraphGenerator(StreamExecutionEnvironment env)（:100-106）
StreamGraphGenerator::StreamGraphGenerator(int defaultParallelism,
                                           int64_t defaultBufferTimeout,
                                           bool chainingEnabled,
                                           const StateBackendPtr& stateBackend)
    : streamGraph_(
          new StreamGraph(defaultParallelism, defaultBufferTimeout)) {
  // 1.0: this.streamGraph.setChaining(env.isChainingEnabled());
  streamGraph_->setChaining(chainingEnabled);
  // 1.0: this.streamGraph.setStateBackend(env.getStateBackend());
  streamGraph_->setStateBackend(stateBackend);
  // 1.0: this.alreadyTransformed = new HashMap<>();
}

// 1.0 static generate(env, transformations)（:118-120）
std::shared_ptr<StreamGraph> StreamGraphGenerator::generate(
    int defaultParallelism, int64_t defaultBufferTimeout, bool chainingEnabled,
    const StateBackendPtr& stateBackend,
    const std::vector<TransformationPtr>& transformations) {
  StreamGraphGenerator generator(defaultParallelism, defaultBufferTimeout,
                                 chainingEnabled, stateBackend);
  return generator.generateInternal(transformations);
}

// 1.0 generateInternal（:125-130）
std::shared_ptr<StreamGraph> StreamGraphGenerator::generateInternal(
    const std::vector<TransformationPtr>& transformations) {
  for (std::size_t i = 0; i < transformations.size(); ++i) {
    transform(transformations[i].get());
  }
  return streamGraph_;
}

// 1.0 transform(StreamTransformation<?>)（:139-189）
std::vector<int> StreamGraphGenerator::transform(
    StreamTransformation* transform) {
  // 1.0: if (alreadyTransformed.containsKey(transform)) return ...get(...)
  std::map<StreamTransformation*, std::vector<int> >::const_iterator hit =
      alreadyTransformed_.find(transform);
  if (hit != alreadyTransformed_.end()) {
    return hit->second;
  }

  // 1.0: LOG.debug("Transforming " + transform);  —— 偏差 7

  // 1.0 原注释：「call at least once to trigger exceptions about
  // MissingTypeInfo」。本仓库没有 MissingTypeInfo（偏差 2），但 getOutputType()
  // 还有把 typeUsed 置 true 的副作用，这一步必须保留。
  transform->getOutputType();

  std::vector<int> transformedIds;
  // 1.0 的 instanceof 链，顺序照搬（偏差 3）
  if (tr::OneInputTransformation* t =
          dynamic_cast<tr::OneInputTransformation*>(transform)) {
    transformedIds = transformOnInputTransform(t);
  } else if (tr::TwoInputTransformation* t =
                 dynamic_cast<tr::TwoInputTransformation*>(transform)) {
    transformedIds = transformTwoInputTransform(t);
  } else if (tr::SourceTransformation* t =
                 dynamic_cast<tr::SourceTransformation*>(transform)) {
    transformedIds = transformSource(t);
  } else if (tr::SinkTransformation* t =
                 dynamic_cast<tr::SinkTransformation*>(transform)) {
    transformedIds = transformSink(t);
  } else if (tr::UnionTransformation* t =
                 dynamic_cast<tr::UnionTransformation*>(transform)) {
    transformedIds = transformUnion(t);
  } else if (tr::SplitTransformation* t =
                 dynamic_cast<tr::SplitTransformation*>(transform)) {
    transformedIds = transformSplit(t);
  } else if (tr::SelectTransformation* t =
                 dynamic_cast<tr::SelectTransformation*>(transform)) {
    transformedIds = transformSelect(t);
  } else if (tr::FeedbackTransformation* t =
                 dynamic_cast<tr::FeedbackTransformation*>(transform)) {
    transformedIds = transformFeedback(t);
  } else if (tr::CoFeedbackTransformation* t =
                 dynamic_cast<tr::CoFeedbackTransformation*>(transform)) {
    transformedIds = transformCoFeedback(t);
  } else if (tr::PartitionTransformation* t =
                 dynamic_cast<tr::PartitionTransformation*>(transform)) {
    transformedIds = transformPartition(t);
  } else {
    // 1.0: throw new IllegalStateException("Unknown transformation: " + ...)
    throw std::logic_error("Unknown transformation: " + transform->toString());
  }

  // 1.0 原注释：「need this check because the iterate transformation adds
  // itself before transforming the feedback edges」
  if (alreadyTransformed_.find(transform) == alreadyTransformed_.end()) {
    alreadyTransformed_[transform] = transformedIds;
  }

  if (transform->getBufferTimeout() > 0) {
    streamGraph_->setBufferTimeout(transform->getId(),
                                   transform->getBufferTimeout());
  }
  // 1.0: if (transform.getUid() != null)   —— 偏差 6
  if (!transform->getUid().empty()) {
    streamGraph_->setTransformationId(transform->getId(), transform->getUid());
  }

  return transformedIds;
}

// 1.0 transformUnion（:198-207）
// 「This is easy, we only have to transform the inputs and return all the IDs
//  in a list so that downstream operations can connect to all upstream nodes.」
std::vector<int> StreamGraphGenerator::transformUnion(
    tr::UnionTransformation* unionTr) {
  const std::vector<TransformationPtr>& inputs = unionTr->getInputs();
  std::vector<int> resultIds;

  for (std::size_t i = 0; i < inputs.size(); ++i) {
    const std::vector<int> ids = transform(inputs[i].get());
    resultIds.insert(resultIds.end(), ids.begin(), ids.end());
  }

  return resultIds;
}

// 1.0 transformPartition（:216-228）
// 「For this we create a virtual node in the StreamGraph that holds the
//  partition property.」
std::vector<int> StreamGraphGenerator::transformPartition(
    tr::PartitionTransformation* partition) {
  StreamTransformation* input = partition->getInput().get();
  std::vector<int> resultIds;

  const std::vector<int> transformedIds = transform(input);
  for (std::size_t i = 0; i < transformedIds.size(); ++i) {
    const int virtualId = StreamTransformation::getNewNodeId();
    streamGraph_->addVirtualPartitionNode(transformedIds[i], virtualId,
                                          partition->getPartitioner());
    resultIds.push_back(virtualId);
  }

  return resultIds;
}

// 1.0 transformSplit（:236-252）
// 「We add the output selector to previously transformed nodes.」
std::vector<int> StreamGraphGenerator::transformSplit(
    tr::SplitTransformation* split) {
  StreamTransformation* input = split->getInput().get();
  const std::vector<int> resultIds = transform(input);

  // 1.0 原注释：「the recursive transform call might have transformed this
  // already」
  std::map<StreamTransformation*, std::vector<int> >::const_iterator hit =
      alreadyTransformed_.find(split);
  if (hit != alreadyTransformed_.end()) {
    return hit->second;
  }

  for (std::size_t i = 0; i < resultIds.size(); ++i) {
    streamGraph_->addOutputSelector(resultIds[i], split->getOutputSelector());
  }

  return resultIds;
}

// 1.0 transformSelect（:261-279）
// 「For this we create a virtual node in the StreamGraph holds the selected
//  names.」
std::vector<int> StreamGraphGenerator::transformSelect(
    tr::SelectTransformation* select) {
  StreamTransformation* input = select->getInput().get();
  const std::vector<int> resultIds = transform(input);

  // 1.0 原注释：「the recursive transform might have already transformed this」
  std::map<StreamTransformation*, std::vector<int> >::const_iterator hit =
      alreadyTransformed_.find(select);
  if (hit != alreadyTransformed_.end()) {
    return hit->second;
  }

  std::vector<int> virtualResultIds;
  for (std::size_t i = 0; i < resultIds.size(); ++i) {
    const int virtualId = StreamTransformation::getNewNodeId();
    streamGraph_->addVirtualSelectNode(resultIds[i], virtualId,
                                       select->getSelectedNames());
    virtualResultIds.push_back(virtualId);
  }
  return virtualResultIds;
}

// 1.0 transformFeedback（:292-353）
// 「This will recursively transform the input and the feedback edges. We return
//  the concatenation of the input IDs and the feedback IDs so that downstream
//  operations can be wired to both. This is responsible for creating the
//  IterationSource and IterationSink which are used to feed back the
//  elements.」
std::vector<int> StreamGraphGenerator::transformFeedback(
    tr::FeedbackTransformation* iterate) {
  if (iterate->getFeedbackEdges().empty()) {
    // 1.0: "Iteration " + iterate + " does not have any feedback edges."
    throw std::logic_error("Iteration " + iterate->toString() +
                           " does not have any feedback edges.");
  }

  StreamTransformation* input = iterate->getInput().get();
  std::vector<int> resultIds;

  // 1.0 原注释：「first transform the input stream(s) and store the result IDs」
  const std::vector<int> inputIds = transform(input);
  resultIds.insert(resultIds.end(), inputIds.begin(), inputIds.end());

  // 1.0 原注释：「the recursive transform might have already transformed this」
  std::map<StreamTransformation*, std::vector<int> >::const_iterator hit =
      alreadyTransformed_.find(iterate);
  if (hit != alreadyTransformed_.end()) {
    return hit->second;
  }

  // 1.0 原注释：「create the fake iteration source/sink pair」
  // 1.0 是 createIterationSourceAndSink(id, getNewIterationNodeId(),
  // getNewIterationNodeId(), ...)，Java 的实参求值**从左到右**，所以 source
  // 拿到 -1、sink 拿到 -2。C++ 的实参求值顺序未定义（gcc 实际是从右往左），
  // 必须先按顺序取出来再传，否则两个 id 会对调。
  const int itSourceId = getNewIterationNodeId();
  const int itSinkId = getNewIterationNodeId();
  flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*> itSourceAndSink =
      streamGraph_->createIterationSourceAndSink(
          iterate->getId(), itSourceId, itSinkId, iterate->getWaitTime(),
          iterate->getParallelism());

  StreamNode* itSource = itSourceAndSink.f0;
  StreamNode* itSink = itSourceAndSink.f1;

  // 1.0 原注释：「We set the proper serializers for the sink/source」
  // 【偏差 2】1.0 这里是 getOutputType().createSerializer(env.getConfig())
  streamGraph_->setSerializers(itSource->getId(), StreamGraph::SerializerPtr(),
                               StreamGraph::SerializerPtr(),
                               iterate->getOutputType());
  streamGraph_->setSerializers(itSink->getId(), iterate->getOutputType(),
                               StreamGraph::SerializerPtr(),
                               StreamGraph::SerializerPtr());

  // 1.0 原注释：「also add the feedback source ID to the result IDs, so that
  // downstream operators will add both as input」
  resultIds.push_back(itSource->getId());

  // 1.0 原注释：「at the iterate to the already-seen-set with the result IDs,
  // so that we can transform the feedback edges and let them stop when
  // encountering the iterate node」（原文 "at" 应为 "add"）
  alreadyTransformed_[iterate] = resultIds;

  // 1.0 原注释：「so that we can determine the slot sharing group from all
  // feedback edges」
  std::vector<int> allFeedbackIds;

  const std::vector<TransformationPtr>& feedbackEdges =
      iterate->getFeedbackEdges();
  for (std::size_t i = 0; i < feedbackEdges.size(); ++i) {
    const std::vector<int> feedbackIds = transform(feedbackEdges[i].get());
    allFeedbackIds.insert(allFeedbackIds.end(), feedbackIds.begin(),
                          feedbackIds.end());
    for (std::size_t j = 0; j < feedbackIds.size(); ++j) {
      streamGraph_->addEdge(feedbackIds[j], itSink->getId(), 0);
    }
  }

  // 1.0: determineSlotSharingGroup(null, allFeedbackIds)   —— 偏差 6
  const std::string slotSharingGroup =
      determineSlotSharingGroup(std::string(), allFeedbackIds);

  itSink->setSlotSharingGroup(slotSharingGroup);
  itSource->setSlotSharingGroup(slotSharingGroup);

  return resultIds;
}

// 1.0 transformCoFeedback（:367-415）
std::vector<int> StreamGraphGenerator::transformCoFeedback(
    tr::CoFeedbackTransformation* coIterate) {
  // 1.0 原注释：「For Co-Iteration we don't need to transform the input and
  // wire the input to the head operator by returning the input IDs, the input
  // is directly wired to the left input of the co-operation. This transform
  // only needs to return the ids of the feedback edges, since they need to be
  // wired to the second input of the co-operation.」

  // 1.0 原注释：「create the fake iteration source/sink pair」
  // 实参求值顺序的说明见 transformFeedback 里同一处
  const int itSourceId = getNewIterationNodeId();
  const int itSinkId = getNewIterationNodeId();
  flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*> itSourceAndSink =
      streamGraph_->createIterationSourceAndSink(
          coIterate->getId(), itSourceId, itSinkId, coIterate->getWaitTime(),
          coIterate->getParallelism());

  StreamNode* itSource = itSourceAndSink.f0;
  StreamNode* itSink = itSourceAndSink.f1;

  // 1.0 原注释：「We set the proper serializers for the sink/source」
  streamGraph_->setSerializers(itSource->getId(), StreamGraph::SerializerPtr(),
                               StreamGraph::SerializerPtr(),
                               coIterate->getOutputType());
  streamGraph_->setSerializers(itSink->getId(), coIterate->getOutputType(),
                               StreamGraph::SerializerPtr(),
                               StreamGraph::SerializerPtr());

  // 1.0: Collections.singleton(itSource.getId())
  const std::vector<int> resultIds(1, itSource->getId());

  // 1.0 原注释：「at the iterate to the already-seen-set with the result IDs,
  // so that we can transform the feedback edges and let them stop when
  // encountering the iterate node」
  alreadyTransformed_[coIterate] = resultIds;

  // 1.0 原注释：「so that we can determine the slot sharing group from all
  // feedback edges」
  std::vector<int> allFeedbackIds;

  const std::vector<TransformationPtr>& feedbackEdges =
      coIterate->getFeedbackEdges();
  for (std::size_t i = 0; i < feedbackEdges.size(); ++i) {
    const std::vector<int> feedbackIds = transform(feedbackEdges[i].get());
    allFeedbackIds.insert(allFeedbackIds.end(), feedbackIds.begin(),
                          feedbackIds.end());
    for (std::size_t j = 0; j < feedbackIds.size(); ++j) {
      streamGraph_->addEdge(feedbackIds[j], itSink->getId(), 0);
    }
  }

  const std::string slotSharingGroup =
      determineSlotSharingGroup(std::string(), allFeedbackIds);

  itSink->setSlotSharingGroup(slotSharingGroup);
  itSource->setSlotSharingGroup(slotSharingGroup);

  // 1.0 这里又 new 了一个 singleton（不是返回上面那个 resultIds），等价
  return std::vector<int>(1, itSource->getId());
}

// 1.0 transformSource（:420-434）
std::vector<int> StreamGraphGenerator::transformSource(
    tr::SourceTransformation* source) {
  const std::string slotSharingGroup = determineSlotSharingGroup(
      source->getSlotSharingGroup(), std::vector<int>());

  // 1.0 名字加 "Source: " 前缀
  streamGraph_->addSource(source->getId(), slotSharingGroup,
                          source->getOperator(), StreamGraph::SerializerPtr(),
                          source->getOutputType(),
                          "Source: " + source->getName());
  // 1.0 这里还有 FileSourceFunction -> setInputFormat（偏差 4，省掉）
  streamGraph_->setParallelism(source->getId(), source->getParallelism());
  // 1.0: Collections.singleton(source.getId())
  return std::vector<int>(1, source->getId());
}

// 1.0 transformSink（:439-468）（1.0 的 javadoc 这里也写成了
// 「Transforms a SourceTransformation」—— 复制粘贴笔误）
std::vector<int> StreamGraphGenerator::transformSink(
    tr::SinkTransformation* sink) {
  const std::vector<int> inputIds = transform(sink->getInput().get());

  const std::string slotSharingGroup =
      determineSlotSharingGroup(sink->getSlotSharingGroup(), inputIds);

  // 1.0 名字加 "Sink: " 前缀；in 取的是 input 的输出类型，out 传 null
  streamGraph_->addSink(sink->getId(), slotSharingGroup, sink->getOperator(),
                        sink->getInput()->getOutputType(),
                        StreamGraph::SerializerPtr(),
                        "Sink: " + sink->getName());

  streamGraph_->setParallelism(sink->getId(), sink->getParallelism());

  for (std::size_t i = 0; i < inputIds.size(); ++i) {
    streamGraph_->addEdge(inputIds[i], sink->getId(), 0);
  }

  if (sink->getStateKeySelector()) {
    // 【偏差 2】1.0: sink.getStateKeyType().createSerializer(env.getConfig())
    streamGraph_->setOneInputStateKey(sink->getId(), sink->getStateKeySelector(),
                                      sink->getStateKeyType());
  }

  // 1.0: Collections.emptyList()
  return std::vector<int>();
}

// 1.0 transformOnInputTransform（:477-507）
// 「This recusively transforms the inputs, creates a new StreamNode in the
//  graph and wired the inputs to this new node.」（原文的 typo 一并照录）
std::vector<int> StreamGraphGenerator::transformOnInputTransform(
    tr::OneInputTransformation* transform) {
  const std::vector<int> inputIds = this->transform(transform->getInput().get());

  // 1.0 原注释：「the recursive call might have already transformed this」
  std::map<StreamTransformation*, std::vector<int> >::const_iterator hit =
      alreadyTransformed_.find(transform);
  if (hit != alreadyTransformed_.end()) {
    return hit->second;
  }

  const std::string slotSharingGroup =
      determineSlotSharingGroup(transform->getSlotSharingGroup(), inputIds);

  streamGraph_->addOperator(transform->getId(), slotSharingGroup,
                            transform->getOperator(), transform->getInputType(),
                            transform->getOutputType(), transform->getName());

  if (transform->getStateKeySelector()) {
    streamGraph_->setOneInputStateKey(transform->getId(),
                                      transform->getStateKeySelector(),
                                      transform->getStateKeyType());
  }

  streamGraph_->setParallelism(transform->getId(), transform->getParallelism());

  for (std::size_t i = 0; i < inputIds.size(); ++i) {
    streamGraph_->addEdge(inputIds[i], transform->getId(), 0);
  }

  return std::vector<int>(1, transform->getId());
}

// 1.0 transformTwoInputTransform（:516-564）
std::vector<int> StreamGraphGenerator::transformTwoInputTransform(
    tr::TwoInputTransformation* transform) {
  const std::vector<int> inputIds1 =
      this->transform(transform->getInput1().get());
  const std::vector<int> inputIds2 =
      this->transform(transform->getInput2().get());

  // 1.0 原注释：「the recursive call might have already transformed this」
  std::map<StreamTransformation*, std::vector<int> >::const_iterator hit =
      alreadyTransformed_.find(transform);
  if (hit != alreadyTransformed_.end()) {
    return hit->second;
  }

  std::vector<int> allInputIds;
  allInputIds.insert(allInputIds.end(), inputIds1.begin(), inputIds1.end());
  allInputIds.insert(allInputIds.end(), inputIds2.begin(), inputIds2.end());

  const std::string slotSharingGroup =
      determineSlotSharingGroup(transform->getSlotSharingGroup(), allInputIds);

  streamGraph_->addCoOperator(
      transform->getId(), slotSharingGroup, transform->getOperator(),
      transform->getInputType1(), transform->getInputType2(),
      transform->getOutputType(), transform->getName());

  if (transform->getStateKeySelector1()) {
    streamGraph_->setTwoInputStateKey(
        transform->getId(), transform->getStateKeySelector1(),
        transform->getStateKeySelector2(), transform->getStateKeyType());
  }

  streamGraph_->setParallelism(transform->getId(), transform->getParallelism());

  // input1 的边 typeNumber = 1
  for (std::size_t i = 0; i < inputIds1.size(); ++i) {
    streamGraph_->addEdge(inputIds1[i], transform->getId(), 1);
  }
  // input2 的边 typeNumber = 2
  for (std::size_t i = 0; i < inputIds2.size(); ++i) {
    streamGraph_->addEdge(inputIds2[i], transform->getId(), 2);
  }

  return std::vector<int>(1, transform->getId());
}

// 1.0 determineSlotSharingGroup（:577-592）
// 「If the user specifies a group name, this is taken as is. If nothing is
//  specified and the input operations all have the same group name then this
//  name is taken. Otherwise the default group is choosen.」（原文 typo 照录）
std::string StreamGraphGenerator::determineSlotSharingGroup(
    const std::string& specifiedGroup, const std::vector<int>& inputIds) const {
  // 1.0: if (specifiedGroup != null)   —— 偏差 6
  if (!specifiedGroup.empty()) {
    return specifiedGroup;
  }

  // 1.0 的 inputGroup 初值是 null，用「还没取过」这个标志位表达（偏差 6：
  // getSlotSharingGroup 返回的组名不会是空串，但为了语义严格还是用标志位）
  bool inputGroupSet = false;
  std::string inputGroup;
  for (std::size_t i = 0; i < inputIds.size(); ++i) {
    const std::string inputGroupCandidate =
        streamGraph_->getSlotSharingGroup(inputIds[i]);
    if (!inputGroupSet) {
      inputGroup = inputGroupCandidate;
      inputGroupSet = true;
    } else if (inputGroup != inputGroupCandidate) {
      return "default";
    }
  }
  return inputGroupSet ? inputGroup : std::string("default");
}

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
