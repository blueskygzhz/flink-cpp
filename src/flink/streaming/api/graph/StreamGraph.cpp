#include "flink/streaming/api/graph/StreamGraph.h"

#include <stdexcept>

#include "flink/streaming/api/operators/SourceOperator.h"
// StreamNode.h 只前置声明了 StreamOperator，addOperator 里要 dynamic_cast
// 就必须见到完整定义
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/runtime/partitioner/ForwardPartitioner.h"
#include "flink/streaming/runtime/partitioner/RebalancePartitioner.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

namespace {
typedef flink::streaming::api::operators::SourceOperator SourceOperator;
typedef flink::streaming::runtime::partitioner::ForwardPartitioner
    ForwardPartitioner;
typedef flink::streaming::runtime::partitioner::RebalancePartitioner
    RebalancePartitioner;
}  // namespace

// 【偏差 3】1.0 存的是 Class<? extends AbstractInvokable>
const char* StreamGraph::SOURCE_STREAM_TASK = "SourceStreamTask";
const char* StreamGraph::ONE_INPUT_STREAM_TASK = "OneInputStreamTask";
const char* StreamGraph::TWO_INPUT_STREAM_TASK = "TwoInputStreamTask";
const char* StreamGraph::STREAM_ITERATION_HEAD = "StreamIterationHead";
const char* StreamGraph::STREAM_ITERATION_TAIL = "StreamIterationTail";

// 1.0 StreamGraph(StreamExecutionEnvironment)（:95）
StreamGraph::StreamGraph(int defaultParallelism, int64_t defaultBufferTimeout)
    : jobName_(defaultJobName()),
      chaining_(false),
      defaultParallelism_(defaultParallelism),
      defaultBufferTimeout_(defaultBufferTimeout) {
  // 1.0: "create an empty new stream graph."
  clear();
}

// 1.0 clear()（:107）：「Remove all registered nodes etc.」
void StreamGraph::clear() {
  streamNodes_.clear();
  virtualSelectNodes_.clear();
  virtualPartitionNodes_.clear();
  vertexIDtoBrokerID_.clear();
  vertexIDtoLoopTimeout_.clear();
  iterationSourceSinkPairs_.clear();
  sources_.clear();
  sinks_.clear();
}

// ------------------------------- 建顶点 -------------------------------

// 1.0 addSource(...)（:162）
void StreamGraph::addSource(int vertexID, const std::string& slotSharingGroup,
                            const StreamOperatorPtr& operatorObject,
                            const SerializerPtr& inSerializer,
                            const SerializerPtr& outSerializer,
                            const std::string& operatorName) {
  addOperator(vertexID, slotSharingGroup, operatorObject, inSerializer,
              outSerializer, operatorName);
  sources_.insert(vertexID);
}

// 1.0 addSink(...)（:172）
void StreamGraph::addSink(int vertexID, const std::string& slotSharingGroup,
                          const StreamOperatorPtr& operatorObject,
                          const SerializerPtr& inSerializer,
                          const SerializerPtr& outSerializer,
                          const std::string& operatorName) {
  addOperator(vertexID, slotSharingGroup, operatorObject, inSerializer,
              outSerializer, operatorName);
  sinks_.insert(vertexID);
}

// 1.0 addOperator(...)（:182）
void StreamGraph::addOperator(int vertexID,
                              const std::string& slotSharingGroup,
                              const StreamOperatorPtr& operatorObject,
                              const SerializerPtr& inSerializer,
                              const SerializerPtr& outSerializer,
                              const std::string& operatorName) {
  // 1.0:190-196 的 instanceof 链。【偏差 3】StoppableStreamSource 这一支
  // 没有对应实现，省掉
  if (dynamic_cast<SourceOperator*>(operatorObject.get()) != NULL) {
    addNode(vertexID, slotSharingGroup, SOURCE_STREAM_TASK, operatorObject,
            operatorName);
  } else {
    addNode(vertexID, slotSharingGroup, ONE_INPUT_STREAM_TASK, operatorObject,
            operatorName);
  }

  // 1.0:198-202 先 createSerializer 再 setSerializers。【偏差 2】这里直接收
  // serializer，1.0 的 `inTypeInfo != null && !(... MissingTypeInfo)` 判定
  // 退化成空 shared_ptr
  setSerializers(vertexID, inSerializer, SerializerPtr(), outSerializer);

  // 1.0:204-214 OutputTypeConfigurable / InputTypeConfigurable 回填
  // —— 【偏差 2】本实现没有这两个接口
}

// 1.0 addCoOperator(...)（:221）
void StreamGraph::addCoOperator(int vertexID,
                                const std::string& slotSharingGroup,
                                const StreamOperatorPtr& taskOperatorObject,
                                const SerializerPtr& in1Serializer,
                                const SerializerPtr& in2Serializer,
                                const SerializerPtr& outSerializer,
                                const std::string& operatorName) {
  addNode(vertexID, slotSharingGroup, TWO_INPUT_STREAM_TASK, taskOperatorObject,
          operatorName);
  setSerializers(vertexID, in1Serializer, in2Serializer, outSerializer);
  // 1.0:237-242 OutputTypeConfigurable 回填 —— 【偏差 2】
}

// 1.0 protected addNode(...)（:249）
StreamNode* StreamGraph::addNode(int vertexID,
                                 const std::string& slotSharingGroup,
                                 const std::string& vertexClassName,
                                 const StreamOperatorPtr& operatorObject,
                                 const std::string& operatorName) {
  if (streamNodes_.count(vertexID) != 0) {
    // 1.0: throw new RuntimeException("Duplicate vertexID " + vertexID)
    throw std::runtime_error("Duplicate vertexID " + std::to_string(vertexID));
  }

  StreamNodePtr vertex(new StreamNode(
      vertexID, slotSharingGroup, operatorObject, operatorName,
      std::vector<OutputSelectorPtr>(), vertexClassName, defaultParallelism_,
      defaultBufferTimeout_));

  streamNodes_[vertexID] = vertex;
  return vertex.get();
}

// ------------------------------ 虚拟节点 ------------------------------

// 1.0 addVirtualSelectNode(...)（:283）
void StreamGraph::addVirtualSelectNode(
    int originalId, int virtualId,
    const std::vector<std::string>& selectedNames) {
  if (virtualSelectNodes_.count(virtualId) != 0) {
    throw std::logic_error("Already has virtual select node with id " +
                           std::to_string(virtualId));
  }
  virtualSelectNodes_[virtualId] =
      flink::api::java::tuple::Tuple2<int, std::vector<std::string> >(
          originalId, selectedNames);
}

// 1.0 addVirtualPartitionNode(...)（:304）
void StreamGraph::addVirtualPartitionNode(int originalId, int virtualId,
                                          const PartitionerPtr& partitioner) {
  if (virtualPartitionNodes_.count(virtualId) != 0) {
    throw std::logic_error("Already has virtual partition node with id " +
                           std::to_string(virtualId));
  }
  virtualPartitionNodes_[virtualId] =
      flink::api::java::tuple::Tuple2<int, PartitionerPtr>(originalId,
                                                           partitioner);
}

// 1.0 getSlotSharingGroup(id)（:317）
std::string StreamGraph::getSlotSharingGroup(int id) const {
  auto selectIt = virtualSelectNodes_.find(id);
  if (selectIt != virtualSelectNodes_.end()) {
    return getSlotSharingGroup(selectIt->second.f0);
  }
  auto partitionIt = virtualPartitionNodes_.find(id);
  if (partitionIt != virtualPartitionNodes_.end()) {
    return getSlotSharingGroup(partitionIt->second.f0);
  }
  // 1.0 这里直接 node.getSlotSharingGroup()，node 为 null 就 NPE
  StreamNode* node = getStreamNode(id);
  if (node == NULL) {
    throw std::logic_error("No such vertex in stream graph: " +
                           std::to_string(id));
  }
  return node->getSlotSharingGroup();
}

// -------------------------------- 建边 --------------------------------

// 1.0 addEdge(up, down, typeNumber)（:330）
void StreamGraph::addEdge(int upStreamVertexID, int downStreamVertexID,
                          int typeNumber) {
  addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber,
                  PartitionerPtr(), std::vector<std::string>());
}

// 1.0 private addEdgeInternal(...)（:339）
void StreamGraph::addEdgeInternal(int upStreamVertexID, int downStreamVertexID,
                                  int typeNumber, PartitionerPtr partitioner,
                                  std::vector<std::string> outputNames) {
  auto selectIt = virtualSelectNodes_.find(upStreamVertexID);
  if (selectIt != virtualSelectNodes_.end()) {
    const int virtualId = upStreamVertexID;
    upStreamVertexID = virtualSelectNodes_[virtualId].f0;
    if (outputNames.empty()) {
      // 1.0:350 "selections that happen downstream override earlier
      //          selections"
      outputNames = virtualSelectNodes_[virtualId].f1;
    }
    addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber,
                    partitioner, outputNames);
    return;
  }

  auto partitionIt = virtualPartitionNodes_.find(upStreamVertexID);
  if (partitionIt != virtualPartitionNodes_.end()) {
    const int virtualId = upStreamVertexID;
    upStreamVertexID = virtualPartitionNodes_[virtualId].f0;
    if (!partitioner) {
      partitioner = virtualPartitionNodes_[virtualId].f1;
    }
    addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber,
                    partitioner, outputNames);
    return;
  }

  StreamNode* upstreamNode = getStreamNode(upStreamVertexID);
  StreamNode* downstreamNode = getStreamNode(downStreamVertexID);
  // 1.0 这里 null 会在下一行 getParallelism() 上 NPE
  if (upstreamNode == NULL || downstreamNode == NULL) {
    throw std::logic_error("No such edge in stream graph: " +
                           std::to_string(upStreamVertexID) + " -> " +
                           std::to_string(downStreamVertexID));
  }

  // 1.0:365-371 "If no partitioner was specified and the parallelism of
  // upstream and downstream operator matches use forward partitioning, use
  // rebalance otherwise."
  if (!partitioner &&
      upstreamNode->getParallelism() == downstreamNode->getParallelism()) {
    partitioner.reset(new ForwardPartitioner());
  } else if (!partitioner) {
    partitioner.reset(new RebalancePartitioner());
  }

  // 1.0:373-380
  if (dynamic_cast<ForwardPartitioner*>(partitioner.get()) != NULL) {
    if (upstreamNode->getParallelism() != downstreamNode->getParallelism()) {
      throw std::logic_error(
          "Forward partitioning does not allow change of parallelism. "
          "Upstream operation: " + upstreamNode->toString() +
          " parallelism: " + std::to_string(upstreamNode->getParallelism()) +
          ", downstream operation: " + downstreamNode->toString() +
          " parallelism: " + std::to_string(downstreamNode->getParallelism()) +
          " You must use another partitioning strategy, such as broadcast, "
          "rebalance, shuffle or global.");
    }
  }

  StreamEdgePtr edge(new StreamEdge(upstreamNode, downstreamNode, typeNumber,
                                    outputNames, partitioner));

  getStreamNode(edge->getSourceId())->addOutEdge(edge);
  getStreamNode(edge->getTargetId())->addInEdge(edge);
}

// ------------------------------ 顶点属性 ------------------------------

// 1.0 addOutputSelector(...)（:389）：先 partition 再 select（1.0 的顺序）
void StreamGraph::addOutputSelector(int vertexID,
                                    const OutputSelectorPtr& outputSelector) {
  auto partitionIt = virtualPartitionNodes_.find(vertexID);
  if (partitionIt != virtualPartitionNodes_.end()) {
    addOutputSelector(partitionIt->second.f0, outputSelector);
    return;
  }
  auto selectIt = virtualSelectNodes_.find(vertexID);
  if (selectIt != virtualSelectNodes_.end()) {
    addOutputSelector(selectIt->second.f0, outputSelector);
    return;
  }
  getStreamNode(vertexID)->addOutputSelector(outputSelector);
}

// 1.0 setParallelism(vertexID, parallelism)（:404）：顶点不存在时静默返回
void StreamGraph::setParallelism(int vertexID, int parallelism) {
  StreamNode* node = getStreamNode(vertexID);
  if (node != NULL) {
    node->setParallelism(parallelism);
  }
}

// 1.0 setBufferTimeout(vertexID, bufferTimeout)（:423）
void StreamGraph::setBufferTimeout(int vertexID, int64_t bufferTimeout) {
  StreamNode* node = getStreamNode(vertexID);
  if (node != NULL) {
    node->setBufferTimeout(bufferTimeout);
  }
}

// 1.0 setOneInputStateKey(...)（:410）
void StreamGraph::setOneInputStateKey(int vertexID,
                                      const KeySelectorPtr& keySelector,
                                      const SerializerPtr& keySerializer) {
  StreamNode* node = getStreamNode(vertexID);
  node->setStatePartitioner1(keySelector);
  node->setStateKeySerializer(keySerializer);
}

// 1.0 setTwoInputStateKey(...)（:416）
void StreamGraph::setTwoInputStateKey(int vertexID,
                                      const KeySelectorPtr& keySelector1,
                                      const KeySelectorPtr& keySelector2,
                                      const SerializerPtr& keySerializer) {
  StreamNode* node = getStreamNode(vertexID);
  node->setStatePartitioner1(keySelector1);
  node->setStatePartitioner2(keySelector2);
  node->setStateKeySerializer(keySerializer);
}

// 1.0 setSerializers(vertexID, in1, in2, out)（:429）
void StreamGraph::setSerializers(int vertexID, const SerializerPtr& in1,
                                 const SerializerPtr& in2,
                                 const SerializerPtr& out) {
  StreamNode* vertex = getStreamNode(vertexID);
  vertex->setSerializerIn1(in1);
  vertex->setSerializerIn2(in2);
  vertex->setSerializerOut(out);
}

// 1.0 setSerializersFrom(from, to)（:436）
void StreamGraph::setSerializersFrom(int from, int to) {
  StreamNode* fromVertex = getStreamNode(from);
  StreamNode* toVertex = getStreamNode(to);

  toVertex->setSerializerIn1(fromVertex->getTypeSerializerOut());
  // 1.0 这一行取的确实是 **In1**（不是 Out）——为 iteration 的 head/tail 对
  // 准备的「反向」接法，原样照搬
  toVertex->setSerializerOut(fromVertex->getTypeSerializerIn1());
}

// 1.0 setOutType(vertexID, TypeInformation<OUT>)（:444）—— 偏差 2
void StreamGraph::setOutType(int vertexID,
                             const SerializerPtr& outSerializer) {
  getStreamNode(vertexID)->setSerializerOut(outSerializer);
}

// 1.0 setOperator(vertexID, operatorObject)（:448）
void StreamGraph::setOperator(int vertexID,
                              const StreamOperatorPtr& operatorObject) {
  getStreamNode(vertexID)->setOperator(operatorObject);
}

// 1.0 setTransformationId(nodeId, transformationId)（:456）
void StreamGraph::setTransformationId(int nodeId,
                                      const std::string& transformationId) {
  StreamNode* node = getStreamNode(nodeId);
  if (node != NULL) {
    node->setTransformationId(transformationId);
  }
}

// -------------------------------- 查询 --------------------------------

// 1.0 getStreamNode(vertexID)：streamNodes.get(id)，不存在返回 null
StreamNode* StreamGraph::getStreamNode(int vertexID) const {
  auto it = streamNodes_.find(vertexID);
  return it == streamNodes_.end() ? NULL : it->second.get();
}

// 1.0 protected getVertexIDs()：streamNodes.keySet()
std::vector<int> StreamGraph::getVertexIDs() const {
  std::vector<int> ids;
  ids.reserve(streamNodes_.size());
  for (const auto& entry : streamNodes_) {
    ids.push_back(entry.first);
  }
  return ids;
}

// 1.0 getStreamEdges(sourceId, targetId)（:471）
std::vector<StreamGraph::StreamEdgePtr> StreamGraph::getStreamEdges(
    int sourceId, int targetId) const {
  std::vector<StreamEdgePtr> result;

  StreamNode* source = getStreamNode(sourceId);
  if (source != NULL) {
    for (const StreamEdgePtr& edge : source->getOutEdges()) {
      if (edge->getTargetId() == targetId) {
        result.push_back(edge);
      }
    }
  }

  if (result.empty()) {
    // 1.0: throw new RuntimeException("No such edge in stream graph: " + ...)
    throw std::runtime_error("No such edge in stream graph: " +
                             std::to_string(sourceId) + " -> " +
                             std::to_string(targetId));
  }
  return result;
}

// 1.0 getStreamNodes()：streamNodes.values()
std::vector<StreamNode*> StreamGraph::getStreamNodes() const {
  std::vector<StreamNode*> nodes;
  nodes.reserve(streamNodes_.size());
  for (const auto& entry : streamNodes_) {
    nodes.push_back(entry.second.get());
  }
  return nodes;
}

// 1.0 getOperators()（:500）
std::vector<flink::api::java::tuple::Tuple2<int, StreamGraph::StreamOperatorPtr> >
StreamGraph::getOperators() const {
  std::vector<flink::api::java::tuple::Tuple2<int, StreamOperatorPtr> >
      operatorSet;
  operatorSet.reserve(streamNodes_.size());
  for (const auto& entry : streamNodes_) {
    operatorSet.push_back(
        flink::api::java::tuple::Tuple2<int, StreamOperatorPtr>(
            entry.second->getId(), entry.second->getOperator()));
  }
  return operatorSet;
}

// ------------------------------- iteration -------------------------------

// 1.0 createIterationSourceAndSink(...)（:517）
flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*>
StreamGraph::createIterationSourceAndSink(int loopId, int sourceId, int sinkId,
                                          int64_t timeout, int parallelism) {
  // 1.0 这里给 addNode 传的 operator 是 null，所以 iteration 顶点无算子
  StreamNode* source =
      addNode(sourceId, "", STREAM_ITERATION_HEAD, StreamOperatorPtr(),
              "IterationSource-" + std::to_string(loopId));
  sources_.insert(source->getId());
  setParallelism(source->getId(), parallelism);

  StreamNode* sink =
      addNode(sinkId, "", STREAM_ITERATION_TAIL, StreamOperatorPtr(),
              "IterationSink-" + std::to_string(loopId));
  sinks_.insert(sink->getId());
  setParallelism(sink->getId(), parallelism);

  iterationSourceSinkPairs_.push_back(
      flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*>(source, sink));

  vertexIDtoBrokerID_[source->getId()] = "broker-" + std::to_string(loopId);
  vertexIDtoBrokerID_[sink->getId()] = "broker-" + std::to_string(loopId);
  vertexIDtoLoopTimeout_[source->getId()] = timeout;
  vertexIDtoLoopTimeout_[sink->getId()] = timeout;

  return flink::api::java::tuple::Tuple2<StreamNode*, StreamNode*>(source,
                                                                   sink);
}

// 1.0 getBrokerID(vertexID)：map.get 不存在返回 null
std::string StreamGraph::getBrokerID(int vertexID) const {
  auto it = vertexIDtoBrokerID_.find(vertexID);
  return it == vertexIDtoBrokerID_.end() ? std::string() : it->second;
}

// 1.0 getLoopTimeout(vertexID)：map.get(...) 自动拆箱，不存在则 NPE
int64_t StreamGraph::getLoopTimeout(int vertexID) const {
  auto it = vertexIDtoLoopTimeout_.find(vertexID);
  if (it == vertexIDtoLoopTimeout_.end()) {
    throw std::logic_error("No loop timeout for vertex " +
                           std::to_string(vertexID));
  }
  return it->second;
}

// 【偏差 4】1.0 getJobGraph()（:569）开头的校验
void StreamGraph::checkJobGraphPreconditions() const {
  // 1.0: "temporarily forbid checkpointing for iterative jobs"
  if (isIterative() && checkpointConfig_.isCheckpointingEnabled() &&
      !checkpointConfig_.isForceCheckpointing()) {
    throw std::logic_error(
        "Checkpointing is currently not supported by default for iterative "
        "jobs, as we cannot guarantee exactly once semantics. State "
        "checkpoints happen normally, but records in-transit during the "
        "snapshot will be lost upon failure. \nThe user can force enable "
        "state checkpoints with the reduced guarantees by calling: "
        "env.enableCheckpointing(interval,true)");
  }
}

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
