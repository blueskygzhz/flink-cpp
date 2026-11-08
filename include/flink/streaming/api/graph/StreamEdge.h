/*
 * flink-streaming-java : org.apache.flink.streaming.api.graph.StreamEdge
 *
 * 1.0 class StreamEdge implements Serializable（StreamEdge.java:34）
 *
 * 1.0 原注释：「An edge in the streaming topology. One edge like this does not
 * necessarily gets converted to a connection between two job vertices (due to
 * chaining/optimization).」——这句是算子链的关键：被链进同一个 task 的边不会
 * 变成网络连接，而是变成 OperatorChain 里的一个 ChainingOutput。
 *
 * 字段（:38-56）：
 *   final private String edgeId;
 *   final private StreamNode sourceVertex;
 *   final private StreamNode targetVertex;
 *   final private int typeNumber;        // co-task 的输入序号（1 或 2）
 *   private final List<String> selectedNames;   // split/select 的输出名
 *   private StreamPartitioner<?> outputPartitioner;
 *
 * 构造（:58-69）：
 *   edgeId = sourceVertex + "_" + targetVertex + "_" + typeNumber + "_"
 *            + selectedNames + "_" + outputPartitioner;
 *   （StreamNode.toString() 是 operatorName + "-" + id，
 *     List.toString() 是 "[a, b]"，partitioner.toString() 是 "HASH" 之类）
 *
 * 方法：getSourceVertex / getTargetVertex / getSourceId / getTargetId /
 *       getTypeNumber / getSelectedNames / getPartitioner / setPartitioner /
 *       hashCode()=edgeId.hashCode() / equals() 只比 edgeId / toString()
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 顶点用裸指针】1.0 的 sourceVertex/targetVertex 是 StreamNode
 * 引用，顶点由 StreamGraph 的 streamNodes map 持有，边只是引用。这里同样
 * 「不拥有」：StreamGraph 持 shared_ptr<StreamNode>，边持 StreamNode*。
 * 边的生命周期短于图，故安全。
 *
 * 【偏差 2 —— edgeId 的字符串化】1.0 直接拼 Java 的 toString()。这里逐项复刻：
 * 顶点用 StreamNode::toString()、名字列表用 Java List 的 "[a, b]" 形式、
 * partitioner 用 StreamPartitioner::toString()。这样 edgeId（以及
 * equals/hashCode）与 1.0 一致——OperatorChain 用 StreamEdge 当 map 的 key，
 * 这点必须对。
 */
#ifndef FLINK_STREAMING_API_GRAPH_STREAMEDGE_H
#define FLINK_STREAMING_API_GRAPH_STREAMEDGE_H

#include <memory>
#include <string>
#include <vector>

#include "flink/streaming/api/graph/StreamNode.h"
#include "flink/streaming/runtime/partitioner/StreamPartitioner.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

class StreamEdge {
 public:
  typedef std::shared_ptr<flink::streaming::runtime::partitioner::
                              StreamPartitioner> PartitionerPtr;

  // 1.0 StreamEdge(StreamNode source, StreamNode target, int typeNumber,
  //                List<String> selectedNames, StreamPartitioner<?>)
  StreamEdge(StreamNode* sourceVertex, StreamNode* targetVertex,
             int typeNumber, const std::vector<std::string>& selectedNames,
             const PartitionerPtr& outputPartitioner)
      : sourceVertex_(sourceVertex),
        targetVertex_(targetVertex),
        typeNumber_(typeNumber),
        selectedNames_(selectedNames),
        outputPartitioner_(outputPartitioner) {
    // 1.0: edgeId = source + "_" + target + "_" + typeNumber + "_"
    //               + selectedNames + "_" + outputPartitioner
    edgeId_ = sourceVertex_->toString() + "_" + targetVertex_->toString() +
              "_" + std::to_string(typeNumber_) + "_" +
              namesToString(selectedNames_) + "_" + partitionerToString();
  }

  StreamNode* getSourceVertex() const { return sourceVertex_; }
  StreamNode* getTargetVertex() const { return targetVertex_; }

  int getSourceId() const { return sourceVertex_->getId(); }
  int getTargetId() const { return targetVertex_->getId(); }

  int getTypeNumber() const { return typeNumber_; }

  const std::vector<std::string>& getSelectedNames() const {
    return selectedNames_;
  }

  const PartitionerPtr& getPartitioner() const { return outputPartitioner_; }
  void setPartitioner(const PartitionerPtr& partitioner) {
    outputPartitioner_ = partitioner;
  }

  const std::string& getEdgeId() const { return edgeId_; }

  // 1.0 equals()/hashCode() 只看 edgeId
  bool operator==(const StreamEdge& other) const {
    return edgeId_ == other.edgeId_;
  }
  bool operator<(const StreamEdge& other) const {
    return edgeId_ < other.edgeId_;
  }

  // 1.0 toString(): "(" + source + " -> " + target + ", typeNumber=" + n
  //                 + ", selectedNames=" + names + ", outputPartitioner=" + p + ')'
  std::string toString() const {
    return "(" + sourceVertex_->toString() + " -> " +
           targetVertex_->toString() + ", typeNumber=" +
           std::to_string(typeNumber_) + ", selectedNames=" +
           namesToString(selectedNames_) + ", outputPartitioner=" +
           partitionerToString() + ")";
  }

 private:
  // 复刻 Java List<String>.toString()：空列表是 "[]"，否则 "[a, b]"
  static std::string namesToString(const std::vector<std::string>& names) {
    std::string result = "[";
    for (size_t i = 0; i < names.size(); ++i) {
      if (i > 0) result += ", ";
      result += names[i];
    }
    return result + "]";
  }

  // 复刻 Java 对 null 的字符串拼接（"null"）
  std::string partitionerToString() const {
    return outputPartitioner_ ? outputPartitioner_->toString() : "null";
  }

  std::string edgeId_;
  StreamNode* sourceVertex_;
  StreamNode* targetVertex_;
  const int typeNumber_;
  const std::vector<std::string> selectedNames_;
  PartitionerPtr outputPartitioner_;
};

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_GRAPH_STREAMEDGE_H
