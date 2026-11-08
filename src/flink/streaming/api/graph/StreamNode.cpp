#include "flink/streaming/api/graph/StreamNode.h"

#include "flink/streaming/api/graph/StreamEdge.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

// 1.0 addInEdge(StreamEdge inEdge)（StreamNode.java:93）
void StreamNode::addInEdge(const StreamEdgePtr& inEdge) {
  if (inEdge->getTargetId() != getId()) {
    throw std::invalid_argument(
        "Destination id doesn't match the StreamNode id");
  }
  inEdges_.push_back(inEdge);
}

// 1.0 addOutEdge(StreamEdge outEdge)（:101）
void StreamNode::addOutEdge(const StreamEdgePtr& outEdge) {
  if (outEdge->getSourceId() != getId()) {
    throw std::invalid_argument("Source id doesn't match the StreamNode id");
  }
  outEdges_.push_back(outEdge);
}

// 1.0 getOutEdgeIndices()（:117）：取各出边的 targetId
std::vector<int> StreamNode::getOutEdgeIndices() const {
  std::vector<int> outEdgeIndices;
  outEdgeIndices.reserve(outEdges_.size());
  for (const StreamEdgePtr& edge : outEdges_) {
    outEdgeIndices.push_back(edge->getTargetId());
  }
  return outEdgeIndices;
}

// 1.0 getInEdgeIndices()（:125）：取各入边的 sourceId
std::vector<int> StreamNode::getInEdgeIndices() const {
  std::vector<int> inEdgeIndices;
  inEdgeIndices.reserve(inEdges_.size());
  for (const StreamEdgePtr& edge : inEdges_) {
    inEdgeIndices.push_back(edge->getSourceId());
  }
  return inEdgeIndices;
}

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
