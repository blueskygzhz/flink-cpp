#include "flink/runtime/jobgraph/JobGraph.h"

#include <algorithm>

namespace flink {
namespace runtime {
namespace jobgraph {

namespace {
// 1.0 用 `remaining.contains(v)` / `remaining.remove(v)`（LinkedHashSet）；
// 顶点数很小，这里直接线性查找 vector，顺序语义与 LinkedHashSet 一致
bool contains(const std::vector<JobVertex*>& vertices, JobVertex* vertex) {
  return std::find(vertices.begin(), vertices.end(), vertex) != vertices.end();
}

void remove(std::vector<JobVertex*>& vertices, JobVertex* vertex) {
  vertices.erase(std::remove(vertices.begin(), vertices.end(), vertex),
                 vertices.end());
}
}  // namespace

// 1.0 getVerticesSortedTopologicallyFromSources()（JobGraph.java:360）
std::vector<JobVertex*> JobGraph::getVerticesSortedTopologicallyFromSources()
    const {
  // 1.0: "early out on empty lists"
  if (taskVertices_.empty()) {
    return std::vector<JobVertex*>();
  }

  std::vector<JobVertex*> sorted;
  sorted.reserve(taskVertices_.size());
  std::vector<JobVertex*> remaining = getVertices();

  // 1.0: "start by finding the vertices with no input edges and the ones with
  //       disconnected inputs (that refer to some standalone data set)"
  {
    std::vector<JobVertex*> stillRemaining;
    for (JobVertex* vertex : remaining) {
      if (vertex->hasNoConnectedInputs()) {
        sorted.push_back(vertex);
      } else {
        stillRemaining.push_back(vertex);
      }
    }
    remaining.swap(stillRemaining);
  }

  size_t startNodePos = 0;

  // 1.0: "traverse from the nodes that were added until we found all elements"
  while (!remaining.empty()) {
    // 1.0: "first check if we have more candidates to start traversing from.
    //       if not, then the graph is cyclic, which is not permitted"
    if (startNodePos >= sorted.size()) {
      throw std::logic_error("The job graph is cyclic.");
    }

    JobVertex* current = sorted[startNodePos++];
    addNodesThatHaveNoNewPredecessors(current, sorted, remaining);
  }

  return sorted;
}

// 1.0 private addNodesThatHaveNoNewPredecessors(...)（:396）
void JobGraph::addNodesThatHaveNoNewPredecessors(
    JobVertex* start, std::vector<JobVertex*>& target,
    std::vector<JobVertex*>& remaining) {
  for (IntermediateDataSet* dataSet : start->getProducedDataSets()) {
    for (JobEdge* edge : dataSet->getConsumers()) {
      JobVertex* v = edge->getTarget();
      if (!contains(remaining, v)) {
        continue;
      }

      bool hasNewPredecessors = false;
      for (JobEdge* e : v->getInputs()) {
        if (e == edge) {
          continue;
        }
        IntermediateDataSet* source = e->getSource();
        if (contains(remaining, source->getProducer())) {
          hasNewPredecessors = true;
          break;
        }
      }

      if (!hasNewPredecessors) {
        target.push_back(v);
        remove(remaining, v);
        addNodesThatHaveNoNewPredecessors(v, target, remaining);
      }
    }
  }
}

}  // namespace jobgraph
}  // namespace runtime
}  // namespace flink
