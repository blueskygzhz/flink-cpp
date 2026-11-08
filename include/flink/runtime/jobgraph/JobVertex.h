/*
 * flink-runtime : org.apache.flink.runtime.jobgraph.JobVertex
 *                 org.apache.flink.runtime.jobgraph.IntermediateDataSet
 *                 org.apache.flink.runtime.jobgraph.JobEdge
 *                 org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
 *                 org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup
 *
 * 【偏差 0 —— 五个类合一个头文件】1.0 是五个独立文件，但 JobVertex ↔
 * IntermediateDataSet ↔ JobEdge 三者互相引用（Java 不在乎声明顺序，C++ 在乎），
 * 拆开要靠一串前置声明 + 全部方法外移到 .cpp，反而更难对照。
 * SlotSharingGroup / CoLocationGroup 只被 JobVertex 的两个 setter 用到，
 * 一并放进来。
 *
 * ━━━━━━━━━━━━━━━━━━ JobVertex（JobVertex.java:40）━━━━━━━━━━━━━━━━━━
 *
 * 1.0 类注释：「The base class for job vertexes.」
 *
 * 字段（:45-100）：
 *   public static final int MAX_PARALLELISM_DEFAULT = -1;
 *   private final JobVertexID id;                「The ID of the vertex.」
 *   private final ArrayList<IntermediateDataSet> results;
 *                                「List of produced data sets, one per writer」
 *   private final ArrayList<JobEdge> inputs;
 *                                「List of edges with incoming data.」
 *   private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
 *                                「Number of subtasks to split this task into
 *                                  at runtime.」
 *   private Configuration configuration;         「Custom configuration」
 *   private String invokableClassName;
 *                「The class of the invokable.」
 *   private boolean isStoppable = false;
 *   private String name;                         「Optional name」
 *   private SlotSharingGroup slotSharingGroup;
 *   private CoLocationGroup coLocationGroup;
 *   private String operatorName;  private String operatorDescription;
 *   private String operatorPrettyName;  private String resultOptimizerProperties;
 *
 * 关键方法：
 *   setInvokableClass(Class)  —— 存 invokable.getName()，并顺手算
 *                                isStoppable = StoppableTask.isAssignableFrom
 *   setParallelism(int)       —— < 1 则
 *                                IllegalArgumentException("The parallelism
 *                                must be at least one.")
 *   createAndAddResultDataSet(partitionType)
 *   connectNewDataSetAsInput(input, distPattern, partitionType,
 *                            eagerlyDeployConsumers)：
 *       IntermediateDataSet dataSet = input.createAndAddResultDataSet(type);
 *       dataSet.setEagerlyDeployConsumers(eagerlyDeployConsumers);
 *       JobEdge edge = new JobEdge(dataSet, this, distPattern);
 *       this.inputs.add(edge);  dataSet.addConsumer(edge);  return edge;
 *       —— 注意**数据集挂在上游**（input.createAndAdd...），边挂在下游的
 *       inputs 上，这正是 StreamingJobGraphGenerator.connect 的接法。
 *   isInputVertex()  = inputs.isEmpty()    「Checks if this vertex is a
 *                                            graph source.」
 *   isOutputVertex() = results.isEmpty()
 *   hasNoConnectedInputs()：所有入边都是「id 引用」（即还没接上真实数据集）
 *       才算无输入 —— JobGraph 的拓扑排序用它找起点
 *   setSlotSharingGroup(grp)：先从旧组里摘掉自己，再加入新组
 *   updateCoLocationGroup(group)
 *
 * ━━━━━━━━━━ IntermediateDataSet（IntermediateDataSet.java:32）━━━━━━━━━━
 *
 * 1.0 类注释：「An intermediate data set is the data set produced by an
 * operator - either a source or any intermediate operation. Intermediate data
 * sets may be read by other operators, materialized, or discarded.」
 *   private final IntermediateDataSetID id;
 *   private final JobVertex producer;          「the operation that produced
 *                                                this data set」
 *   private final List<JobEdge> consumers;
 *   private final ResultPartitionType resultType;
 *   private boolean eagerlyDeployConsumers;
 *   addConsumer(JobEdge edge) —— 1.0 这里不做任何校验
 *
 * ━━━━━━━━━━━━━━━━━━━ JobEdge（JobEdge.java:30）━━━━━━━━━━━━━━━━━━━
 *
 * 1.0 类注释：「This class represent edges (communication channels) in a job
 * graph. The edges always go from an intermediate result partition to a job
 * vertex. An edge is parametrized with its IntermediateResultPartitionType
 * and DistributionPattern.」
 *   private final JobVertex target;
 *   private final DistributionPattern distributionPattern;
 *   private IntermediateDataSet source;
 *   private IntermediateDataSetID sourceId;
 *   private String shipStrategyName; / preProcessingOperationName; / operatorLevelCachingDescription;
 *   两个构造：带 IntermediateDataSet 的（source 直连）与只带
 *   IntermediateDataSetID 的（「id 引用」，留待后续解析）
 *   isIdReference() = (source == null)
 *
 * ━━━━━━ SlotSharingGroup / CoLocationGroup（jobmanager.scheduler）━━━━━━
 *
 * 1.0 SlotSharingGroup：「A slot sharing units defines which different task
 * (from different job vertices) can be deployed together within a slot.」
 *   private final Set<JobVertexID> ids;  private final AbstractID slotSharingGroupId;
 *   addVertexToGroup / removeVertexFromGroup / getJobVertexIds
 * 1.0 CoLocationGroup：addVertex / mergeInto / getLocationConstraint
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— Configuration 换成 StreamConfig】见 StreamConfig.h 的同名偏差：
 * 1.0 的 JobVertex.configuration 是一个 key→value 的 Configuration，
 * StreamConfig 只是它的带类型视图；本仓库的 StreamConfig 是独立结构体，
 * 所以 JobVertex 直接持一个 StreamConfig。于是 1.0 的
 * `new StreamConfig(jobVertex.getConfiguration())` 退化为取引用。
 *
 * 【偏差 2 —— invokableClassName 用字符串】1.0 存
 * Class<? extends AbstractInvokable>；这里沿用 StreamNode 的字符串方案
 * （见 StreamNode.h 偏差 2），于是 setInvokableClass 收字符串，
 * isStoppable 的 isAssignableFrom 判定无从实施（StoppableTask 整条线未移植），
 * 该字段省掉。
 *
 * 【偏差 3 —— IntermediateDataSetID 省掉】1.0 每个数据集带一个随机 16 字节
 * id，仅用于「JobEdge 先按 id 引用、后续再解析成真实数据集」这条**批处理**
 * 路径（JobGraph.setInputs）。流式作业一律走 connectNewDataSetAsInput 直连，
 * 所以这里 IntermediateDataSet 不带 id，JobEdge 也只有直连构造，
 * isIdReference() 恒为 false（但方法保留，因为 hasNoConnectedInputs 要用）。
 *
 * 【偏差 4 —— 元信息字段省掉】operatorName / operatorDescription /
 * operatorPrettyName / resultOptimizerProperties / shipStrategyName /
 * preProcessingOperationName / operatorLevelCachingDescription 全部只服务于
 * Web UI 的执行计划展示，不影响调度与运行时语义。
 *
 * 【偏差 5 —— CoLocationGroup 只留 addVertex】mergeInto /
 * getLocationConstraint / CoLocationConstraint 属于调度器运行期，
 * StreamingJobGraphGenerator 只调 addVertex。
 *
 * 【偏差 6 —— maxParallelism / inputSplitSource 省掉】1.0 的
 * MAX_PARALLELISM_DEFAULT 与 setInputSplitSource 分别服务于 rescaling 与
 * InputFormat 批处理源，都不在本次移植范围内。
 */
#ifndef FLINK_RUNTIME_JOBGRAPH_JOBVERTEX_H
#define FLINK_RUNTIME_JOBGRAPH_JOBVERTEX_H

#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "flink/runtime/io/network/partition/ResultPartitionType.h"
#include "flink/runtime/jobgraph/JobVertexID.h"
#include "flink/streaming/api/graph/StreamConfig.h"

namespace flink {
namespace runtime {
namespace jobgraph {

class JobVertex;
class IntermediateDataSet;

// 1.0 org.apache.flink.runtime.jobgraph.JobEdge
class JobEdge {
 public:
  JobEdge(IntermediateDataSet* source, JobVertex* target,
          DistributionPattern distributionPattern)
      : source_(source), target_(target),
        distributionPattern_(distributionPattern) {}

  IntermediateDataSet* getSource() const { return source_; }
  JobVertex* getTarget() const { return target_; }
  DistributionPattern getDistributionPattern() const {
    return distributionPattern_;
  }
  // 1.0 isIdReference() = (source == null)。【偏差 3】流式路径恒为 false
  bool isIdReference() const { return source_ == NULL; }

 private:
  IntermediateDataSet* source_;
  JobVertex* target_;
  DistributionPattern distributionPattern_;
};

// 1.0 org.apache.flink.runtime.jobgraph.IntermediateDataSet
class IntermediateDataSet {
 public:
  typedef flink::runtime::io::network::partition::ResultPartitionType
      ResultPartitionType;

  IntermediateDataSet(JobVertex* producer, ResultPartitionType resultType)
      : producer_(producer), resultType_(resultType),
        eagerlyDeployConsumers_(false) {}

  // 1.0: "the operation that produced this data set"
  JobVertex* getProducer() const { return producer_; }
  ResultPartitionType getResultType() const { return resultType_; }

  const std::vector<JobEdge*>& getConsumers() const { return consumers_; }
  // 1.0 addConsumer(JobEdge)：不做任何校验
  void addConsumer(JobEdge* edge) { consumers_.push_back(edge); }

  bool getEagerlyDeployConsumers() const { return eagerlyDeployConsumers_; }
  void setEagerlyDeployConsumers(bool eagerlyDeployConsumers) {
    eagerlyDeployConsumers_ = eagerlyDeployConsumers;
  }

 private:
  JobVertex* producer_;
  std::vector<JobEdge*> consumers_;
  ResultPartitionType resultType_;
  bool eagerlyDeployConsumers_;
};

// 1.0 org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
class SlotSharingGroup {
 public:
  void addVertexToGroup(const JobVertexID& id) { ids_.insert(id); }
  void removeVertexFromGroup(const JobVertexID& id) { ids_.erase(id); }
  const std::set<JobVertexID>& getJobVertexIds() const { return ids_; }
  const flink::util::AbstractID& getSlotSharingGroupId() const {
    return slotSharingGroupId_;
  }

 private:
  std::set<JobVertexID> ids_;
  flink::util::AbstractID slotSharingGroupId_;
};

// 1.0 org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup
// 【偏差 5】只留 addVertex
class CoLocationGroup {
 public:
  void addVertex(JobVertex* vertex) {
    if (vertex == NULL) {
      throw std::invalid_argument("vertex must not be null");
    }
    vertices_.push_back(vertex);
  }
  const std::vector<JobVertex*>& getVertices() const { return vertices_; }
  const flink::util::AbstractID& getId() const { return id_; }

 private:
  flink::util::AbstractID id_;
  std::vector<JobVertex*> vertices_;
};

// 1.0 org.apache.flink.runtime.jobgraph.JobVertex
class JobVertex {
 public:
  typedef flink::runtime::io::network::partition::ResultPartitionType
      ResultPartitionType;
  typedef flink::streaming::api::graph::StreamConfig StreamConfig;

  // 1.0 的 ExecutionConfig.PARALLELISM_DEFAULT
  static const int PARALLELISM_DEFAULT = -1;

  // 1.0 JobVertex(String name)
  explicit JobVertex(const std::string& name)
      : id_(), parallelism_(PARALLELISM_DEFAULT),
        name_(name.empty() ? "(unnamed)" : name), slotSharingGroup_(NULL),
        coLocationGroup_(NULL) {}

  // 1.0 JobVertex(String name, JobVertexID id)
  JobVertex(const std::string& name, const JobVertexID& id)
      : id_(id), parallelism_(PARALLELISM_DEFAULT),
        name_(name.empty() ? "(unnamed)" : name), slotSharingGroup_(NULL),
        coLocationGroup_(NULL) {}

  const JobVertexID& getID() const { return id_; }
  const std::string& getName() const { return name_; }
  void setName(const std::string& name) { name_ = name; }

  // 1.0 setInvokableClass(Class)。【偏差 2】收字符串
  void setInvokableClass(const std::string& invokableClassName) {
    if (invokableClassName.empty()) {
      throw std::invalid_argument("invokable must not be null");
    }
    invokableClassName_ = invokableClassName;
  }
  const std::string& getInvokableClassName() const {
    return invokableClassName_;
  }

  int getParallelism() const { return parallelism_; }
  // 1.0 setParallelism(int)
  void setParallelism(int parallelism) {
    if (parallelism < 1) {
      throw std::invalid_argument("The parallelism must be at least one.");
    }
    parallelism_ = parallelism;
  }

  // 【偏差 1】1.0 返回 Configuration，调用方再包一层 StreamConfig
  StreamConfig& getConfiguration() { return configuration_; }
  const StreamConfig& getConfiguration() const { return configuration_; }

  int getNumberOfProducedIntermediateDataSets() const {
    return static_cast<int>(results_.size());
  }
  int getNumberOfInputs() const { return static_cast<int>(inputs_.size()); }

  // 1.0 getProducedDataSets() / getInputs()
  std::vector<IntermediateDataSet*> getProducedDataSets() const {
    std::vector<IntermediateDataSet*> result;
    result.reserve(results_.size());
    for (const auto& dataSet : results_) result.push_back(dataSet.get());
    return result;
  }
  std::vector<JobEdge*> getInputs() const {
    std::vector<JobEdge*> result;
    result.reserve(inputs_.size());
    for (const auto& edge : inputs_) result.push_back(edge.get());
    return result;
  }

  // 1.0 createAndAddResultDataSet(ResultPartitionType)
  IntermediateDataSet* createAndAddResultDataSet(
      ResultPartitionType partitionType) {
    std::shared_ptr<IntermediateDataSet> dataSet(
        new IntermediateDataSet(this, partitionType));
    results_.push_back(dataSet);
    return dataSet.get();
  }

  // 1.0 connectNewDataSetAsInput(input, distPattern, partitionType,
  //                              eagerlyDeployConsumers)
  JobEdge* connectNewDataSetAsInput(JobVertex* input,
                                    DistributionPattern distPattern,
                                    ResultPartitionType partitionType,
                                    bool eagerlyDeployConsumers) {
    IntermediateDataSet* dataSet =
        input->createAndAddResultDataSet(partitionType);
    dataSet->setEagerlyDeployConsumers(eagerlyDeployConsumers);

    std::shared_ptr<JobEdge> edge(new JobEdge(dataSet, this, distPattern));
    inputs_.push_back(edge);
    dataSet->addConsumer(edge.get());
    return edge.get();
  }

  // 1.0 isInputVertex()：「Checks if this vertex is a graph source.」
  bool isInputVertex() const { return inputs_.empty(); }
  // 1.0 isOutputVertex()
  bool isOutputVertex() const { return results_.empty(); }

  // 1.0 hasNoConnectedInputs()：JobGraph 的拓扑排序用它找起点
  bool hasNoConnectedInputs() const {
    for (const auto& edge : inputs_) {
      if (!edge->isIdReference()) return false;
    }
    return true;
  }

  // 1.0 setSlotSharingGroup(grp)：先从旧组摘掉自己，再加入新组
  void setSlotSharingGroup(SlotSharingGroup* grp) {
    if (slotSharingGroup_ != NULL) {
      slotSharingGroup_->removeVertexFromGroup(id_);
    }
    slotSharingGroup_ = grp;
    if (grp != NULL) {
      grp->addVertexToGroup(id_);
    }
  }
  SlotSharingGroup* getSlotSharingGroup() const { return slotSharingGroup_; }

  // 1.0 updateCoLocationGroup(group)
  void updateCoLocationGroup(CoLocationGroup* group) {
    coLocationGroup_ = group;
  }
  CoLocationGroup* getCoLocationGroup() const { return coLocationGroup_; }

  std::string toString() const {
    return name_ + " (" + invokableClassName_ + ")";
  }

 private:
  JobVertexID id_;
  // 1.0: "List of produced data sets, one per writer"
  std::vector<std::shared_ptr<IntermediateDataSet> > results_;
  // 1.0: "List of edges with incoming data."
  std::vector<std::shared_ptr<JobEdge> > inputs_;
  // 1.0: "Number of subtasks to split this task into at runtime."
  int parallelism_;
  StreamConfig configuration_;
  std::string invokableClassName_;
  std::string name_;
  SlotSharingGroup* slotSharingGroup_;
  CoLocationGroup* coLocationGroup_;
};

}  // namespace jobgraph
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_JOBGRAPH_JOBVERTEX_H
