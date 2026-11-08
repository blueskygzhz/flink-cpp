/*
 * flink-runtime : org.apache.flink.runtime.jobgraph.JobGraph
 *                 org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings
 *
 * ━━━━━━━━━ JobSnapshottingSettings（JobSnapshottingSettings.java:30）━━━━━━━━━
 *
 * 1.0 类注释：「The JobSnapshottingSettings are attached to a JobGraph and
 * describe the settings for the asynchronous snapshots of operator state.」
 *
 *   private final List<JobVertexID> verticesToTrigger;
 *   private final List<JobVertexID> verticesToAcknowledge;
 *   private final List<JobVertexID> verticesToConfirm;
 *   private final long checkpointInterval;
 *   private final long checkpointTimeout;
 *   private final long minPauseBetweenCheckpoints;
 *   private final int  maxConcurrentCheckpoints;
 *   private String savepointPath;
 *
 * 构造里的「sanity checks」（原注释）：
 *   checkpointInterval < 1 || checkpointTimeout < 1 ||
 *   minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1
 *       -> throw new IllegalArgumentException();     // 1.0 不带消息
 *
 * ━━━━━━━━━━━━━━━━━━━ JobGraph（JobGraph.java:51）━━━━━━━━━━━━━━━━━━━
 *
 * 1.0 类注释：「The JobGraph represents a Flink dataflow program, at the low
 * level that the JobManager accepts. All programs from higher level APIs are
 * transformed into JobGraphs. ... The JobGraph is a graph of vertices and
 * intermediate results that are connected together to form a DAG.」
 *
 * 字段（:57-74）：
 *   private final Map<JobVertexID, JobVertex> taskVertices = new LinkedHashMap<>();
 *                                      —— LinkedHashMap：**插入序**很重要
 *   private final Configuration jobConfiguration = new Configuration();
 *   private final List<Path> userJars; private final List<BlobKey> userJarBlobKeys;
 *   private final JobID jobID;  private final String jobName;
 *   private RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration;
 *   private long sessionTimeout = 0;  private boolean allowQueuedScheduling;
 *   private ScheduleMode scheduleMode = ScheduleMode.FROM_SOURCES;
 *   private JobSnapshottingSettings snapshotSettings;
 *   private List<URL> classpaths = Collections.emptyList();
 *
 *   JobGraph(String jobName)：jobName 为 null 则取 "(unnamed job)"
 *
 *   addVertex(JobVertex)（:216）：
 *       id 已存在则**先把旧值放回去**再
 *       throw new IllegalArgumentException("The JobGraph already contains a
 *       vertex with that id.")
 *
 *   getVerticesSortedTopologicallyFromSources()（:360）—— 唯一的算法部分：
 *     ① 空图直接返回空表
 *     ② 「start by finding the vertices with no input edges and the ones with
 *        disconnected inputs (that refer to some standalone data set)」：
 *        遍历 remaining，hasNoConnectedInputs() 的进 sorted 并从 remaining 移除
 *     ③ startNodePos 从 0 开始往后扫 sorted：
 *        「first check if we have more candidates to start traversing from.
 *         if not, then the graph is cyclic, which is not permitted」
 *            -> throw new InvalidProgramException("The job graph is cyclic.")
 *        否则 addNodesThatHaveNoNewPredecessors(sorted[startNodePos++], ...)
 *
 *   private addNodesThatHaveNoNewPredecessors(start, target, remaining)（:396）：
 *     对 start 产出的每个数据集的每个消费者 v：
 *       v 不在 remaining -> skip
 *       若 v 还有**别的**入边其生产者仍在 remaining（hasNewPredecessors）
 *           -> skip（等那条边的上游先排好）
 *       否则 target.add(v); remaining.remove(v); 递归
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 插入序容器】taskVertices 是 LinkedHashMap，拓扑排序的
 * `new LinkedHashSet<>(taskVertices.values())` 依赖它的插入序来保证
 * 「同一程序两次提交得到同一顺序」。std::map<JobVertexID,...> 是按 id
 * 排序的（而 id 是 murmur hash，顺序没有意义），所以这里改为
 * **vector 保存插入序 + set 做重复检查**，语义与 LinkedHashMap 一致。
 *
 * 【偏差 2 —— 部署/提交相关字段省掉】jobID / userJars / userJarBlobKeys /
 * classpaths / sessionTimeout / allowQueuedScheduling /
 * restartStrategyConfiguration 都只服务于「把作业提交给 JobManager」这条
 * 链路（本仓库没有 JobManager、没有 Blob 服务、没有重启策略体系）。
 * 其中 restartStrategyConfiguration 被 StreamingJobGraphGenerator
 * .configureRestartStrategy() 用到，见该处偏差。
 *
 * 【偏差 3 —— jobConfiguration 省掉】1.0 用它存序列化后的 ExecutionConfig
 * （createJobGraph 末尾的 InstantiationUtil.writeObjectToConfig）；
 * 本仓库没有序列化层，ExecutionConfig 由 StreamGraph 直接持有。
 *
 * 【偏差 4 —— savepointPath 省掉】属于 savepoint 恢复链路，未移植。
 *
 * 【偏差 5 —— JobVertex 的所有权】1.0 的 JobVertex 由调用方 new 出来再
 * addVertex；这里 JobGraph 用 shared_ptr 接管，避免生成器里一堆裸 new 的
 * 生命周期问题。顶点间的 JobEdge/IntermediateDataSet 仍是裸指针（它们的
 * 所有者是各自的 JobVertex）。
 */
#ifndef FLINK_RUNTIME_JOBGRAPH_JOBGRAPH_H
#define FLINK_RUNTIME_JOBGRAPH_JOBGRAPH_H

#include <cstdint>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "flink/runtime/jobgraph/JobVertex.h"

namespace flink {
namespace runtime {
namespace jobgraph {

// 1.0 org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings
class JobSnapshottingSettings {
 public:
  JobSnapshottingSettings(const std::vector<JobVertexID>& verticesToTrigger,
                          const std::vector<JobVertexID>& verticesToAcknowledge,
                          const std::vector<JobVertexID>& verticesToConfirm,
                          int64_t checkpointInterval, int64_t checkpointTimeout,
                          int64_t minPauseBetweenCheckpoints,
                          int maxConcurrentCheckpoints)
      : verticesToTrigger_(verticesToTrigger),
        verticesToAcknowledge_(verticesToAcknowledge),
        verticesToConfirm_(verticesToConfirm),
        checkpointInterval_(checkpointInterval),
        checkpointTimeout_(checkpointTimeout),
        minPauseBetweenCheckpoints_(minPauseBetweenCheckpoints),
        maxConcurrentCheckpoints_(maxConcurrentCheckpoints) {
    // 1.0: "sanity checks"（抛的 IllegalArgumentException 不带消息）
    if (checkpointInterval < 1 || checkpointTimeout < 1 ||
        minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1) {
      throw std::invalid_argument("");
    }
  }

  const std::vector<JobVertexID>& getVerticesToTrigger() const {
    return verticesToTrigger_;
  }
  const std::vector<JobVertexID>& getVerticesToAcknowledge() const {
    return verticesToAcknowledge_;
  }
  const std::vector<JobVertexID>& getVerticesToConfirm() const {
    return verticesToConfirm_;
  }
  int64_t getCheckpointInterval() const { return checkpointInterval_; }
  int64_t getCheckpointTimeout() const { return checkpointTimeout_; }
  int64_t getMinPauseBetweenCheckpoints() const {
    return minPauseBetweenCheckpoints_;
  }
  int getMaxConcurrentCheckpoints() const { return maxConcurrentCheckpoints_; }

 private:
  std::vector<JobVertexID> verticesToTrigger_;
  std::vector<JobVertexID> verticesToAcknowledge_;
  std::vector<JobVertexID> verticesToConfirm_;
  int64_t checkpointInterval_;
  int64_t checkpointTimeout_;
  int64_t minPauseBetweenCheckpoints_;
  int maxConcurrentCheckpoints_;
};

// 1.0 org.apache.flink.runtime.jobgraph.JobGraph
class JobGraph {
 public:
  typedef std::shared_ptr<JobVertex> JobVertexPtr;

  // 1.0 JobGraph(String jobName)
  explicit JobGraph(const std::string& jobName)
      : jobName_(jobName.empty() ? "(unnamed job)" : jobName),
        scheduleMode_(ScheduleMode::FROM_SOURCES) {}

  const std::string& getName() const { return jobName_; }

  void setScheduleMode(ScheduleMode scheduleMode) {
    scheduleMode_ = scheduleMode;
  }
  ScheduleMode getScheduleMode() const { return scheduleMode_; }

  // 1.0 addVertex(JobVertex)（:216）。【偏差 5】收 shared_ptr
  void addVertex(const JobVertexPtr& vertex) {
    const JobVertexID& id = vertex->getID();
    if (vertexIds_.count(id) != 0) {
      // 1.0 先把旧值放回去，再抛
      throw std::invalid_argument(
          "The JobGraph already contains a vertex with that id.");
    }
    vertexIds_.insert(id);
    taskVertices_.push_back(vertex);
  }

  int getNumberOfVertices() const {
    return static_cast<int>(taskVertices_.size());
  }

  // 1.0 getVertices()：taskVertices.values()，LinkedHashMap 的插入序
  std::vector<JobVertex*> getVertices() const {
    std::vector<JobVertex*> vertices;
    vertices.reserve(taskVertices_.size());
    for (const auto& vertex : taskVertices_) vertices.push_back(vertex.get());
    return vertices;
  }

  // 1.0 findVertexByID(JobVertexID)
  JobVertex* findVertexByID(const JobVertexID& id) const {
    for (const auto& vertex : taskVertices_) {
      if (vertex->getID() == id) return vertex.get();
    }
    return NULL;
  }

  void setSnapshotSettings(
      const std::shared_ptr<JobSnapshottingSettings>& settings) {
    snapshotSettings_ = settings;
  }
  const std::shared_ptr<JobSnapshottingSettings>& getSnapshotSettings() const {
    return snapshotSettings_;
  }

  // 1.0 getVerticesSortedTopologicallyFromSources()（:360）
  std::vector<JobVertex*> getVerticesSortedTopologicallyFromSources() const;

 private:
  // 1.0 private addNodesThatHaveNoNewPredecessors(start, target, remaining)
  static void addNodesThatHaveNoNewPredecessors(
      JobVertex* start, std::vector<JobVertex*>& target,
      std::vector<JobVertex*>& remaining);

  // 【偏差 1】LinkedHashMap -> vector（插入序）+ set（重复检查）
  std::vector<JobVertexPtr> taskVertices_;
  std::set<JobVertexID> vertexIds_;

  std::string jobName_;
  ScheduleMode scheduleMode_;
  std::shared_ptr<JobSnapshottingSettings> snapshotSettings_;
};

}  // namespace jobgraph
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_JOBGRAPH_JOBGRAPH_H
