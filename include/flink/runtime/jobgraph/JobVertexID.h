/*
 * flink-runtime : org.apache.flink.runtime.jobgraph.JobVertexID
 *   以及同包的两个小枚举 DistributionPattern / ScheduleMode
 *
 * 1.0 JobVertexID extends AbstractID（JobVertexID.java:26）：
 *   「A class for statistically unique job vertex IDs.」
 *   JobVertexID()                      —— 随机
 *   JobVertexID(byte[] bytes)          —— 由 StreamingJobGraphGenerator 用
 *                                         murmur3_128 的 16 字节结果构造
 *   JobVertexID(long lower, long upper)
 *   static JobVertexID fromHexString(String)
 *
 * 1.0 DistributionPattern（DistributionPattern.java:27）：
 *   ALL_TO_ALL  「Each producing sub task is connected to each sub task of
 *                the consuming task.」
 *   POINTWISE   「Each producing sub task is connected to one or more subtask(s)
 *                of the consuming task.」
 *
 * 1.0 ScheduleMode（ScheduleMode.java:21）：
 *   ALL           「Schedule all tasks immediately.」
 *   BACKTRACKING  「Schedule tasks lazily from the sinks.」
 *   FROM_SOURCES  「Schedule tasks from the sources.」（JobGraph 的默认值）
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— fromHexString 省掉】1.0 只在 REST/日志解析路径上用它，
 * 作业图生成这条链路不需要。
 */
#ifndef FLINK_RUNTIME_JOBGRAPH_JOBVERTEXID_H
#define FLINK_RUNTIME_JOBGRAPH_JOBVERTEXID_H

#include "flink/util/AbstractID.h"

namespace flink {
namespace runtime {
namespace jobgraph {

class JobVertexID : public flink::util::AbstractID {
 public:
  JobVertexID() : flink::util::AbstractID() {}
  explicit JobVertexID(const std::vector<unsigned char>& bytes)
      : flink::util::AbstractID(bytes) {}
  JobVertexID(int64_t lowerPart, int64_t upperPart)
      : flink::util::AbstractID(lowerPart, upperPart) {}
};

// 1.0 enum DistributionPattern
enum class DistributionPattern {
  // 「Each producing sub task is connected to each sub task of the consuming
  //   task.」
  ALL_TO_ALL,
  // 「Each producing sub task is connected to one or more subtask(s) of the
  //   consuming task.」
  POINTWISE
};

// 1.0 enum ScheduleMode
enum class ScheduleMode {
  ALL,           // 「Schedule all tasks immediately.」
  BACKTRACKING,  // 「Schedule tasks lazily from the sinks.」
  FROM_SOURCES   // 「Schedule tasks from the sources.」
};

}  // namespace jobgraph
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_JOBGRAPH_JOBVERTEXID_H
