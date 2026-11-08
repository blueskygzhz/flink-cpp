/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.environment.CheckpointConfig
 *
 * 1.0 class CheckpointConfig implements Serializable（CheckpointConfig.java:32）
 * —— env.enableCheckpointing(...) 配出来的那一组参数，StreamGraph 持一份，
 * StreamingJobGraphGenerator 再把它写进 JobSnapshottingSettings。
 *
 * 常量（:37-49）：
 *   DEFAULT_MODE    = CheckpointingMode.EXACTLY_ONCE   「The default
 *                     checkpoint mode: exactly once」
 *   DEFAULT_TIMEOUT = 10 * 60 * 1000                   「The default timeout
 *                     of a checkpoint attempt: 10 minutes」
 *   DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0          「none」
 *   DEFAULT_MAX_CONCURRENT_CHECKPOINTS    = 1          「one」
 *
 * 字段（:54-72）：
 *   private CheckpointingMode checkpointingMode = DEFAULT_MODE;
 *   private long checkpointInterval = -1;   // disabled
 *   private long checkpointTimeout  = DEFAULT_TIMEOUT;
 *   private long minPauseBetweenCheckpoints = DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS;
 *   private int  maxConcurrentCheckpoints   = DEFAULT_MAX_CONCURRENT_CHECKPOINTS;
 *   private boolean forceCheckpointing;     // 「Flag to force checkpointing
 *                                           //   in iterative jobs」
 *
 * 方法要点：
 *   isCheckpointingEnabled()  ->  checkpointInterval > 0
 *   setCheckpointingMode(m)   ->  requireNonNull
 *   setCheckpointInterval(v)  ->  v <= 0 则
 *       IllegalArgumentException("Checkpoint interval must be larger than zero")
 *   setCheckpointTimeout(v)   ->  v <= 0 则
 *       IllegalArgumentException("Checkpoint timeout must be larger than zero")
 *   setMaxConcurrentCheckpoints(n) -> n < 1 则 IllegalArgumentException(
 *       "The maximum number of concurrent attempts must be at least one.")
 *   isForceCheckpointing() / setForceCheckpointing(boolean)  —— 1.0 已标
 *       @Deprecated，但 StreamGraph.getJobGraph() 的「迭代作业禁 checkpoint」
 *       判定要用它，所以必须留着
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 没有 setMinPauseBetweenCheckpoints】1.0 把这个 setter **整段
 * 注释掉了**（CheckpointConfig.java 里是 `//\tpublic void
 * setMinPauseBetweenCheckpoints(...)`），只留 getter。这里照样只给 getter，
 * 字段恒为默认值 0。
 */
#ifndef FLINK_STREAMING_API_ENVIRONMENT_CHECKPOINTCONFIG_H
#define FLINK_STREAMING_API_ENVIRONMENT_CHECKPOINTCONFIG_H

#include <cstdint>
#include <stdexcept>

#include "flink/streaming/api/CheckpointingMode.h"

namespace flink {
namespace streaming {
namespace api {
namespace environment {

class CheckpointConfig {
 public:
  typedef flink::streaming::api::CheckpointingMode CheckpointingMode;

  // 1.0:37 「The default checkpoint mode: exactly once」
  static CheckpointingMode defaultMode() {
    return CheckpointingMode::EXACTLY_ONCE;
  }
  // 1.0:40 「The default timeout of a checkpoint attempt: 10 minutes」
  static int64_t defaultTimeout() { return 10 * 60 * 1000; }
  // 1.0:43 「The default minimum pause to be made between checkpoints: none」
  static int64_t defaultMinPauseBetweenCheckpoints() { return 0; }
  // 1.0:46 「The default limit of concurrently happening checkpoints: one」
  static int defaultMaxConcurrentCheckpoints() { return 1; }

  CheckpointConfig()
      : checkpointingMode_(defaultMode()),
        checkpointInterval_(-1),  // 1.0: disabled
        checkpointTimeout_(defaultTimeout()),
        minPauseBetweenCheckpoints_(defaultMinPauseBetweenCheckpoints()),
        maxConcurrentCheckpoints_(defaultMaxConcurrentCheckpoints()),
        forceCheckpointing_(false) {}

  // 1.0:79
  bool isCheckpointingEnabled() const { return checkpointInterval_ > 0; }

  CheckpointingMode getCheckpointingMode() const { return checkpointingMode_; }
  void setCheckpointingMode(CheckpointingMode checkpointingMode) {
    checkpointingMode_ = checkpointingMode;
  }

  int64_t getCheckpointInterval() const { return checkpointInterval_; }
  void setCheckpointInterval(int64_t checkpointInterval) {
    if (checkpointInterval <= 0) {
      throw std::invalid_argument(
          "Checkpoint interval must be larger than zero");
    }
    checkpointInterval_ = checkpointInterval;
  }

  int64_t getCheckpointTimeout() const { return checkpointTimeout_; }
  void setCheckpointTimeout(int64_t checkpointTimeout) {
    if (checkpointTimeout <= 0) {
      throw std::invalid_argument(
          "Checkpoint timeout must be larger than zero");
    }
    checkpointTimeout_ = checkpointTimeout;
  }

  // 【偏差 1】1.0 只有 getter，setter 被整段注释掉了
  int64_t getMinPauseBetweenCheckpoints() const {
    return minPauseBetweenCheckpoints_;
  }

  int getMaxConcurrentCheckpoints() const { return maxConcurrentCheckpoints_; }
  void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
    if (maxConcurrentCheckpoints < 1) {
      throw std::invalid_argument(
          "The maximum number of concurrent attempts must be at least one.");
    }
    maxConcurrentCheckpoints_ = maxConcurrentCheckpoints;
  }

  // 1.0 已标 @Deprecated，但 StreamGraph 的迭代作业判定要用
  bool isForceCheckpointing() const { return forceCheckpointing_; }
  void setForceCheckpointing(bool forceCheckpointing) {
    forceCheckpointing_ = forceCheckpointing;
  }

 private:
  CheckpointingMode checkpointingMode_;
  int64_t checkpointInterval_;
  int64_t checkpointTimeout_;
  int64_t minPauseBetweenCheckpoints_;
  int maxConcurrentCheckpoints_;
  bool forceCheckpointing_;
};

}  // namespace environment
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_ENVIRONMENT_CHECKPOINTCONFIG_H
