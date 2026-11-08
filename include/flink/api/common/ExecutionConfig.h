/*
 * flink-core : org.apache.flink.api.common.ExecutionConfig
 *
 * 1.0 ExecutionConfig.java 有 40+ 个配置项（闭包清理、Kryo 注册、重启策略、
 * 对象重用、累加器…）。这里只保留流式执行真正读取的几项，其余依赖 JVM 生态
 * 或集群调度，照搬只是摆设。
 *
 * 【本实现新增】autoWatermarkRecordInterval：
 *   1.0 的 autoWatermarkInterval 是毫秒周期（定时线程发水印）。本实现两种都支持：
 *   毫秒周期（与 1.0 一致，由 StreamTask 的 ProcessingTimeService 驱动），
 *   以及「每 N 条记录探测一次」——后者让有界数据源的示例输出完全可复现，
 *   不受机器负载影响。设为 0 表示关闭该模式、走 1.0 的毫秒周期。
 */
#ifndef FLINK_API_COMMON_EXECUTIONCONFIG_H
#define FLINK_API_COMMON_EXECUTIONCONFIG_H

#include <cstddef>
#include <cstdint>

namespace flink {
namespace api {
namespace common {

class ExecutionConfig {
 public:
  // 1.0 ExecutionConfig.PARALLELISM_DEFAULT = -1
  static int parallelismDefault() { return -1; }
  // 1.0 ExecutionConfig.java: private long autoWatermarkInterval = 0
  ExecutionConfig()
      : parallelism_(1),
        autoWatermarkInterval_(0),
        autoWatermarkRecordInterval_(0) {}

  int getParallelism() const { return parallelism_; }
  ExecutionConfig& setParallelism(int parallelism) {
    parallelism_ = parallelism;
    return *this;
  }

  // 1.0 getAutoWatermarkInterval() / setAutoWatermarkInterval(long)
  int64_t getAutoWatermarkInterval() const { return autoWatermarkInterval_; }
  ExecutionConfig& setAutoWatermarkInterval(int64_t intervalMillis) {
    autoWatermarkInterval_ = intervalMillis;
    return *this;
  }

  // 本实现新增，见文件头说明
  size_t getAutoWatermarkRecordInterval() const {
    return autoWatermarkRecordInterval_;
  }
  ExecutionConfig& setAutoWatermarkRecordInterval(size_t numRecords) {
    autoWatermarkRecordInterval_ = numRecords;
    return *this;
  }

 private:
  int parallelism_;
  int64_t autoWatermarkInterval_;
  size_t autoWatermarkRecordInterval_;
};

}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_EXECUTIONCONFIG_H
