/*
 * flink-runtime :
 *   org.apache.flink.runtime.io.network.partition.ResultPartitionType
 *
 * 1.0 enum ResultPartitionType（ResultPartitionType.java:21）：
 *   BLOCKING            (isPersistent=true,  isPipelined=false, hasBackPressure=false)
 *   PIPELINED           (false, true, true)
 *   PIPELINED_PERSISTENT(true,  true, true)
 *
 *   hasBackPressure() / isBlocking() = !isPipelined / isPipelined() /
 *   isPersistent()
 *
 * 流式作业只用 PIPELINED（StreamingJobGraphGenerator.connect 三个分支一律传
 * ResultPartitionType.PIPELINED）。
 *
 * 【偏差】Java 的 enum 带构造参数与方法；C++ 的 enum class 不行，所以三个
 * 属性改为同命名空间下的自由函数，取值表逐条照抄。
 */
#ifndef FLINK_RUNTIME_IO_NETWORK_PARTITION_RESULTPARTITIONTYPE_H
#define FLINK_RUNTIME_IO_NETWORK_PARTITION_RESULTPARTITIONTYPE_H

namespace flink {
namespace runtime {
namespace io {
namespace network {
namespace partition {

enum class ResultPartitionType { BLOCKING, PIPELINED, PIPELINED_PERSISTENT };

// 1.0 的三个构造参数（isPersistent, isPipelined, hasBackPressure）
inline bool isPipelined(ResultPartitionType type) {
  return type == ResultPartitionType::PIPELINED ||
         type == ResultPartitionType::PIPELINED_PERSISTENT;
}
inline bool isPersistent(ResultPartitionType type) {
  return type == ResultPartitionType::BLOCKING ||
         type == ResultPartitionType::PIPELINED_PERSISTENT;
}
inline bool hasBackPressure(ResultPartitionType type) {
  return isPipelined(type);
}
// 1.0 isBlocking() { return !isPipelined; }
inline bool isBlocking(ResultPartitionType type) { return !isPipelined(type); }

}  // namespace partition
}  // namespace network
}  // namespace io
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_IO_NETWORK_PARTITION_RESULTPARTITIONTYPE_H
