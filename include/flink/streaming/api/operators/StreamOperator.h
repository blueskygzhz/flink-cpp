/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.StreamOperator
 *
 * 1.0 interface StreamOperator<OUT> extends Serializable（StreamOperator.java:44）：
 *
 *   // life cycle
 *   void setup(StreamTask<?, ?> containingTask, StreamConfig config,
 *              Output<StreamRecord<OUT>> output);
 *   void open() throws Exception;
 *   void close() throws Exception;
 *   void dispose();
 *
 *   // state snapshots
 *   StreamTaskState snapshotOperatorState(long checkpointId, long timestamp);
 *   void restoreState(StreamTaskState state, long recoveryTimestamp);
 *   void notifyOfCompletedCheckpoint(long checkpointId);
 *
 *   // miscellaneous
 *   void setKeyContextElement1(StreamRecord<?> record);
 *   void setKeyContextElement2(StreamRecord<?> record);
 *   boolean isInputCopyingDisabled();
 *   ChainingStrategy getChainingStrategy();
 *   void setChainingStrategy(ChainingStrategy strategy);
 *
 * 1.0 文档里的两条保证，本实现同样成立（StreamTask 单线程 + checkpointLock）：
 *   「Methods of StreamOperator are guaranteed not to be called concurrently」
 *   「timer callbacks are also guaranteed not to be called concurrently with
 *     methods on the operator」
 *
 * 【偏差】1.0 是 StreamOperator<OUT>，OUT 只出现在 setup 的 Output<StreamRecord<OUT>>
 * 参数里。本实现的 Output / StreamRecord 已经用 Any 擦除了元素类型（见
 * StreamRecord.h），所以这个接口不带模板参数，OUT 由具体算子的模板参数保证。
 *
 * 【偏差】isInputCopyingDisabled() 对应 1.0 的对象复用开关：Java 里下游算子可能
 * 改写上游传下来的对象，所以默认要拷贝。本实现的记录在通道里本就是值语义传递，
 * 这个标志保留但没有实际作用，仅为保持接口与 StreamConfig 的对齐。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMOPERATOR_H
#define FLINK_STREAMING_API_OPERATORS_STREAMOPERATOR_H

#include <cstdint>

#include "flink/streaming/api/operators/ChainingStrategy.h"
#include "flink/streaming/api/operators/Output.h"
#include "flink/streaming/runtime/streamrecord/StreamRecord.h"
#include "flink/streaming/runtime/tasks/StreamTaskState.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {
class StreamConfig;
}  // namespace graph
}  // namespace api
namespace runtime {
namespace tasks {
class StreamTask;
}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink

namespace flink {
namespace streaming {
namespace api {
namespace operators {

class StreamOperator {
 public:
  typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;
  typedef flink::streaming::runtime::tasks::StreamTaskState StreamTaskState;
  typedef flink::streaming::runtime::tasks::StreamTask StreamTask;
  typedef flink::streaming::api::graph::StreamConfig StreamConfig;

  virtual ~StreamOperator() {}

  // ---------------------------- life cycle ----------------------------

  // 1.0 setup(StreamTask<?,?>, StreamConfig, Output<StreamRecord<OUT>>)
  virtual void setup(StreamTask* containingTask, StreamConfig* config,
                     Output* output) = 0;
  virtual void open() = 0;
  virtual void close() = 0;
  virtual void dispose() = 0;

  // -------------------------- state snapshots --------------------------

  virtual StreamTaskState snapshotOperatorState(int64_t checkpointId,
                                                int64_t timestamp) = 0;
  virtual void restoreState(const StreamTaskState& state,
                            int64_t recoveryTimestamp) = 0;
  virtual void notifyOfCompletedCheckpoint(int64_t checkpointId) = 0;

  // --------------------------- miscellaneous ---------------------------

  virtual void setKeyContextElement1(const StreamRecord& record) = 0;
  virtual void setKeyContextElement2(const StreamRecord& record) = 0;

  virtual bool isInputCopyingDisabled() const = 0;

  virtual ChainingStrategy getChainingStrategy() const = 0;
  virtual void setChainingStrategy(ChainingStrategy strategy) = 0;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMOPERATOR_H
