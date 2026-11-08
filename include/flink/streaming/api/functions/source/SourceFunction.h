/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.functions.source.SourceFunction
 *
 * 1.0 interface SourceFunction<T> extends Function, Serializable
 * （SourceFunction.java:87）
 *
 * 1.0 原注释：「Base interface for all stream data sources in Flink. The
 * contract of a stream source is the following: When the source should start
 * emitting elements the run method is called with a Collector that can be used
 * for emitting elements. The run method can run for as long as necessary. The
 * source must, however, react to an invocation of cancel by breaking out of
 * its main loop.」
 *
 *   void run(SourceContext<T> ctx) throws Exception;   (:99)
 *   void cancel();                                     (:107)
 *
 * 1.0 关于 checkpointed source 的约定（:36-42），本实现同样成立：
 *   「Sources that also implement the Checkpointed interface must ensure that
 *     state checkpointing, updating of internal state and emission of elements
 *     are not done concurrently. This is achieved by using the provided
 *     checkpointing lock object to protect update of state and emission of
 *     elements in a synchronized block.」
 * 1.0 给出的标准写法（:48-74）在 C++ 里就是：
 *   void run(SourceContext<int64_t>& ctx) override {
 *     while (isRunning_ && count_ < 1000) {
 *       std::lock_guard<std::recursive_mutex> guard(ctx.getCheckpointLock());
 *       ctx.collect(count_);
 *       ++count_;
 *     }
 *   }
 *
 * 嵌套接口 SourceContext<T>（:119）：
 *   void collect(T element);                           (:127)
 *   void collectWithTimestamp(T element, long ts);     (:136)
 *   void emitWatermark(Watermark mark);                (:151)
 *   Object getCheckpointLock();                        (:160)
 *   void close();                                      (:166)
 * 三个实现都在 StreamSource 里（NonTimestampContext /
 * AutomaticWatermarkContext / ManualWatermarkContext）。
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— SourceContext 不是嵌套类型】1.0 把 SourceContext 定义在
 * SourceFunction 内部（`SourceFunction.SourceContext<T>`）。C++ 的嵌套模板
 * 在前置声明与 using 上都很别扭（StreamSource 的三个 context 要分别特化），
 * 所以提到同一命名空间下的独立模板 SourceContext<T>，用处与可见性不变。
 *
 * 【偏差 2 —— getCheckpointLock() 的返回类型】1.0 返回 Object（就是
 * StreamTask 的那个 `private final Object lock`），用 synchronized 加锁。
 * 这里返回 std::recursive_mutex&，UDF 用 std::lock_guard 加锁。锁必须可重入：
 * 上面这段 1.0 的标准写法是「UDF 加锁 -> ctx.collect() 再加同一把锁」
 * （SourceContext 的三个实现都在 collect 里加锁），Java 的 synchronized 本来
 * 就可重入，C++ 这边就得是 recursive_mutex（见 StreamTask.h 偏差 4）。
 *
 * 【偏差 3 —— EventTimeSourceFunction 不复刻】1.0 用这个空的子接口做标记，
 * 让 StreamGraphGenerator 判断「这个 source 自己发水位线」。它只在
 * StreamExecutionEnvironment.addSource 的类型推断里用到，那条线还没落地。
 */
#ifndef FLINK_STREAMING_API_FUNCTIONS_SOURCE_SOURCEFUNCTION_H
#define FLINK_STREAMING_API_FUNCTIONS_SOURCE_SOURCEFUNCTION_H

#include <cstdint>
#include <mutex>

#include "flink/api/common/functions/Function.h"
#include "flink/streaming/api/watermark/Watermark.h"

namespace flink {
namespace streaming {
namespace api {
namespace functions {
namespace source {

// 1.0 SourceFunction.SourceContext<T>（SourceFunction.java:119）
template <class T>
class SourceContext {
 public:
  virtual ~SourceContext() {}

  // 1.0 collect(T element)：「The result of System.currentTimeMillis() is set
  // as the timestamp of the emitted element.」—— 注意这句只对
  // AutomaticWatermarkContext 成立
  virtual void collect(const T& element) = 0;

  // 1.0 collectWithTimestamp(T element, long timestamp)
  virtual void collectWithTimestamp(const T& element, int64_t timestamp) = 0;

  // 1.0 emitWatermark(Watermark mark)：「Sources must only manually emit
  // watermarks when they implement EventTimeSourceFunction.」
  virtual void emitWatermark(
      const flink::streaming::api::watermark::Watermark& mark) = 0;

  // 1.0 getCheckpointLock()（偏差 2）
  virtual std::recursive_mutex& getCheckpointLock() = 0;

  // 1.0 close()：「This must be called when closing the source operator to
  // allow the SourceContext to clean up internal state.」
  virtual void close() = 0;
};

// 1.0 的继承是 `extends Function, Serializable`。这里与 MapFunction /
// FilterFunction 等保持一致：非虚继承 Function（本仓库的 function 体系都这样）
template <class T>
class SourceFunction : public flink::api::common::functions::Function {
 public:
  // 1.0 run(SourceContext<T> ctx) throws Exception
  virtual void run(SourceContext<T>& ctx) = 0;

  // 1.0 cancel()
  virtual void cancel() = 0;
};

}  // namespace source
}  // namespace functions
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_FUNCTIONS_SOURCE_SOURCEFUNCTION_H
