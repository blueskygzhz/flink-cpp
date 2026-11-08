/*
 * flink-streaming-java : org.apache.flink.streaming.api.operators.StreamSource
 *
 * 1.0 class StreamSource<OUT, SRC extends SourceFunction<OUT>>
 *       extends AbstractUdfStreamOperator<OUT, SRC>
 *       implements StreamOperator<OUT>            (StreamSource.java:38)
 *
 * 字段（:43-45）：
 *   private transient SourceFunction.SourceContext<OUT> ctx;
 *   private transient volatile boolean canceledOrStopped = false;
 *
 * 构造（:48）：super(sourceFunction); chainingStrategy = ChainingStrategy.HEAD;
 *
 * run(Object lockingObject, Output<StreamRecord<OUT>> collector)（:55）：
 *   按 getOperatorConfig().getTimeCharacteristic() 三选一建 SourceContext
 *     EventTime      -> ManualWatermarkContext
 *     IngestionTime  -> AutomaticWatermarkContext(..., getRuntimeContext()
 *                         .getExecutionConfig().getAutoWatermarkInterval())
 *     ProcessingTime -> NonTimestampContext
 *     default        -> throw new Exception(String.valueOf(timeCharacteristic))
 *   1.0 注释：「copy to a field to give the 'cancel()' method access」
 *   try { userFunction.run(ctx);
 *         // 「if we get here, then the user function either exited after being
 *         //   done (finite source) or the function was canceled or stopped.
 *         //   For the finite source case, we should emit a final watermark
 *         //   that indicates that we reached the end of event-time」
 *         if (!isCanceledOrStopped()) ctx.emitWatermark(MAX_WATERMARK); }
 *   finally { ctx.close(); }   // 「make sure that the context is closed in
 *                             //   any case」
 *
 * cancel()（:92）：
 *   1.0 注释：「important: marking the source as stopped has to happen before
 *   the function is stopped. the flag that tracks this status is volatile, so
 *   the memory model also guarantees the happens-before relationship」
 *   markCanceledOrStopped(); userFunction.cancel();
 *   if (ctx != null) ctx.close();   // 「the context may not be initialized if
 *                                   //   the source was never running」
 *
 * markCanceledOrStopped()（:111）/ isCanceledOrStopped()（:119）
 * checkAsyncException()（:128）-> getContainingTask().checkTimerException()
 *   1.0 注释：「Checks whether any asynchronous thread (checkpoint trigger,
 *   timer, watermark generator, ...) has caused an exception.」
 *
 * 三个 SourceContext 实现：
 *   NonTimestampContext（:140）      给所有记录**不带**时间戳，水位线丢弃
 *   AutomaticWatermarkContext（:187）记录带 currentTimeMillis，周期发水位线
 *   ManualWatermarkContext（:299）   时间戳与水位线全由 source 自己给
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— SourceOperator 接口】1.0 靠泛型上界让 SourceStreamTask 调
 * run()/cancel()，这里实现不带模板参数的 SourceOperator（见 SourceOperator.h）。
 *
 * 【偏差 2 —— 三个 context 不再带自己的类型参数】1.0 它们是
 * `public static class XxxContext<T>`，所以各带一个 T。这里是 StreamSource
 * 的嵌套类，直接用外层的 OUT —— 1.0 全仓库也只有 StreamSource 自己构造它们
 * （已核对：没有其他引用点）。
 *
 * 【偏差 3 —— autoWatermarkInterval 的来源】1.0 走
 * getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval()，
 * 由 StreamExecutionEnvironment.setStreamTimeCharacteristic 写入
 * （ProcessingTime 写 0，其余写 200，见 StreamExecutionEnvironment.java:599）。
 * ExecutionConfig / StreamingRuntimeContext 都还没落地，所以这里是算子自己的
 * 字段，默认值取 1.0 的 200，并提供 setAutoWatermarkInterval()。
 *
 * 【偏差 4 —— 水位线定时器】1.0 用
 * Executors.newScheduledThreadPool(1).scheduleAtFixedRate(.., 0, interval, MS)。
 * C++ 标准库没有 ScheduledExecutorService，这里自己起一条线程 + 条件变量，
 * 「首次延迟 0、之后每 interval 一次」的节奏与回调体内的判定逻辑都照搬 1.0。
 * watermarkTimer.cancel(true) + scheduleExecutor.shutdownNow() 合成
 * terminateTimer()（设置标志 -> 唤醒 -> join）。
 *
 * 【偏差 5 —— reuse 记录】1.0 的 StreamRecord.replace() 是**原地改**并返回
 * this，所以 reuse 字段真的在复用同一个对象。本实现的 replace() 返回新值
 * （见 StreamRecord.h），reuse_ 字段保留只为对齐 1.0 的调用形状——它承载的
 * 「有没有时间戳」初始状态仍然是有意义的（NonTimestampContext /
 * ManualWatermarkContext 的 collect 由此产出**不带**时间戳的记录）。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_STREAMSOURCE_H
#define FLINK_STREAMING_API_OPERATORS_STREAMSOURCE_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "flink/streaming/api/TimeCharacteristic.h"
#include "flink/streaming/api/functions/source/SourceFunction.h"
#include "flink/streaming/api/graph/StreamConfig.h"
#include "flink/streaming/api/operators/AbstractUdfStreamOperator.h"
#include "flink/streaming/api/operators/SourceOperator.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class OUT, class SRC>
class StreamSource : public AbstractUdfStreamOperator<SRC>,
                     public SourceOperator {
 public:
  typedef flink::streaming::api::functions::source::SourceContext<OUT>
      SourceContext;
  typedef flink::streaming::api::watermark::Watermark Watermark;
  typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;

  // 1.0 的 ExecutionConfig 默认值（见偏差 3）
  static int64_t defaultAutoWatermarkInterval() { return 200; }

  // 1.0 StreamSource(SRC sourceFunction)
  explicit StreamSource(std::shared_ptr<SRC> sourceFunction)
      : AbstractUdfStreamOperator<SRC>(sourceFunction),
        ctx_(NULL),
        canceledOrStopped_(false),
        autoWatermarkInterval_(defaultAutoWatermarkInterval()) {
    // 1.0: this.chainingStrategy = ChainingStrategy.HEAD;
    this->chainingStrategy_ = ChainingStrategy::HEAD;
  }

  std::string getSimpleName() const override { return "StreamSource"; }

  // 【偏差 3】1.0 从 ExecutionConfig 取
  void setAutoWatermarkInterval(int64_t interval) {
    autoWatermarkInterval_ = interval;
  }
  int64_t getAutoWatermarkInterval() const { return autoWatermarkInterval_; }

  // 1.0 run(final Object lockingObject, final Output<StreamRecord<OUT>>)（:55）
  void run(std::recursive_mutex& lockingObject, Output* collector) override {
    const flink::streaming::api::TimeCharacteristic timeCharacteristic =
        this->getOperatorConfig()->getTimeCharacteristic();

    std::unique_ptr<SourceContext> ctx;
    switch (timeCharacteristic) {
      case flink::streaming::api::TimeCharacteristic::EventTime:
        ctx.reset(new ManualWatermarkContext(this, lockingObject, collector));
        break;
      case flink::streaming::api::TimeCharacteristic::IngestionTime:
        ctx.reset(new AutomaticWatermarkContext(this, lockingObject, collector,
                                                autoWatermarkInterval_));
        break;
      case flink::streaming::api::TimeCharacteristic::ProcessingTime:
        ctx.reset(new NonTimestampContext(this, lockingObject, collector));
        break;
      default:
        // 1.0: throw new Exception(String.valueOf(timeCharacteristic))
        throw std::runtime_error("Unrecognized TimeCharacteristic");
    }

    // 1.0: "copy to a field to give the 'cancel()' method access"
    ctx_ = ctx.get();

    try {
      this->userFunction_->run(*ctx);

      // 1.0: "if we get here, then the user function either exited after being
      // done (finite source) or the function was canceled or stopped. For the
      // finite source case, we should emit a final watermark that indicates
      // that we reached the end of event-time"
      if (!isCanceledOrStopped()) {
        ctx->emitWatermark(Watermark::maxWatermark());
      }
    } catch (...) {
      // 1.0: "make sure that the context is closed in any case"
      ctx->close();
      ctx_ = NULL;
      throw;
    }
    ctx->close();
    ctx_ = NULL;
  }

  // 1.0 cancel()（:92）
  void cancel() override {
    // 1.0: "important: marking the source as stopped has to happen before the
    // function is stopped. the flag that tracks this status is volatile, so
    // the memory model also guarantees the happens-before relationship"
    markCanceledOrStopped();
    this->userFunction_->cancel();

    // 1.0: "the context may not be initialized if the source was never running"
    SourceContext* ctx = ctx_;
    if (ctx != NULL) {
      ctx->close();
    }
  }

  // 1.0 checkAsyncException()（:128）
  void checkAsyncException() {
    this->getContainingTask()->checkTimerException();
  }

 protected:
  // 1.0 markCanceledOrStopped()（:111）
  void markCanceledOrStopped() { canceledOrStopped_ = true; }
  // 1.0 isCanceledOrStopped()（:119）
  bool isCanceledOrStopped() const { return canceledOrStopped_; }

 public:
  // --------------------------------------------------------------------
  //  Source contexts for various stream time characteristics（1.0 :132）
  // --------------------------------------------------------------------

  // 1.0 NonTimestampContext<T>（:140）
  // 1.0 注释：「A source context that attached -1 as a timestamp to all
  // records, and that does not forward watermarks.」（注释里的 -1 与代码不符：
  // 代码是 new StreamRecord<T>(null) 再 replace(element)，产出的记录
  // hasTimestamp == false，取 getTimestamp() 才拿到 Long.MIN_VALUE）
  class NonTimestampContext : public SourceContext {
   public:
    NonTimestampContext(StreamSource* owner, std::recursive_mutex& lockingObject,
                        Output* output)
        : owner_(owner),
          lockingObject_(lockingObject),
          output_(output),
          reuse_(flink::types::Any()) {}

    void collect(const OUT& element) override {
      owner_->checkAsyncException();
      std::lock_guard<std::recursive_mutex> guard(lockingObject_);
      output_->collect(reuse_.replace(flink::types::Any::of(element)));
    }

    // 1.0: "ignore the timestamp"
    void collectWithTimestamp(const OUT& element, int64_t timestamp) override {
      (void)timestamp;
      collect(element);
    }

    void emitWatermark(const Watermark& mark) override {
      (void)mark;
      owner_->checkAsyncException();
      // 1.0: "do nothing else"
    }

    std::recursive_mutex& getCheckpointLock() override { return lockingObject_; }

    void close() override {}

   private:
    StreamSource* owner_;
    std::recursive_mutex& lockingObject_;
    Output* output_;
    StreamRecord reuse_;
  };

  // 1.0 AutomaticWatermarkContext<T>（:187）
  class AutomaticWatermarkContext : public SourceContext {
   public:
    AutomaticWatermarkContext(StreamSource* owner, std::recursive_mutex& lockingObject,
                              Output* output, int64_t watermarkInterval)
        : owner_(owner),
          lockingObject_(lockingObject),
          output_(output),
          reuse_(flink::types::Any()),
          watermarkInterval_(watermarkInterval),
          nextWatermarkTime_(0),
          timerShutdown_(false) {
      // 1.0: throw new IllegalArgumentException("The watermark interval cannot
      //      be smaller than one.")
      if (watermarkInterval < 1) {
        throw std::invalid_argument(
            "The watermark interval cannot be smaller than one.");
      }
      // 1.0: scheduleExecutor.scheduleAtFixedRate(.., 0, interval, MS)
      timerThread_ =
          std::thread(&AutomaticWatermarkContext::watermarkTimerLoop, this);
    }

    ~AutomaticWatermarkContext() override { terminateTimer(); }

    void collect(const OUT& element) override {
      owner_->checkAsyncException();

      std::lock_guard<std::recursive_mutex> guard(lockingObject_);
      const int64_t currentTime = currentTimeMillis();
      output_->collect(
          reuse_.replace(flink::types::Any::of(element), currentTime));

      if (currentTime > nextWatermarkTime_) {
        // 1.0: "in case we jumped some watermarks, recompute the next
        // watermark time"
        const int64_t watermarkTime =
            currentTime - (currentTime % watermarkInterval_);
        nextWatermarkTime_ = watermarkTime + watermarkInterval_;
        output_->emitWatermark(Watermark(watermarkTime));
      }
    }

    void collectWithTimestamp(const OUT& element, int64_t timestamp) override {
      (void)timestamp;
      collect(element);
    }

    void emitWatermark(const Watermark& mark) override {
      owner_->checkAsyncException();

      if (mark.getTimestamp() == Watermark::maxTimestamp()) {
        // 1.0: "allow it since this is the special end-watermark that for
        // example the Kafka source emits"
        {
          std::lock_guard<std::recursive_mutex> guard(lockingObject_);
          nextWatermarkTime_ = Watermark::maxTimestamp();
          output_->emitWatermark(mark);
        }
        // 1.0: "we can shutdown the timer now, no watermarks will be needed
        // any more"
        terminateTimer();
      }
    }

    std::recursive_mutex& getCheckpointLock() override { return lockingObject_; }

    void close() override { terminateTimer(); }

   private:
    static int64_t currentTimeMillis() {
      using namespace std::chrono;
      return duration_cast<milliseconds>(
                 system_clock::now().time_since_epoch())
          .count();
    }

    // 1.0 scheduleAtFixedRate 里那个 Runnable 的 run()（:220）
    void emitWatermarkIfDue() {
      const int64_t currentTime = currentTimeMillis();

      if (currentTime > nextWatermarkTime_) {
        // 1.0: "align the watermarks across all machines. this will ensure
        // that we don't have watermarks that creep along at different
        // intervals because the machine clocks are out of sync"
        const int64_t watermarkTime =
            currentTime - (currentTime % watermarkInterval_);

        std::lock_guard<std::recursive_mutex> guard(lockingObject_);
        if (currentTime > nextWatermarkTime_) {
          output_->emitWatermark(Watermark(watermarkTime));
          nextWatermarkTime_ += watermarkInterval_;
        }
      }
    }

    // 【偏差 4】1.0 的 ScheduledExecutorService：首次延迟 0，之后每
    // watermarkInterval 一次
    void watermarkTimerLoop() {
      std::unique_lock<std::mutex> guard(timerMutex_);
      while (!timerShutdown_) {
        guard.unlock();
        emitWatermarkIfDue();
        guard.lock();
        if (timerShutdown_) break;
        timerCondition_.wait_for(
            guard, std::chrono::milliseconds(watermarkInterval_));
      }
    }

    // 1.0 watermarkTimer.cancel(true) + scheduleExecutor.shutdownNow()
    void terminateTimer() {
      {
        std::lock_guard<std::mutex> guard(timerMutex_);
        timerShutdown_ = true;
      }
      timerCondition_.notify_all();
      // joinMutex_ 让「close() 与析构都调 terminateTimer()」是安全的
      std::lock_guard<std::mutex> joinGuard(joinMutex_);
      if (timerThread_.joinable()) timerThread_.join();
    }

    StreamSource* owner_;
    std::recursive_mutex& lockingObject_;
    Output* output_;
    StreamRecord reuse_;

    const int64_t watermarkInterval_;
    // 1.0 的 `private volatile long nextWatermarkTime`（读写都在
    // lockingObject 的临界区里）
    int64_t nextWatermarkTime_;

    std::thread timerThread_;
    std::mutex timerMutex_;
    std::mutex joinMutex_;
    std::condition_variable timerCondition_;
    bool timerShutdown_;
  };

  // 1.0 ManualWatermarkContext<T>（:299）
  // 1.0 注释：「A SourceContext for event time. Sources may directly attach
  // timestamps and generate watermarks, but if records are emitted without
  // timestamps, no timetamps are automatically generated and attached. The
  // records will simply have no timestamp in that case.」
  class ManualWatermarkContext : public SourceContext {
   public:
    ManualWatermarkContext(StreamSource* owner, std::recursive_mutex& lockingObject,
                           Output* output)
        : owner_(owner),
          lockingObject_(lockingObject),
          output_(output),
          reuse_(flink::types::Any()) {}

    void collect(const OUT& element) override {
      owner_->checkAsyncException();
      std::lock_guard<std::recursive_mutex> guard(lockingObject_);
      output_->collect(reuse_.replace(flink::types::Any::of(element)));
    }

    void collectWithTimestamp(const OUT& element, int64_t timestamp) override {
      owner_->checkAsyncException();
      std::lock_guard<std::recursive_mutex> guard(lockingObject_);
      output_->collect(
          reuse_.replace(flink::types::Any::of(element), timestamp));
    }

    void emitWatermark(const Watermark& mark) override {
      owner_->checkAsyncException();
      std::lock_guard<std::recursive_mutex> guard(lockingObject_);
      output_->emitWatermark(mark);
    }

    std::recursive_mutex& getCheckpointLock() override { return lockingObject_; }

    void close() override {}

   private:
    StreamSource* owner_;
    std::recursive_mutex& lockingObject_;
    Output* output_;
    StreamRecord reuse_;
  };

 private:
  // 1.0 的 `private transient SourceFunction.SourceContext<OUT> ctx`
  // （run() 里的局部 unique_ptr 持所有权，这个字段只是给 cancel() 看的引用）
  SourceContext* ctx_;
  // 1.0 的 `private transient volatile boolean canceledOrStopped`
  volatile bool canceledOrStopped_;
  int64_t autoWatermarkInterval_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_STREAMSOURCE_H
