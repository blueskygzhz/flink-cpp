/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.tasks.OperatorChain
 *
 * 1.0 class OperatorChain<OUT>（OperatorChain.java:58）
 *
 * 1.0 原注释：「The OperatorChain contains all operators that are executed as
 * one chain within a single StreamTask.」—— 算子链就是「一串算子跑在同一个
 * task、同一条线程里，彼此之间直接调方法而不走网络」。
 *
 * 字段（:62-66）：
 *   private final StreamOperator<?>[] allOperators;
 *   private final RecordWriterOutput<?>[] streamOutputs;
 *   private final Output<StreamRecord<OUT>> chainEntryPoint;
 *
 * 构造（:69-126）顺序严格照搬：
 *   1) chainedConfigs = configuration.getTransitiveChainedTaskConfigs(cl)
 *      chainedConfigs.put(configuration.getVertexID(), configuration)
 *      1.0 注释：「we read the chained configs, and the order of record writer
 *      registrations by output name」
 *   2) outEdgesInOrder = configuration.getOutEdgesInOrder(cl)
 *      对每条出边建一个 RecordWriterOutput，下标 i 即
 *      Environment.getWriter(i) 的编号 —— **顺序必须与 JobGraph 一致**
 *   3) chainEntryPoint = createOutputCollector(...)（递归建链，顺带填 allOps）
 *   4) allOperators = allOps + [headOperator]（头算子放在**最后**）
 *   5) finally: 失败则把已建的 streamOutput 全部 close() + clearBuffers()
 *      1.0 注释：「make sure we clean up after ourselves in case of a failure
 *      after acquiring the first resources」
 *
 * allOperators 的顺序很关键：createOutputCollector 是从**链尾**往链头递归建的，
 * 所以 allOps 里先进来的是靠下游的算子，headOperator 最后追加。于是
 *   StreamTask.openAllOperators()  正序遍历 = 链尾 -> 链头
 *   StreamTask.closeAllOperators() 倒序遍历 = 链头 -> 链尾
 * （1.0 StreamTask.java:311/319，后者还特意注释
 *   「We need to close them first to last, since upstream operators in the
 *    chain might emit elements in their close methods.」）
 *
 * 其余方法：
 *   broadcastCheckpointBarrier(long id, long timestamp)（:129）
 *       new CheckpointBarrier(id, timestamp) 再逐个 streamOutput.broadcastEvent
 *   getStreamOutputs / getAllOperators / getChainEntryPoint
 *   flushOutputs()（:156）  1.0 注释：「This method should be called before
 *       finishing the record emission, to make sure any data that is still
 *       buffered will be sent.」
 *   releaseOutputs()（:169） close() 全部，finally 里 clearBuffers() 全部；
 *       1.0 注释：「This method should never fail.」
 *
 * 三个 private static 工厂：
 *   createOutputCollector(:187)  / createChainedOperator(:245)
 *   / createStreamOutput(:272)
 * 三个链内 Output 实现（1.0 的 private 嵌套类）：
 *   ChainingOutput(:297) / CopyingChainingOutput(:337)
 *   / BroadcastingOutputCollector(:359)
 * 另有 DirectedOutput（在 api.collector.selector 包，见 DirectedOutput.h）
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 对象所有权】1.0 把 RecordWriterOutput / StreamRecordWriter /
 * ChainingOutput / BroadcastingOutputCollector 都交给 GC。C++ 这边
 * OperatorChain 用几个 owned* 向量持有它们，结构字段
 * （allOperators_ / streamOutputs_ / chainEntryPoint_）仍是裸指针，
 * 以保持与 1.0 数组字段一一对应的形状。
 *
 * 【偏差 2 —— 不复刻 CopyingChainingOutput】1.0 在
 * `!objectReuseEnabled && !chainedOperator.isInputCopyingDisabled()` 时用
 * CopyingChainingOutput，每条记录先 serializer.copy(value) 再交给下游——
 * 因为 Java 里下游算子可能改写上游传下来的对象。本实现的记录值装在 Any 里
 * （as<T>() 只给出 const 引用，装箱后不可改），StreamRecord 也是按值传递，
 * 下游根本改不到上游的对象，所以这条拷贝分支没有意义，统一用 ChainingOutput。
 * ExecutionConfig（objectReuseEnabled 的来源）也因此不需要。
 * 这一点与 StreamOperator.h 里 isInputCopyingDisabled() 的偏差说明一致。
 *
 * 【偏差 3 —— 不复刻 AccumulatorRegistry.Reporter】1.0 构造参数里的 reporter
 * 用于统计 numRecordsIn/Out（StreamRecordWriter.setReporter），累加器体系
 * 还没落地，整项省掉。
 *
 * 【偏差 4 —— streamOutputMap 的键】1.0 用 Map<StreamEdge, RecordWriterOutput>
 * （StreamEdge.equals/hashCode 比的是 edgeId）。这里直接用 edgeId 字符串做键，
 * 语义相同（见 StreamEdge.h 的偏差 2）。
 *
 * 【偏差 5 —— 链内算子的类型】1.0 `OneInputStreamOperator<IN,OUT>
 * chainedOperator = operatorConfig.getStreamOperator(cl)` 是一次 unchecked
 * cast。这里用 dynamic_cast，转不过去就抛 —— 把 1.0 运行期的
 * ClassCastException 变成一条明确的错误信息。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_OPERATORCHAIN_H
#define FLINK_STREAMING_RUNTIME_TASKS_OPERATORCHAIN_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "flink/streaming/api/graph/StreamConfig.h"
#include "flink/streaming/api/graph/StreamEdge.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"
#include "flink/streaming/api/operators/Output.h"
#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/runtime/io/RecordWriterOutput.h"
#include "flink/streaming/runtime/io/StreamRecordWriter.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class StreamTask;

class OperatorChain {
 public:
  typedef flink::streaming::api::graph::StreamConfig StreamConfig;
  typedef flink::streaming::api::graph::StreamEdge StreamEdge;
  typedef flink::streaming::api::operators::Output Output;
  typedef flink::streaming::api::operators::StreamOperator StreamOperator;
  typedef flink::streaming::api::operators::OneInputStreamOperator
      OneInputStreamOperator;
  typedef flink::streaming::runtime::io::RecordWriterOutput RecordWriterOutput;
  typedef flink::streaming::runtime::io::StreamRecordWriter StreamRecordWriter;
  typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;
  typedef flink::streaming::api::watermark::Watermark Watermark;
  typedef std::shared_ptr<const flink::runtime::event::AbstractEvent> EventPtr;

  // 1.0 OperatorChain(StreamTask<OUT,?>, StreamOperator<OUT>,
  //                   AccumulatorRegistry.Reporter)  —— 偏差 3 去掉 reporter
  OperatorChain(StreamTask* containingTask, StreamOperator* headOperator);

  // 1.0 broadcastCheckpointBarrier(long id, long timestamp)（:129）
  void broadcastCheckpointBarrier(int64_t id, int64_t timestamp);

  const std::vector<RecordWriterOutput*>& getStreamOutputs() const {
    return streamOutputs_;
  }
  const std::vector<StreamOperator*>& getAllOperators() const {
    return allOperators_;
  }
  Output* getChainEntryPoint() const { return chainEntryPoint_; }

  // 1.0 flushOutputs()（:156）
  void flushOutputs();

  // 1.0 releaseOutputs()（:169）—— 「This method should never fail.」
  void releaseOutputs();

 private:
  // 1.0 的 Map<StreamEdge, RecordWriterOutput<?>>（偏差 4：键用 edgeId）
  typedef std::map<std::string, RecordWriterOutput*> StreamOutputMap;
  // 1.0 的 Map<Integer, StreamConfig>
  typedef std::map<int, StreamConfig*> ChainedConfigs;
  // 1.0 的 Tuple2<Output<StreamRecord<T>>, StreamEdge>
  typedef std::pair<Output*, const StreamEdge*> OutputWithEdge;

  // 1.0 private static createOutputCollector(...)（:187）
  Output* createOutputCollector(StreamTask* containingTask,
                                StreamConfig* operatorConfig,
                                const ChainedConfigs& chainedConfigs,
                                const StreamOutputMap& streamOutputs,
                                std::vector<StreamOperator*>& allOperators);

  // 1.0 private static createChainedOperator(...)（:245）
  Output* createChainedOperator(StreamTask* containingTask,
                                StreamConfig* operatorConfig,
                                const ChainedConfigs& chainedConfigs,
                                const StreamOutputMap& streamOutputs,
                                std::vector<StreamOperator*>& allOperators);

  // 1.0 private static createStreamOutput(...)（:272）
  RecordWriterOutput* createStreamOutput(const StreamEdge& edge,
                                         StreamConfig* upStreamConfig,
                                         int outputIndex,
                                         StreamTask* containingTask);

  // ---------------- 1.0 的链内 Output 实现（private 嵌套类）----------------

  // 1.0 private static class ChainingOutput<T>（:297）
  class ChainingOutput : public Output {
   public:
    explicit ChainingOutput(OneInputStreamOperator* op) : operator_(op) {}

    void collect(const StreamRecord& record) override;
    void emitWatermark(const Watermark& mark) override;
    void close() override;

   protected:
    // 1.0: protected final OneInputStreamOperator<T, ?> operator;
    OneInputStreamOperator* operator_;
  };

  // 1.0 private static final class BroadcastingOutputCollector<T>（:359）
  class BroadcastingOutputCollector : public Output {
   public:
    explicit BroadcastingOutputCollector(const std::vector<Output*>& outputs)
        : outputs_(outputs) {}

    void emitWatermark(const Watermark& mark) override;
    void collect(const StreamRecord& record) override;
    void close() override;

   private:
    std::vector<Output*> outputs_;
  };

  // 1.0 的三个数组字段
  std::vector<StreamOperator*> allOperators_;
  std::vector<RecordWriterOutput*> streamOutputs_;
  Output* chainEntryPoint_;

  // 【偏差 1】C++ 侧的所有权
  std::vector<std::shared_ptr<RecordWriterOutput> > ownedStreamOutputs_;
  std::vector<std::shared_ptr<StreamRecordWriter> > ownedRecordWriters_;
  std::vector<std::shared_ptr<Output> > ownedCollectors_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_OPERATORCHAIN_H
