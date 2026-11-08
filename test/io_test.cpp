// streaming IO 层测试
//
// 用例直接移植自 1.0 的测试（flink-streaming-java/src/test/java/
// org/apache/flink/streaming/runtime/io/）：
//   BarrierTrackerTest.testSingleChannelNoBarriers
//   BarrierTrackerTest.testSingleChannelWithBarriers
//   BarrierTrackerTest.testMultiChannelWithBarriers
//   BarrierTrackerTest.testMultiChannelSkippingCheckpoints
//   BarrierTrackerTest.testCompleteCheckpointsOnLateBarriers
//   BarrierBufferTest.testSingleChannelWithBarriers
//   BarrierBufferTest.testMultiChannelWithBarriers
// 另加几条本实现特有的：
//   UnionInputGate 的扁平通道索引重映射
//   StreamInputProcessor 的多通道水位线「取 min 且单调不减」
//   RoundRobinChannelSelector 的「先自增再取模」
//   RecordWriter.emit / broadcastEmit / broadcastEvent 的分发
//   StreamRecordWriter 构造里 timeout 的三分支
//   RecordWriterOutput -> StreamRecordWriter -> ResultPartitionWriter
//     -> SingleInputGate -> StreamInputProcessor 的端到端贯通
//
// 【偏差】1.0 的 createBuffer(channel) 造的是 PAGE_SIZE 字节的内存页，
// 测试里用 checkNoTempFilesRemain() 验证 BufferSpiller 的临时文件被清掉。
// 本实现的 Buffer 就是一个 StreamElement，BufferSpiller 是内存队列
// （见 BufferSpiller.h 的偏差），所以不存在临时文件这项检查；
// 「两个 BufferOrEvent 是否同一个」改用 shared_ptr 指针身份比较，
// 等价于 1.0 的 assertEquals(boe, ...)（1.0 的 BufferOrEvent 没有重写
// equals，比的也是对象身份）。

#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "flink/runtime/io/network/api/CheckpointBarrier.h"
#include "flink/runtime/io/network/api/EndOfPartitionEvent.h"
#include "flink/runtime/io/network/api/writer/RecordWriter.h"
#include "flink/runtime/io/network/api/writer/RoundRobinChannelSelector.h"
#include "flink/runtime/io/network/partition/consumer/UnionInputGate.h"
#include "flink/streaming/api/operators/OneInputStreamOperator.h"
#include "flink/streaming/runtime/io/BarrierBuffer.h"
#include "flink/streaming/runtime/io/BarrierTracker.h"
#include "flink/streaming/runtime/io/RecordWriterOutput.h"
#include "flink/streaming/runtime/io/StreamInputProcessor.h"

using flink::runtime::io::network::api::CheckpointBarrier;
using flink::runtime::io::network::api::EndOfPartitionEvent;
using flink::runtime::io::network::api::writer::RecordWriter;
using flink::runtime::io::network::api::writer::ResultPartitionWriter;
using flink::runtime::io::network::api::writer::RoundRobinChannelSelector;
using flink::runtime::io::network::partition::consumer::BufferOrEvent;
using flink::runtime::io::network::partition::consumer::InputGate;
using flink::runtime::io::network::partition::consumer::SingleInputGate;
using flink::runtime::io::network::partition::consumer::UnionInputGate;
using flink::runtime::util::event::EventListener;
using flink::streaming::api::operators::ChainingStrategy;
using flink::streaming::api::operators::OneInputStreamOperator;
using flink::streaming::api::operators::Output;
using flink::streaming::api::watermark::Watermark;
using flink::streaming::runtime::io::BarrierBuffer;
using flink::streaming::runtime::io::BarrierTracker;
using flink::streaming::runtime::io::RecordWriterOutput;
using flink::streaming::runtime::io::StreamInputProcessor;
using flink::streaming::runtime::io::StreamRecordWriter;
using flink::streaming::runtime::streamrecord::StreamElement;
using flink::streaming::runtime::streamrecord::StreamRecord;
using flink::streaming::runtime::tasks::StreamTaskState;
using flink::types::Any;

namespace {

int failures = 0;

void check(bool condition, const std::string& what) {
  std::printf(condition ? "ok    %s\n" : "FAIL  %s\n", what.c_str());
  if (!condition) ++failures;
}

// ---------------------------------------------------------------- test doubles

// 1.0 测试里的 MockInputGate（MockInputGate.java）
// 1.0: isFinished() { return boes.isEmpty(); }
//      getNextBufferOrEvent() 遇到已关闭通道还有数据就抛
//      "Inconsistent: Channel X has data even though it is already closed."
class MockInputGate : public InputGate {
 public:
  MockInputGate(int numChannels, const std::vector<BufferOrEvent>& boes)
      : numChannels_(numChannels),
        boes_(boes.begin(), boes.end()),
        closed_(numChannels, false) {}

  int getNumberOfInputChannels() const override { return numChannels_; }

  bool isFinished() const override { return boes_.empty(); }

  void requestPartitions() override {}

  bool getNextBufferOrEvent(BufferOrEvent& target) override {
    if (boes_.empty()) return false;  // 1.0: return null;
    BufferOrEvent next = boes_.front();
    boes_.pop_front();

    const int channelIdx = next.getChannelIndex();
    if (closed_[channelIdx]) {
      throw std::runtime_error("Inconsistent: Channel " +
                               std::to_string(channelIdx) +
                               " has data even though it is already closed.");
    }
    if (next.isEvent() &&
        dynamic_cast<const EndOfPartitionEvent*>(next.getEvent().get()) !=
            NULL) {
      closed_[channelIdx] = true;
    }
    target = next;
    return true;
  }

 private:
  int numChannels_;
  std::deque<BufferOrEvent> boes_;
  std::vector<bool> closed_;
};

// 1.0 BarrierBufferTest.ValidatingCheckpointHandler：屏障必须按 id 递增到达
class ValidatingCheckpointHandler : public EventListener<CheckpointBarrier> {
 public:
  explicit ValidatingCheckpointHandler(int64_t nextExpected)
      : nextExpectedCheckpointId(nextExpected) {}

  void onEvent(const CheckpointBarrier& barrier) override {
    check(barrier.getId() == nextExpectedCheckpointId,
          "handler: expected checkpoint " +
              std::to_string(nextExpectedCheckpointId) + ", got " +
              std::to_string(barrier.getId()));
    nextExpectedCheckpointId++;
  }

  int64_t nextExpectedCheckpointId;
};

// 1.0 BarrierTrackerTest.CheckpointSequenceValidator：按给定序列校验通知
class CheckpointSequenceValidator : public EventListener<CheckpointBarrier> {
 public:
  explicit CheckpointSequenceValidator(const std::vector<int64_t>& expected)
      : expected_(expected), i_(0) {}

  void onEvent(const CheckpointBarrier& barrier) override {
    if (i_ >= expected_.size()) {
      check(false, "validator: unexpected extra checkpoint " +
                       std::to_string(barrier.getId()));
      return;
    }
    check(barrier.getId() == expected_[i_],
          "validator[" + std::to_string(i_) + "]: expected " +
              std::to_string(expected_[i_]) + ", got " +
              std::to_string(barrier.getId()));
    ++i_;
  }

  bool exhausted() const { return i_ == expected_.size(); }

 private:
  std::vector<int64_t> expected_;
  size_t i_;
};

// --------------------------------------------------------------------- helpers

int nextBufferValue = 0;

// 1.0 createBuffer(int channel)：每次造一个**新**对象，测试靠身份区分
BufferOrEvent createBuffer(int channel) {
  return BufferOrEvent(
      std::shared_ptr<const StreamElement>(
          new StreamRecord(Any::of<int>(nextBufferValue++))),
      channel);
}

BufferOrEvent createWatermark(int64_t timestamp, int channel) {
  return BufferOrEvent(
      std::shared_ptr<const StreamElement>(new Watermark(timestamp)), channel);
}

// 1.0 createBarrier(long id, int channel)
BufferOrEvent createBarrier(int64_t id, int channel) {
  return BufferOrEvent(
      std::shared_ptr<const flink::runtime::event::AbstractEvent>(
          new CheckpointBarrier(id, id * 1000)),
      channel);
}

// 1.0 createEndOfPartition(int channel)
BufferOrEvent createEndOfPartition(int channel) {
  return BufferOrEvent(
      std::shared_ptr<const flink::runtime::event::AbstractEvent>(
          new EndOfPartitionEvent()),
      channel);
}

bool isBarrier(const BufferOrEvent& boe) {
  return boe.isEvent() &&
         dynamic_cast<const CheckpointBarrier*>(boe.getEvent().get()) != NULL;
}

// 对应 1.0 的 assertEquals(expected, actual)：比对象身份 + 通道号
bool same(const BufferOrEvent& a, const BufferOrEvent& b) {
  return a.getChannelIndex() == b.getChannelIndex() &&
         a.getBuffer().get() == b.getBuffer().get() &&
         a.getEvent().get() == b.getEvent().get();
}

// 1.0 的 `for (boe : sequence) if (非 barrier) assertEquals(boe, handler.getNextNonBlocked())`
void expectAllNonBarriersInOrder(
    flink::streaming::runtime::io::CheckpointBarrierHandler& handler,
    const std::vector<BufferOrEvent>& sequence, const std::string& label) {
  size_t idx = 0;
  for (size_t i = 0; i < sequence.size(); ++i) {
    if (isBarrier(sequence[i])) continue;
    BufferOrEvent actual;
    bool hasNext = handler.getNextNonBlocked(actual);
    check(hasNext && same(sequence[i], actual),
          label + ": element " + std::to_string(idx));
    ++idx;
  }
  BufferOrEvent trailing;
  check(!handler.getNextNonBlocked(trailing), label + ": end of stream");
  check(!handler.getNextNonBlocked(trailing), label + ": end of stream (again)");
}

// ------------------------------------------------- BarrierTracker（AT_LEAST_ONCE）

// 1.0 BarrierTrackerTest.testSingleChannelNoBarriers
void testTrackerSingleChannelNoBarriers() {
  std::vector<BufferOrEvent> sequence;
  sequence.push_back(createBuffer(0));
  sequence.push_back(createBuffer(0));
  sequence.push_back(createBuffer(0));

  MockInputGate gate(1, sequence);
  BarrierTracker tracker(&gate);
  expectAllNonBarriersInOrder(tracker, sequence, "tracker/singleNoBarriers");
}

// 1.0 BarrierTrackerTest.testSingleChannelWithBarriers
void testTrackerSingleChannelWithBarriers() {
  std::vector<BufferOrEvent> s;
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(1, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(2, 0));
  s.push_back(createBarrier(3, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(4, 0));
  s.push_back(createBarrier(5, 0));
  s.push_back(createBarrier(6, 0));
  s.push_back(createBuffer(0));

  MockInputGate gate(1, s);
  BarrierTracker tracker(&gate);
  // 1.0: CheckpointSequenceValidator(1, 2, 3, 4, 5, 6) —— 单通道走 fast path
  CheckpointSequenceValidator validator({1, 2, 3, 4, 5, 6});
  tracker.registerCheckpointEventHandler(&validator);

  expectAllNonBarriersInOrder(tracker, s, "tracker/singleWithBarriers");
  check(validator.exhausted(), "tracker/singleWithBarriers: all 6 notified");
}

// 1.0 BarrierTrackerTest.testMultiChannelWithBarriers
void testTrackerMultiChannelWithBarriers() {
  std::vector<BufferOrEvent> s;
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(2));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(1, 1));
  s.push_back(createBarrier(1, 2));
  s.push_back(createBuffer(2));
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(1, 0));

  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(2, 0));
  s.push_back(createBarrier(2, 1));
  s.push_back(createBarrier(2, 2));

  s.push_back(createBuffer(2));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(3, 2));
  s.push_back(createBuffer(2));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(3, 0));
  s.push_back(createBarrier(3, 1));

  s.push_back(createBarrier(4, 1));
  s.push_back(createBarrier(4, 2));
  s.push_back(createBarrier(4, 0));

  s.push_back(createBuffer(0));

  MockInputGate gate(3, s);
  BarrierTracker tracker(&gate);
  CheckpointSequenceValidator validator({1, 2, 3, 4});
  tracker.registerCheckpointEventHandler(&validator);

  expectAllNonBarriersInOrder(tracker, s, "tracker/multiWithBarriers");
  check(validator.exhausted(), "tracker/multiWithBarriers: all 4 notified");
}

// 1.0 BarrierTrackerTest.testMultiChannelSkippingCheckpoints
// checkpoint 3 只攒到 1 个屏障就被 4 顶掉 -> 只通知 1, 2, 4
void testTrackerMultiChannelSkippingCheckpoints() {
  std::vector<BufferOrEvent> s;
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(2));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(1, 1));
  s.push_back(createBarrier(1, 2));
  s.push_back(createBuffer(2));
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(1, 0));

  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(2, 0));
  s.push_back(createBarrier(2, 1));
  s.push_back(createBarrier(2, 2));

  s.push_back(createBuffer(2));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(3, 2));
  s.push_back(createBuffer(2));
  s.push_back(createBuffer(2));

  // 1.0 注释: "jump to checkpoint 4"
  s.push_back(createBarrier(4, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(4, 1));
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(4, 2));

  s.push_back(createBuffer(0));

  MockInputGate gate(3, s);
  BarrierTracker tracker(&gate);
  CheckpointSequenceValidator validator({1, 2, 4});
  tracker.registerCheckpointEventHandler(&validator);

  expectAllNonBarriersInOrder(tracker, s, "tracker/multiSkipping");
  check(validator.exhausted(), "tracker/multiSkipping: 1,2,4 notified");
}

// 1.0 BarrierTrackerTest.testCompleteCheckpointsOnLateBarriers
// 1.0 原注释：「validates that the barrier tracker does not immediately
// discard a pending checkpoint as soon as it sees a barrier from a later
// checkpoint from some channel」—— 这正是 pendingCheckpoints 队列的意义。
void testTrackerCompleteCheckpointsOnLateBarriers() {
  std::vector<BufferOrEvent> s;
  // checkpoint 2
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(2, 1));
  s.push_back(createBarrier(2, 0));
  s.push_back(createBarrier(2, 2));

  // incomplete checkpoint 3
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(3, 1));
  s.push_back(createBarrier(3, 2));

  // some barriers from checkpoint 4
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(4, 2));
  s.push_back(createBarrier(4, 1));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(2));

  // last barrier from checkpoint 3
  s.push_back(createBarrier(3, 0));

  // complete checkpoint 4
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(4, 0));

  // regular checkpoint 5
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(5, 1));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(5, 0));
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(5, 2));

  // checkpoint 6 (incomplete)
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(6, 1));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(6, 0));

  // checkpoint 7, with early barriers for checkpoints 8 and 9
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(7, 1));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(7, 2));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(8, 2));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(8, 1));
  s.push_back(createBuffer(1));
  s.push_back(createBarrier(9, 1));

  // complete checkpoint 7, first barriers from checkpoint 10
  s.push_back(createBarrier(7, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(9, 2));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(10, 2));

  // complete checkpoint 8 and 9
  s.push_back(createBarrier(8, 0));
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(2));
  s.push_back(createBarrier(9, 0));

  // trailing data
  s.push_back(createBuffer(1));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(2));

  MockInputGate gate(3, s);
  BarrierTracker tracker(&gate);
  CheckpointSequenceValidator validator({2, 3, 4, 5, 7, 8, 9});
  tracker.registerCheckpointEventHandler(&validator);

  expectAllNonBarriersInOrder(tracker, s, "tracker/lateBarriers");
  check(validator.exhausted(), "tracker/lateBarriers: 2,3,4,5,7,8,9 notified");
}

// -------------------------------------------------- BarrierBuffer（EXACTLY_ONCE）

// 1.0 BarrierBufferTest.testSingleChannelWithBarriers
void testBufferSingleChannelWithBarriers() {
  std::vector<BufferOrEvent> s;
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(1, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(2, 0));
  s.push_back(createBarrier(3, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(4, 0));
  s.push_back(createBarrier(5, 0));
  s.push_back(createBarrier(6, 0));
  s.push_back(createBuffer(0));
  s.push_back(createEndOfPartition(0));

  MockInputGate gate(1, s);
  BarrierBuffer buffer(&gate);
  ValidatingCheckpointHandler handler(1);
  buffer.registerCheckpointEventHandler(&handler);

  expectAllNonBarriersInOrder(buffer, s, "buffer/singleWithBarriers");
  check(handler.nextExpectedCheckpointId == 7,
        "buffer/singleWithBarriers: checkpoints 1..6 triggered");
  buffer.cleanup();
}

// 1.0 BarrierBufferTest.testMultiChannelWithBarriers
// 1.0 原注释：「Validates that the buffer correctly aligns the streams for
// inputs with multiple input channels, by buffering and blocking certain
// inputs.」—— 期望的取出顺序逐行照抄 1.0 的断言。
void testBufferMultiChannelWithBarriers() {
  std::vector<BufferOrEvent> s;
  // checkpoint with blocked data                     索引
  s.push_back(createBuffer(0));        // 0
  s.push_back(createBuffer(2));        // 1
  s.push_back(createBuffer(0));        // 2
  s.push_back(createBarrier(1, 1));    // 3
  s.push_back(createBarrier(1, 2));    // 4
  s.push_back(createBuffer(2));        // 5
  s.push_back(createBuffer(1));        // 6
  s.push_back(createBuffer(0));        // 7
  s.push_back(createBarrier(1, 0));    // 8

  // checkpoint without blocked data
  s.push_back(createBuffer(0));        // 9
  s.push_back(createBuffer(0));        // 10
  s.push_back(createBuffer(1));        // 11
  s.push_back(createBuffer(1));        // 12
  s.push_back(createBuffer(2));        // 13
  s.push_back(createBarrier(2, 0));    // 14
  s.push_back(createBarrier(2, 1));    // 15
  s.push_back(createBarrier(2, 2));    // 16

  // checkpoint with data only from one channel
  s.push_back(createBuffer(2));        // 17
  s.push_back(createBuffer(2));        // 18
  s.push_back(createBarrier(3, 2));    // 19
  s.push_back(createBuffer(2));        // 20
  s.push_back(createBuffer(2));        // 21
  s.push_back(createBarrier(3, 0));    // 22
  s.push_back(createBarrier(3, 1));    // 23

  // empty checkpoint
  s.push_back(createBarrier(4, 1));    // 24
  s.push_back(createBarrier(4, 2));    // 25
  s.push_back(createBarrier(4, 0));    // 26

  // checkpoint with blocked data in mixed order
  s.push_back(createBuffer(0));        // 27
  s.push_back(createBuffer(2));        // 28
  s.push_back(createBuffer(0));        // 29
  s.push_back(createBarrier(5, 1));    // 30
  s.push_back(createBuffer(2));        // 31
  s.push_back(createBuffer(0));        // 32
  s.push_back(createBuffer(2));        // 33
  s.push_back(createBuffer(1));        // 34
  s.push_back(createBarrier(5, 2));    // 35
  s.push_back(createBuffer(1));        // 36
  s.push_back(createBuffer(0));        // 37
  s.push_back(createBuffer(2));        // 38
  s.push_back(createBuffer(1));        // 39
  s.push_back(createBarrier(5, 0));    // 40

  // some trailing data
  s.push_back(createBuffer(0));          // 41
  s.push_back(createEndOfPartition(0));  // 42
  s.push_back(createEndOfPartition(1));  // 43
  s.push_back(createEndOfPartition(2));  // 44

  MockInputGate gate(3, s);
  BarrierBuffer buffer(&gate);
  ValidatingCheckpointHandler handler(1);
  buffer.registerCheckpointEventHandler(&handler);

  // 1.0 的断言序列（BarrierBufferTest.java:227 起）
  const size_t expectedOrder[] = {
      // pre checkpoint 1
      0, 1, 2,
      // blocking while aligning for checkpoint 1
      7,
      // checkpoint 1 done, returning buffered data
      5, 6,
      // pre checkpoint 2
      9, 10, 11, 12, 13,
      // checkpoint 2 barriers come together
      17, 18,
      // checkpoint 3 starts, data buffered
      20, 21,
      // checkpoint 4 happens without extra data; pre checkpoint 5
      27, 28, 29,
      // checkpoint 5 aligning
      31, 32, 33, 37,
      // buffered data from checkpoint 5 alignment
      34, 36, 38, 39,
      // remaining data
      41, 42, 43, 44};
  const size_t expectedCount = sizeof(expectedOrder) / sizeof(size_t);

  // 1.0 在断言之间插了 getNextExpectedCheckpointId() 的检查，用来确认
  // 「屏障对齐完成的时刻」。这里把它们合并成下面的断言点映射：
  //   取出 sequence[2] 之后仍是 1；取出 sequence[7] 之后仍是 1；
  //   取出 sequence[5] 之后是 2；sequence[13] 之后是 2；
  //   sequence[17] 之后是 3；sequence[20] 之后是 4；sequence[27] 之后是 5。
  for (size_t i = 0; i < expectedCount; ++i) {
    BufferOrEvent actual;
    const bool hasNext = buffer.getNextNonBlocked(actual);
    check(hasNext && same(s[expectedOrder[i]], actual),
          "buffer/multiWithBarriers: step " + std::to_string(i) +
              " -> sequence[" + std::to_string(expectedOrder[i]) + "]");

    switch (expectedOrder[i]) {
      case 2:
      case 7:
        check(handler.nextExpectedCheckpointId == 1,
              "buffer/multiWithBarriers: still aligning checkpoint 1");
        break;
      case 5:
      case 13:
        check(handler.nextExpectedCheckpointId == 2,
              "buffer/multiWithBarriers: checkpoint 1 complete");
        break;
      case 17:
        check(handler.nextExpectedCheckpointId == 3,
              "buffer/multiWithBarriers: checkpoint 2 complete");
        break;
      case 20:
        check(handler.nextExpectedCheckpointId == 4,
              "buffer/multiWithBarriers: checkpoint 3 complete");
        break;
      case 27:
        check(handler.nextExpectedCheckpointId == 5,
              "buffer/multiWithBarriers: checkpoint 4 complete");
        break;
      default:
        break;
    }
  }

  BufferOrEvent trailing;
  check(!buffer.getNextNonBlocked(trailing),
        "buffer/multiWithBarriers: end of stream");
  check(!buffer.getNextNonBlocked(trailing),
        "buffer/multiWithBarriers: end of stream (again)");
  check(handler.nextExpectedCheckpointId == 6,
        "buffer/multiWithBarriers: checkpoints 1..5 triggered");

  buffer.cleanup();
  check(buffer.isEmpty(), "buffer/multiWithBarriers: empty after cleanup");
}

// -------------------------------------------------------------- UnionInputGate

// 1.0 UnionInputGate.java:148 的扁平通道索引重映射
void testUnionInputGateChannelIndexOffsets() {
  std::vector<BufferOrEvent> left;
  left.push_back(createBuffer(0));
  left.push_back(createBuffer(1));
  std::vector<BufferOrEvent> right;
  right.push_back(createBuffer(0));
  right.push_back(createBuffer(2));

  MockInputGate gate1(2, left);   // 占扁平空间 [0, 2)
  MockInputGate gate2(3, right);  // 占扁平空间 [2, 5)

  std::vector<InputGate*> gates;
  gates.push_back(&gate1);
  gates.push_back(&gate2);
  UnionInputGate united(gates);

  check(united.getNumberOfInputChannels() == 5,
        "union: 2 + 3 = 5 input channels");
  check(!united.isFinished(), "union: not finished while data remains");

  // 轮转顺序：gate1 -> gate2 -> gate1 -> gate2
  const int expectedChannels[] = {0, 2, 1, 4};
  for (int i = 0; i < 4; ++i) {
    BufferOrEvent boe;
    const bool hasNext = united.getNextBufferOrEvent(boe);
    check(hasNext && boe.getChannelIndex() == expectedChannels[i],
          "union: element " + std::to_string(i) + " mapped to channel " +
              std::to_string(expectedChannels[i]));
  }

  BufferOrEvent boe;
  check(!united.getNextBufferOrEvent(boe), "union: exhausted");
  check(united.isFinished(), "union: finished when all gates finished");
}

// -------------------------------------------------------- StreamInputProcessor

// 只实现 StreamInputProcessor 用到的那部分：processElement / processWatermark /
// setKeyContextElement1。其余是 StreamOperator 的接口义务。
class CollectingOperator : public OneInputStreamOperator {
 public:
  void setup(flink::streaming::runtime::tasks::StreamTask*,
             flink::streaming::api::graph::StreamConfig*, Output*) override {}
  void open() override {}
  void close() override {}
  void dispose() override {}

  StreamTaskState snapshotOperatorState(int64_t, int64_t) override {
    return StreamTaskState();
  }
  void restoreState(const StreamTaskState&, int64_t) override {}
  void notifyOfCompletedCheckpoint(int64_t) override {}

  void setKeyContextElement1(const StreamRecord& record) override {
    keyContextCalls.push_back(record.getValueAs<int>());
  }
  void setKeyContextElement2(const StreamRecord&) override {}

  bool isInputCopyingDisabled() const override { return false; }
  ChainingStrategy getChainingStrategy() const override {
    return ChainingStrategy::HEAD;
  }
  void setChainingStrategy(ChainingStrategy) override {}

  void processElement(const StreamRecord& element) override {
    records.push_back(element.getValueAs<int>());
  }
  void processWatermark(const Watermark& mark) override {
    watermarks.push_back(mark.getTimestamp());
  }

  std::vector<int> records;
  std::vector<int> keyContextCalls;
  std::vector<int64_t> watermarks;
};

// 1.0 StreamInputProcessor.processInput 的水位线推进：per-channel 记最大值，
// 下发全通道最小值，且必须大于已下发值。
void testStreamInputProcessorWatermarks() {
  std::vector<BufferOrEvent> s;
  s.push_back(createBuffer(0));             // 0: 记录
  s.push_back(createWatermark(10, 0));      // ch0 = 10，min 仍是 MIN_VALUE
  s.push_back(createWatermark(5, 1));       // ch1 = 5，min = 5 -> 下发 5
  s.push_back(createWatermark(20, 0));      // ch0 = 20，min 仍是 5 -> 不下发
  s.push_back(createWatermark(3, 1));       // 比 ch1 当前值小 -> 整段跳过
  s.push_back(createWatermark(30, 1));      // ch1 = 30，min = 20 -> 下发 20
  s.push_back(createBuffer(1));             // 6: 记录
  s.push_back(createEndOfPartition(0));
  s.push_back(createEndOfPartition(1));

  const int firstValue = s[0].getBuffer()->as<StreamRecord>().getValueAs<int>();
  const int secondValue = s[6].getBuffer()->as<StreamRecord>().getValueAs<int>();

  MockInputGate gate(2, s);
  std::vector<InputGate*> gates;
  gates.push_back(&gate);

  StreamInputProcessor processor(
      gates, NULL, flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE);

  CollectingOperator op;
  std::mutex lock;
  while (processor.processInput(&op, lock)) {
  }
  processor.cleanup();

  check(op.records.size() == 2 && op.records[0] == firstValue &&
            op.records[1] == secondValue,
        "processor: both records delivered in order");
  check(op.keyContextCalls == op.records,
        "processor: setKeyContextElement1 called once before each element");
  check(op.watermarks.size() == 2, "processor: exactly 2 watermarks emitted");
  if (op.watermarks.size() == 2) {
    check(op.watermarks[0] == 5, "processor: first emitted watermark is min 5");
    check(op.watermarks[1] == 20,
          "processor: second emitted watermark is min 20");
  }
  // 再调一次仍然是 false（1.0: isFinished 之后直接返回）
  check(!processor.processInput(&op, lock),
        "processor: stays finished after end of input");
}

// 单通道时 EXACTLY_ONCE 的屏障会回调注册的 listener（BarrierBuffer 路径）
void testStreamInputProcessorExactlyOnceBarrier() {
  std::vector<BufferOrEvent> s;
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(1, 0));
  s.push_back(createBuffer(0));
  s.push_back(createBarrier(2, 0));
  s.push_back(createEndOfPartition(0));

  MockInputGate gate(1, s);
  std::vector<InputGate*> gates;
  gates.push_back(&gate);

  ValidatingCheckpointHandler handler(1);
  StreamInputProcessor processor(
      gates, &handler, flink::streaming::api::CheckpointingMode::EXACTLY_ONCE);

  CollectingOperator op;
  std::mutex lock;
  while (processor.processInput(&op, lock)) {
  }
  processor.cleanup();

  check(op.records.size() == 2, "processor/exactlyOnce: 2 records delivered");
  check(handler.nextExpectedCheckpointId == 3,
        "processor/exactlyOnce: checkpoints 1 and 2 triggered");
}

// ---------------------------------------------------------------- 输出侧（写）

// 1.0 RoundRobinChannelSelector：**先自增再取模**，所以第一条记录去通道 1
void testRoundRobinChannelSelector() {
  RoundRobinChannelSelector selector;
  const int expected[] = {1, 2, 0, 1, 2, 0};
  for (int i = 0; i < 6; ++i) {
    const std::vector<int>& channels = selector.selectChannels(
        std::shared_ptr<const StreamElement>(), 3);
    check(channels.size() == 1 && channels[0] == expected[i],
          "roundRobin: call " + std::to_string(i) + " -> channel " +
              std::to_string(expected[i]));
  }
}

// 把 gate 里剩下的东西全排出来，便于断言「谁被写到了哪个通道」
std::vector<BufferOrEvent> drain(SingleInputGate& gate) {
  gate.releaseAllResources();
  std::vector<BufferOrEvent> out;
  BufferOrEvent boe;
  while (gate.getNextBufferOrEvent(boe)) {
    out.push_back(boe);
  }
  return out;
}

// 1.0 RecordWriter.emit / broadcastEmit / broadcastEvent 的分发语义
void testRecordWriterDistribution() {
  SingleInputGate gate(3, 64);
  std::vector<ResultPartitionWriter::Target> targets;
  targets.push_back(ResultPartitionWriter::Target(&gate, 0));
  targets.push_back(ResultPartitionWriter::Target(&gate, 1));
  targets.push_back(ResultPartitionWriter::Target(&gate, 2));
  ResultPartitionWriter partitionWriter(targets);

  check(partitionWriter.getNumberOfOutputChannels() == 3,
        "writer: 3 output channels (subpartitions)");

  // 1.0 RecordWriter(ResultPartitionWriter) 默认用 RoundRobinChannelSelector
  RecordWriter writer(&partitionWriter);

  std::shared_ptr<const StreamElement> r1(new StreamRecord(Any::of<int>(1)));
  std::shared_ptr<const StreamElement> r2(new StreamRecord(Any::of<int>(2)));
  std::shared_ptr<const StreamElement> r3(new StreamRecord(Any::of<int>(3)));
  std::shared_ptr<const StreamElement> wm(new Watermark(42));
  std::shared_ptr<const flink::runtime::event::AbstractEvent> barrier(
      new CheckpointBarrier(7, 7000));

  writer.emit(r1);            // round robin -> 1
  writer.emit(r2);            // -> 2
  writer.emit(r3);            // -> 0
  writer.broadcastEmit(wm);   // -> 0, 1, 2（无视 ChannelSelector）
  writer.broadcastEvent(barrier);  // -> 0, 1, 2
  writer.flush();

  std::vector<BufferOrEvent> drained = drain(gate);
  check(drained.size() == 3 + 3 + 3,
        "writer: 3 records + 3 watermark copies + 3 barriers");

  if (drained.size() == 9) {
    // 三条记录按 round robin 落在 1 / 2 / 0
    check(drained[0].getBuffer() == r1 && drained[0].getChannelIndex() == 1,
          "writer: emit #1 -> channel 1");
    check(drained[1].getBuffer() == r2 && drained[1].getChannelIndex() == 2,
          "writer: emit #2 -> channel 2");
    check(drained[2].getBuffer() == r3 && drained[2].getChannelIndex() == 0,
          "writer: emit #3 -> channel 0");
    // 水位线广播到 0,1,2
    bool watermarkOk = true;
    for (int i = 0; i < 3; ++i) {
      watermarkOk = watermarkOk && drained[3 + i].getBuffer() == wm &&
                    drained[3 + i].getChannelIndex() == i;
    }
    check(watermarkOk, "writer: broadcastEmit -> channels 0,1,2");
    // 屏障广播到 0,1,2
    bool barrierOk = true;
    for (int i = 0; i < 3; ++i) {
      barrierOk = barrierOk && drained[6 + i].getEvent() == barrier &&
                  drained[6 + i].getChannelIndex() == i;
    }
    check(barrierOk, "writer: broadcastEvent -> channels 0,1,2");
  }
}

// 1.0 StreamRecordWriter 构造里 timeout 的三分支（:63-80）
void testStreamRecordWriterTimeoutModes() {
  SingleInputGate gate(1, 8);
  std::vector<ResultPartitionWriter::Target> targets;
  targets.push_back(ResultPartitionWriter::Target(&gate, 0));
  ResultPartitionWriter partitionWriter(targets);
  std::shared_ptr<flink::runtime::io::network::api::writer::ChannelSelector>
      selector(new RoundRobinChannelSelector());

  {  // timeout == -1：攒满一页才发
    StreamRecordWriter w(&partitionWriter, selector, -1);
    check(!w.isFlushAlways() && w.getFlushThreadName().empty(),
          "streamRecordWriter: timeout -1 -> no flushAlways, no flusher");
  }
  {  // timeout == 0：每条记录立即发
    StreamRecordWriter w(&partitionWriter, selector, 0);
    check(w.isFlushAlways() && w.getFlushThreadName().empty(),
          "streamRecordWriter: timeout 0 -> flushAlways, no flusher");
  }
  {  // timeout > 0：1.0 会起 OutputFlusher（本实现只保留命名，见偏差 1）
    StreamRecordWriter w(&partitionWriter, selector, 100);
    check(!w.isFlushAlways() &&
              w.getFlushThreadName() == "OutputFlusher",
          "streamRecordWriter: timeout 100 -> default flusher name");
    StreamRecordWriter named(&partitionWriter, selector, 100, "Map (1/2)");
    check(named.getFlushThreadName() == "Output Timeout Flusher - Map (1/2)",
          "streamRecordWriter: named flusher thread");
  }
  {  // 1.0: checkArgument(timeout >= -1)
    bool threw = false;
    try {
      StreamRecordWriter w(&partitionWriter, selector, -2);
      (void)w;
    } catch (const std::invalid_argument&) {
      threw = true;
    }
    check(threw, "streamRecordWriter: timeout < -1 rejected");
  }
}

// 端到端：RecordWriterOutput -> StreamRecordWriter -> ResultPartitionWriter
//        -> SingleInputGate -> StreamInputProcessor -> 算子
void testRecordWriterOutputEndToEnd() {
  SingleInputGate gate(2, 64);
  std::vector<ResultPartitionWriter::Target> targets;
  targets.push_back(ResultPartitionWriter::Target(&gate, 0));
  targets.push_back(ResultPartitionWriter::Target(&gate, 1));
  ResultPartitionWriter partitionWriter(targets);

  std::shared_ptr<flink::runtime::io::network::api::writer::ChannelSelector>
      selector(new RoundRobinChannelSelector());
  StreamRecordWriter recordWriter(&partitionWriter, selector, 0);
  RecordWriterOutput output(&recordWriter);

  output.collect(StreamRecord(Any::of<int>(11)));  // -> 通道 1
  output.collect(StreamRecord(Any::of<int>(22)));  // -> 通道 0
  output.emitWatermark(Watermark(7));              // -> 通道 0 和 1
  output.broadcastEvent(
      std::shared_ptr<const flink::runtime::event::AbstractEvent>(
          new EndOfPartitionEvent()));
  output.flush();
  gate.releaseAllResources();

  std::vector<InputGate*> gates;
  gates.push_back(&gate);
  StreamInputProcessor processor(
      gates, NULL, flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE);

  CollectingOperator op;
  std::mutex lock;
  while (processor.processInput(&op, lock)) {
  }
  processor.cleanup();

  check(op.records.size() == 2 && op.records[0] == 11 && op.records[1] == 22,
        "e2e: both records arrive in emit order");
  // 水位线先到通道 0（min 仍是 MIN_VALUE，不下发），再到通道 1 -> min = 7
  check(op.watermarks.size() == 1 && op.watermarks[0] == 7,
        "e2e: broadcast watermark emitted once as min 7");
  output.close();
}

}  // namespace

int main() {
  testTrackerSingleChannelNoBarriers();
  testTrackerSingleChannelWithBarriers();
  testTrackerMultiChannelWithBarriers();
  testTrackerMultiChannelSkippingCheckpoints();
  testTrackerCompleteCheckpointsOnLateBarriers();

  testBufferSingleChannelWithBarriers();
  testBufferMultiChannelWithBarriers();

  testUnionInputGateChannelIndexOffsets();

  testStreamInputProcessorWatermarks();
  testStreamInputProcessorExactlyOnceBarrier();

  testRoundRobinChannelSelector();
  testRecordWriterDistribution();
  testStreamRecordWriterTimeoutModes();
  testRecordWriterOutputEndToEnd();

  if (failures == 0) {
    std::printf("\nall io tests passed\n");
    return 0;
  }
  std::printf("\n%d io test(s) failed\n", failures);
  return 1;
}
