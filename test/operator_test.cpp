// 算子层 + StreamTask 冒烟测试
//
// 覆盖 1.0 的这几条路径：
//   StreamTask.invoke()                     init -> restoreState -> open ->
//                                           run -> close -> flush -> dispose
//   StreamTask.registerTimer / TriggerTask  定时器持 checkpointLock 回调
//   StreamTask.triggerCheckpoint            广播屏障 + 抽快照 + acknowledge
//   AbstractStreamOperator.setup            建 state backend、装 keySelector
//   AbstractStreamOperator.setKeyContextElement1
//   AbstractUdfStreamOperator               open/close UDF、Checkpointed 状态
//   StreamMap / StreamFilter / StreamFlatMap  processElement 语义与时间戳处理

#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "flink/api/common/functions/AbstractRichFunction.h"
#include "flink/api/common/state/ValueStateDescriptor.h"
#include "flink/api/common/typeutils/TypedAnySerializer.h"
#include "flink/api/java/functions/TypedAnyKeySelector.h"
#include "flink/runtime/io/network/api/CheckpointBarrier.h"
#include "flink/runtime/state/memory/MemoryStateBackend.h"
#include "flink/streaming/api/checkpoint/Checkpointed.h"
#include "flink/streaming/api/operators/StreamFilter.h"
#include "flink/streaming/api/operators/StreamFlatMap.h"
#include "flink/streaming/api/operators/StreamGroupedFold.h"
#include "flink/streaming/api/operators/StreamGroupedReduce.h"
#include "flink/streaming/api/operators/StreamMap.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

using flink::api::common::functions::FilterFunction;
using flink::api::common::functions::FlatMapFunction;
using flink::api::common::functions::MapFunction;
using flink::api::common::typeutils::TypedAnySerializer;
using flink::api::java::functions::KeySelector;
using flink::runtime::io::network::api::CheckpointBarrier;
using flink::runtime::state::memory::MemoryStateBackend;
using flink::streaming::api::graph::StreamConfig;
using flink::streaming::api::operators::Output;
using flink::streaming::api::operators::StreamFilter;
using flink::streaming::api::operators::StreamFlatMap;
using flink::streaming::api::operators::StreamMap;
using flink::streaming::api::watermark::Watermark;
using flink::streaming::runtime::streamrecord::StreamRecord;
using flink::streaming::runtime::tasks::StreamTask;
using flink::streaming::runtime::tasks::StreamTaskStateList;
using flink::types::Any;

namespace {

int failures = 0;

void check(bool condition, const char* what) {
  std::printf(condition ? "ok    %s\n" : "FAIL  %s\n", what);
  if (!condition) ++failures;
}

// ---------------------------------------------------------------- test doubles

// 1.0 Environment 的测试替身（对应 1.0 测试里的 MockEnvironment）
class TestEnvironment : public flink::runtime::execution::Environment {
 public:
  std::string getTaskName() const override { return "TestTask"; }
  std::string getTaskNameWithSubtasks() const override {
    return "TestTask (1/1)";
  }
  int getIndexInSubtaskGroup() const override { return 0; }
  int getNumberOfSubtasks() const override { return 1; }

  void acknowledgeCheckpoint(int64_t checkpointId) override {
    ackedWithoutState.push_back(checkpointId);
  }
  void acknowledgeCheckpoint(int64_t checkpointId,
                             const StreamTaskStateList& state) override {
    ackedWithState.push_back(checkpointId);
    lastState = state;
  }

  std::vector<int64_t> ackedWithoutState;
  std::vector<int64_t> ackedWithState;
  StreamTaskStateList lastState;
};

// 1.0 RecordWriterOutput 的测试替身
class CollectingOutput : public Output {
 public:
  void collect(const StreamRecord& record) override {
    records.push_back(record);
  }
  void emitWatermark(const Watermark& mark) override {
    watermarks.push_back(mark.getTimestamp());
  }
  void broadcastEvent(
      const std::shared_ptr<const flink::runtime::event::AbstractEvent>& event)
      override {
    const CheckpointBarrier* barrier =
        dynamic_cast<const CheckpointBarrier*>(event.get());
    if (barrier != NULL) barrierIds.push_back(barrier->getId());
  }
  void close() override { closed = true; }

  std::vector<StreamRecord> records;
  std::vector<int64_t> watermarks;
  std::vector<int64_t> barrierIds;
  bool closed = false;
};

// 1.0 OneInputStreamTask 的最小替身：run() 里做的事等价于
// StreamInputProcessor.processInput —— 每条记录先 setKeyContextElement1
// 再 processElement，水位线走 processWatermark
class TestOneInputStreamTask : public StreamTask {
 public:
  typedef flink::streaming::api::operators::OneInputStreamOperator
      OneInputStreamOperator;

  TestOneInputStreamTask(TestEnvironment* env, const StreamConfig& config,
                         Output* output)
      : StreamTask(env, config) {
    setChainEntryPoint(output);
    addStreamOutput(output);
  }

  // 测试脚本：按顺序喂记录 / 水位线 / 触发 checkpoint / 睡一会儿
  std::vector<StreamRecord> inputRecords;
  std::vector<int64_t> inputWatermarks;
  int64_t checkpointAfterRecords = -1;
  int sleepMillisInRun = 0;

 protected:
  void init() override { initCalled = true; }

  void run() override {
    OneInputStreamOperator* op =
        dynamic_cast<OneInputStreamOperator*>(headOperator_);
    for (size_t i = 0; i < inputRecords.size(); ++i) {
      {
        // 1.0 StreamInputProcessor 在 checkpointLock 里处理每条记录
        std::lock_guard<std::mutex> guard(getCheckpointLock());
        op->setKeyContextElement1(inputRecords[i]);
        op->processElement(inputRecords[i]);
      }
      if (checkpointAfterRecords >= 0 &&
          static_cast<int64_t>(i + 1) == checkpointAfterRecords) {
        triggerCheckpoint(7, 700);
      }
    }
    for (int64_t ts : inputWatermarks) {
      std::lock_guard<std::mutex> guard(getCheckpointLock());
      op->processWatermark(Watermark(ts));
    }
    if (sleepMillisInRun > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepMillisInRun));
      checkTimerException();
    }
  }

  void cleanup() override { cleanupCalled = true; }
  void cancelTask() override {}

 public:
  bool initCalled = false;
  bool cleanupCalled = false;
};

// ---------------------------------------------------------------------- UDFs

class ToUpper : public MapFunction<std::string, std::string> {
 public:
  std::string map(const std::string& value) override {
    std::string result = value;
    for (char& c : result) c = static_cast<char>(std::toupper(c));
    return result;
  }
};

class IsEven : public FilterFunction<int32_t> {
 public:
  bool filter(const int32_t& value) override { return value % 2 == 0; }
};

class SplitWords : public FlatMapFunction<std::string, std::string> {
 public:
  void flatMap(const std::string& value,
               flink::util::Collector<std::string>& out) override {
    std::istringstream stream(value);
    std::string word;
    while (stream >> word) out.collect(word);
  }
};

// 同时是 RichFunction（测 open/close）和 Checkpointed（测 functionState）
class CountingMapper
    : public flink::api::common::functions::AbstractRichFunction,
      public MapFunction<int32_t, int32_t>,
      public flink::streaming::api::checkpoint::Checkpointed {
 public:
  int32_t map(const int32_t& value) override {
    ++count;
    return value * 2;
  }

  void open() override { openCalled = true; }
  void close() override { closeCalled = true; }

  // 1.0 Checkpointed.snapshotState：这里把 count 序列化成字节
  std::string snapshotState(int64_t, int64_t) override {
    return std::to_string(count);
  }
  void restoreState(const std::string& state) override {
    count = std::stoi(state);
  }

  int count = 0;
  bool openCalled = false;
  bool closeCalled = false;
};

// 一个只为验证 registerTimer 的算子
class TimerOperator
    : public flink::streaming::api::operators::AbstractStreamOperator,
      public flink::streaming::api::operators::OneInputStreamOperator,
      public flink::streaming::runtime::operators::Triggerable {
 public:
  std::string getSimpleName() const override { return "TimerOperator"; }

  void open() override {
    AbstractStreamOperator::open();
    // 1.0 WindowOperator 就是这样注册 processing-time 窗口触发的
    registerTimer(nowMillis() + 10, this);
  }

  void processElement(const StreamRecord&) override {}
  void processWatermark(const Watermark& mark) override {
    output_->emitWatermark(mark);
  }

  void trigger(int64_t timestamp) override {
    triggeredAt = timestamp;
    ++triggerCount;
  }

  static int64_t nowMillis() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
        .count();
  }

  int64_t triggeredAt = 0;
  int triggerCount = 0;
};

// -------------------------------------------------------------------- helpers

StreamConfig makeConfig(
    std::shared_ptr<flink::streaming::api::operators::StreamOperator> op) {
  StreamConfig config;
  config.setVertexID(1);
  config.setNumberOfInputs(1);
  config.setNumberOfOutputs(1);
  config.setStreamOperator(op);
  config.setStateBackend(std::make_shared<MemoryStateBackend>());
  return config;
}

// ---------------------------------------------------------------------- tests

void testStreamMapLifecycle() {
  auto op = std::make_shared<StreamMap<std::string, std::string> >(
      std::make_shared<ToUpper>());
  CollectingOutput output;
  TestEnvironment env;
  TestOneInputStreamTask task(&env, makeConfig(op), &output);

  // 1.0 StreamMap 用 element.replace(...)：时间戳必须原样保留
  task.inputRecords.push_back(StreamRecord(Any::of(std::string("abc")), 111));
  task.inputRecords.push_back(StreamRecord(Any::of(std::string("dEf"))));
  task.inputWatermarks.push_back(999);

  task.invoke();

  check(task.initCalled && task.cleanupCalled, "StreamTask 走完 init/cleanup");
  check(output.records.size() == 2, "StreamMap 逐条输出");
  check(output.records[0].getValueAs<std::string>() == "ABC" &&
            output.records[1].getValueAs<std::string>() == "DEF",
        "MapFunction 被调用");
  check(output.records[0].getTimestamp() == 111,
        "replace() 保留输入记录的时间戳");
  check(!output.records[1].hasTimestamp(), "无时间戳的记录仍然无时间戳");
  check(output.watermarks.size() == 1 && output.watermarks[0] == 999,
        "processWatermark 转发水位线");
  check(output.closed, "releaseOutputs 关掉了输出");
}

void testStreamFilterAndFlatMap() {
  {
    auto op = std::make_shared<StreamFilter<int32_t> >(
        std::make_shared<IsEven>());
    CollectingOutput output;
    TestEnvironment env;
    TestOneInputStreamTask task(&env, makeConfig(op), &output);
    for (int32_t i = 1; i <= 5; ++i) {
      task.inputRecords.push_back(StreamRecord(Any::of(i), i * 10));
    }
    task.invoke();
    check(output.records.size() == 2 &&
              output.records[0].getValueAs<int32_t>() == 2 &&
              output.records[1].getValueAs<int32_t>() == 4,
          "StreamFilter 只放过满足条件的记录");
    check(output.records[0].getTimestamp() == 20,
          "StreamFilter 原样转发记录（含时间戳）");
  }
  {
    auto op = std::make_shared<StreamFlatMap<std::string, std::string> >(
        std::make_shared<SplitWords>());
    CollectingOutput output;
    TestEnvironment env;
    TestOneInputStreamTask task(&env, makeConfig(op), &output);
    task.inputRecords.push_back(
        StreamRecord(Any::of(std::string("to be or not")), 42));
    task.invoke();
    check(output.records.size() == 4, "StreamFlatMap 一进多出");
    check(output.records[3].getValueAs<std::string>() == "not",
          "FlatMapFunction 的 Collector 生效");
    check(output.records[0].getTimestamp() == 42 &&
              output.records[3].getTimestamp() == 42,
          "TimestampedCollector 让所有输出继承输入时间戳");
  }
}

// StreamGroupedReduce / StreamGroupedFold：keyed state + 首元素分支 +
// keyed state 快照恢复
void testGroupedReduceAndFold() {
  class KeyByParity : public KeySelector<int32_t, int32_t> {
   public:
    int32_t getKey(const int32_t& value) override { return value % 2; }
  };

  class SumReducer : public flink::api::common::functions::ReduceFunction<int32_t> {
   public:
    int32_t reduce(const int32_t& v1, const int32_t& v2) override {
      return v1 + v2;
    }
  };

  class CountFolder
      : public flink::api::common::functions::FoldFunction<int32_t, std::string> {
   public:
    std::string fold(const std::string& accumulator,
                     const int32_t& value) override {
      return accumulator + "-" + std::to_string(value);
    }
  };

  // keyBy(i -> i % 2) 的配置，两个算子共用
  auto keyedConfig = [](std::shared_ptr<
                         flink::streaming::api::operators::StreamOperator> op) {
    StreamConfig config = makeConfig(op);
    config.setStatePartitioner(
        0, flink::api::java::functions::eraseKeySelector<int32_t, int32_t>(
               std::make_shared<KeyByParity>()));
    config.setStateKeySerializer(TypedAnySerializer<int32_t>::create());
    return config;
  };

  StreamTaskStateList reduceSnapshot;
  {
    auto op = std::make_shared<
        flink::streaming::api::operators::StreamGroupedReduce<int32_t> >(
        std::make_shared<SumReducer>(),
        flink::api::java::typeutils::TypeExtractor<int32_t>::createSerializer());
    CollectingOutput output;
    TestEnvironment env;
    TestOneInputStreamTask task(&env, keyedConfig(op), &output);
    for (int32_t i = 1; i <= 6; ++i) {
      task.inputRecords.push_back(StreamRecord(Any::of(i)));
    }
    task.checkpointAfterRecords = 6;
    task.invoke();

    std::vector<int32_t> got;
    for (const auto& record : output.records) {
      got.push_back(record.getValueAs<int32_t>());
    }
    // key 1: 1, 1+3=4, 4+5=9 ；key 0: 2, 2+4=6, 6+6=12
    check(got == std::vector<int32_t>({1, 2, 4, 6, 9, 12}),
          "StreamGroupedReduce 按 key 做滚动聚合");
    check(got[0] == 1 && got[1] == 2,
          "首个元素原样输出（1.0 的 currentValue == null 分支）");
    check(op->getChainingStrategy() ==
              flink::streaming::api::operators::ChainingStrategy::HEAD,
          "StreamGroupedReduce 沿用默认 HEAD 链接策略");
    reduceSnapshot = env.lastState;
  }

  // 用上面那份快照恢复：key 1 的累加值应当是 9，key 0 是 12
  {
    auto op = std::make_shared<
        flink::streaming::api::operators::StreamGroupedReduce<int32_t> >(
        std::make_shared<SumReducer>(),
        flink::api::java::typeutils::TypeExtractor<int32_t>::createSerializer());
    CollectingOutput output;
    TestEnvironment env;
    TestOneInputStreamTask task(&env, keyedConfig(op), &output);
    task.setInitialState(reduceSnapshot, 999);
    task.inputRecords.push_back(StreamRecord(Any::of(7)));   // key 1 -> 9+7
    task.inputRecords.push_back(StreamRecord(Any::of(10)));  // key 0 -> 12+10
    task.invoke();

    check(output.records.size() == 2 &&
              output.records[0].getValueAs<int32_t>() == 16 &&
              output.records[1].getValueAs<int32_t>() == 22,
          "keyed state 从 checkpoint 恢复后继续累加");
  }

  {
    auto op = std::make_shared<
        flink::streaming::api::operators::StreamGroupedFold<int32_t,
                                                            std::string> >(
        std::make_shared<CountFolder>(), std::string("start"));
    CollectingOutput output;
    TestEnvironment env;
    TestOneInputStreamTask task(&env, keyedConfig(op), &output);
    for (int32_t i = 1; i <= 4; ++i) {
      task.inputRecords.push_back(StreamRecord(Any::of(i)));
    }
    task.invoke();

    std::vector<std::string> got;
    for (const auto& record : output.records) {
      got.push_back(record.getValueAs<std::string>());
    }
    check(got == std::vector<std::string>({"start-1", "start-2", "start-1-3",
                                           "start-2-4"}),
          "StreamGroupedFold 首元素用 initialValue，之后接着折叠");
  }
}

void testUdfLifecycleAndCheckpointedState() {
  auto function = std::make_shared<CountingMapper>();
  auto op = std::make_shared<StreamMap<int32_t, int32_t> >(function);
  CollectingOutput output;
  TestEnvironment env;
  TestOneInputStreamTask task(&env, makeConfig(op), &output);

  for (int32_t i = 1; i <= 3; ++i) {
    task.inputRecords.push_back(StreamRecord(Any::of(i)));
  }
  task.checkpointAfterRecords = 2;  // 第 2 条之后触发 checkpoint 7
  task.invoke();

  check(function->openCalled, "AbstractUdfStreamOperator.open 开了 UDF");
  check(function->closeCalled, "AbstractUdfStreamOperator.close 关了 UDF");
  check(output.records.size() == 3 &&
            output.records[2].getValueAs<int32_t>() == 6,
        "UDF 被正确调用");

  check(output.barrierIds.size() == 1 && output.barrierIds[0] == 7,
        "triggerCheckpoint 先广播 CheckpointBarrier");
  check(env.ackedWithState.size() == 1 && env.ackedWithState[0] == 7,
        "有状态时走 acknowledgeCheckpoint(id, state)");

  // 快照里应当带上 Checkpointed UDF 的 functionState（当时 count == 2）
  const auto& states = env.lastState.getState();
  check(states.size() == 1 && states[0] && states[0]->getFunctionState(),
        "StreamTaskState 带上了 functionState");
  if (states.size() == 1 && states[0] && states[0]->getFunctionState()) {
    check(states[0]->getFunctionState()->getState() == "2",
          "functionState 是 checkpoint 那一刻的 UDF 状态");
  }

  // 用这份快照恢复到一个新算子上（1.0 StreamTask.restoreState 的路径）
  auto restoredFunction = std::make_shared<CountingMapper>();
  auto restoredOp =
      std::make_shared<StreamMap<int32_t, int32_t> >(restoredFunction);
  CollectingOutput output2;
  TestEnvironment env2;
  TestOneInputStreamTask task2(&env2, makeConfig(restoredOp), &output2);
  task2.setInitialState(env.lastState, 1234);
  task2.invoke();
  check(restoredFunction->count == 2,
        "Checkpointed.restoreState 把 UDF 状态还回去了");
}

void testKeyedStateThroughOperator() {
  // 1.0 的 keyBy：StreamConfig.setStatePartitioner(0, keySelector) +
  // setStateKeySerializer，算子每条记录前 setKeyContextElement1
  class KeyByParity : public KeySelector<int32_t, int32_t> {
   public:
    int32_t getKey(const int32_t& value) override { return value % 2; }
  };

  // 一个直接用 keyed ValueState 做计数的算子（StreamGroupedReduce 的雏形）
  class PerKeyCounter
      : public flink::streaming::api::operators::AbstractStreamOperator,
        public flink::streaming::api::operators::OneInputStreamOperator {
   public:
    std::string getSimpleName() const override { return "PerKeyCounter"; }

    void open() override {
      AbstractStreamOperator::open();
      // 1.0 StreamGroupedReduce.java:50 就是这样取状态的
      flink::api::common::state::ValueStateDescriptor<int64_t> descriptor(
          "_op_state", 0);
      state_ = getPartitionedState(descriptor);
    }

    void processElement(const StreamRecord& element) override {
      const int64_t next = state_->value() + 1;
      state_->update(next);
      output_->collect(element.replace(flink::types::Any::of(next)));
    }
    void processWatermark(const Watermark& mark) override {
      output_->emitWatermark(mark);
    }

   private:
    std::shared_ptr<flink::api::common::state::ValueState<int64_t> > state_;
  };

  auto op = std::make_shared<PerKeyCounter>();
  StreamConfig config = makeConfig(op);
  config.setStatePartitioner(
      0, flink::api::java::functions::eraseKeySelector<int32_t, int32_t>(
             std::make_shared<KeyByParity>()));
  config.setStateKeySerializer(TypedAnySerializer<int32_t>::create());

  CollectingOutput output;
  TestEnvironment env;
  TestOneInputStreamTask task(&env, config, &output);
  for (int32_t i = 1; i <= 6; ++i) {
    task.inputRecords.push_back(StreamRecord(Any::of(i)));
  }
  task.invoke();

  // 1,3,5 -> key 1（计数 1,2,3）；2,4,6 -> key 0（计数 1,2,3）
  std::vector<int64_t> counts;
  for (const auto& record : output.records) {
    counts.push_back(record.getValueAs<int64_t>());
  }
  check(counts == std::vector<int64_t>({1, 1, 2, 2, 3, 3}),
        "setKeyContextElement1 让状态按 key 分开累加");
}

void testRegisterTimer() {
  auto op = std::make_shared<TimerOperator>();
  CollectingOutput output;
  TestEnvironment env;
  TestOneInputStreamTask task(&env, makeConfig(op), &output);
  task.sleepMillisInRun = 150;
  task.invoke();

  check(op->triggerCount == 1, "registerTimer 的回调被触发了一次");
  check(op->triggeredAt > 0, "回调收到的是注册时指定的绝对时间戳");
}

}  // namespace

int main() {
  testStreamMapLifecycle();
  testStreamFilterAndFlatMap();
  testGroupedReduceAndFold();
  testUdfLifecycleAndCheckpointedState();
  testKeyedStateThroughOperator();
  testRegisterTimer();
  std::printf(failures == 0 ? "\nALL PASSED\n" : "\n%d FAILED\n", failures);
  return failures == 0 ? 0 : 1;
}
