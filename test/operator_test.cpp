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
#include "flink/runtime/io/network/api/EndOfPartitionEvent.h"
#include "flink/runtime/io/network/api/writer/ResultPartitionWriter.h"
#include "flink/runtime/io/network/partition/consumer/SingleInputGate.h"
#include "flink/runtime/state/memory/MemoryStateBackend.h"
#include "flink/streaming/api/checkpoint/Checkpointed.h"
#include "flink/streaming/api/collector/selector/TypedAnyOutputSelector.h"
#include "flink/streaming/api/functions/co/CoFlatMapFunction.h"
#include "flink/streaming/api/functions/co/CoMapFunction.h"
#include "flink/streaming/api/functions/source/SourceFunction.h"
#include "flink/streaming/api/graph/StreamEdge.h"
#include "flink/streaming/api/operators/co/CoStreamFlatMap.h"
#include "flink/streaming/api/operators/co/CoStreamMap.h"
#include "flink/streaming/api/operators/StreamFilter.h"
#include "flink/streaming/api/operators/StreamFlatMap.h"
#include "flink/streaming/api/operators/StreamGroupedFold.h"
#include "flink/streaming/api/operators/StreamGroupedReduce.h"
#include "flink/streaming/api/operators/StreamMap.h"
#include "flink/streaming/api/operators/StreamSource.h"
#include "flink/streaming/runtime/partitioner/ForwardPartitioner.h"
#include "flink/streaming/runtime/tasks/ExceptionInChainedOperatorException.h"
#include "flink/streaming/runtime/tasks/OneInputStreamTask.h"
#include "flink/streaming/runtime/tasks/SourceStreamTask.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"
#include "flink/streaming/runtime/tasks/TwoInputStreamTask.h"

using flink::api::common::functions::FilterFunction;
using flink::api::common::functions::FlatMapFunction;
using flink::api::common::functions::MapFunction;
using flink::api::common::typeutils::TypedAnySerializer;
using flink::api::java::functions::KeySelector;
using flink::runtime::io::network::api::CheckpointBarrier;
using flink::runtime::io::network::api::writer::ResultPartitionWriter;
using flink::runtime::io::network::partition::consumer::BufferOrEvent;
using flink::runtime::io::network::partition::consumer::SingleInputGate;
using flink::runtime::state::memory::MemoryStateBackend;
using flink::streaming::api::graph::StreamConfig;
using flink::streaming::api::graph::StreamEdge;
using flink::streaming::api::graph::StreamNode;
using flink::streaming::api::operators::Output;
using flink::streaming::api::operators::StreamFilter;
using flink::streaming::api::operators::StreamFlatMap;
using flink::streaming::api::operators::StreamMap;
using flink::streaming::api::watermark::Watermark;
using flink::streaming::runtime::partitioner::ForwardPartitioner;
using flink::streaming::runtime::streamrecord::StreamElement;
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

// 1.0 StreamTaskTestHarness 的最小版。StreamTask 现在无条件建 OperatorChain，
// 而 OperatorChain.createStreamOutput 要走 Environment.getWriter(outputIndex)，
// 所以测试必须提供真实的输出链路：
//   RecordWriterOutput -> StreamRecordWriter -> ResultPartitionWriter
//   -> SingleInputGate
// invoke() 结束后把 gate 排空，再按元素类型分到 records / watermarks /
// barrierIds 三个篮子里做断言（1.0 的 harness 也是从输出队列里取 StreamElement
// 做断言的）。
class TestOutput {
 public:
  // 一个下游通道；队列容量取得足够大，这样生产侧不会在无人消费时被背压堵住
  TestOutput() : gate_(1, 1024) {
    std::vector<ResultPartitionWriter::Target> targets;
    targets.push_back(ResultPartitionWriter::Target(&gate_, 0));
    partitionWriter_.reset(new ResultPartitionWriter(targets));
  }

  ResultPartitionWriter* getPartitionWriter() { return partitionWriter_.get(); }

  void drain() {
    gate_.releaseAllResources();
    BufferOrEvent boe;
    while (gate_.getNextBufferOrEvent(boe)) {
      if (boe.isEvent()) {
        const CheckpointBarrier* barrier =
            dynamic_cast<const CheckpointBarrier*>(boe.getEvent().get());
        if (barrier != NULL) {
          barrierIds.push_back(barrier->getId());
          kinds.push_back('B');
        }
        continue;
      }
      const StreamElement* element = boe.getBuffer().get();
      const StreamRecord* record = dynamic_cast<const StreamRecord*>(element);
      if (record != NULL) {
        records.push_back(*record);
        kinds.push_back('R');
        continue;
      }
      const Watermark* mark = dynamic_cast<const Watermark*>(element);
      if (mark != NULL) {
        watermarks.push_back(mark->getTimestamp());
        kinds.push_back('W');
      }
    }
  }

  std::vector<StreamRecord> records;
  std::vector<int64_t> watermarks;
  std::vector<int64_t> barrierIds;
  // 记录/水位线/屏障三者**混在一起的到达顺序**（'R' / 'W' / 'B'）。1.0 的
  // harness 输出队列本来就是一条混合序列，三个分篮子看不出相对位置，所以
  // 额外留这一条用于顺序断言。
  std::vector<char> kinds;

 private:
  SingleInputGate gate_;
  std::unique_ptr<ResultPartitionWriter> partitionWriter_;
};

// 1.0 Environment 的测试替身（对应 1.0 测试里的 MockEnvironment）
class TestEnvironment : public flink::runtime::execution::Environment {
 public:
  explicit TestEnvironment(ResultPartitionWriter* writer = NULL) {
    if (writer != NULL) writers_.push_back(writer);
  }
  explicit TestEnvironment(const std::vector<ResultPartitionWriter*>& writers)
      : writers_(writers) {}

  std::string getTaskName() const override { return "TestTask"; }
  std::string getTaskNameWithSubtasks() const override {
    return "TestTask (1/1)";
  }
  int getIndexInSubtaskGroup() const override { return 0; }
  int getNumberOfSubtasks() const override { return 1; }

  // 1.0 getWriter(int)：OperatorChain 按 outEdgesInOrder 的下标来取
  ResultPartitionWriter* getWriter(int index) override {
    if (index < 0 || index >= static_cast<int>(writers_.size())) return NULL;
    return writers_[index];
  }
  int getNumberOfWriters() const override {
    return static_cast<int>(writers_.size());
  }

  // 1.0 getInputGate(int) / getAllInputGates()：OneInputStreamTask.init() 用
  void addInputGate(SingleInputGate* gate) { inputGates_.push_back(gate); }
  flink::runtime::io::network::partition::consumer::InputGate* getInputGate(
      int index) override {
    if (index < 0 || index >= static_cast<int>(inputGates_.size())) return NULL;
    return inputGates_[index];
  }
  int getNumberOfInputGates() const override {
    return static_cast<int>(inputGates_.size());
  }

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

 private:
  std::vector<ResultPartitionWriter*> writers_;
  std::vector<SingleInputGate*> inputGates_;
};

// 1.0 OneInputStreamTask 的最小替身：run() 里做的事等价于
// StreamInputProcessor.processInput —— 每条记录先 setKeyContextElement1
// 再 processElement，水位线走 processWatermark
class TestOneInputStreamTask : public StreamTask {
 public:
  typedef flink::streaming::api::operators::OneInputStreamOperator
      OneInputStreamOperator;

  TestOneInputStreamTask(TestEnvironment* env, const StreamConfig& config)
      : StreamTask(env, config) {}


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
        std::lock_guard<std::recursive_mutex> guard(getCheckpointLock());
        op->setKeyContextElement1(inputRecords[i]);
        op->processElement(inputRecords[i]);
      }
      if (checkpointAfterRecords >= 0 &&
          static_cast<int64_t>(i + 1) == checkpointAfterRecords) {
        triggerCheckpoint(7, 700);
      }
    }
    for (int64_t ts : inputWatermarks) {
      std::lock_guard<std::recursive_mutex> guard(getCheckpointLock());
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

// 1.0 的 StreamEdge 由 StreamGraph 建、顶点由 StreamGraph 的 streamNodes map
// 持有（边只存 StreamNode*，见 StreamEdge.h 偏差 1）。这里手搭最小的一条出边，
// 顶点放进一个静态池里代替 StreamGraph 的所有权。
std::vector<std::shared_ptr<StreamNode> >& nodePool() {
  static std::vector<std::shared_ptr<StreamNode> > pool;
  return pool;
}

std::shared_ptr<StreamEdge> makeForwardEdge(
    int sourceId, int targetId,
    const std::vector<std::string>& selectedNames =
        std::vector<std::string>(),
    int typeNumber = 1) {
  const std::vector<StreamNode::OutputSelectorPtr> noSelectors;
  std::shared_ptr<StreamNode> source(new StreamNode(
      sourceId, "", StreamNode::StreamOperatorPtr(), "Operator", noSelectors,
      "OneInputStreamTask", 1, StreamConfig::DEFAULT_TIMEOUT));
  std::shared_ptr<StreamNode> target(new StreamNode(
      targetId, "", StreamNode::StreamOperatorPtr(), "Sink", noSelectors,
      "OneInputStreamTask", 1, StreamConfig::DEFAULT_TIMEOUT));
  nodePool().push_back(source);
  nodePool().push_back(target);

  // 1.0: typeNumber 1（单输入 / 两输入的第一条）；2 是两输入的第二条；
  // selectedNames 为空表示没有 split/select
  std::shared_ptr<StreamEdge> edge(
      new StreamEdge(source.get(), target.get(), typeNumber, selectedNames,
                     std::make_shared<ForwardPartitioner>()));
  source->addOutEdge(edge);
  target->addInEdge(edge);
  return edge;
}

// 一个算子的基础配置，不带任何出边（链内算子的 config 就长这样）
StreamConfig makeBareConfig(
    int vertexId,
    std::shared_ptr<flink::streaming::api::operators::StreamOperator> op) {
  StreamConfig config;
  config.setVertexID(vertexId);
  config.setNumberOfInputs(1);
  config.setNumberOfOutputs(1);
  config.setStreamOperator(op);
  config.setStateBackend(std::make_shared<MemoryStateBackend>());
  return config;
}

StreamConfig makeConfig(
    std::shared_ptr<flink::streaming::api::operators::StreamOperator> op) {
  StreamConfig config = makeBareConfig(1, op);

  // OperatorChain 的输入（1.0 由 StreamingJobGraphGenerator 写进 StreamConfig）：
  // 一条出边，走网络（nonChainedOutputs），在 outEdgesInOrder 里的下标 0
  // 对应 Environment.getWriter(0)
  StreamConfig::StreamEdgeList outEdges;
  outEdges.push_back(makeForwardEdge(config.getVertexID(), 2));
  config.setOutEdges(outEdges);
  config.setOutEdgesInOrder(outEdges);
  config.setNonChainedOutputs(outEdges);
  return config;
}

// ---------------------------------------------------------------------- tests

void testStreamMapLifecycle() {
  auto op = std::make_shared<StreamMap<std::string, std::string> >(
      std::make_shared<ToUpper>());
  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestOneInputStreamTask task(&env, makeConfig(op));

  // 1.0 StreamMap 用 element.replace(...)：时间戳必须原样保留
  task.inputRecords.push_back(StreamRecord(Any::of(std::string("abc")), 111));
  task.inputRecords.push_back(StreamRecord(Any::of(std::string("dEf"))));
  task.inputWatermarks.push_back(999);

  task.invoke();
  output.drain();

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

  // OperatorChain 建出来的形状：一条网络出边 + 只有 head 算子的链
  check(task.getStreamOutputs().size() == 1,
        "OperatorChain 为 outEdgesInOrder 的每条边建一个 RecordWriterOutput");
  check(task.getAllOperators().size() == 1 &&
            task.getAllOperators().back() == op.get(),
        "allOperators 的最后一个是 headOperator");
  check(task.getHeadOutput() == task.getStreamOutputs()[0],
        "单个非链接输出时 chainEntryPoint 就是那个 RecordWriterOutput");
}

void testStreamFilterAndFlatMap() {
  {
    auto op = std::make_shared<StreamFilter<int32_t> >(
        std::make_shared<IsEven>());
    TestOutput output;
    TestEnvironment env(output.getPartitionWriter());
    TestOneInputStreamTask task(&env, makeConfig(op));
    for (int32_t i = 1; i <= 5; ++i) {
      task.inputRecords.push_back(StreamRecord(Any::of(i), i * 10));
    }
    task.invoke();
    output.drain();
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
    TestOutput output;
    TestEnvironment env(output.getPartitionWriter());
    TestOneInputStreamTask task(&env, makeConfig(op));
    task.inputRecords.push_back(
        StreamRecord(Any::of(std::string("to be or not")), 42));
    task.invoke();
    output.drain();
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
    TestOutput output;
    TestEnvironment env(output.getPartitionWriter());
    TestOneInputStreamTask task(&env, keyedConfig(op));
    for (int32_t i = 1; i <= 6; ++i) {
      task.inputRecords.push_back(StreamRecord(Any::of(i)));
    }
    task.checkpointAfterRecords = 6;
    task.invoke();
    output.drain();

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
    TestOutput output;
    TestEnvironment env(output.getPartitionWriter());
    TestOneInputStreamTask task(&env, keyedConfig(op));
    task.setInitialState(reduceSnapshot, 999);
    task.inputRecords.push_back(StreamRecord(Any::of(7)));   // key 1 -> 9+7
    task.inputRecords.push_back(StreamRecord(Any::of(10)));  // key 0 -> 12+10
    task.invoke();
    output.drain();

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
    TestOutput output;
    TestEnvironment env(output.getPartitionWriter());
    TestOneInputStreamTask task(&env, keyedConfig(op));
    for (int32_t i = 1; i <= 4; ++i) {
      task.inputRecords.push_back(StreamRecord(Any::of(i)));
    }
    task.invoke();
    output.drain();

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
  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestOneInputStreamTask task(&env, makeConfig(op));

  for (int32_t i = 1; i <= 3; ++i) {
    task.inputRecords.push_back(StreamRecord(Any::of(i)));
  }
  task.checkpointAfterRecords = 2;  // 第 2 条之后触发 checkpoint 7
  task.invoke();
  output.drain();

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
  TestOutput output2;
  TestEnvironment env2(output2.getPartitionWriter());
  TestOneInputStreamTask task2(&env2, makeConfig(restoredOp));
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

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestOneInputStreamTask task(&env, config);
  for (int32_t i = 1; i <= 6; ++i) {
    task.inputRecords.push_back(StreamRecord(Any::of(i)));
  }
  task.invoke();
  output.drain();

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
  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestOneInputStreamTask task(&env, makeConfig(op));
  task.sleepMillisInRun = 150;
  task.invoke();

  check(op->triggerCount == 1, "registerTimer 的回调被触发了一次");
  check(op->triggeredAt > 0, "回调收到的是注册时指定的绝对时间戳");
}

// ------------------------------------------------------- OperatorChain 的测试

// 一个「输入 + delta 再下发」的算子，顺带把 open/close 记到共享的 trace 里，
// 用来验证 1.0 的 open/close 遍历方向
class TracingOperator
    : public flink::streaming::api::operators::AbstractStreamOperator,
      public flink::streaming::api::operators::OneInputStreamOperator {
 public:
  TracingOperator(const std::string& name, int32_t delta,
                  std::vector<std::string>* trace)
      : name_(name), delta_(delta), trace_(trace) {}

  std::string getSimpleName() const override { return name_; }

  void open() override {
    AbstractStreamOperator::open();
    trace_->push_back("open " + name_);
  }
  void close() override {
    trace_->push_back("close " + name_);
    AbstractStreamOperator::close();
  }

  void processElement(const StreamRecord& element) override {
    output_->collect(
        element.replace(Any::of(element.getValueAs<int32_t>() + delta_)));
  }
  void processWatermark(const Watermark& mark) override {
    output_->emitWatermark(mark);
  }

 private:
  const std::string name_;
  const int32_t delta_;
  std::vector<std::string>* trace_;
};

// 1.0 OperatorChain.createChainedOperator + ChainingOutput：
// head(vertex 1) --链内--> chained(vertex 2) --网络--> vertex 3
void testChainedOperators() {
  std::vector<std::string> trace;
  auto headOp = std::make_shared<TracingOperator>("head", 1, &trace);
  auto chainedOp = std::make_shared<TracingOperator>("chained", 10, &trace);

  std::shared_ptr<StreamEdge> chainEdge = makeForwardEdge(1, 2);
  std::shared_ptr<StreamEdge> networkEdge = makeForwardEdge(2, 3);
  StreamConfig::StreamEdgeList networkEdges;
  networkEdges.push_back(networkEdge);

  // 链内算子的配置：它的出边才是真正走网络的那条
  auto chainedConfig =
      std::make_shared<StreamConfig>(makeBareConfig(2, chainedOp));
  chainedConfig->setNonChainedOutputs(networkEdges);

  StreamConfig config = makeBareConfig(1, headOp);
  StreamConfig::StreamEdgeList chainedOutputs;
  chainedOutputs.push_back(chainEdge);
  config.setChainedOutputs(chainedOutputs);
  // 1.0: outEdgesInOrder 挂在链头的配置上，装的是**整条链**对外的出边；
  // createStreamOutput 用 edge.getSourceId() 回查该边所属算子的配置
  config.setOutEdgesInOrder(networkEdges);
  StreamConfig::ChainedConfigMap chainedConfigs;
  chainedConfigs[2] = chainedConfig;
  config.setTransitiveChainedTaskConfigs(chainedConfigs);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestOneInputStreamTask task(&env, config);
  task.inputRecords.push_back(StreamRecord(Any::of<int32_t>(1), 5));
  task.inputWatermarks.push_back(77);
  task.invoke();
  output.drain();

  check(output.records.size() == 1 &&
            output.records[0].getValueAs<int32_t>() == 12,
        "ChainingOutput 把 head 的输出直接交给链内下一个算子（1+1+10）");
  check(output.records.size() == 1 && output.records[0].getTimestamp() == 5,
        "链内直接方法调用不丢时间戳");
  check(output.watermarks.size() == 1 && output.watermarks[0] == 77,
        "水位线沿链一路传到网络输出");

  std::vector<StreamTask::StreamOperator*> all = task.getAllOperators();
  check(all.size() == 2 && all[0] == chainedOp.get() && all[1] == headOp.get(),
        "allOperators：链尾在前，headOperator 追加在最后");
  check(trace == std::vector<std::string>({"open chained", "open head",
                                           "close head", "close chained"}),
        "open 正序（链尾->链头）、close 倒序（链头->链尾）");
  check(task.getStreamOutputs().size() == 1,
        "链内的边不建 RecordWriterOutput，只有链尾那条网络边建");
}

// 1.0 OperatorChain.BroadcastingOutputCollector：多个输出时包成一个
void testBroadcastingOutputCollector() {
  std::vector<std::string> trace;
  auto op = std::make_shared<TracingOperator>("head", 0, &trace);

  StreamConfig config = makeBareConfig(1, op);
  StreamConfig::StreamEdgeList outEdges;
  outEdges.push_back(makeForwardEdge(1, 2));
  outEdges.push_back(makeForwardEdge(1, 3));
  config.setOutEdges(outEdges);
  config.setOutEdgesInOrder(outEdges);
  config.setNonChainedOutputs(outEdges);

  TestOutput out0;
  TestOutput out1;
  std::vector<ResultPartitionWriter*> writers;
  writers.push_back(out0.getPartitionWriter());
  writers.push_back(out1.getPartitionWriter());
  TestEnvironment env(writers);

  TestOneInputStreamTask task(&env, config);
  task.inputRecords.push_back(StreamRecord(Any::of<int32_t>(42)));
  task.inputWatermarks.push_back(5);
  task.checkpointAfterRecords = 1;
  task.invoke();
  out0.drain();
  out1.drain();

  check(task.getStreamOutputs().size() == 2,
        "outEdgesInOrder 的每条边各建一个 RecordWriterOutput");
  check(task.getHeadOutput() != task.getStreamOutputs()[0] &&
            task.getHeadOutput() != task.getStreamOutputs()[1],
        "多个输出时 chainEntryPoint 是 BroadcastingOutputCollector");
  check(out0.records.size() == 1 && out1.records.size() == 1 &&
            out0.records[0].getValueAs<int32_t>() == 42 &&
            out1.records[0].getValueAs<int32_t>() == 42,
        "BroadcastingOutputCollector.collect 发给全部输出");
  check(out0.watermarks == std::vector<int64_t>({5}) &&
            out1.watermarks == std::vector<int64_t>({5}),
        "BroadcastingOutputCollector.emitWatermark 发给全部输出");
  check(out0.barrierIds == std::vector<int64_t>({7}) &&
            out1.barrierIds == std::vector<int64_t>({7}),
        "broadcastCheckpointBarrier 逐个 streamOutput 广播屏障");
}

// 1.0 DirectedOutputTest.outputSelectorTest（DirectedOutputTest.java:96）的
// 逐条移植：1.0 是 job 级 ITCase（generateSequence(1,11).split(...)），这里
// 把同一套选择器与四个下游搬到 OperatorChain 这一层，输入与期望值照抄。
void testDirectedOutput() {
  const std::string TEN = "ten";
  const std::string ODD = "odd";
  const std::string EVEN = "even";
  const std::string NON_SELECTED = "nonSelected";

  // 1.0 的 MyOutputSelector（:44）
  class MyOutputSelector
      : public flink::streaming::api::collector::selector::OutputSelector<
            int32_t> {
   public:
    std::vector<std::string> select(const int32_t& value) override {
      std::vector<std::string> outputs;
      if (value % 2 == 0) {
        outputs.push_back("even");
      } else {
        outputs.push_back("odd");
      }
      if (value == 10) outputs.push_back("ten");
      if (value == 11) outputs.push_back("nonSelected");
      return outputs;
    }
  };

  // 原样转发的算子（1.0 那边是 source 直接 split）
  class ForwardingOperator
      : public flink::streaming::api::operators::AbstractStreamOperator,
        public flink::streaming::api::operators::OneInputStreamOperator {
   public:
    std::string getSimpleName() const override { return "Forwarding"; }
    void processElement(const StreamRecord& element) override {
      output_->collect(element);
    }
    void processWatermark(const Watermark& mark) override {
      output_->emitWatermark(mark);
    }
  };

  auto op = std::make_shared<ForwardingOperator>();
  StreamConfig config = makeBareConfig(1, op);

  // 1.0: source.select(EVEN) / select(ODD, TEN) / select(EVEN, ODD) /
  //      addSink(allSink)（最后这个没 select，selectedNames 为空 = 全收）
  StreamConfig::StreamEdgeList outEdges;
  outEdges.push_back(makeForwardEdge(1, 2, std::vector<std::string>({EVEN})));
  outEdges.push_back(
      makeForwardEdge(1, 3, std::vector<std::string>({ODD, TEN})));
  outEdges.push_back(
      makeForwardEdge(1, 4, std::vector<std::string>({EVEN, ODD})));
  outEdges.push_back(makeForwardEdge(1, 5, std::vector<std::string>()));
  config.setOutEdges(outEdges);
  config.setOutEdgesInOrder(outEdges);
  config.setNonChainedOutputs(outEdges);

  std::vector<StreamConfig::OutputSelectorPtr> selectors;
  selectors.push_back(
      flink::streaming::api::collector::selector::eraseOutputSelector<int32_t>(
          std::make_shared<MyOutputSelector>()));
  config.setOutputSelectors(selectors);
  (void)NON_SELECTED;  // 1.0 里这个名字没有任何下游订阅，记录被丢弃

  std::vector<std::unique_ptr<TestOutput> > outputs;
  std::vector<ResultPartitionWriter*> writers;
  for (int i = 0; i < 4; ++i) {
    outputs.push_back(std::unique_ptr<TestOutput>(new TestOutput()));
    writers.push_back(outputs.back()->getPartitionWriter());
  }
  TestEnvironment env(writers);

  TestOneInputStreamTask task(&env, config);
  for (int32_t i = 1; i <= 11; ++i) {
    task.inputRecords.push_back(StreamRecord(Any::of(i)));
  }
  task.inputWatermarks.push_back(1000);
  task.invoke();

  std::vector<std::vector<int32_t> > got(4);
  for (size_t i = 0; i < outputs.size(); ++i) {
    outputs[i]->drain();
    for (const StreamRecord& record : outputs[i]->records) {
      got[i].push_back(record.getValueAs<int32_t>());
    }
  }

  check(task.getHeadOutput() != NULL &&
            task.getStreamOutputs().size() == 4,
        "有 OutputSelector 时 chainEntryPoint 是 DirectedOutput");
  // 1.0: assertEquals(Arrays.asList(2,4,6,8,10), evenSink)
  check(got[0] == std::vector<int32_t>({2, 4, 6, 8, 10}),
        "select(EVEN) 只收偶数");
  // 1.0: assertEquals(Arrays.asList(1,3,5,7,9,10,11), oddAndTenSink)
  check(got[1] == std::vector<int32_t>({1, 3, 5, 7, 9, 10, 11}),
        "select(ODD, TEN) 收奇数外加 10");
  // 1.0: assertEquals(1..11, evenAndOddSink)
  check(got[2] == std::vector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
        "select(EVEN, ODD) 收全部");
  // 1.0: assertEquals(1..11, allSink)
  check(got[3] == std::vector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
        "没 select 的下游（selectedNames 为空）无条件全收");
  // 1.0 DirectedOutput.emitWatermark：水位线不经过选择器，发给 allOutputs
  bool allGotWatermark = true;
  for (size_t i = 0; i < outputs.size(); ++i) {
    allGotWatermark = allGotWatermark &&
                      outputs[i]->watermarks == std::vector<int64_t>({1000});
  }
  check(allGotWatermark, "水位线不经过选择器，发给全部输出");
}

// 1.0 ChainingOutput 的三个 catch：链内算子抛的异常包成
// ExceptionInChainedOperatorException
void testExceptionInChainedOperator() {
  class ThrowingOperator
      : public flink::streaming::api::operators::AbstractStreamOperator,
        public flink::streaming::api::operators::OneInputStreamOperator {
   public:
    std::string getSimpleName() const override { return "Throwing"; }
    void processElement(const StreamRecord&) override {
      throw std::runtime_error("boom");
    }
    void processWatermark(const Watermark& mark) override {
      output_->emitWatermark(mark);
    }
  };

  std::vector<std::string> trace;
  auto headOp = std::make_shared<TracingOperator>("head", 0, &trace);
  auto chainedOp = std::make_shared<ThrowingOperator>();

  std::shared_ptr<StreamEdge> chainEdge = makeForwardEdge(1, 2);
  std::shared_ptr<StreamEdge> networkEdge = makeForwardEdge(2, 3);
  StreamConfig::StreamEdgeList networkEdges;
  networkEdges.push_back(networkEdge);

  auto chainedConfig =
      std::make_shared<StreamConfig>(makeBareConfig(2, chainedOp));
  chainedConfig->setNonChainedOutputs(networkEdges);

  StreamConfig config = makeBareConfig(1, headOp);
  StreamConfig::StreamEdgeList chainedOutputs;
  chainedOutputs.push_back(chainEdge);
  config.setChainedOutputs(chainedOutputs);
  config.setOutEdgesInOrder(networkEdges);
  StreamConfig::ChainedConfigMap chainedConfigs;
  chainedConfigs[2] = chainedConfig;
  config.setTransitiveChainedTaskConfigs(chainedConfigs);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestOneInputStreamTask task(&env, config);
  task.inputRecords.push_back(StreamRecord(Any::of<int32_t>(1)));

  bool threw = false;
  std::string originalCause;
  try {
    task.invoke();
  } catch (const flink::streaming::runtime::tasks::
               ExceptionInChainedOperatorException& e) {
    threw = true;
    originalCause = e.getOriginalCause();
  }

  check(threw, "链内算子的异常以 ExceptionInChainedOperatorException 往上抛");
  check(originalCause == "boom", "getOriginalCause 剥到最初那个异常");
}

// ------------------------------------------- OneInputStreamTask / SourceStreamTask

// 输入侧的喂数据：直接 put 进 SingleInputGate，再 releaseAllResources() 表示
// 「上游全部结束」（与 io_test 同一手法；1.0 那边是 StreamTaskTestHarness
//  往 MockInputGate 里塞 Buffer）。channel 对应 1.0
//  harness.processElement(element, inputIndex, channelIndex) 的第三个参数。
void putAnyRecord(SingleInputGate& gate, const Any& value, int64_t timestamp,
                  int channel = 0) {
  gate.put(BufferOrEvent(
      std::shared_ptr<const StreamElement>(new StreamRecord(value, timestamp)),
      channel));
}

void putRecord(SingleInputGate& gate, int32_t value, int64_t timestamp,
               int channel = 0) {
  putAnyRecord(gate, Any::of(value), timestamp, channel);
}

void putWatermark(SingleInputGate& gate, int64_t timestamp, int channel = 0) {
  gate.put(BufferOrEvent(
      std::shared_ptr<const StreamElement>(new Watermark(timestamp)), channel));
}

void putEvent(SingleInputGate& gate,
              const std::shared_ptr<const flink::runtime::event::AbstractEvent>&
                  event,
              int channel = 0) {
  gate.put(BufferOrEvent(event, channel));
}

// 每个通道一个 EndOfPartitionEvent（1.0 harness.endInput() 对每条通道都发）
void putEndOfPartition(SingleInputGate& gate, int channel = 0) {
  putEvent(gate,
           std::make_shared<
               const flink::runtime::io::network::api::EndOfPartitionEvent>(),
           channel);
}

// 1.0 OneInputStreamTask：init() 建 StreamInputProcessor，run() 就是
// `while (running && inputProcessor.processInput(operator, lock))`
void testOneInputStreamTask() {
  auto function = std::make_shared<CountingMapper>();  // value * 2
  auto op = std::make_shared<StreamMap<int32_t, int32_t> >(function);

  SingleInputGate inputGate(1, 64);
  putRecord(inputGate, 1, 100);
  putRecord(inputGate, 2, 200);
  putWatermark(inputGate, 300);
  putEndOfPartition(inputGate);
  inputGate.releaseAllResources();

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  env.addInputGate(&inputGate);

  StreamConfig config = makeConfig(op);
  config.setCheckpointMode(flink::streaming::api::CheckpointingMode::
                               AT_LEAST_ONCE);

  flink::streaming::runtime::tasks::OneInputStreamTask task(&env, config);
  task.invoke();
  output.drain();

  check(output.records.size() == 2 &&
            output.records[0].getValueAs<int32_t>() == 2 &&
            output.records[1].getValueAs<int32_t>() == 4,
        "OneInputStreamTask 把输入 gate 的记录逐条喂给 headOperator");
  check(output.records.size() == 2 && output.records[0].getTimestamp() == 100 &&
            output.records[1].getTimestamp() == 200,
        "记录的时间戳一路带到输出");
  check(output.watermarks == std::vector<int64_t>({300}),
        "StreamInputProcessor 把水位线交给 processWatermark 再转发");
  check(function->openCalled && function->closeCalled,
        "OneInputStreamTask 走完 open/close（UDF 生命周期完整）");
  check(function->count == 2, "processInput 的返回值驱动主循环，两条都处理了");
}

// 1.0 的 checkpoint 触发路径之二：屏障从**输入**进来 ->
// CheckpointBarrierHandler -> getCheckpointBarrierListener() ->
// triggerCheckpoint（StreamTask.java:673）
void testOneInputStreamTaskCheckpointBarrier() {
  auto function = std::make_shared<CountingMapper>();
  auto op = std::make_shared<StreamMap<int32_t, int32_t> >(function);

  SingleInputGate inputGate(1, 64);
  putRecord(inputGate, 1, 100);
  putEvent(inputGate, std::make_shared<const CheckpointBarrier>(9, 900));
  putRecord(inputGate, 2, 200);
  putEndOfPartition(inputGate);
  inputGate.releaseAllResources();

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  env.addInputGate(&inputGate);

  StreamConfig config = makeConfig(op);
  config.setCheckpointMode(flink::streaming::api::CheckpointingMode::
                               AT_LEAST_ONCE);

  flink::streaming::runtime::tasks::OneInputStreamTask task(&env, config);
  task.invoke();
  output.drain();

  check(output.records.size() == 2, "屏障不影响记录的处理");
  check(output.barrierIds == std::vector<int64_t>({9}),
        "收到输入屏障后向下游广播同一个屏障");
  check(env.ackedWithState == std::vector<int64_t>({9}),
        "屏障触发的 checkpoint 也会 acknowledge");
  const auto& states = env.lastState.getState();
  check(states.size() == 1 && states[0] && states[0]->getFunctionState() &&
            states[0]->getFunctionState()->getState() == "1",
        "快照抓到的是「屏障那一刻」的 UDF 状态（只处理过 1 条）");
}

// ------------------------------- source 用的 UDF -------------------------------

// 1.0 SourceFunction 文档里的标准写法（SourceFunction.java:48-74）
class CountingSource
    : public flink::streaming::api::functions::source::SourceFunction<int32_t> {
 public:
  explicit CountingSource(int32_t limit)
      : limit_(limit), emitted_(0), isRunning_(true) {}

  void run(flink::streaming::api::functions::source::SourceContext<int32_t>& ctx)
      override {
    while (isRunning_ && emitted_ < limit_) {
      // 注意这里嵌套加了两次同一把锁（外面一层 + ctx.collect 里一层）——
      // 这正是 checkpointLock 必须可重入的原因（StreamTask.h 偏差 4）
      std::lock_guard<std::recursive_mutex> guard(ctx.getCheckpointLock());
      ctx.collect(emitted_ + 1);
      ++emitted_;
    }
  }

  void cancel() override { isRunning_ = false; }

  int32_t emittedCount() const { return emitted_; }

 private:
  const int32_t limit_;
  int32_t emitted_;
  volatile bool isRunning_;
};

// 自己带时间戳与水位线的 source（1.0 的 EventTimeSourceFunction 场景）
class TimestampedSource
    : public flink::streaming::api::functions::source::SourceFunction<int32_t> {
 public:
  void run(flink::streaming::api::functions::source::SourceContext<int32_t>& ctx)
      override {
    for (int32_t i = 1; i <= 3; ++i) {
      std::lock_guard<std::recursive_mutex> guard(ctx.getCheckpointLock());
      ctx.collectWithTimestamp(i, i * 1000);
      ctx.emitWatermark(Watermark(i * 1000));
    }
  }
  void cancel() override {}
};

// 发完 3 条就空转，等着被 cancel —— 用来验证 isCanceledOrStopped() 的分支
class CancellableSource
    : public flink::streaming::api::functions::source::SourceFunction<int32_t> {
 public:
  CancellableSource() : emitted_(0), isRunning_(true) {}

  void run(flink::streaming::api::functions::source::SourceContext<int32_t>& ctx)
      override {
    while (isRunning_) {
      if (emitted_ < 3) {
        std::lock_guard<std::recursive_mutex> guard(ctx.getCheckpointLock());
        ctx.collect(emitted_ + 1);
        ++emitted_;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
  }
  void cancel() override { isRunning_ = false; }

  int32_t emittedCount() const { return emitted_; }

 private:
  volatile int32_t emitted_;
  volatile bool isRunning_;
};

// 1.0 SourceStreamTask.run()：headOperator.run(getCheckpointLock(),
// getHeadOutput())；ProcessingTime -> NonTimestampContext
void testSourceStreamTaskProcessingTime() {
  auto source = std::make_shared<CountingSource>(5);
  auto op = std::make_shared<
      flink::streaming::api::operators::StreamSource<int32_t, CountingSource> >(
      source);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  StreamConfig config = makeConfig(op);  // 默认就是 ProcessingTime

  flink::streaming::runtime::tasks::SourceStreamTask task(&env, config);
  task.invoke();
  output.drain();

  std::vector<int32_t> got;
  bool anyTimestamp = false;
  for (const StreamRecord& record : output.records) {
    got.push_back(record.getValueAs<int32_t>());
    anyTimestamp = anyTimestamp || record.hasTimestamp();
  }
  check(got == std::vector<int32_t>({1, 2, 3, 4, 5}),
        "SourceStreamTask 跑完 SourceFunction.run，元素按序发出");
  check(!anyTimestamp, "NonTimestampContext 产出的记录不带时间戳");
  check(output.watermarks.empty(),
        "NonTimestampContext 丢弃水位线，连收尾的 MAX_WATERMARK 也不发");
  check(source->emittedCount() == 5, "UDF 自己也数到 5");
  check(op->getChainingStrategy() ==
            flink::streaming::api::operators::ChainingStrategy::HEAD,
        "1.0 StreamSource 构造里把链接策略设成 HEAD");
}

// EventTime -> ManualWatermarkContext：时间戳与水位线全听 source 的
void testSourceStreamTaskEventTime() {
  auto source = std::make_shared<TimestampedSource>();
  auto op = std::make_shared<flink::streaming::api::operators::StreamSource<
      int32_t, TimestampedSource> >(source);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  StreamConfig config = makeConfig(op);
  config.setTimeCharacteristic(
      flink::streaming::api::TimeCharacteristic::EventTime);

  flink::streaming::runtime::tasks::SourceStreamTask task(&env, config);
  task.invoke();
  output.drain();

  std::vector<int64_t> timestamps;
  for (const StreamRecord& record : output.records) {
    timestamps.push_back(record.getTimestamp());
  }
  check(timestamps == std::vector<int64_t>({1000, 2000, 3000}),
        "ManualWatermarkContext.collectWithTimestamp 原样带上时间戳");
  check(output.watermarks ==
            std::vector<int64_t>({1000, 2000, 3000,
                                  Watermark::maxTimestamp()}),
        "source 自己发的水位线被转发；跑完还补一个 MAX_WATERMARK");
}

// IngestionTime -> AutomaticWatermarkContext：记录带 currentTimeMillis，
// 水位线由后台定时器按 autoWatermarkInterval 周期发
void testSourceStreamTaskIngestionTime() {
  auto source = std::make_shared<CountingSource>(3);
  auto op = std::make_shared<
      flink::streaming::api::operators::StreamSource<int32_t, CountingSource> >(
      source);
  // 【偏差 3】1.0 从 ExecutionConfig 取（非 ProcessingTime 时默认 200）
  check(op->getAutoWatermarkInterval() == 200,
        "autoWatermarkInterval 默认值取 1.0 的 200");
  op->setAutoWatermarkInterval(5);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  StreamConfig config = makeConfig(op);
  config.setTimeCharacteristic(
      flink::streaming::api::TimeCharacteristic::IngestionTime);

  flink::streaming::runtime::tasks::SourceStreamTask task(&env, config);
  task.invoke();
  output.drain();

  bool allTimestamped = output.records.size() == 3;
  for (const StreamRecord& record : output.records) {
    allTimestamped = allTimestamped && record.hasTimestamp();
  }
  check(allTimestamped,
        "AutomaticWatermarkContext 给每条记录打上 currentTimeMillis");

  bool monotonic = !output.watermarks.empty();
  for (size_t i = 1; i < output.watermarks.size(); ++i) {
    monotonic = monotonic && output.watermarks[i] >= output.watermarks[i - 1];
  }
  check(monotonic, "自动水位线单调不减（1.0 的 nextWatermarkTime 推进逻辑）");
  check(!output.watermarks.empty() &&
            output.watermarks.back() == Watermark::maxTimestamp(),
        "收尾的 MAX_WATERMARK 也发出去了（并顺手停掉水位线定时器）");
}

// 1.0 cancelTask() -> headOperator.cancel()：markCanceledOrStopped() 在前，
// 于是 run() 退出时**不**补 MAX_WATERMARK
void testSourceStreamTaskCancel() {
  auto source = std::make_shared<CancellableSource>();
  auto op = std::make_shared<flink::streaming::api::operators::StreamSource<
      int32_t, CancellableSource> >(source);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  StreamConfig config = makeConfig(op);
  config.setTimeCharacteristic(
      flink::streaming::api::TimeCharacteristic::EventTime);

  flink::streaming::runtime::tasks::SourceStreamTask task(&env, config);

  std::thread canceller([&task, &source]() {
    while (source->emittedCount() < 3) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    task.cancel();
  });
  task.invoke();
  canceller.join();
  output.drain();

  check(output.records.size() == 3, "cancel 之前发出的记录都到了下游");
  check(output.watermarks.empty(),
        "被 cancel 的 source 不补 MAX_WATERMARK（isCanceledOrStopped 分支）");
  check(task.isCanceled(), "StreamTask.cancel 置上了 canceled 标志");
}

// ------------------------- 两输入：CoStreamMap / CoStreamFlatMap -------------

// 1.0 CoStreamMapTest.MyCoMap（CoStreamMapTest.java:45）
// Java 的 Double.toString(1.1) 是 "1.1"，C++ 的 std::to_string 会给
// "1.100000"，所以用 ostringstream 的默认格式（6 位有效数字）对齐。
class MyCoMap : public flink::streaming::api::functions::co::CoMapFunction<
                    double, int32_t, std::string> {
 public:
  std::string map1(const double& value) override {
    std::ostringstream os;
    os << value;
    return os.str();
  }
  std::string map2(const int32_t& value) override {
    return std::to_string(value);
  }
};

// 1.0 CoStreamMapTest.TestOpenCloseCoMapFunction（:122）：
// RichCoMapFunction，open 必须在 map1/map2 之前、close 之后
class TestOpenCloseCoMapFunction
    : public flink::api::common::functions::AbstractRichFunction,
      public flink::streaming::api::functions::co::CoMapFunction<
          double, int32_t, std::string> {
 public:
  void open() override {
    if (closeCalled) ++ordering;  // 1.0: Assert.fail("Close called before open.")
    openCalled = true;
  }
  void close() override {
    if (!openCalled) ++ordering;  // 1.0: "Open was not called before close."
    closeCalled = true;
  }
  std::string map1(const double& value) override {
    if (!openCalled) ++ordering;  // 1.0: "Open was not called before run."
    std::ostringstream os;
    os << value;
    return os.str();
  }
  std::string map2(const int32_t& value) override {
    if (!openCalled) ++ordering;
    return std::to_string(value);
  }

  bool openCalled = false;
  bool closeCalled = false;
  int ordering = 0;  // 任何一条时序被违反就 > 0
};

// 1.0 CoStreamFlatMapTest.MyCoFlatMap（CoStreamFlatMapTest.java:48）：
// flatMap1 把字符串逐字符发出，flatMap2 发 value.toString()
class MyCoFlatMap : public flink::streaming::api::functions::co::
                        CoFlatMapFunction<std::string, int32_t, std::string> {
 public:
  void flatMap1(const std::string& value,
                flink::util::Collector<std::string>& coll) override {
    for (size_t i = 0; i < value.size(); ++i) {
      coll.collect(value.substr(i, 1));
    }
  }
  void flatMap2(const int32_t& value,
                flink::util::Collector<std::string>& coll) override {
    coll.collect(std::to_string(value));
  }
};

// 1.0 TwoInputStreamTaskTest.IdentityMap（TwoInputStreamTaskTest.java:362）
class IdentityCoMap : public flink::streaming::api::functions::co::
                          CoMapFunction<std::string, int32_t, std::string> {
 public:
  std::string map1(const std::string& value) override { return value; }
  std::string map2(const int32_t& value) override {
    return std::to_string(value);
  }
};

// 1.0 TwoInputStreamOperatorTestHarness 的最小版：按脚本**直接**调算子的
// processElement1/2 与 processWatermark1/2（1.0 的 harness 同样是直接调，不
// 经过 StreamTwoInputProcessor）。这样元素的全局顺序完全由脚本决定，就能逐条
// 照抄 1.0 CoStreamMapTest / CoStreamFlatMapTest 的期望输出；而
// TwoInputStreamTask 本身（含输入分组、水位线取 min、屏障）由下面几个用真实
// InputGate 的测试覆盖。
class TestTwoInputStreamTask : public StreamTask {
 public:
  typedef flink::streaming::api::operators::TwoInputStreamOperator
      TwoInputStreamOperator;

  struct Action {
    int input;  // 1.0 harness 的 inputIndex：1 = 第一条输入，2 = 第二条
    bool isWatermark;
    StreamRecord record;
    int64_t watermark;
  };

  TestTwoInputStreamTask(TestEnvironment* env, const StreamConfig& config)
      : StreamTask(env, config) {}

  // 1.0 harness.processElement1 / processElement2
  void processElement1(const Any& value, int64_t timestamp) {
    script.push_back(Action{1, false, StreamRecord(value, timestamp), 0});
  }
  void processElement2(const Any& value, int64_t timestamp) {
    script.push_back(Action{2, false, StreamRecord(value, timestamp), 0});
  }
  // 1.0 harness.processWatermark1 / processWatermark2
  void processWatermark1(int64_t timestamp) {
    script.push_back(Action{1, true, StreamRecord(), timestamp});
  }
  void processWatermark2(int64_t timestamp) {
    script.push_back(Action{2, true, StreamRecord(), timestamp});
  }

  std::vector<Action> script;

 protected:
  void init() override {}

  void run() override {
    TwoInputStreamOperator* op =
        dynamic_cast<TwoInputStreamOperator*>(headOperator_);
    for (const Action& action : script) {
      // 1.0 StreamTwoInputProcessor 在 checkpointLock 里调算子
      std::lock_guard<std::recursive_mutex> guard(getCheckpointLock());
      if (action.isWatermark) {
        if (action.input == 1) {
          op->processWatermark1(Watermark(action.watermark));
        } else {
          op->processWatermark2(Watermark(action.watermark));
        }
      } else if (action.input == 1) {
        op->setKeyContextElement1(action.record);
        op->processElement1(action.record);
      } else {
        op->setKeyContextElement2(action.record);
        op->processElement2(action.record);
      }
    }
  }

  void cleanup() override {}
  void cancelTask() override {}
};

// 两输入顶点的 StreamConfig：numberOfInputs = 2，两条物理入边的 typeNumber
// 分别是 1 和 2（1.0 由 StreamGraph.connect / StreamingJobGraphGenerator 写入）
StreamConfig makeTwoInputConfig(
    std::shared_ptr<flink::streaming::api::operators::StreamOperator> op) {
  StreamConfig config = makeConfig(op);
  config.setNumberOfInputs(2);
  StreamConfig::StreamEdgeList inEdges;
  inEdges.push_back(makeForwardEdge(10, 1, std::vector<std::string>(), 1));
  inEdges.push_back(makeForwardEdge(11, 1, std::vector<std::string>(), 2));
  config.setInPhysicalEdges(inEdges);
  return config;
}

// 1.0 CoStreamMapTest.testCoMap（CoStreamMapTest.java:61-99）：输入与期望值
// 逐条照抄
void testCoStreamMap() {
  auto function = std::make_shared<MyCoMap>();
  auto op = std::make_shared<
      flink::streaming::api::operators::co::CoStreamMap<double, int32_t,
                                                        std::string> >(
      function);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestTwoInputStreamTask task(&env, makeTwoInputConfig(op));

  const int64_t initialTime = 0;
  task.processElement1(Any::of(1.1), initialTime + 1);
  task.processElement1(Any::of(1.2), initialTime + 2);
  task.processElement1(Any::of(1.3), initialTime + 3);
  task.processWatermark1(initialTime + 3);
  task.processElement1(Any::of(1.4), initialTime + 4);
  task.processElement1(Any::of(1.5), initialTime + 5);

  task.processElement2(Any::of<int32_t>(1), initialTime + 1);
  task.processElement2(Any::of<int32_t>(2), initialTime + 2);
  task.processWatermark2(initialTime + 2);
  task.processElement2(Any::of<int32_t>(3), initialTime + 3);
  task.processElement2(Any::of<int32_t>(4), initialTime + 4);
  task.processElement2(Any::of<int32_t>(5), initialTime + 5);

  task.invoke();
  output.drain();

  // 1.0 expectedOutput:85-96
  std::vector<std::string> values;
  std::vector<int64_t> timestamps;
  for (const StreamRecord& record : output.records) {
    values.push_back(record.getValueAs<std::string>());
    timestamps.push_back(record.getTimestamp());
  }
  check(values == std::vector<std::string>({"1.1", "1.2", "1.3", "1.4", "1.5",
                                            "1", "2", "3", "4", "5"}),
        "CoStreamMap：map1/map2 共享同一个 UDF 实例，两条输入各自映射");
  check(timestamps == std::vector<int64_t>({1, 2, 3, 4, 5, 1, 2, 3, 4, 5}),
        "element.replace(...) 让输出继承输入记录的时间戳");
  // 1.0:93 期望只有一个 Watermark(2)：processWatermark1(3) 时 input2 还是
  // MIN_VALUE，min 没推进；processWatermark2(2) 才让 min 变成 2
  check(output.watermarks == std::vector<int64_t>({2}),
        "两条输入之间取 min 由算子负责（combinedWatermark 推进才下发）");
  check(output.kinds == std::vector<char>({'R', 'R', 'R', 'R', 'R', 'R', 'R',
                                           'W', 'R', 'R', 'R'}),
        "Watermark(2) 正好落在第 2 条与第 3 条 input-2 记录之间");
}

// 1.0 CoStreamMapTest.testOpenClose（:102-118）
void testCoStreamMapOpenClose() {
  auto function = std::make_shared<TestOpenCloseCoMapFunction>();
  auto op = std::make_shared<
      flink::streaming::api::operators::co::CoStreamMap<double, int32_t,
                                                        std::string> >(
      function);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestTwoInputStreamTask task(&env, makeTwoInputConfig(op));

  const int64_t initialTime = 0;
  task.processElement1(Any::of(74.0), initialTime);
  task.processElement2(Any::of<int32_t>(42), initialTime);
  task.invoke();
  output.drain();

  check(function->closeCalled,
        "RichCoMapFunction 的 open/close 都被调到（1.0: \"RichFunction methods "
        "where not called.\"）");
  check(function->ordering == 0, "open 在 map1/map2 之前、close 在它们之后");
  check(output.records.size() == 2,
        "1.0: \"Output contains no elements.\" —— 两条输入各出一条");
}

// 1.0 CoStreamFlatMapTest.testCoFlatMap（CoStreamFlatMapTest.java:65-105）
void testCoStreamFlatMap() {
  auto function = std::make_shared<MyCoFlatMap>();
  auto op = std::make_shared<
      flink::streaming::api::operators::co::CoStreamFlatMap<
          std::string, int32_t, std::string> >(function);

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  TestTwoInputStreamTask task(&env, makeTwoInputConfig(op));

  const int64_t initialTime = 0;
  task.processElement1(Any::of(std::string("abc")), initialTime + 1);
  task.processElement1(Any::of(std::string("def")), initialTime + 2);
  task.processWatermark1(initialTime + 2);
  task.processElement1(Any::of(std::string("ghi")), initialTime + 3);

  task.processElement2(Any::of<int32_t>(1), initialTime + 1);
  task.processElement2(Any::of<int32_t>(2), initialTime + 2);
  task.processWatermark2(initialTime + 3);
  task.processElement2(Any::of<int32_t>(3), initialTime + 3);
  task.processElement2(Any::of<int32_t>(4), initialTime + 4);
  task.processElement2(Any::of<int32_t>(5), initialTime + 5);

  task.invoke();
  output.drain();

  std::vector<std::string> values;
  std::vector<int64_t> timestamps;
  for (const StreamRecord& record : output.records) {
    values.push_back(record.getValueAs<std::string>());
    timestamps.push_back(record.getTimestamp());
  }
  // 1.0 expectedOutput:87-102
  check(values == std::vector<std::string>({"a", "b", "c", "d", "e", "f", "g",
                                            "h", "i", "1", "2", "3", "4", "5"}),
        "CoStreamFlatMap：flatMap1 逐字符发出、flatMap2 发一条");
  check(timestamps == std::vector<int64_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 2, 3,
                                            4, 5}),
        "TimestampedCollector.setTimestamp 让一条输入拆出的多条共享时间戳");
  check(output.watermarks == std::vector<int64_t>({2}),
        "min(input1=2, input2=3) = 2，只下发一次");
}

// ----------------------------- TwoInputStreamTask -----------------------------

// 1.0 TwoInputStreamTask.init()：按 inPhysicalEdges[i].getTypeNumber() 把
// Environment.getInputGate(i) 分到 inputList1 / inputList2
void testTwoInputStreamTask() {
  auto function = std::make_shared<IdentityCoMap>();
  auto op = std::make_shared<
      flink::streaming::api::operators::co::CoStreamMap<std::string, int32_t,
                                                        std::string> >(
      function);

  // gate0 -> typeNumber 1（IN1 = std::string），gate1 -> typeNumber 2（IN2 = int）
  SingleInputGate gate1(1, 64);
  putAnyRecord(gate1, Any::of(std::string("Hello")), 1);
  putEndOfPartition(gate1);
  gate1.releaseAllResources();

  SingleInputGate gate2(1, 64);
  putAnyRecord(gate2, Any::of<int32_t>(1337), 2);
  putEndOfPartition(gate2);
  gate2.releaseAllResources();

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  env.addInputGate(&gate1);
  env.addInputGate(&gate2);

  StreamConfig config = makeTwoInputConfig(op);
  config.setCheckpointMode(
      flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE);

  flink::streaming::runtime::tasks::TwoInputStreamTask task(&env, config);
  task.invoke();
  output.drain();

  // 分组错了的话 processElement1 会拿 int32 的 Any 去 getValueAs<std::string>()
  // 而抛异常，所以这条断言同时验证了 typeNumber 的分流
  std::vector<std::string> values;
  for (const StreamRecord& record : output.records) {
    values.push_back(record.getValueAs<std::string>());
  }
  check(values == std::vector<std::string>({"Hello", "1337"}),
        "TwoInputStreamTask 按 typeNumber 把 gate 分到 input1 / input2");
  check(output.records.size() == 2 && output.records[0].getTimestamp() == 1 &&
            output.records[1].getTimestamp() == 2,
        "1.0 \"timestamps of emitted elements are correct\"：时间戳一路带到输出");
  check(output.watermarks.empty(),
        "没有输入水位线时不会凭空下发（两条输入都停在 MIN_VALUE）");
}

// 1.0 TwoInputStreamTaskTest.testWatermarkForwarding（:102-175）：
// 两条输入各 2 个通道。期望值逐条照抄（Watermark 0 / 2 / 3 / 4，外加 2 条记录）。
//
// 【与 1.0 的差异】1.0 的 harness 用 waitForInputProcessing() 控制跨输入的
// 先后；本实现的 UnionInputGate 是**轮询**（见 UnionInputGate.h 的偏差），
// 两个 gate 严格交替出数。下面的 put 顺序按交替后的全局顺序正好等价于 1.0 的
// 脚本，所以期望输出一字不改。
void testTwoInputStreamTaskWatermarkForwarding() {
  auto function = std::make_shared<IdentityCoMap>();
  auto op = std::make_shared<
      flink::streaming::api::operators::co::CoStreamMap<std::string, int32_t,
                                                        std::string> >(
      function);

  const int64_t initialTime = 0;

  // input 1：扁平通道 0、1
  SingleInputGate gate1(2, 64);
  putWatermark(gate1, initialTime, 0);      // 1.0: (0, 0)
  putWatermark(gate1, initialTime, 1);      // 1.0: (0, 1)
  putAnyRecord(gate1, Any::of(std::string("Hello")), initialTime, 0);
  putWatermark(gate1, initialTime + 4, 0);
  putWatermark(gate1, initialTime + 3, 1);
  putWatermark(gate1, initialTime + 4, 1);
  putEndOfPartition(gate1, 0);
  gate1.releaseAllResources();

  // input 2：扁平通道 2、3
  SingleInputGate gate2(2, 64);
  putWatermark(gate2, initialTime, 0);      // 1.0: (1, 0)
  putWatermark(gate2, initialTime, 1);      // 1.0: (1, 1)
  putAnyRecord(gate2, Any::of<int32_t>(42), initialTime, 1);
  putWatermark(gate2, initialTime + 3, 0);
  putWatermark(gate2, initialTime + 2, 1);
  putWatermark(gate2, initialTime + 4, 1);
  putWatermark(gate2, initialTime + 4, 0);
  putEndOfPartition(gate2, 0);
  gate2.releaseAllResources();

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  env.addInputGate(&gate1);
  env.addInputGate(&gate2);

  StreamConfig config = makeTwoInputConfig(op);
  config.setCheckpointMode(
      flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE);

  flink::streaming::runtime::tasks::TwoInputStreamTask task(&env, config);
  task.invoke();
  output.drain();

  // 1.0:130/149/158/166 —— 四条水位线
  check(output.watermarks == std::vector<int64_t>({0, 2, 3, 4}),
        "两层 min：输入内部多通道取 min（processor）+ 两输入之间取 min（算子）");
  // 1.0:173-174 Assert.assertEquals(2, resultElements.size())
  check(output.records.size() == 2,
        "1.0 \"contrary to checkpoint barriers these elements are not blocked "
        "by watermarks\"：两条记录照常通过");
  check(output.kinds == std::vector<char>({'W', 'R', 'R', 'W', 'W', 'W'}),
        "第一条水位线要等到**两条输入的所有通道**都报过才下发");
}

// 1.0 TwoInputStreamTaskTest.testCheckpointBarriers（:182-239）：
// EXACTLY_ONCE 下，收到屏障的通道被阻塞，该通道后续的记录要缓冲到屏障之后；
// 其它通道的记录照常通过。
void testTwoInputStreamTaskCheckpointBarriers() {
  auto function = std::make_shared<IdentityCoMap>();
  auto op = std::make_shared<
      flink::streaming::api::operators::co::CoStreamMap<std::string, int32_t,
                                                        std::string> >(
      function);

  const int64_t initialTime = 0;

  // input 1：扁平通道 0、1
  SingleInputGate gate1(2, 64);
  putEvent(gate1, std::make_shared<const CheckpointBarrier>(0, 0), 0);
  // 1.0:198 "This element should be buffered since we received a checkpoint
  //          barrier on this input"
  putAnyRecord(gate1, Any::of(std::string("Hello-0-0")), initialTime, 0);
  // 1.0:202 "This one should go through"
  putAnyRecord(gate1, Any::of(std::string("Ciao-0-0")), initialTime, 1);
  putEvent(gate1, std::make_shared<const CheckpointBarrier>(0, 0), 1);
  putEndOfPartition(gate1, 0);
  gate1.releaseAllResources();

  // input 2：扁平通道 2、3
  SingleInputGate gate2(2, 64);
  // 1.0:206 "These elements should be forwarded, since we did not yet receive
  //          a checkpoint barrier on that input"
  putAnyRecord(gate2, Any::of<int32_t>(11), initialTime, 1);
  putAnyRecord(gate2, Any::of<int32_t>(111), initialTime, 1);
  putEvent(gate2, std::make_shared<const CheckpointBarrier>(0, 0), 0);
  putEvent(gate2, std::make_shared<const CheckpointBarrier>(0, 0), 1);
  putEndOfPartition(gate2, 0);
  gate2.releaseAllResources();

  TestOutput output;
  TestEnvironment env(output.getPartitionWriter());
  env.addInputGate(&gate1);
  env.addInputGate(&gate2);

  StreamConfig config = makeTwoInputConfig(op);
  config.setCheckpointMode(
      flink::streaming::api::CheckpointingMode::EXACTLY_ONCE);

  flink::streaming::runtime::tasks::TwoInputStreamTask task(&env, config);
  task.invoke();
  output.drain();

  // 1.0:237-238 Assert.assertEquals(4, resultElements.size())
  check(output.records.size() == 4, "四条记录最终都出来了");
  check(output.barrierIds == std::vector<int64_t>({0}),
        "四个通道的屏障到齐才触发一次 checkpoint 并向下游广播");
  // IdentityCoMap 既不是 Checkpointed 也没有 keyed state，所以 1.0 走的是
  // 「states 全为 null -> acknowledgeCheckpoint(checkpointId)」那条分支
  check(env.ackedWithoutState == std::vector<int64_t>({0}) &&
            env.ackedWithState.empty(),
        "屏障触发的 checkpoint 做了 acknowledge（无状态算子走不带 state 的重载）");
  // 1.0:226 "now we should see the barrier and after that the buffered
  //          elements"
  check(output.kinds == std::vector<char>({'R', 'R', 'R', 'B', 'R'}),
        "被阻塞通道的那条记录排在屏障之后（BarrierBuffer 缓冲 + releaseBlocks）");
  check(output.records.size() == 4 &&
            output.records[3].getValueAs<std::string>() == "Hello-0-0",
        "屏障之后出来的正是被阻塞通道上的 \"Hello-0-0\"");
}

}  // namespace

int main() {
  testStreamMapLifecycle();
  testStreamFilterAndFlatMap();
  testGroupedReduceAndFold();
  testUdfLifecycleAndCheckpointedState();
  testKeyedStateThroughOperator();
  testRegisterTimer();
  testChainedOperators();
  testBroadcastingOutputCollector();
  testDirectedOutput();
  testExceptionInChainedOperator();
  testOneInputStreamTask();
  testOneInputStreamTaskCheckpointBarrier();
  testSourceStreamTaskProcessingTime();
  testSourceStreamTaskEventTime();
  testSourceStreamTaskIngestionTime();
  testSourceStreamTaskCancel();
  testCoStreamMap();
  testCoStreamMapOpenClose();
  testCoStreamFlatMap();
  testTwoInputStreamTask();
  testTwoInputStreamTaskWatermarkForwarding();
  testTwoInputStreamTaskCheckpointBarriers();
  std::printf(failures == 0 ? "\nALL PASSED\n" : "\n%d FAILED\n", failures);
  return failures == 0 ? 0 : 1;
}
