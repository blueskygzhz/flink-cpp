// 事件时间 / 周期水印 / watermark 驱动窗口 / Flink 1.0 的迟到数据行为
//
// 链路：Source -> assignTimestampsAndWatermarks -> FlatMap -> keyBy
//       -> timeWindow(5000) [TumblingEventTimeWindows + EventTimeTrigger]
//       -> WindowFunction.apply -> Sink
//
// 预期输出 4 行：
//   A [0,5000) = 2       正常窗口，被 watermark 推进触发
//   A [0,5000) = 1       ★迟到元素(ts=1500)在窗口 purge 后立即重复触发一次
//                         （1.0 不丢弃迟到数据，也没有 allowedLateness/侧输出）
//   A [5000,10000) = 1   由 MAX_WATERMARK 冲刷
//   A [10000,15000) = 1  由 MAX_WATERMARK 冲刷
//
// 环境变量：
//   FLINKCPP_SLIDE=N        滑动步长，>0 则换成 SlidingEventTimeWindows(5000, N)
//   FLINKCPP_EVICT_COUNT=N  >0 则挂 CountEvictor(N)，走 EvictingWindowOperator
// 都不设时行为与原版逐字一致。
#include "flinkcpp/api.hpp"

#include <cstdlib>
#include <sstream>

using namespace flinkcpp;

static int64_t envLong(const char* key, int64_t defaultValue) {
  const char* v = std::getenv(key);
  return (v != nullptr && *v != '\0') ? std::atoll(v) : defaultValue;
}

struct TimedWord {
  std::string word;
  int64_t ts;
  TimedWord() : ts(0) {}
  TimedWord(std::string w, int64_t t) : word(std::move(w)), ts(t) {}
};

class TimedWordSource : public SourceFunction<TimedWord> {
 public:
  std::vector<TimedWord> events;
  void run(SourceContext<TimedWord>& ctx) override {
    for (const auto& w : events) ctx.collect(w);
  }
};

// 1.0 常用写法：watermark = 已见最大时间戳 - 乱序容忍度
class BoundedOutOfOrdernessAssigner
    : public AssignerWithPeriodicWatermarks<TimedWord> {
 public:
  int64_t extractTimestamp(const TimedWord& v, int64_t) override {
    if (v.ts > maxTs_) maxTs_ = v.ts;
    return v.ts;
  }
  int64_t getCurrentWatermark() override {
    return maxTs_ == NO_TIMESTAMP ? NO_TIMESTAMP : maxTs_ - outOfOrderness_;
  }

 private:
  int64_t maxTs_ = NO_TIMESTAMP;
  int64_t outOfOrderness_ = 2000;
};

class ToWordCount : public FlatMapFunction<TimedWord, Tuple2<std::string, int>> {
 public:
  void flatMap(const TimedWord& v,
               Collector<Tuple2<std::string, int>>& out) override {
    out.collect(Tuple2<std::string, int>(v.word, 1));
  }
};

class WordKey : public KeySelector<Tuple2<std::string, int>, std::string> {
 public:
  std::string getKey(const Tuple2<std::string, int>& v) override { return v.f0; }
};

// 1.0 唯一的窗口函数签名 apply(KEY, W, Iterable<IN>, Collector<OUT>)
class ReportWindowFunction
    : public WindowFunction<Tuple2<std::string, int>, std::string,
                            TimeWindow, std::string> {
 public:
  void apply(const std::string& key, const TimeWindow& window,
             const std::vector<Tuple2<std::string, int>>& input,
             Collector<std::string>& out) override {
    int sum = 0;
    for (const auto& v : input) sum += v.f1;
    std::ostringstream os;
    os << key << " [" << window.start << "," << window.end << ") = " << sum;
    out.collect(os.str());
  }
};

class StringSink : public SinkFunction<std::string> {
 public:
  void invoke(const std::string& v) override { std::cout << v << std::endl; }
};

int main() {
  auto env = StreamExecutionEnvironment::getExecutionEnvironment();
  env->setStreamTimeCharacteristic(TimeCharacteristic::EVENT_TIME);
  env->setAutoWatermarkInterval(2);  // 每 2 条记录探测一次水印（模拟周期定时器）
  env->setBufferSize(16);

  auto source = std::make_shared<TimedWordSource>();
  source->events = {
      TimedWord("A", 1000),
      TimedWord("A", 2000),
      TimedWord("A", 6000),
      TimedWord("A", 11000),
      TimedWord("A", 1500),  // 迟到：watermark 已推进到 9000
  };

  const int64_t slide = envLong("FLINKCPP_SLIDE", 0);
  const int64_t evictCount = envLong("FLINKCPP_EVICT_COUNT", 0);

  auto keyed = env->addSource(source, "TimedWords")
                   .assignTimestampsAndWatermarks(
                       std::make_shared<BoundedOutOfOrdernessAssigner>())
                   .flatMap(std::make_shared<ToWordCount>())
                   .keyBy(std::make_shared<WordKey>());

  // 1.0 KeyedStream.timeWindow(size) / timeWindow(size, slide)
  auto windowed = slide > 0 ? keyed.timeWindow(5000, slide)
                            : keyed.timeWindow(5000);
  // 1.0 WindowedStream.evictor(Evictor) -> EvictingWindowOperator
  if (evictCount > 0) {
    windowed.evictor(std::make_shared<CountEvictor>(evictCount));
  }
  windowed.apply(std::make_shared<ReportWindowFunction>())
      .addSink(std::make_shared<StringSink>(), "Report");

  std::cout << "# assigner=" << (slide > 0 ? "Sliding(5000," : "Tumbling(5000")
            << (slide > 0 ? std::to_string(slide) + ")" : std::string(")"))
            << ", evictor="
            << (evictCount > 0 ? "CountEvictor(" + std::to_string(evictCount) + ")"
                               : std::string("none"))
            << std::endl;

  env->execute("EventTimeWindowWordCount (C++ port of Flink 1.0.0)");
  return 0;
}
