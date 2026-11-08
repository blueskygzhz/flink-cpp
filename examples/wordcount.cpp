// 对齐 Flink 1.0.0 示例 WindowWordCount：
//   env.addSource(...).flatMap(Tokenizer).keyBy(0).countWindow(3).sum(1).print()
//
// 1.0 KeyedStream.java:207-208
//   countWindow(size) = window(GlobalWindows.create())
//                         .trigger(PurgingTrigger.of(CountTrigger.of(size)))
// 所以每个 key 每攒满 3 条记录触发一次求和，触发后清空状态。
//
// 环境变量 FLINKCPP_PARALLELISM=N 可把下游算子并行度开到 N，用来验证
// HashPartitioner 重分区 + 多输入通道（默认 1，输出与单线程版逐字一致）。
#include "flinkcpp/api.hpp"

#include <algorithm>
#include <cstdlib>
#include <sstream>

using namespace flinkcpp;

using WordCount = Tuple2<std::string, int>;

static int envInt(const char* key, int defaultValue) {
  const char* v = std::getenv(key);
  return (v != nullptr && *v != '\0') ? std::atoi(v) : defaultValue;
}

static std::mutex g_mutex;
static std::vector<std::string> g_out;

class LineSource : public SourceFunction<std::string> {
 public:
  std::vector<std::string> lines;
  void run(SourceContext<std::string>& ctx) override {
    for (const auto& line : lines) ctx.collect(line);
  }
};

class Tokenizer : public FlatMapFunction<std::string, WordCount> {
 public:
  void flatMap(const std::string& line, Collector<WordCount>& out) override {
    std::istringstream iss(line);
    std::string word;
    while (iss >> word) out.collect(WordCount(word, 1));
  }
};

class WordKeySelector : public KeySelector<WordCount, std::string> {
 public:
  std::string getKey(const WordCount& v) override { return v.f0; }
};

class CountSink : public SinkFunction<WordCount> {
 public:
  void invoke(const WordCount& v) override {
    std::ostringstream os;
    os << v.f0 << " : " << v.f1;
    {
      std::lock_guard<std::mutex> lk(g_mutex);
      g_out.push_back(os.str());
    }
    std::cout << os.str() << std::endl;
  }
};

int main() {
  const int parallelism = envInt("FLINKCPP_PARALLELISM", 1);

  auto env = StreamExecutionEnvironment::getExecutionEnvironment();
  env->setParallelism(parallelism);
  env->setBufferSize(4);  // LocalBufferPool 容量，满则阻塞上游 = 背压

  auto source = std::make_shared<LineSource>();
  source->lines = {
      "hello flink cpp",
      "hello flink cpp",
      "hello flink cpp",
  };

  auto lines = env->addSource(source, "Lines");
  // addSource 让所有 subtask 共享同一个 SourceFunction 实例（1.0 是每个 Task
  // 反序列化出自己那份），所以 source 固定并行度 1，否则同一份 lines 会被读 N 遍。
  lines.setParallelism(1);
  lines.flatMap(std::make_shared<Tokenizer>())
      .keyBy(std::make_shared<WordKeySelector>())
      .countWindow(3)
      .sum()
      .addSink(std::make_shared<CountSink>(), "Word Count Sink");

  env->execute("WindowWordCount (C++ port of Flink 1.0.0)");

  if (parallelism > 1) {
    std::cout << "--- execution graph (parallelism=" << parallelism << ") ---"
              << std::endl;
    const auto& edges = env->lastEdges();
    for (size_t i = 0; i < edges.size(); ++i) {
      std::cout << "  edge " << edges[i].from << " -> " << edges[i].to << "  p"
                << edges[i].parallelismFrom << "->p" << edges[i].parallelismTo
                << "  " << edges[i].partitioner
                << (edges[i].pointwise ? " [POINTWISE]" : " [ALL_TO_ALL]")
                << std::endl;
    }
    const auto& subs = env->lastSubtasks();
    for (size_t i = 0; i < subs.size(); ++i) {
      std::cout << "  subtask " << subs[i].name << "[" << subs[i].subtaskIndex
                << "/" << subs[i].parallelism << "] inputChannels="
                << subs[i].numInputChannels << std::endl;
    }
  }

  // 自检：无论并行度多少，每个 key 攒满 3 条触发一次，结果必须一致。
  // 并行时 sink 输出顺序不确定，所以排序后比较。
  std::vector<std::string> got = g_out;
  std::sort(got.begin(), got.end());
  const std::vector<std::string> want = {"cpp : 3", "flink : 3", "hello : 3"};
  const bool ok = (got == want);
  std::cout << "=== " << (ok ? "PASS" : "FAIL") << " (parallelism="
            << parallelism << ") ===" << std::endl;
  if (!ok) {
    for (size_t i = 0; i < got.size(); ++i) {
      std::cout << "  got: " << got[i] << std::endl;
    }
  }
  return ok ? 0 : 1;
}
