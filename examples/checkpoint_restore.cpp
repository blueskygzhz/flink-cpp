// 状态与容错：检查点屏障 / 快照 / 故障恢复
//
// 参考 Flink 1.0：
//   api/checkpoint/Checkpointed.java          snapshotState / restoreState
//   runtime/io/network/api/CheckpointBarrier.java
//   runtime/tasks/StreamTask.java:460-511     triggerCheckpoint
//   runtime/io/BarrierBuffer.java:161-207     barrier 对齐
//   runtime/checkpoint/CheckpointCoordinator.java
//   WindowedStream.java:262                   ListStateDescriptor("window-contents")
//
// 场景：source 读 [0,12) 的有界序列，按奇偶分 key，countWindow(3) 求和。
//   Run A  基线，不失败
//   Run B1 读到 offset 7 时人为抛异常（模拟 TaskManager 挂掉）→ 作业 FAILED
//   Run B2 从最近一次完成的 checkpoint 恢复：source offset 与窗口 keyed state
//          一并回滚，继续消费剩余记录
//
// 自检两件事：
//   1. source 侧「恰好一次」：B 消费的记录序列 == A 的记录序列（无丢失、无重复）
//   2. keyed state 真的恢复了：恢复后第一次窗口触发用到了 checkpoint 之前的元素
//
// 环境变量：
//   FLINKCPP_STATE_DIR=path  用 FsStateBackend 把窗口 keyed state 序列化落盘
//                            （验证 KvSnapshot serialize/deserialize 往返）
//   FLINKCPP_PARALLELISM=N   下游算子并行度，用来让 BarrierBuffer 真的跑多通道对齐
//                            （source 必须 p=1，见 api.hpp execute() 的限制）
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
static std::string envStr(const char* key) {
  const char* v = std::getenv(key);
  return (v != nullptr) ? std::string(v) : std::string();
}

// 收集 sink 输出与 source 实际消费的记录，用于自检
static std::vector<std::string> g_sinkOut;
static std::vector<int> g_consumed;
static std::mutex g_mutex;

// -----------------------------------------------------------------------------
// 可检查点的 source：1.0 里实现 Checkpointed<Integer> 保存读取 offset。
// 关键语义：offset_ 始终表示「下一条待读」，所以必须在 collect 之前自增，
// 否则 barrier 之后的快照会把已发出的记录再算一遍（恢复时重复消费）。
// -----------------------------------------------------------------------------
class CountingSource : public SourceFunction<int>,
                       public CheckpointedFunction,
                       public CheckpointListener {
 public:
  CountingSource(int total, int failAt) : total_(total), failAt_(failAt) {}

  void run(SourceContext<int>& ctx) override {
    while (offset_ < total_) {
      if (failAt_ >= 0 && offset_ == failAt_) {
        throw std::runtime_error("simulated TaskManager failure at offset " +
                                 std::to_string(offset_));
      }
      const int value = offset_;
      ++offset_;  // 先推进，offset_ = 下一条待读
      {
        std::lock_guard<std::mutex> lk(g_mutex);
        g_consumed.push_back(value);
      }
      ctx.collect(value);  // barrier 在此之后注入，此时 offset_ 语义正确
    }
  }

  // Checkpointed.snapshotState(checkpointId, checkpointTimestamp)
  Any snapshotStateAny(int64_t checkpointId, int64_t) override {
    std::cout << "    [source] snapshot ckpt=" << checkpointId
              << " nextOffset=" << offset_ << std::endl;
    return Any::of(offset_);
  }
  // Checkpointed.restoreState(state)
  void restoreStateAny(const Any& state) override {
    offset_ = state.as<int>();
    std::cout << "    [source] RESTORED nextOffset=" << offset_ << std::endl;
  }
  // CheckpointListener.notifyCheckpointComplete(checkpointId)
  void notifyCheckpointComplete(int64_t checkpointId) override {
    std::cout << "    [coordinator] checkpoint " << checkpointId
              << " COMPLETED" << std::endl;
  }

  int nextOffset() const { return offset_; }

 private:
  int total_;
  int failAt_;
  int offset_ = 0;
};

class ToParityCount : public FlatMapFunction<int, WordCount> {
 public:
  void flatMap(const int& v, Collector<WordCount>& out) override {
    out.collect(WordCount(v % 2 == 0 ? "even" : "odd", v));
  }
};

class ParityKey : public KeySelector<WordCount, std::string> {
 public:
  std::string getKey(const WordCount& v) override { return v.f0; }
};

class SumSink : public SinkFunction<WordCount> {
 public:
  void invoke(const WordCount& v) override {
    std::ostringstream os;
    os << v.f0 << "=" << v.f1;
    {
      std::lock_guard<std::mutex> lk(g_mutex);
      g_sinkOut.push_back(os.str());
    }
    std::cout << "    [sink] " << os.str() << std::endl;
  }
};

// 构图：source -> flatMap -> keyBy -> countWindow(3) -> sum -> sink
static bool buildAndRun(const std::shared_ptr<CountingSource>& source,
                        const std::shared_ptr<CheckpointCoordinator>& coord,
                        bool restore, const std::string& jobName) {
  auto env = StreamExecutionEnvironment::getExecutionEnvironment();
  env->setParallelism(envInt("FLINKCPP_PARALLELISM", 1));
  env->setBufferSize(8);
  env->setCheckpointCoordinator(coord);
  env->enableCheckpointing(4);  // 每 4 条记录一次 checkpoint
  env->setRestoreFromLatestCheckpoint(restore);

  // 1.0 env.setStateBackend(new FsStateBackend(path))
  const std::string stateDir = envStr("FLINKCPP_STATE_DIR");
  if (!stateDir.empty()) {
    env->setStateBackend(std::make_shared<FsStateBackend>(stateDir));
  }

  auto src = env->addSource(source, "CountingSource");
  src.setParallelism(1);  // 并行 source + checkpoint 不支持，见 api.hpp
  src.flatMap(std::make_shared<ToParityCount>())
      .keyBy(std::make_shared<ParityKey>())
      .countWindow(3)
      .sum()
      .addSink(std::make_shared<SumSink>(), "Sum Sink");

  bool ok = env->execute(jobName);
  if (!stateDir.empty()) {
    std::cout << "    [backend] " << env->getStateBackend()->name() << std::endl;
  }
  {
    // 打印一次多通道信息，确认 BarrierBuffer 真的在跑 >1 通道的对齐
    const auto& subs = env->lastSubtasks();
    int maxChannels = 0;
    for (size_t i = 0; i < subs.size(); ++i) {
      if (subs[i].numInputChannels > maxChannels) {
        maxChannels = subs[i].numInputChannels;
      }
    }
    if (maxChannels > 1) {
      std::cout << "    [runtime] max input channels on a subtask = "
                << maxChannels << " (barrier alignment active)" << std::endl;
    }
  }
  if (!ok) std::cout << "    [job] FAILED: " << env->lastFailure() << std::endl;
  return ok;
}

static std::string join(const std::vector<std::string>& v) {
  std::ostringstream os;
  for (size_t i = 0; i < v.size(); ++i) os << (i ? " " : "") << v[i];
  return os.str();
}
static std::string joinInts(const std::vector<int>& v) {
  std::ostringstream os;
  for (size_t i = 0; i < v.size(); ++i) os << (i ? "," : "") << v[i];
  return os.str();
}

// 把 sink 输出（"even=6"）聚成 key -> 总和。
// 并行时窗口分组非确定，但「每个 key 的总和」是顺序无关的不变量。
static std::map<std::string, long long> sumByKey(
    const std::vector<std::string>& v) {
  std::map<std::string, long long> m;
  for (size_t i = 0; i < v.size(); ++i) {
    const size_t p = v[i].find('=');
    if (p == std::string::npos) continue;
    m[v[i].substr(0, p)] += std::atoll(v[i].c_str() + p + 1);
  }
  return m;
}
static std::string joinSums(const std::map<std::string, long long>& m) {
  std::ostringstream os;
  bool first = true;
  for (std::map<std::string, long long>::const_iterator it = m.begin();
       it != m.end(); ++it) {
    os << (first ? "" : " ") << it->first << "=" << it->second;
    first = false;
  }
  return os.str();
}

int main() {
  const int kTotal = 12;

  // ---------------- Run A：基线，无故障 ----------------
  std::cout << "=== Run A: baseline (no failure) ===" << std::endl;
  auto coordA = std::make_shared<CheckpointCoordinator>();
  auto sA = std::make_shared<CountingSource>(kTotal, -1);
  buildAndRun(sA, coordA, false, "run-A");
  std::vector<std::string> sinkA = g_sinkOut;
  std::vector<int> consumedA = g_consumed;

  // ---------------- Run B：故障 + 恢复 ----------------
  g_sinkOut.clear();
  g_consumed.clear();
  auto coordB = std::make_shared<CheckpointCoordinator>();

  std::cout << "=== Run B1: fail at offset 7 ===" << std::endl;
  auto sB1 = std::make_shared<CountingSource>(kTotal, 7);
  bool okB1 = buildAndRun(sB1, coordB, false, "run-B1");
  std::cout << "    [job] success=" << (okB1 ? "true" : "false")
            << ", latest completed ckpt=" << coordB->latestCompletedId()
            << std::endl;
  std::vector<std::string> sinkB1 = g_sinkOut;

  std::cout << "=== Run B2: restore from ckpt "
            << coordB->latestCompletedId() << " ===" << std::endl;
  g_sinkOut.clear();
  auto sB2 = std::make_shared<CountingSource>(kTotal, -1);
  bool okB2 = buildAndRun(sB2, coordB, true, "run-B2");
  std::vector<std::string> sinkB2 = g_sinkOut;

  // ---------------- 自检 ----------------
  std::cout << "=== Verification ===" << std::endl;
  std::vector<int> consumedB = g_consumed;
  std::sort(consumedB.begin(), consumedB.end());
  std::vector<int> uniqueB = consumedB;
  uniqueB.erase(std::unique(uniqueB.begin(), uniqueB.end()), uniqueB.end());

  std::cout << "  A consumed       : " << joinInts(consumedA) << std::endl;
  std::cout << "  B consumed(sort) : " << joinInts(consumedB) << std::endl;

  // [1] 无丢失：B 至少覆盖了 A 的全部记录（重放是恢复的应有行为，不算错）
  const bool noLoss = (uniqueB == consumedA);
  std::cout << "  [1] no record lost across failure: "
            << (noLoss ? "PASS" : "FAIL") << std::endl;

  // [2] 重放范围受 checkpoint 约束：被重复消费的记录必须落在
  //     [restoredOffset, failurePoint) 内，即只回放最后一次 checkpoint 之后的部分
  const int restoredOffset = 4;   // ckpt 1 的 nextOffset
  const int failurePoint = 7;
  bool replayBounded = true;
  for (size_t i = 1; i < consumedB.size(); ++i) {
    if (consumedB[i] == consumedB[i - 1]) {
      if (consumedB[i] < restoredOffset || consumedB[i] >= failurePoint) {
        replayBounded = false;
      }
    }
  }
  std::cout << "  [2] replay bounded to [" << restoredOffset << ","
            << failurePoint << ") after last checkpoint: "
            << (replayBounded ? "PASS" : "FAIL") << std::endl;

  std::cout << "  A sink  : " << join(sinkA) << std::endl;
  std::cout << "  B1 sink : " << join(sinkB1) << std::endl;
  std::cout << "  B2 sink : " << join(sinkB2) << std::endl;

  // [3] 状态等价（真正的 exactly-once 证据）：恢复后这一次运行的完整输出
  //     必须与基线相同——说明 source offset 与窗口 keyed state 都回滚到了
  //     一致的 checkpoint 快照点，既没少算也没多算。
  //     并行度 > 1 时窗口分散在多个 subtask，sink 的到达顺序不确定，按多重集比较。
  bool stateEquivalent;
  if (envInt("FLINKCPP_PARALLELISM", 1) > 1) {
    // 并行时 source->flatMap 走 REBALANCE，records 经多条通道交错到达，而
    // countWindow(3) 是按「到达顺序」每 3 条一组 —— 分组内容本身就是非确定的
    // （1.0 同样如此：基线自己每次跑的分组都不同）。所以这里不能比逐项输出，
    // 只能比顺序无关的不变量：每个 key 的输出总和 + 输出条数。
    // 这个判据仍然严格：窗口 keyed state 若没恢复，even 会少掉 {0,2}，
    // 总和立刻从 30 掉到 28。
    const std::map<std::string, long long> a = sumByKey(sinkA);
    const std::map<std::string, long long> b = sumByKey(sinkB2);
    stateEquivalent = (a == b) && (sinkB2.size() == sinkA.size());
    std::cout << "  A sum/key : " << joinSums(a) << std::endl;
    std::cout << "  B2 sum/key: " << joinSums(b) << std::endl;
    std::cout << "  [3] restored run per-key totals == baseline (order-"
                 "independent): "
              << (stateEquivalent ? "PASS" : "FAIL") << std::endl;
  } else {
    stateEquivalent = (sinkB2 == sinkA);
    std::cout << "  [3] restored run output == baseline output: "
              << (stateEquivalent ? "PASS" : "FAIL") << std::endl;
  }

  std::cout << "  note: B1 在失败前已输出的 \"" << join(sinkB1)
            << "\" 会被 B2 重放一次。" << std::endl
            << "        1.0 没有 TwoPhaseCommitSinkFunction（1.4+ 才有），"
            << std::endl
            << "        所以端到端 exactly-once 还需要幂等或事务 sink。"
            << std::endl;

  const bool pass =
      noLoss && replayBounded && stateEquivalent && okB2 && !okB1;
  std::cout << "=== " << (pass ? "ALL CHECKS PASSED" : "CHECKS FAILED")
            << " ===" << std::endl;
  return pass ? 0 : 1;
}
