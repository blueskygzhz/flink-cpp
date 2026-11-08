// 状态后端层冒烟测试
//
// 覆盖 1.0 的这几条路径：
//   AbstractStateBackend.getPartitionedState(ns, nsSerializer, descriptor)
//   AbstractStateBackend.setCurrentKey(Object)
//   AbstractStateBackend.snapshotPartitionedState(checkpointId, timestamp)
//   AbstractStateBackend.injectKeyValueStateSnapshots(..) 的 lazy restore
//   MemValueState / MemListState 的 value/update/add/get/clear 语义
//   AbstractMemState 的快照字节格式（namespace -> key -> value 两层）

#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

#include "flink/api/common/state/ListStateDescriptor.h"
#include "flink/api/common/state/ValueStateDescriptor.h"
#include "flink/api/common/typeutils/TypedAnySerializer.h"
#include "flink/api/common/typeutils/base/VoidSerializer.h"
#include "flink/api/java/tuple/Tuple2.h"
#include "flink/runtime/state/memory/MemoryStateBackend.h"
#include "flink/types/Any.h"

using flink::api::common::state::ListStateDescriptor;
using flink::api::common::state::ValueStateDescriptor;
using flink::api::common::typeutils::TypedAnySerializer;
using flink::api::common::typeutils::base::VoidSerializer;
using flink::api::java::tuple::Tuple2;
using flink::runtime::state::memory::MemoryStateBackend;
using flink::types::Any;

namespace {

// 1.0 AbstractStreamOperator.java:260 的非窗口取法：
//   getPartitionedState(null, VoidSerializer.INSTANCE, descriptor)
const Any& voidNamespace() {
  static Any instance;  // isNull()，对应 1.0 传的 null
  return instance;
}

int failures = 0;

void check(bool condition, const char* what) {
  if (!condition) {
    std::printf("FAIL  %s\n", what);
    ++failures;
  } else {
    std::printf("ok    %s\n", what);
  }
}

// ValueState：对应 StreamGroupedReduce.java:50 的 "_op_state"
void testValueState() {
  MemoryStateBackend backend;
  backend.initializeForJob("StreamGroupedReduce_1_0",
                           TypedAnySerializer<std::string>::create());

  ValueStateDescriptor<int64_t> descriptor("_op_state", 0);

  backend.setCurrentKey(Any::of(std::string("hello")));
  auto state = backend.getPartitionedState(voidNamespace(),
                                           VoidSerializer::INSTANCE(),
                                           descriptor);
  check(state->value() == 0, "ValueState.value() 回落到 descriptor 默认值");
  state->update(7);

  backend.setCurrentKey(Any::of(std::string("world")));
  check(state->value() == 0, "换 key 后互不干扰");
  state->update(100);

  backend.setCurrentKey(Any::of(std::string("hello")));
  check(state->value() == 7, "切回原 key 读到原值");

  // 1.0 getPartitionedState 同名返回同一个状态对象
  auto again = backend.getPartitionedState(voidNamespace(),
                                           VoidSerializer::INSTANCE(),
                                           descriptor);
  check(again.get() == state.get(), "同名 descriptor 复用同一状态对象");

  state->clear();
  check(state->value() == 0, "clear() 只清当前 key");
  backend.setCurrentKey(Any::of(std::string("world")));
  check(state->value() == 100, "clear() 没伤到别的 key");
}

// ListState + namespace：对应 WindowOperator 的 "window-contents"
// （namespace 就是窗口本身，这里用 Tuple2<long,long> 代替 TimeWindow）
void testListStateWithNamespace() {
  typedef Tuple2<int64_t, int64_t> Window;

  MemoryStateBackend backend;
  backend.initializeForJob("WindowOperator_2_0",
                           TypedAnySerializer<std::string>::create());

  ListStateDescriptor<int32_t> descriptor("window-contents");
  auto nsSerializer = TypedAnySerializer<Window>::create();

  const Any w0 = Any::of(Window(0, 5));
  const Any w1 = Any::of(Window(5, 10));

  backend.setCurrentKey(Any::of(std::string("a")));
  backend.getPartitionedState(w0, nsSerializer, descriptor)->add(1);
  backend.getPartitionedState(w0, nsSerializer, descriptor)->add(2);
  backend.getPartitionedState(w1, nsSerializer, descriptor)->add(3);

  backend.setCurrentKey(Any::of(std::string("b")));
  backend.getPartitionedState(w0, nsSerializer, descriptor)->add(9);

  backend.setCurrentKey(Any::of(std::string("a")));
  std::vector<int32_t> a0 =
      backend.getPartitionedState(w0, nsSerializer, descriptor)->get();
  std::vector<int32_t> a1 =
      backend.getPartitionedState(w1, nsSerializer, descriptor)->get();
  check(a0.size() == 2 && a0[0] == 1 && a0[1] == 2, "(key a, window [0,5)) 攒到 2 个元素");
  check(a1.size() == 1 && a1[0] == 3, "同 key 不同 namespace 互相隔离");

  backend.setCurrentKey(Any::of(std::string("b")));
  std::vector<int32_t> b0 =
      backend.getPartitionedState(w0, nsSerializer, descriptor)->get();
  check(b0.size() == 1 && b0[0] == 9, "同 namespace 不同 key 互相隔离");

  // 窗口触发后 1.0 会 windowState.clear()
  backend.getPartitionedState(w0, nsSerializer, descriptor)->clear();
  check(backend.getPartitionedState(w0, nsSerializer, descriptor)->get().empty(),
        "窗口触发后 clear() 清空该 (key, window)");
  backend.setCurrentKey(Any::of(std::string("a")));
  check(backend.getPartitionedState(w0, nsSerializer, descriptor)->get().size() == 2,
        "clear() 没动别的 key");
}

// snapshot -> 新后端 inject -> lazy restore
void testSnapshotAndRestore() {
  typedef Tuple2<int64_t, int64_t> Window;
  ValueStateDescriptor<int64_t> counter("count", 0);
  ListStateDescriptor<int32_t> contents("window-contents");
  auto keySerializer = TypedAnySerializer<std::string>::create();
  auto nsSerializer = TypedAnySerializer<Window>::create();
  const Any w0 = Any::of(Window(0, 5));

  MemoryStateBackend::StateHandleMap snapshots;
  {
    MemoryStateBackend backend;
    backend.initializeForJob("op", keySerializer);

    backend.setCurrentKey(Any::of(std::string("a")));
    backend.getPartitionedState(voidNamespace(), VoidSerializer::INSTANCE(),
                                counter)
        ->update(42);
    backend.getPartitionedState(w0, nsSerializer, contents)->add(1);
    backend.getPartitionedState(w0, nsSerializer, contents)->add(2);

    backend.setCurrentKey(Any::of(std::string("b")));
    backend.getPartitionedState(voidNamespace(), VoidSerializer::INSTANCE(),
                                counter)
        ->update(-1);

    snapshots = backend.snapshotPartitionedState(1, 1000);
    check(snapshots.size() == 2, "两个状态各出一份快照");
    check(snapshots.count("count") == 1 && snapshots.count("window-contents") == 1,
          "快照按 state 名归档");
  }

  MemoryStateBackend restored;
  restored.initializeForJob("op", keySerializer);
  restored.injectKeyValueStateSnapshots(snapshots, 2000);

  restored.setCurrentKey(Any::of(std::string("a")));
  check(restored
            .getPartitionedState(voidNamespace(), VoidSerializer::INSTANCE(),
                                 counter)
            ->value() == 42,
        "ValueState 恢复 (key a)");
  std::vector<int32_t> list =
      restored.getPartitionedState(w0, nsSerializer, contents)->get();
  check(list.size() == 2 && list[0] == 1 && list[1] == 2,
        "ListState 连 namespace 一起恢复");

  restored.setCurrentKey(Any::of(std::string("b")));
  check(restored
            .getPartitionedState(voidNamespace(), VoidSerializer::INSTANCE(),
                                 counter)
            ->value() == -1,
        "ValueState 恢复 (key b)");

  restored.dispose();
  restored.setCurrentKey(Any::of(std::string("a")));
  check(restored
            .getPartitionedState(voidNamespace(), VoidSerializer::INSTANCE(),
                                 counter)
            ->value() == 0,
        "dispose() 之后状态清空");
}

}  // namespace

int main() {
  testValueState();
  testListStateWithNamespace();
  testSnapshotAndRestore();
  std::printf(failures == 0 ? "\nALL PASSED\n" : "\n%d FAILED\n", failures);
  return failures == 0 ? 0 : 1;
}
