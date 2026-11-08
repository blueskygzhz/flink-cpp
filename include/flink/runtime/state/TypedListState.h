/*
 * 【与 Flink 1.0 的偏差 —— 新增类，1.0 无对应物】
 * 见 TypedValueState.h 的说明：把 MemListState 的「KvState + ListState<V>」
 * 两副面孔拆成 HeapListState（存 Any）+ 本类（装箱/拆箱）。
 */
#ifndef FLINK_RUNTIME_STATE_TYPEDLISTSTATE_H
#define FLINK_RUNTIME_STATE_TYPEDLISTSTATE_H

#include <memory>
#include <vector>

#include "flink/api/common/state/ListState.h"
#include "flink/runtime/state/KvState.h"

namespace flink {
namespace runtime {
namespace state {

template <class T>
class TypedListState : public flink::api::common::state::ListState<T> {
 public:
  explicit TypedListState(std::shared_ptr<KvState> kvState)
      : kvState_(kvState) {}

  std::vector<T> get() override {
    std::vector<flink::types::Any> boxed = kvState_->get();
    std::vector<T> result;
    result.reserve(boxed.size());
    for (const auto& value : boxed) {
      result.push_back(value.as<T>());
    }
    return result;
  }

  void add(const T& value) override {
    kvState_->add(flink::types::Any::of(value));
  }

  void clear() override { kvState_->clear(); }

 private:
  std::shared_ptr<KvState> kvState_;
};

}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_TYPEDLISTSTATE_H
