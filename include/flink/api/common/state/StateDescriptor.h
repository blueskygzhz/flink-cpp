/*
 * flink-core : org.apache.flink.api.common.state.StateDescriptor
 *
 * 1.0 StateDescriptor<S extends State, T>（StateDescriptor.java:48）:
 *   private final String name;
 *   private TypeSerializer<T> serializer;
 *   protected transient T defaultValue;
 *   getName() / getSerializer() / getDefaultValue()
 *   boolean isSerializerInitialized()
 *   void initializeSerializerUnlessSet(ExecutionConfig)
 *   public abstract S bind(StateBackend stateBackend) throws Exception;
 *
 * 第一个泛型参数 S 就是该 descriptor 对应的 State 子接口
 * （ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>, T>，
 *  ListStateDescriptor<T>  extends StateDescriptor<ListState<T>, T>），
 * 1.0 靠它让 `<S extends State> S getPartitionedState(StateDescriptor<S, ?>)`
 * 在编译期就确定返回类型。这里用 typedef StateType 暴露同一信息，供
 * AbstractStreamOperator.getPartitionedState 写出返回类型。
 *
 * 【偏差】bind(StateBackend) 不复刻：C++ 用重载决议代替这条虚分派链，
 * 详见 AbstractStateBackend.h 偏差 1。
 *
 * 【偏差】initializeSerializerUnlessSet(ExecutionConfig) 不复刻：1.0 在这里从
 * TypeInformation 现造 serializer；本实现由 TypeExtractor<T> 在编译期给出，
 * 判定逻辑落在 AbstractStateBackend.resolveSerializer。
 */
#ifndef FLINK_API_COMMON_STATE_STATEDESCRIPTOR_H
#define FLINK_API_COMMON_STATE_STATEDESCRIPTOR_H

#include <memory>
#include <string>
#include <utility>

#include "flink/api/common/typeutils/TypeSerializer.h"

namespace flink {
namespace api {
namespace common {
namespace state {

template <class S, class T>
class StateDescriptor {
 public:
  // 1.0 的 `S extends State` 泛型参数
  typedef S StateType;
  typedef T ValueType;
  typedef flink::api::common::typeutils::TypeSerializer<T> SerializerType;

  StateDescriptor(std::string name, T defaultValue)
      : name_(std::move(name)), defaultValue_(std::move(defaultValue)) {}
  StateDescriptor(std::string name, T defaultValue,
                  std::shared_ptr<SerializerType> serializer)
      : name_(std::move(name)),
        defaultValue_(std::move(defaultValue)),
        serializer_(serializer) {}
  virtual ~StateDescriptor() {}

  const std::string& getName() const { return name_; }
  const T& getDefaultValue() const { return defaultValue_; }
  const std::shared_ptr<SerializerType>& getSerializer() const {
    return serializer_;
  }

  // 1.0 isSerializerInitialized()
  bool isSerializerInitialized() const {
    return static_cast<bool>(serializer_);
  }

 private:
  std::string name_;
  T defaultValue_;
  std::shared_ptr<SerializerType> serializer_;
};

}  // namespace state
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_STATE_STATEDESCRIPTOR_H
