/*
 * flink-core : org.apache.flink.types
 *
 * 【与 Flink 1.0 的偏差 —— 刻意为之，不是省略】
 * 1.0 的网络栈与状态后端面对的是 StreamRecord<Object>：Java 有统一的 Object
 * 根类型 + 反射，任意用户类型都能被装箱、按 TypeSerializer 还原。
 * C++ 没有公共根类型，所以这里引入 Any 作为「运行时装箱容器」：
 *   - 编译期类型由 DataStream<T> / 各算子的模板参数保证；
 *   - 运行期统一以 Any 在通道、状态、快照之间流转；
 *   - as<T>() 做类型校验，类型不符时抛异常（对应 Java 的 ClassCastException）。
 *
 * hashCode() / operator== 是 keyed state 的硬需求：1.0 要求 keyBy 的 KEY
 * 实现 hashCode/equals（HashPartitioner 分区 + HashMap 状态分桶都依赖），
 * 这里在装箱时一并捕获 T 的哈希与相等语义，使 Any 能直接做 unordered_map 的 key。
 * 装入的类型若没有 JavaHashCode 特化、或没有 operator==，会在该处编译报错
 * —— 等价于 1.0 里「KEY 没实现 hashCode/equals」的约束，只是从运行期提前到编译期。
 *
 * 对应 Java 概念: Object + TypeInformation 的运行时装箱部分。
 */
#ifndef FLINK_TYPES_ANY_H
#define FLINK_TYPES_ANY_H

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <typeinfo>

#include "flink/util/JavaHashCode.h"

namespace flink {
namespace types {

class Any {
 public:
  Any() : holder_(), type_(typeid(void)) {}

  template <class T>
  static Any of(const T& value) {
    Any a;
    a.holder_.reset(new Holder<T>(value));
    a.type_ = std::type_index(typeid(T));
    return a;
  }

  bool isNull() const { return !holder_; }

  template <class T>
  bool isInstanceOf() const {
    return holder_ && type_ == std::type_index(typeid(T));
  }

  template <class T>
  const T& as() const {
    if (!holder_) {
      throw std::runtime_error(std::string("Any: null value, expected ") +
                               typeid(T).name());
    }
    if (type_ != std::type_index(typeid(T))) {
      throw std::runtime_error(std::string("Any: type mismatch, expected ") +
                               typeid(T).name() + " but was " + type_.name());
    }
    return static_cast<const Holder<T>*>(holder_.get())->value;
  }

  const std::type_index& getTypeIndex() const { return type_; }

  // 对应 Java Object.hashCode()；null 的哈希为 0（同 Java 容器约定）
  int32_t hashCode() const { return holder_ ? holder_->hashCode() : 0; }

  // 对应 Java Object.equals()：类型不同即不等
  bool operator==(const Any& other) const {
    if (!holder_ && !other.holder_) return true;
    if (!holder_ || !other.holder_) return false;
    if (type_ != other.type_) return false;
    return holder_->equals(*other.holder_);
  }
  bool operator!=(const Any& other) const { return !(*this == other); }

 private:
  struct HolderBase {
    virtual ~HolderBase() {}
    virtual int32_t hashCode() const = 0;
    virtual bool equals(const HolderBase& other) const = 0;
  };
  template <class T>
  struct Holder : HolderBase {
    T value;
    explicit Holder(const T& v) : value(v) {}
    int32_t hashCode() const override {
      return flink::util::JavaHashCode<T>::apply(value);
    }
    bool equals(const HolderBase& other) const override {
      const Holder<T>* typed = dynamic_cast<const Holder<T>*>(&other);
      return typed != NULL && value == typed->value;
    }
  };

  std::shared_ptr<HolderBase> holder_;
  std::type_index type_;
};

}  // namespace types
}  // namespace flink

namespace std {
// 让 Any 能直接作为 unordered_map 的 key（keyed state 的状态分桶）
template <>
struct hash<flink::types::Any> {
  size_t operator()(const flink::types::Any& value) const {
    return static_cast<size_t>(static_cast<uint32_t>(value.hashCode()));
  }
};
}  // namespace std
#endif  // FLINK_TYPES_ANY_H
