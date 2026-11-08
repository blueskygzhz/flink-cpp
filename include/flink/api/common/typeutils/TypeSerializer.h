/*
 * flink-core : org.apache.flink.api.common.typeutils.TypeSerializer
 *
 * 1.0 TypeSerializer.java 的方法：
 *   boolean isImmutableType()
 *   TypeSerializer<T> duplicate()
 *   T createInstance()
 *   T copy(T from) / T copy(T from, T reuse)
 *   int getLength()
 *   void serialize(T record, DataOutputView target)
 *   T deserialize(DataInputView source) / T deserialize(T reuse, DataInputView)
 *   void copy(DataInputView source, DataOutputView target)
 *
 * 这里省略 reuse 重载：1.0 的 reuse 是为了在 JVM 上减少 GC 压力，
 * C++ 按值返回配合 move 语义已无该问题，保留只会是同构重复。
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_TYPESERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_TYPESERIALIZER_H

#include "flink/core/memory/DataInputView.h"
#include "flink/core/memory/DataOutputView.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {

template <class T>
class TypeSerializer {
 public:
  virtual ~TypeSerializer() {}

  virtual bool isImmutableType() const = 0;
  virtual T createInstance() const { return T(); }
  virtual T copy(const T& from) const { return from; }
  // 1.0: 定长类型返回字节数，变长返回 -1
  virtual int getLength() const = 0;

  virtual void serialize(const T& record,
                         flink::core::memory::DataOutputView& target) const = 0;
  virtual T deserialize(flink::core::memory::DataInputView& source) const = 0;
};

}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_TYPESERIALIZER_H
