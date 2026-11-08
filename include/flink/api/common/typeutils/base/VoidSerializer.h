/*
 * flink-core : org.apache.flink.api.common.typeutils.base.VoidSerializer
 *
 * 1.0 VoidSerializer extends TypeSerializerSingleton<Void>：
 *   isImmutableType()=true, getLength()=1, createInstance()=null
 *   serialize(Void, target) { target.write(0); }  // "make progress in the
 *                                                 //  stream, write one byte"
 *   deserialize(source)    { source.readByte(); return null; }
 *
 * 用处：非窗口的 keyed state 在 1.0 里 namespace 恒为 null，取状态时传的就是
 * `getPartitionedState(null, VoidSerializer.INSTANCE, descriptor)`
 * （AbstractStreamOperator.java:260）。
 *
 * 【偏差】C++ 没有 Void 类型，也没有「TypeSerializer<Void>」可言，所以直接做成
 * AnySerializer（状态后端的 namespaceSerializer 就是这个类型），输入输出都是
 * isNull() 的 Any。另外 DataOutputView 没保留 write(int)/readByte()，这里用
 * writeBoolean(false)/readBoolean() 占同样的 1 个字节。
 */
#ifndef FLINK_API_COMMON_TYPEUTILS_BASE_VOIDSERIALIZER_H
#define FLINK_API_COMMON_TYPEUTILS_BASE_VOIDSERIALIZER_H

#include <memory>

#include "flink/api/common/typeutils/AnySerializer.h"

namespace flink {
namespace api {
namespace common {
namespace typeutils {
namespace base {

class VoidSerializer : public AnySerializer {
 public:
  int getLength() const { return 1; }

  void serialize(const flink::types::Any&,
                 flink::core::memory::DataOutputView& target) const override {
    target.writeBoolean(false);
  }

  flink::types::Any deserialize(
      flink::core::memory::DataInputView& source) const override {
    source.readBoolean();
    return flink::types::Any();
  }

  // 1.0 VoidSerializer.INSTANCE
  static std::shared_ptr<const AnySerializer> INSTANCE() {
    static std::shared_ptr<const AnySerializer> instance =
        std::make_shared<VoidSerializer>();
    return instance;
  }
};

}  // namespace base
}  // namespace typeutils
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_TYPEUTILS_BASE_VOIDSERIALIZER_H
