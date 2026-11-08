/*
 * flink-core : org.apache.flink.api.java.typeutils.runtime.TupleSerializer
 *
 * 1.0 TupleSerializer<T extends Tuple> 持有 TypeSerializer[] fieldSerializers，
 * serialize() 按字段顺序逐个委派。这里是 Tuple2 的特化版本（见 Tuple2.h 说明）。
 * getLength()：字段全定长则为长度之和，否则 -1（同 1.0 TupleSerializerBase）。
 */
#ifndef FLINK_API_JAVA_TYPEUTILS_RUNTIME_TUPLE2SERIALIZER_H
#define FLINK_API_JAVA_TYPEUTILS_RUNTIME_TUPLE2SERIALIZER_H

#include <memory>

#include "flink/api/common/typeutils/TypeSerializer.h"
#include "flink/api/java/tuple/Tuple2.h"

namespace flink {
namespace api {
namespace java {
namespace typeutils {
namespace runtime {

template <class T0, class T1>
class Tuple2Serializer
    : public flink::api::common::typeutils::TypeSerializer<
          flink::api::java::tuple::Tuple2<T0, T1> > {
 public:
  typedef flink::api::java::tuple::Tuple2<T0, T1> TupleType;
  typedef flink::api::common::typeutils::TypeSerializer<T0> Field0Serializer;
  typedef flink::api::common::typeutils::TypeSerializer<T1> Field1Serializer;

  Tuple2Serializer(std::shared_ptr<Field0Serializer> field0,
                   std::shared_ptr<Field1Serializer> field1)
      : field0_(field0), field1_(field1) {}

  bool isImmutableType() const override { return false; }
  TupleType createInstance() const override { return TupleType(); }

  int getLength() const override {
    const int l0 = field0_->getLength();
    const int l1 = field1_->getLength();
    return (l0 < 0 || l1 < 0) ? -1 : l0 + l1;
  }

  void serialize(const TupleType& record,
                 flink::core::memory::DataOutputView& target) const override {
    field0_->serialize(record.f0, target);
    field1_->serialize(record.f1, target);
  }

  TupleType deserialize(
      flink::core::memory::DataInputView& source) const override {
    T0 f0 = field0_->deserialize(source);
    T1 f1 = field1_->deserialize(source);
    return TupleType(f0, f1);
  }

 private:
  std::shared_ptr<Field0Serializer> field0_;
  std::shared_ptr<Field1Serializer> field1_;
};

}  // namespace runtime
}  // namespace typeutils
}  // namespace java
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_JAVA_TYPEUTILS_RUNTIME_TUPLE2SERIALIZER_H
