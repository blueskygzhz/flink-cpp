/*
 * flink-core : org.apache.flink.api.java.typeutils.TypeExtractor
 *
 * 【与 Flink 1.0 的偏差】
 * 1.0 的 TypeExtractor 用反射从 UDF 的泛型签名推导出 TypeInformation，再由
 * TypeInformation.createSerializer(ExecutionConfig) 造出 TypeSerializer
 * （flink-core 里这套类型系统约 2.3 万行）。C++ 没有运行期反射，但模板在**编译期**
 * 就知道确切类型，所以这里把「类型 -> serializer」做成可特化的模板：
 *   - 内置类型已特化；
 *   - 用户自定义类型由用户自己特化 TypeExtractor<MyType>；
 *   - 未特化时返回 nullptr，等价于 1.0 推导失败走 GenericTypeInfo/Kryo 的位置，
 *     本实现的后果是该类型的状态无法序列化落盘（只能留在内存）。
 */
#ifndef FLINK_API_JAVA_TYPEUTILS_TYPEEXTRACTOR_H
#define FLINK_API_JAVA_TYPEUTILS_TYPEEXTRACTOR_H

#include <memory>
#include <string>

#include "flink/api/common/typeutils/TypeSerializer.h"
#include "flink/api/common/typeutils/base/BooleanSerializer.h"
#include "flink/api/common/typeutils/base/DoubleSerializer.h"
#include "flink/api/common/typeutils/base/IntSerializer.h"
#include "flink/api/common/typeutils/base/LongSerializer.h"
#include "flink/api/common/typeutils/base/StringSerializer.h"
#include "flink/api/java/tuple/Tuple2.h"
#include "flink/api/java/typeutils/runtime/Tuple2Serializer.h"

namespace flink {
namespace api {
namespace java {
namespace typeutils {

template <class T>
struct TypeExtractor {
  static std::shared_ptr<flink::api::common::typeutils::TypeSerializer<T> >
  createSerializer() {
    return std::shared_ptr<
        flink::api::common::typeutils::TypeSerializer<T> >();
  }
};

// 对应 1.0 BasicTypeInfo.INT_TYPE_INFO 等
template <>
struct TypeExtractor<int32_t> {
  static std::shared_ptr<
      flink::api::common::typeutils::TypeSerializer<int32_t> >
  createSerializer() {
    return std::make_shared<
        flink::api::common::typeutils::base::IntSerializer>();
  }
};

template <>
struct TypeExtractor<int64_t> {
  static std::shared_ptr<
      flink::api::common::typeutils::TypeSerializer<int64_t> >
  createSerializer() {
    return std::make_shared<
        flink::api::common::typeutils::base::LongSerializer>();
  }
};

template <>
struct TypeExtractor<double> {
  static std::shared_ptr<flink::api::common::typeutils::TypeSerializer<double> >
  createSerializer() {
    return std::make_shared<
        flink::api::common::typeutils::base::DoubleSerializer>();
  }
};

template <>
struct TypeExtractor<bool> {
  static std::shared_ptr<flink::api::common::typeutils::TypeSerializer<bool> >
  createSerializer() {
    return std::make_shared<
        flink::api::common::typeutils::base::BooleanSerializer>();
  }
};

template <>
struct TypeExtractor<std::string> {
  static std::shared_ptr<
      flink::api::common::typeutils::TypeSerializer<std::string> >
  createSerializer() {
    return std::make_shared<
        flink::api::common::typeutils::base::StringSerializer>();
  }
};

// 对应 1.0 TupleTypeInfo.createSerializer()：逐字段取 serializer 再组合
template <class T0, class T1>
struct TypeExtractor<flink::api::java::tuple::Tuple2<T0, T1> > {
  typedef flink::api::java::tuple::Tuple2<T0, T1> TupleType;
  static std::shared_ptr<
      flink::api::common::typeutils::TypeSerializer<TupleType> >
  createSerializer() {
    std::shared_ptr<flink::api::common::typeutils::TypeSerializer<T0> > f0 =
        TypeExtractor<T0>::createSerializer();
    std::shared_ptr<flink::api::common::typeutils::TypeSerializer<T1> > f1 =
        TypeExtractor<T1>::createSerializer();
    if (!f0 || !f1) {
      return std::shared_ptr<
          flink::api::common::typeutils::TypeSerializer<TupleType> >();
    }
    return std::make_shared<
        flink::api::java::typeutils::runtime::Tuple2Serializer<T0, T1> >(f0,
                                                                        f1);
  }
};

}  // namespace typeutils
}  // namespace java
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_JAVA_TYPEUTILS_TYPEEXTRACTOR_H
