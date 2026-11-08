/*
 * flink-core : org.apache.flink.api.java.tuple.Tuple2
 *
 * 1.0 Tuple2.java: public T0 f0; public T1 f1;
 *   getField(int pos) / setField(T value, int pos) / getArity() / copy()
 *   toString() 形如 "(f0,f1)"
 * 1.0 的 Tuple0..Tuple25 是代码生成的同构重复，这里只实现最常用的 Tuple2
 * （1.0 的 WindowWordCount / SocketTextStreamWordCount 示例都只用 Tuple2）。
 */
#ifndef FLINK_API_JAVA_TUPLE_TUPLE2_H
#define FLINK_API_JAVA_TUPLE_TUPLE2_H

#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "flink/util/JavaHashCode.h"

namespace flink {
namespace api {
namespace java {
namespace tuple {

template <class T0, class T1>
class Tuple2 {
 public:
  T0 f0;
  T1 f1;

  Tuple2() : f0(), f1() {}
  Tuple2(T0 value0, T1 value1) : f0(std::move(value0)), f1(std::move(value1)) {}

  // 1.0 Tuple.getArity()
  int getArity() const { return 2; }

  // 1.0 Tuple2.toString(): "(" + f0 + "," + f1 + ")"
  std::string toString() const {
    std::ostringstream os;
    os << "(" << f0 << "," << f1 << ")";
    return os.str();
  }

  bool operator==(const Tuple2<T0, T1>& other) const {
    return f0 == other.f0 && f1 == other.f1;
  }
  bool operator!=(const Tuple2<T0, T1>& other) const {
    return !(*this == other);
  }
};

template <class T0, class T1>
std::ostream& operator<<(std::ostream& os, const Tuple2<T0, T1>& value) {
  return os << value.toString();
}

}  // namespace tuple
}  // namespace java
}  // namespace api

namespace util {
// java.lang.Tuple2.hashCode():
//   result = f0.hashCode(); result = 31 * result + f1.hashCode()
// keyBy(Tuple2) 要与 Java 版分到同一通道，必须复刻这个组合方式。
template <class T0, class T1>
struct JavaHashCode<flink::api::java::tuple::Tuple2<T0, T1> > {
  static int32_t apply(const flink::api::java::tuple::Tuple2<T0, T1>& value) {
    uint32_t result = static_cast<uint32_t>(JavaHashCode<T0>::apply(value.f0));
    result = result * 31u +
             static_cast<uint32_t>(JavaHashCode<T1>::apply(value.f1));
    return static_cast<int32_t>(result);
  }
};
}  // namespace util
}  // namespace flink
#endif  // FLINK_API_JAVA_TUPLE_TUPLE2_H
