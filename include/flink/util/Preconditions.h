/*
 * flink-core : org.apache.flink.util.Preconditions
 *
 * 1.0 Preconditions.java: checkNotNull / checkArgument / checkState，
 * 失败时抛 NullPointerException / IllegalArgumentException / IllegalStateException。
 * C++ 这边统一映射到 std::invalid_argument / std::logic_error。
 */
#ifndef FLINK_UTIL_PRECONDITIONS_H
#define FLINK_UTIL_PRECONDITIONS_H

#include <memory>
#include <stdexcept>
#include <string>

namespace flink {
namespace util {

class Preconditions {
 public:
  // 1.0 Preconditions.checkNotNull(T reference, String errorMessage)
  template <class T>
  static const std::shared_ptr<T>& checkNotNull(const std::shared_ptr<T>& ref,
                                                const std::string& message) {
    if (!ref) throw std::invalid_argument(message);
    return ref;
  }
  template <class T>
  static T* checkNotNull(T* ref, const std::string& message) {
    if (ref == NULL) throw std::invalid_argument(message);
    return ref;
  }
  // 1.0 Preconditions.checkArgument(boolean, Object errorMessage)
  static void checkArgument(bool condition, const std::string& message) {
    if (!condition) throw std::invalid_argument(message);
  }
  // 1.0 Preconditions.checkState(boolean, Object errorMessage)
  static void checkState(bool condition, const std::string& message) {
    if (!condition) throw std::logic_error(message);
  }

 private:
  Preconditions() {}
};

}  // namespace util
}  // namespace flink
#endif  // FLINK_UTIL_PRECONDITIONS_H
