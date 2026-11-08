/*
 * flink-core : org.apache.flink.util.Collector
 *
 * 1.0 Collector.java:
 *   void collect(T record)
 *   void close()
 */
#ifndef FLINK_UTIL_COLLECTOR_H
#define FLINK_UTIL_COLLECTOR_H

namespace flink {
namespace util {

template <class T>
class Collector {
 public:
  virtual ~Collector() {}
  virtual void collect(const T& record) = 0;
  virtual void close() {}
};

}  // namespace util
}  // namespace flink
#endif  // FLINK_UTIL_COLLECTOR_H
