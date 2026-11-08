/*
 * flink-core : org.apache.flink.api.common.functions.Partitioner
 * 1.0 Partitioner.java:  int partition(K key, int numPartitions)
 *   DataStream.partitionCustom 用它，返回目标分区号。
 */
#ifndef FLINK_API_COMMON_FUNCTIONS_PARTITIONER_H
#define FLINK_API_COMMON_FUNCTIONS_PARTITIONER_H

#include "flink/api/common/functions/Function.h"

namespace flink {
namespace api {
namespace common {
namespace functions {

template <class K>
class Partitioner : public Function {
 public:
  typedef K KEY;

  virtual int partition(const K& key, int numPartitions) = 0;
};

}  // namespace functions
}  // namespace common
}  // namespace api
}  // namespace flink
#endif  // FLINK_API_COMMON_FUNCTIONS_PARTITIONER_H
