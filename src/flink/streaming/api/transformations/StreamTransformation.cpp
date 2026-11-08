/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.StreamTransformation
 *
 * 1.0 里 `protected static Integer idCounter = 0;` 是类里就地初始化的
 * （StreamTransformation.java:100）。C++11 没有 inline 变量，静态数据成员必须有
 * 一处 out-of-line 定义，所以单独开这个 .cpp —— 它是本类唯一的实现体。
 */
#include "flink/streaming/api/transformations/StreamTransformation.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

// 1.0: protected static Integer idCounter = 0;
int StreamTransformation::idCounter_ = 0;

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
