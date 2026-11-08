/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.collector.selector.OutputSelector
 *
 * 1.0 OutputSelector.java 全文（除注释）：
 *   public interface OutputSelector<OUT> extends Serializable {
 *       Iterable<String> select(OUT value);
 *   }
 *
 * 1.0 原注释：「Interface for defining an OutputSelector for a SplitStream
 * using the SingleOutputStreamOperator#split call. Every output object of a
 * SplitStream will run through this operator to select outputs. The values
 * will be emitted only to output names which are contained in the returned
 * iterable.」
 *
 * 用处链路：DataStream.split(OutputSelector) -> SplitStream.select(names) ->
 * StreamEdge.selectedNames -> OperatorChain 建 DirectedOutput 按名字路由。
 *
 * 【偏差】Iterable<String> -> std::vector<std::string>：1.0 允许返回惰性
 * Iterable，DirectedOutput 只做一次 for-each 遍历，所以用 vector 等价。
 */
#ifndef FLINK_STREAMING_API_COLLECTOR_SELECTOR_OUTPUTSELECTOR_H
#define FLINK_STREAMING_API_COLLECTOR_SELECTOR_OUTPUTSELECTOR_H

#include <string>
#include <vector>

namespace flink {
namespace streaming {
namespace api {
namespace collector {
namespace selector {

template <class OUT>
class OutputSelector {
 public:
  virtual ~OutputSelector() {}

  // 1.0 Iterable<String> select(OUT value)
  virtual std::vector<std::string> select(const OUT& value) = 0;
};

}  // namespace selector
}  // namespace collector
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_COLLECTOR_SELECTOR_OUTPUTSELECTOR_H
