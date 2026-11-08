/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.collector.selector.DirectedOutput
 *
 * 1.0 class DirectedOutput<OUT> implements Output<StreamRecord<OUT>>
 * （DirectedOutput.java:35）—— split()/select() 的运行时实现：按 OutputSelector
 * 返回的名字，把记录只发给订阅了这些名字的那几个下游。
 *
 * 字段（:37-43）：
 *   private final OutputSelector<OUT>[] outputSelectors;
 *   private final Output<StreamRecord<OUT>>[] selectAllOutputs;
 *   private final HashMap<String, Output<StreamRecord<OUT>>[]> outputMap;
 *   private final Output<StreamRecord<OUT>>[] allOutputs;
 *
 * 构造（:47-93）：遍历 (output, edge) 对
 *   - edge.getSelectedNames().isEmpty()  -> 放进 selectAllOutputs（无条件接收）
 *   - 否则对每个名字放进 outputMap[name]（去重）
 *
 * collect(record)（:104）：
 *   selectedOutputs = selectAllOutputs 的副本
 *   对每个 selector：select(record.getValue()) 拿到名字集合，
 *                    把 outputMap[name] 并进 selectedOutputs（HashSet 去重）
 *   逐个 out.collect(record)
 *   —— 注意 **watermark 不走选择器**：emitWatermark 发给 allOutputs 的全部
 *      （:97），否则下游分支的水位线会停住。
 * close()（:125）：allOutputs 全部 close
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 去重容器】1.0 用 HashSet<Output>（按对象身份去重），因此
 * collect 的下发顺序是**未定义**的。这里用 vector + 线性查重，保持
 * 「selectAllOutputs 在前、按 selector 顺序追加」的确定顺序。
 * 任何顺序都满足 1.0 的语义，确定顺序更好测。
 *
 * 【偏差 2 —— 类型擦除】OutputSelector<OUT> -> AnyOutputSelector（见
 * AnyOutputSelector.h）；Output<StreamRecord<OUT>> -> Output（不带模板参数）。
 *
 * 【偏差 3 —— 不拥有 output】1.0 的数组元素由 OperatorChain 的
 * streamOutputs/链内 ChainingOutput 持有；这里同样用裸指针，所有权在
 * OperatorChain。
 */
#ifndef FLINK_STREAMING_API_COLLECTOR_SELECTOR_DIRECTEDOUTPUT_H
#define FLINK_STREAMING_API_COLLECTOR_SELECTOR_DIRECTEDOUTPUT_H

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flink/streaming/api/collector/selector/AnyOutputSelector.h"
#include "flink/streaming/api/graph/StreamEdge.h"
#include "flink/streaming/api/operators/Output.h"

namespace flink {
namespace streaming {
namespace api {
namespace collector {
namespace selector {

class DirectedOutput : public flink::streaming::api::operators::Output {
 public:
  typedef flink::streaming::api::operators::Output Output;
  typedef flink::streaming::runtime::streamrecord::StreamRecord StreamRecord;
  typedef flink::streaming::api::watermark::Watermark Watermark;
  typedef std::shared_ptr<AnyOutputSelector> OutputSelectorPtr;
  typedef std::pair<Output*, const flink::streaming::api::graph::StreamEdge*>
      OutputWithEdge;

  DirectedOutput(const std::vector<OutputSelectorPtr>& outputSelectors,
                 const std::vector<OutputWithEdge>& outputs)
      : outputSelectors_(outputSelectors) {
    allOutputs_.reserve(outputs.size());
    for (const OutputWithEdge& pair : outputs) {
      allOutputs_.push_back(pair.first);
    }

    for (const OutputWithEdge& pair : outputs) {
      Output* output = pair.first;
      const std::vector<std::string>& selectedNames =
          pair.second->getSelectedNames();

      if (selectedNames.empty()) {
        // 1.0: selectAllOutputs.add(output)（HashSet 去重）
        addUnique(selectAllOutputs_, output);
      } else {
        for (const std::string& selectedName : selectedNames) {
          // 1.0: outputMap.get(name).contains(output) 判重后 add
          addUnique(outputMap_[selectedName], output);
        }
      }
    }
  }

  // 1.0 emitWatermark(Watermark)（:97）：水位线**不经过选择器**，发给全部输出
  void emitWatermark(const Watermark& mark) override {
    for (Output* out : allOutputs_) {
      out->emitWatermark(mark);
    }
  }

  // 1.0 collect(StreamRecord<OUT>)（:104）
  void collect(const StreamRecord& record) override {
    // 1.0: selectedOutputs = new HashSet<>(selectAllOutputs)
    std::vector<Output*> selectedOutputs(selectAllOutputs_);

    for (const OutputSelectorPtr& outputSelector : outputSelectors_) {
      const std::vector<std::string> outputNames =
          outputSelector->select(record.getValue());

      for (const std::string& outputName : outputNames) {
        auto it = outputMap_.find(outputName);
        if (it != outputMap_.end()) {
          // 1.0: Collections.addAll(selectedOutputs, outputList)
          for (Output* out : it->second) {
            addUnique(selectedOutputs, out);
          }
        }
      }
    }

    for (Output* out : selectedOutputs) {
      out->collect(record);
    }
  }

  // 1.0 close()（:125）
  void close() override {
    for (Output* out : allOutputs_) {
      out->close();
    }
  }

 private:
  static void addUnique(std::vector<Output*>& target, Output* output) {
    if (std::find(target.begin(), target.end(), output) == target.end()) {
      target.push_back(output);
    }
  }

  std::vector<OutputSelectorPtr> outputSelectors_;
  std::vector<Output*> selectAllOutputs_;
  std::unordered_map<std::string, std::vector<Output*> > outputMap_;
  std::vector<Output*> allOutputs_;
};

}  // namespace selector
}  // namespace collector
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_COLLECTOR_SELECTOR_DIRECTEDOUTPUT_H
