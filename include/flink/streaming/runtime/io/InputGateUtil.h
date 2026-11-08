/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.io.InputGateUtil
 *
 * 1.0 InputGateUtil.java:
 *   static InputGate createInputGate(Collection<InputGate>, Collection<InputGate>)
 *   static InputGate createInputGate(InputGate[] inputGates) {
 *       if (inputGates.length <= 0) throw new RuntimeException("No such input gate.");
 *       if (inputGates.length < 2)  return inputGates[0];
 *       else                        return new UnionInputGate(inputGates);
 *   }
 *
 * 1.0 原注释：「This will either just return the single InputGate that was
 * passed in or create a UnionInputGate if several input gates are given.」
 *
 * 【偏差】1.0 返回裸 InputGate（单 gate 时直接返回入参，不转移所有权；多 gate
 * 时新建对象交给 GC）。C++ 要显式管生命周期，所以返回 shared_ptr：单 gate 时
 * 用「非拥有」的空删除器包一层，多 gate 时真正持有 UnionInputGate。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_INPUTGATEUTIL_H
#define FLINK_STREAMING_RUNTIME_IO_INPUTGATEUTIL_H

#include <memory>
#include <stdexcept>
#include <vector>

#include "flink/runtime/io/network/partition/consumer/UnionInputGate.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class InputGateUtil {
 public:
  typedef flink::runtime::io::network::partition::consumer::InputGate InputGate;
  typedef flink::runtime::io::network::partition::consumer::UnionInputGate
      UnionInputGate;

  static std::shared_ptr<InputGate> createInputGate(
      const std::vector<InputGate*>& inputGates) {
    if (inputGates.empty()) {
      throw std::runtime_error("No such input gate.");
    }
    if (inputGates.size() < 2) {
      // 【偏差】非拥有包装：所有权仍在调用方（1.0 直接返回入参）
      return std::shared_ptr<InputGate>(inputGates[0], [](InputGate*) {});
    }
    return std::make_shared<UnionInputGate>(inputGates);
  }

  static std::shared_ptr<InputGate> createInputGate(
      const std::vector<InputGate*>& inputGates1,
      const std::vector<InputGate*>& inputGates2) {
    std::vector<InputGate*> gates;
    gates.reserve(inputGates1.size() + inputGates2.size());
    gates.insert(gates.end(), inputGates1.begin(), inputGates1.end());
    gates.insert(gates.end(), inputGates2.begin(), inputGates2.end());
    return createInputGate(gates);
  }

 private:
  InputGateUtil() {}
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_INPUTGATEUTIL_H
