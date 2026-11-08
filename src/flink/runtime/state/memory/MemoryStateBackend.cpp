#include "flink/runtime/state/memory/MemoryStateBackend.h"

#include <sstream>
#include <stdexcept>

#include "flink/runtime/state/HeapListState.h"
#include "flink/runtime/state/HeapValueState.h"

namespace flink {
namespace runtime {
namespace state {
namespace memory {

std::shared_ptr<KvState> MemoryStateBackend::createValueState(
    SerializerPtr namespaceSerializer, SerializerPtr valueSerializer) {
  return std::make_shared<HeapValueState>(keySerializer_, namespaceSerializer,
                                          valueSerializer);
}

std::shared_ptr<KvState> MemoryStateBackend::createListState(
    SerializerPtr namespaceSerializer, SerializerPtr valueSerializer) {
  return std::make_shared<HeapListState>(keySerializer_, namespaceSerializer,
                                         valueSerializer);
}

StateHandle MemoryStateBackend::checkpointStateSerializable(
    const std::string& bytes, int64_t checkpointId, int64_t timestamp) {
  (void)checkpointId;  // 1.0 内存后端同样不用这两个参数
  (void)timestamp;
  checkSize(bytes.size(), maxStateSize_);
  return StateHandle(bytes);
}

// 1.0 MemoryStateBackend.checkSize(int, int)：原文抛 IOException
void MemoryStateBackend::checkSize(size_t size, int maxSize) {
  if (size > static_cast<size_t>(maxSize)) {
    std::ostringstream message;
    message << "Size of the state is larger than the maximum permitted "
               "memory-backed state. Size="
            << size << " , maxSize=" << maxSize
            << " . Consider using a different state backend, like the File "
               "System State backend.";
    throw std::runtime_error(message.str());
  }
}

}  // namespace memory
}  // namespace state
}  // namespace runtime
}  // namespace flink
