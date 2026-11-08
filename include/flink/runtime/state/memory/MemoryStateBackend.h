/*
 * flink-runtime : org.apache.flink.runtime.state.memory.MemoryStateBackend
 *
 * 1.0 MemoryStateBackend extends AbstractStateBackend：
 *   DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024    // 5 MiB
 *   MemoryStateBackend() / MemoryStateBackend(int maxStateSize)
 *   void disposeAllStateForCurrentJob()   // "nothing to do here, GC will do it"
 *   void close()
 *   createValueState(..) -> new MemValueState<>(keySerializer, nsSerializer, desc)
 *   createListState(..)  -> new MemListState<>(...)
 *   <S extends Serializable> StateHandle<S> checkpointStateSerializable(
 *       S state, long checkpointID, long timestamp)   // 带 checkSize
 *   static void checkSize(int size, int maxSize)       // 超限抛 IOException
 *   String toString() = "MemoryStateBackend (data in heap memory / checkpoints
 *                        to JobManager)"
 *   static MemoryStateBackend create()
 *
 * 【偏差】1.0 的 createValueState/createListState 返回的 MemValueState /
 * MemListState 同时是 KvState 与 ValueState<V>；本实现返回纯 KvState
 * （HeapValueState / HeapListState），类型化包装由基类套上
 * （见 TypedValueState.h）。
 *
 * 【偏差】不复刻 MemoryCheckpointOutputStream / createCheckpointStateOutputStream
 * ——那是给非分区状态（如 source 的 offset）用的，本实现只做分区状态。
 */
#ifndef FLINK_RUNTIME_STATE_MEMORY_MEMORYSTATEBACKEND_H
#define FLINK_RUNTIME_STATE_MEMORY_MEMORYSTATEBACKEND_H

#include <memory>
#include <string>

#include "flink/runtime/state/AbstractStateBackend.h"

namespace flink {
namespace runtime {
namespace state {
namespace memory {

class MemoryStateBackend : public AbstractStateBackend {
 public:
  // 1.0 DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024
  static const int DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024;

  MemoryStateBackend() : maxStateSize_(DEFAULT_MAX_STATE_SIZE) {}
  explicit MemoryStateBackend(int maxStateSize)
      : maxStateSize_(maxStateSize) {}

  // 1.0: "nothing to do here, GC will do it"
  void disposeAllStateForCurrentJob() override {}

  void close() override {}

  // 1.0 checkpointStateSerializable(S, long, long)：序列化后做尺寸校验
  StateHandle checkpointStateSerializable(const std::string& bytes,
                                          int64_t checkpointId,
                                          int64_t timestamp) override;

  // 1.0 toString()
  std::string toString() const {
    return "MemoryStateBackend (data in heap memory / checkpoints to "
           "JobManager)";
  }

  // 1.0 static void checkSize(int size, int maxSize) throws IOException
  static void checkSize(size_t size, int maxSize);

  // 1.0 static MemoryStateBackend create()
  static std::shared_ptr<MemoryStateBackend> create() {
    return std::make_shared<MemoryStateBackend>();
  }

 protected:
  // 1.0 createValueState -> new MemValueState<>(keySerializer, nsSerializer, desc)
  std::shared_ptr<KvState> createValueState(
      SerializerPtr namespaceSerializer,
      SerializerPtr valueSerializer) override;

  // 1.0 createListState -> new MemListState<>(keySerializer, nsSerializer, desc)
  std::shared_ptr<KvState> createListState(
      SerializerPtr namespaceSerializer,
      SerializerPtr valueSerializer) override;

 private:
  int maxStateSize_;
};

}  // namespace memory
}  // namespace state
}  // namespace runtime
}  // namespace flink
#endif  // FLINK_RUNTIME_STATE_MEMORY_MEMORYSTATEBACKEND_H
