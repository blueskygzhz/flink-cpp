/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.partitioner.StreamPartitioner
 *
 * 1.0 StreamPartitioner.java 全文（除 import）：
 *   public abstract class StreamPartitioner<T> implements
 *           ChannelSelector<SerializationDelegate<StreamRecord<T>>>, Serializable {
 *       private static final long serialVersionUID = 1L;
 *       public abstract StreamPartitioner<T> copy();
 *   }
 *
 * 也就是说 partitioner 就是 runtime 的 ChannelSelector，只是把元素类型钉成
 * 「即将序列化的那条 StreamRecord」，并额外要求一个 copy()。
 *
 * 八个实现（1.0 同包）及其 toString()：
 *   ForwardPartitioner   "FORWARD"    恒 {0}，只发本地下游（要求上下游同并行度）
 *   RebalancePartitioner "REBALANCE"  轮转，初值 -1
 *   RescalePartitioner   "RESCALE"    轮转，初值 -1（与 REBALANCE 代码相同，
 *                                     区别在 JobGraph 侧用 POINTWISE 连接模式）
 *   GlobalPartitioner    "GLOBAL"     恒 {0}，全部发往 subtask 0
 *   ShufflePartitioner   "SHUFFLE"    random.nextInt(numChannels)
 *   BroadcastPartitioner "BROADCAST"  {0,1,...,n-1}，带 numChannels 缓存
 *   HashPartitioner      "HASH"       MathUtils.murmurHash(key.hashCode()) % n
 *   CustomPartitionerWrapper          包装用户的 Partitioner<K>
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 元素类型】理由见 ChannelSelector.h 的偏差 1：T 固定为
 * RecordPtr（= shared_ptr<const StreamElement>），不再是模板参数。
 * 取值处 `record->as<StreamRecord>().getValue()` 对应 1.0 的
 * `record.getInstance().getValue()`。
 *
 * 【偏差 2 —— copy() 的返回类型】1.0 返回 StreamPartitioner<T>，且多数实现
 * **return this**（与原对象共享同一实例），只有 ShufflePartitioner 返回
 * new（因为它持有 Random）。C++ 这边返回 shared_ptr，为了能表达「return this」
 * 这层语义而继承 std::enable_shared_from_this —— 因此 partitioner 必须由
 * shared_ptr 持有（StreamEdge 正是这么持的）。
 * 顺便记一笔：copy() 在 1.0 全仓库里没有任何调用点，是声明了却没人用的方法；
 * 这里仍按 1.0 原样落地，以免日后接 DataStream 时发现缺。
 */
#ifndef FLINK_STREAMING_RUNTIME_PARTITIONER_STREAMPARTITIONER_H
#define FLINK_STREAMING_RUNTIME_PARTITIONER_STREAMPARTITIONER_H

#include <memory>
#include <string>

#include "flink/runtime/io/network/api/writer/ChannelSelector.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace partitioner {

class StreamPartitioner
    : public flink::runtime::io::network::api::writer::ChannelSelector,
      public std::enable_shared_from_this<StreamPartitioner> {
 public:
  // 1.0: public abstract StreamPartitioner<T> copy();
  virtual std::shared_ptr<StreamPartitioner> copy() = 0;

  // 1.0 各实现都重写了 toString()，StreamEdge.toString() 与
  // StreamingJobGraphGenerator 的日志都用它
  virtual std::string toString() const = 0;
};

}  // namespace partitioner
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_PARTITIONER_STREAMPARTITIONER_H
