/*
 * flink-streaming-java : org.apache.flink.streaming.runtime.io.BufferSpiller
 *
 * 1.0 BufferSpiller（BufferSpiller.java）：
 *   BufferSpiller(IOManager ioManager, int pageSize)
 *   void add(BufferOrEvent boe)                  把被阻塞通道的数据写到临时文件
 *   SpilledBufferOrEventSequence rollOver()      封存当前文件，换下一个
 *   SpilledBufferOrEventSequence rollOverWithNewBuffer()
 *   void close()
 *   static class SpilledBufferOrEventSequence {
 *       void open(); BufferOrEvent getNext(); void cleanup(); }
 *
 * 1.0 要落盘是因为：exactly-once 对齐期间，先到屏障的通道会被阻塞，而它后续的
 * 数据必须留着（属于下一个 checkpoint），数据量无上界，所以写进
 * ioManager 的临时目录，按 pageSize 页管理，用 FileChannel + 直接内存。
 *
 * 【偏差 —— 改成纯内存队列】本实现的「Buffer」是一个 StreamElement 的
 * shared_ptr（见 BufferOrEvent.h 的偏差），不是序列化后的字节页，没有
 * pageSize 概念也没有 MemorySegment；而且整个作业跑在一个进程里，
 * 这些 shared_ptr 本来就在堆上。所以这里用 deque 代替文件，
 * add/rollOver/open/getNext/cleanup 的**调用契约逐个保留**，
 * 让 BarrierBuffer 的代码能原样对照 1.0。
 * 代价：对齐期间的缓存量受内存限制（1.0 受磁盘限制）。
 */
#ifndef FLINK_STREAMING_RUNTIME_IO_BUFFERSPILLER_H
#define FLINK_STREAMING_RUNTIME_IO_BUFFERSPILLER_H

#include <cstddef>
#include <deque>
#include <memory>
#include <utility>

#include "flink/runtime/io/network/partition/consumer/BufferOrEvent.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace io {

class BufferSpiller {
 public:
  typedef flink::runtime::io::network::partition::consumer::BufferOrEvent
      BufferOrEvent;

  // 1.0 BufferSpiller.SpilledBufferOrEventSequence
  class SpilledBufferOrEventSequence {
   public:
    explicit SpilledBufferOrEventSequence(std::deque<BufferOrEvent> sequence)
        : sequence_(std::move(sequence)) {}

    // 1.0 open()：把文件 channel 定位到开头准备读
    void open() {}

    // 1.0 getNext()：读完返回 null
    bool getNext(BufferOrEvent& target) {
      if (sequence_.empty()) return false;
      target = sequence_.front();
      sequence_.pop_front();
      return true;
    }

    // 1.0 cleanup()：关掉并删掉临时文件
    void cleanup() { sequence_.clear(); }

    size_t size() const { return sequence_.size(); }

   private:
    std::deque<BufferOrEvent> sequence_;
  };

  typedef std::shared_ptr<SpilledBufferOrEventSequence> SequencePtr;

  BufferSpiller() {}

  // 1.0 add(BufferOrEvent)
  void add(const BufferOrEvent& boe) { current_.push_back(boe); }

  // 1.0 rollOver()：没攒下任何东西就返回 null
  SequencePtr rollOver() {
    if (current_.empty()) return SequencePtr();
    SequencePtr sequence = std::make_shared<SpilledBufferOrEventSequence>(
        std::move(current_));
    current_.clear();
    return sequence;
  }

  // 1.0 rollOverWithNewBuffer()：语义同上，区别只在 1.0 要为新序列另分配一块
  // 写缓冲（因为上一个序列还没读完，不能复用同一块内存）。内存队列天然独立，
  // 所以这里与 rollOver() 等价。
  SequencePtr rollOverWithNewBuffer() { return rollOver(); }

  // 1.0 close()
  void close() { current_.clear(); }

 private:
  std::deque<BufferOrEvent> current_;
};

}  // namespace io
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_IO_BUFFERSPILLER_H
