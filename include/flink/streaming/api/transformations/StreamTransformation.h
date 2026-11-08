/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.transformations.StreamTransformation
 *
 * 1.0 abstract class StreamTransformation<T>（StreamTransformation.java:98）
 *
 * 1.0 类注释（节选）：
 *   「A StreamTransformation represents the operation that creates a DataStream.
 *    Every DataStream has an underlying StreamTransformation that is the origin
 *    of said DataStream.
 *    API operations such as DataStream#map create a tree of
 *    StreamTransformations underneath. When the stream program is to be
 *    executed this graph is translated to a StreamGraph using
 *    StreamGraphGenerator.
 *    A StreamTransformation does not necessarily correspond to a physical
 *    operation at runtime. Some operations are only logical concepts. Examples
 *    of this are union, split/select data stream, partitioning.」
 *
 * 并给了那张关键的「前后对照图」：
 *   Source   Source              Source        Source
 *      |        |                   |             |
 *   Rebalance HashPartition         +---> Map <---+
 *      +---> Union <---+                   |
 *              |                           v
 *            Split                        Sink
 *              |
 *            Select
 *              |
 *             Map
 *              |
 *            Sink
 *   「The information about partitioning, union, split/select end up being
 *     encoded in the edges that connect the sources to the map operation.」
 *   —— 也就是 StreamGraph 的虚拟节点机制（见 StreamGraph.h 的机制 ①）。
 *
 * 静态 id 计数器（:100-105）：
 *   protected static Integer idCounter = 0;
 *   public static int getNewNodeId() { idCounter++; return idCounter; }
 *   —— 注意是**先加再返回**，所以第一个 id 是 1，不是 0。
 *
 * 字段（:107-129）：
 *   protected final int id;
 *   protected String name;
 *   protected TypeInformation<T> outputType;
 *   protected boolean typeUsed;
 *       1.0 原注释：「This is used to handle MissingTypeInfo. As long as the
 *       outputType has not been queried it can still be changed using
 *       setOutputType(). Afterwards an exception is thrown when trying to
 *       change the output type.」
 *   private int parallelism;
 *   private String uid;
 *       1.0 原注释：「User-specified ID for this transformation. This is used to
 *       assign the same operator ID across job restarts. There is also the
 *       automatically generated id, which is assigned from a static counter.
 *       That field is independent from this.」
 *   protected long bufferTimeout = -1;
 *   private String slotSharingGroup;    // 构造里显式置 null
 *
 * 方法要点：
 *   setParallelism(int)  -> checkArgument(parallelism > 0,
 *                              "Parallelism must be bigger than zero.")
 *   setOutputType(t)     -> typeUsed 已为 true 则 IllegalStateException(
 *       "TypeInformation cannot be filled in for the type after it has been
 *        used. Please make sure that the type info hints are the first call
 *        after the transformation function, before any access to types or
 *        semantic properties, etc.")
 *   getOutputType()      -> MissingTypeInfo 则 InvalidTypesException；
 *                           否则 typeUsed = true 后返回
 *   setSlotSharingGroup(String)：1.0 注释「Parallel instances of operations
 *       that are in the same slot sharing group will be co-located in the same
 *       TaskManager slot, if possible. Initially, an operation is in the
 *       default slot sharing group. This can be explicitly set using
 *       setSlotSharingGroup("default").」
 *   abstract setChainingStrategy(ChainingStrategy)
 *   abstract Collection<StreamTransformation<?>> getTransitivePredecessors()
 *       1.0 注释：「Returns all transitive predecessor StreamTransformations of
 *       this StreamTransformation. This is, for example, used when determining
 *       whether a feedback edge of an iteration actually has the iteration head
 *       as a predecessor.」
 *   setBufferTimeout(long)：1.0 注释「The timeout is used when sending elements
 *       over the network. ... A higher timeout means that more elements will be
 *       sent in one buffer, this increases throughput. The latency, however, is
 *       negatively affected by a higher timeout.」
 *   equals / hashCode（按 id + name + outputType + parallelism + bufferTimeout）
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 类型擦除：没有 <T>，TypeInformation 换成 AnySerializer】
 * StreamGraphGenerator 要在一个容器里混放各种 transformation 并 instanceof
 * 分派（1.0 的 `Collection<StreamTransformation<?>>`），C++ 这边如果带模板
 * 参数就连 `StreamTransformation*` 这个公共指针类型都没有。所以本类**不带**
 * 模板参数，元素类型由 DataStream 那一层的模板参数在编译期保证 —— 与
 * OneInputStreamOperator / TwoInputStreamOperator 同一手法。
 * 相应地 TypeInformation<T> 换成 AnySerializer（见 StreamGraph.h 偏差 2）：
 *   - setOutputType / getOutputType 收发 SerializerPtr
 *   - 没有 MissingTypeInfo，所以 getOutputType() 里那段 InvalidTypesException
 *     不存在；但 typeUsed_ 与 setOutputType 的 IllegalStateException **保留**
 *     （DataStream.returns(...) 这条路径将来要用）
 *
 * 【偏差 2 —— uid / slotSharingGroup 的「未设置」用空串表示】1.0 用 null。
 * 于是 StreamGraphGenerator 里的 `transform.getUid() != null` 变成
 * `!getUid().empty()`、`specifiedGroup != null` 变成 `!specifiedGroup.empty()`。
 * 语义等价（1.0 不可能把 uid 或组名设成空串 —— setSlotSharingGroup("") 在
 * 1.0 里也是无意义的）。
 *
 * 【偏差 3 —— equals / hashCode 省掉】1.0 的 equals 比的是 id + name +
 * outputType + parallelism + bufferTimeout。但 StreamGraphGenerator 的
 * alreadyTransformed 是 `Map<StreamTransformation<?>, ...>`，它真正需要的是
 * **对象同一性**（同一个 transformation 只转换一次）—— 由于 id 来自静态计数器
 * 且 final，1.0 的 equals 在那里恰好等价于同一性。本实现直接用指针做 key，
 * 语义更直白，equals/hashCode 不需要。
 *
 * 【偏差 4 —— idCounter 不是线程安全的】1.0 的 `protected static Integer
 * idCounter` 同样不是（Integer++ 不是原子的）。照搬：静态计数器 + 非原子自增。
 * 另外提供 resetIdCounter()，1.0 没有 —— 测试里需要让 id 可预测（1.0 的测试
 * 从不断言具体 id 值，所以不需要）。
 */
#ifndef FLINK_STREAMING_API_TRANSFORMATIONS_STREAMTRANSFORMATION_H
#define FLINK_STREAMING_API_TRANSFORMATIONS_STREAMTRANSFORMATION_H

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "flink/api/common/typeutils/AnySerializer.h"
#include "flink/streaming/api/operators/ChainingStrategy.h"

namespace flink {
namespace streaming {
namespace api {
namespace transformations {

class StreamTransformation {
 public:
  typedef std::shared_ptr<const flink::api::common::typeutils::AnySerializer>
      SerializerPtr;
  typedef flink::streaming::api::operators::ChainingStrategy ChainingStrategy;

  // 1.0: protected static Integer idCounter = 0;
  //      public static int getNewNodeId() { idCounter++; return idCounter; }
  static int getNewNodeId() {
    ++idCounter_;
    return idCounter_;
  }
  // 1.0 没有这个方法（偏差 4）
  static void resetIdCounter() { idCounter_ = 0; }

  // 1.0 StreamTransformation(String name, TypeInformation<T> outputType,
  //                          int parallelism)
  StreamTransformation(const std::string& name, const SerializerPtr& outputType,
                       int parallelism)
      : id_(getNewNodeId()),
        name_(name),
        outputType_(outputType),
        typeUsed_(false),
        parallelism_(parallelism),
        bufferTimeout_(-1) {
    // 1.0: this.name = Preconditions.checkNotNull(name);
    //      this.slotSharingGroup = null;   （这里是空串，见偏差 2）
  }

  virtual ~StreamTransformation() {}

  int getId() const { return id_; }

  void setName(const std::string& name) { name_ = name; }
  const std::string& getName() const { return name_; }

  int getParallelism() const { return parallelism_; }
  void setParallelism(int parallelism) {
    // 1.0: checkArgument(parallelism > 0, "Parallelism must be bigger than
    //      zero.")
    if (parallelism <= 0) {
      throw std::invalid_argument("Parallelism must be bigger than zero.");
    }
    parallelism_ = parallelism;
  }

  // 1.0: "The specified ID is used to assign the same operator ID across job
  // submissions (for example when starting a job from a savepoint).
  // Important: this ID needs to be unique per transformation and job.
  // Otherwise, job submission will fail."
  void setUid(const std::string& uid) { uid_ = uid; }
  const std::string& getUid() const { return uid_; }

  const std::string& getSlotSharingGroup() const { return slotSharingGroup_; }
  void setSlotSharingGroup(const std::string& slotSharingGroup) {
    slotSharingGroup_ = slotSharingGroup;
  }

  // 1.0 setOutputType(TypeInformation<T>)：
  // 「Tries to fill in the type information. ... This method checks whether the
  //   type information has ever been accessed before and does not allow
  //   modifications if the type was accessed already. This ensures consistency
  //   by making sure different parts of the operation do not assume different
  //   type information.」
  void setOutputType(const SerializerPtr& outputType) {
    if (typeUsed_) {
      throw std::logic_error(
          "TypeInformation cannot be filled in for the type after it has been "
          "used. Please make sure that the type info hints are the first call "
          "after the transformation function, before any access to types or "
          "semantic properties, etc.");
    }
    outputType_ = outputType;
  }

  // 1.0 getOutputType()：「Once this is used once the output type cannot be
  // changed anymore using setOutputType.」
  // 【偏差 1】没有 MissingTypeInfo，所以不抛 InvalidTypesException
  const SerializerPtr& getOutputType() {
    typeUsed_ = true;
    return outputType_;
  }

  // 1.0: abstract void setChainingStrategy(ChainingStrategy strategy)
  virtual void setChainingStrategy(ChainingStrategy strategy) = 0;

  // 1.0 setBufferTimeout(long)
  void setBufferTimeout(int64_t bufferTimeout) {
    bufferTimeout_ = bufferTimeout;
  }
  int64_t getBufferTimeout() const { return bufferTimeout_; }

  // 1.0: abstract Collection<StreamTransformation<?>>
  //          getTransitivePredecessors()
  virtual std::vector<StreamTransformation*> getTransitivePredecessors() = 0;

  // 1.0 toString()：getClass().getSimpleName() + "{id=..., name='...', ...}"
  // 【偏差 1】没有反射，用虚方法给出类名（同 AbstractStreamOperator
  // .getSimpleName() 的手法）
  virtual std::string getSimpleName() const = 0;

  std::string toString() const {
    return getSimpleName() + "{id=" + std::to_string(id_) + ", name='" + name_ +
           "', parallelism=" + std::to_string(parallelism_) + "}";
  }

 protected:
  const int id_;
  std::string name_;
  SerializerPtr outputType_;
  // 1.0: "As long as the outputType has not been queried it can still be
  // changed using setOutputType()."
  bool typeUsed_;

 private:
  static int idCounter_;

  int parallelism_;
  std::string uid_;
  int64_t bufferTimeout_;
  std::string slotSharingGroup_;
};

}  // namespace transformations
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_TRANSFORMATIONS_STREAMTRANSFORMATION_H
