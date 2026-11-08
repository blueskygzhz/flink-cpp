/*
 * flink-streaming-java : org.apache.flink.streaming.api.graph.StreamNode
 *
 * 1.0 class StreamNode implements Serializable（StreamNode.java:44）
 * —— StreamGraph 里的一个顶点，对应一个算子。
 *
 * 字段（:48-74）：
 *   transient private StreamExecutionEnvironment env;
 *   private final int id;
 *   private Integer parallelism = null;
 *   private Long bufferTimeout = null;
 *   private final String operatorName;
 *   private String slotSharingGroup;
 *   private KeySelector<?,?> statePartitioner1;
 *   private KeySelector<?,?> statePartitioner2;
 *   private TypeSerializer<?> stateKeySerializer;
 *   private transient StreamOperator<?> operator;
 *   private List<OutputSelector<?>> outputSelectors;
 *   private TypeSerializer<?> typeSerializerIn1;
 *   private TypeSerializer<?> typeSerializerIn2;
 *   private TypeSerializer<?> typeSerializerOut;
 *   private List<StreamEdge> inEdges  = new ArrayList<>();
 *   private List<StreamEdge> outEdges = new ArrayList<>();
 *   private final Class<? extends AbstractInvokable> jobVertexClass;
 *   private InputFormat<?, ?> inputFormat;
 *   private String transformationId;
 *
 * 构造（:76-91）：
 *   StreamNode(env, id, slotSharingGroup, operator, operatorName,
 *              outputSelector, jobVertexClass)
 *
 * 方法要点：
 *   addInEdge(StreamEdge)   边的 targetId 必须等于本点 id，否则
 *       IllegalArgumentException("Destination id doesn't match the StreamNode id")
 *   addOutEdge(StreamEdge)  边的 sourceId 必须等于本点 id，否则
 *       IllegalArgumentException("Source id doesn't match the StreamNode id")
 *   getOutEdgeIndices() / getInEdgeIndices()  取对端 id 列表
 *   getParallelism()   parallelism == -1 时回落到 env.getParallelism()
 *   getBufferTimeout() bufferTimeout == null 时回落到 env.getBufferTimeout()
 *   isSameSlotSharingGroup(StreamNode)  两边都为 null 或字符串相等
 *   toString()  operatorName + "-" + id
 *   equals/hashCode  只看 id
 *
 * ──────────────── 与 1.0 的偏差 ────────────────
 *
 * 【偏差 1 —— 不持 StreamExecutionEnvironment】1.0 靠 env 为 parallelism 与
 * bufferTimeout 提供缺省值。StreamExecutionEnvironment 还没落地，而且让图顶点
 * 反向依赖 env 在 C++ 里会把头文件绕成环，所以改为**建点时把两个缺省值注入**
 * （StreamGraph 持有 env，建点时传进来）。回落判定原样保留：
 * parallelism_ == -1 时用 defaultParallelism_，bufferTimeout_ 未显式设过
 * （hasBufferTimeout_ == false，对应 1.0 的 Long == null）时用
 * defaultBufferTimeout_。
 *
 * 【偏差 2 —— jobVertexClass 用字符串】1.0 存的是 Java Class 对象
 * （SourceStreamTask / OneInputStreamTask / TwoInputStreamTask /
 * StreamIterationHead / StreamIterationTail），StreamingJobGraphGenerator
 * 把它塞进 JobVertex 让 TaskManager 反射实例化。C++ 没有反射，这里存 1.0 的
 * **类名字符串**，等 StreamingJobGraphGenerator 落地时由它按名字建对应的
 * StreamTask 子类。
 *
 * 【偏差 3 —— inputFormat 不落地】1.0 的 inputFormat 服务于
 * readFile/createInput 这类 InputFormat 数据源（StreamGraphGenerator 会给
 * source 顶点挂 InputFormat，由 StreamInputFormatSource 读）。InputFormat
 * 整条线还没到，字段省掉。
 *
 * 【偏差 4 —— 类型擦除】KeySelector<?,?> -> AnyKeySelector，
 * TypeSerializer<?> -> AnySerializer，OutputSelector<?> -> AnyOutputSelector，
 * StreamOperator<?> -> StreamOperator（本实现的 StreamOperator 不带模板参数）。
 */
#ifndef FLINK_STREAMING_API_GRAPH_STREAMNODE_H
#define FLINK_STREAMING_API_GRAPH_STREAMNODE_H

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "flink/api/common/typeutils/AnySerializer.h"
#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/streaming/api/collector/selector/AnyOutputSelector.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {
class StreamOperator;
}  // namespace operators
namespace graph {
class StreamEdge;
}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink

namespace flink {
namespace streaming {
namespace api {
namespace graph {

class StreamNode {
 public:
  typedef std::shared_ptr<const flink::api::common::typeutils::AnySerializer>
      SerializerPtr;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;
  typedef std::shared_ptr<
      flink::streaming::api::collector::selector::AnyOutputSelector>
      OutputSelectorPtr;
  typedef std::shared_ptr<flink::streaming::api::operators::StreamOperator>
      StreamOperatorPtr;
  typedef std::shared_ptr<StreamEdge> StreamEdgePtr;

  // 1.0 StreamNode(env, id, slotSharingGroup, operator, operatorName,
  //                outputSelector, jobVertexClass)
  // 【偏差 1】env 换成两个缺省值；【偏差 2】jobVertexClass 换成类名字符串
  StreamNode(int id, const std::string& slotSharingGroup,
             const StreamOperatorPtr& op, const std::string& operatorName,
             const std::vector<OutputSelectorPtr>& outputSelectors,
             const std::string& jobVertexClassName, int defaultParallelism,
             int64_t defaultBufferTimeout)
      : id_(id),
        parallelism_(-1),
        hasBufferTimeout_(false),
        bufferTimeout_(0),
        operatorName_(operatorName),
        slotSharingGroup_(slotSharingGroup),
        operator_(op),
        outputSelectors_(outputSelectors),
        jobVertexClassName_(jobVertexClassName),
        defaultParallelism_(defaultParallelism),
        defaultBufferTimeout_(defaultBufferTimeout) {}

  // 1.0 addInEdge(StreamEdge)
  void addInEdge(const StreamEdgePtr& inEdge);
  // 1.0 addOutEdge(StreamEdge)
  void addOutEdge(const StreamEdgePtr& outEdge);

  const std::vector<StreamEdgePtr>& getOutEdges() const { return outEdges_; }
  const std::vector<StreamEdgePtr>& getInEdges() const { return inEdges_; }

  // 1.0 getOutEdgeIndices() / getInEdgeIndices()
  std::vector<int> getOutEdgeIndices() const;
  std::vector<int> getInEdgeIndices() const;

  int getId() const { return id_; }

  // 1.0: parallelism == -1 ? env.getParallelism() : parallelism
  int getParallelism() const {
    return parallelism_ == -1 ? defaultParallelism_ : parallelism_;
  }
  void setParallelism(int parallelism) { parallelism_ = parallelism; }

  // 1.0: bufferTimeout != null ? bufferTimeout : env.getBufferTimeout()
  int64_t getBufferTimeout() const {
    return hasBufferTimeout_ ? bufferTimeout_ : defaultBufferTimeout_;
  }
  void setBufferTimeout(int64_t bufferTimeout) {
    bufferTimeout_ = bufferTimeout;
    hasBufferTimeout_ = true;
  }

  const StreamOperatorPtr& getOperator() const { return operator_; }
  void setOperator(const StreamOperatorPtr& op) { operator_ = op; }

  const std::string& getOperatorName() const { return operatorName_; }

  const std::vector<OutputSelectorPtr>& getOutputSelectors() const {
    return outputSelectors_;
  }
  void addOutputSelector(const OutputSelectorPtr& outputSelector) {
    outputSelectors_.push_back(outputSelector);
  }

  const SerializerPtr& getTypeSerializerIn1() const {
    return typeSerializerIn1_;
  }
  void setSerializerIn1(const SerializerPtr& s) { typeSerializerIn1_ = s; }
  const SerializerPtr& getTypeSerializerIn2() const {
    return typeSerializerIn2_;
  }
  void setSerializerIn2(const SerializerPtr& s) { typeSerializerIn2_ = s; }
  const SerializerPtr& getTypeSerializerOut() const {
    return typeSerializerOut_;
  }
  void setSerializerOut(const SerializerPtr& s) { typeSerializerOut_ = s; }

  // 【偏差 2】1.0 getJobVertexClass()
  const std::string& getJobVertexClassName() const {
    return jobVertexClassName_;
  }

  void setSlotSharingGroup(const std::string& slotSharingGroup) {
    slotSharingGroup_ = slotSharingGroup;
  }
  const std::string& getSlotSharingGroup() const { return slotSharingGroup_; }

  // 1.0 isSameSlotSharingGroup(StreamNode)
  // 1.0 的 null 与 ""（空串）在这里都映射成空串，语义仍是「要么都没组、
  // 要么组名相同」
  bool isSameSlotSharingGroup(const StreamNode& downstreamVertex) const {
    return slotSharingGroup_ == downstreamVertex.slotSharingGroup_;
  }

  const KeySelectorPtr& getStatePartitioner1() const {
    return statePartitioner1_;
  }
  const KeySelectorPtr& getStatePartitioner2() const {
    return statePartitioner2_;
  }
  void setStatePartitioner1(const KeySelectorPtr& p) { statePartitioner1_ = p; }
  void setStatePartitioner2(const KeySelectorPtr& p) { statePartitioner2_ = p; }

  const SerializerPtr& getStateKeySerializer() const {
    return stateKeySerializer_;
  }
  void setStateKeySerializer(const SerializerPtr& s) {
    stateKeySerializer_ = s;
  }

  const std::string& getTransformationId() const { return transformationId_; }
  void setTransformationId(const std::string& id) { transformationId_ = id; }

  // 1.0 toString(): operatorName + "-" + id
  std::string toString() const {
    return operatorName_ + "-" + std::to_string(id_);
  }

  // 1.0 equals/hashCode 只看 id
  bool operator==(const StreamNode& other) const { return id_ == other.id_; }
  int hashCode() const { return id_; }

 private:
  const int id_;
  int parallelism_;
  bool hasBufferTimeout_;  // 对应 1.0 的 `Long bufferTimeout == null` 判定
  int64_t bufferTimeout_;
  const std::string operatorName_;
  std::string slotSharingGroup_;
  KeySelectorPtr statePartitioner1_;
  KeySelectorPtr statePartitioner2_;
  SerializerPtr stateKeySerializer_;
  StreamOperatorPtr operator_;
  std::vector<OutputSelectorPtr> outputSelectors_;
  SerializerPtr typeSerializerIn1_;
  SerializerPtr typeSerializerIn2_;
  SerializerPtr typeSerializerOut_;
  std::vector<StreamEdgePtr> inEdges_;
  std::vector<StreamEdgePtr> outEdges_;
  const std::string jobVertexClassName_;
  std::string transformationId_;
  const int defaultParallelism_;
  const int64_t defaultBufferTimeout_;
};

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_GRAPH_STREAMNODE_H
