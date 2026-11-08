/*
 * flink-streaming-java : org.apache.flink.streaming.api.graph.StreamConfig
 *
 * 1.0 StreamConfig 是对 Configuration（String -> byte[] 的扁平 map）的强类型门面：
 * 每个 setter 把值写进 Configuration（对象走 InstantiationUtil.writeObjectToConfig
 * 做 Java 序列化），每个 getter 带 ClassLoader 参数再反序列化回来。配置键见
 * StreamConfig.java:51-78，本实现按同样的语义提供同名 setter/getter：
 *
 *   setVertexID / getVertexID                     "vertexID"
 *   setTimeCharacteristic / getTimeCharacteristic "timechar"
 *   setTypeSerializerIn1 / getTypeSerializerIn1   "typeSerializer_in_1"
 *   setTypeSerializerOut / getTypeSerializerOut   "typeSerializer_out"
 *   setBufferTimeout / getBufferTimeout           "bufferTimeout"  (默认 100ms)
 *   setStreamOperator / getStreamOperator         "serializedUDF"
 *   setNumberOfInputs / getNumberOfInputs         "numberOfInputs"
 *   setNumberOfOutputs / getNumberOfOutputs       "numberOfOutputs"
 *   setCheckpointingEnabled / isCheckpointingEnabled  "checkpointing"
 *   setCheckpointMode / getCheckpointMode         "checkpointMode"
 *   setStateBackend / getStateBackend             "statebackend"
 *   setStatePartitioner / getStatePartitioner     "statePartitioner" + input 序号
 *   setStateKeySerializer / getStateKeySerializer "statekeyser"
 *   setChainStart / isChainStart                  "isChainedSubtask"
 *   setTypeSerializerIn2 / getTypeSerializerIn2   "typeSerializer_in_2"
 *   setOutputSelectors / getOutputSelectors       "outputSelectorWrapper"
 *   setIterationId / getIterationId               "iterationId"
 *   setIterationWaitTime / getIterationWaitTime   "iterationWait"
 *   setNonChainedOutputs / getNonChainedOutputs   "nonChainedOutputs"
 *   setChainedOutputs / getChainedOutputs         "chainedOutputs"
 *   setOutEdges / getOutEdges                     "outStreamEdges"
 *   setInPhysicalEdges / getInPhysicalEdges       "inStreamEdges"
 *   setOutEdgesInOrder / getOutEdgesInOrder       "edgesInOrder"
 *   setTransitiveChainedTaskConfigs /
 *     getTransitiveChainedTaskConfigs             "chainedTaskConfig_"
 *
 * 【偏差 1 —— 没有 Configuration 这一层】1.0 之所以把一切塞进 Configuration 再
 * Java 序列化，是因为 JobGraph 要通过网络下发到远端 TaskManager，算子和 UDF 必须
 * 能被序列化携带。本实现是进程内运行，所以直接持强类型字段 / shared_ptr，
 * getter 不需要 ClassLoader 参数。这也让 1.0 里 `getStreamOperator(cl)` 的那段
 * ClassNotFoundException 处理失去意义。
 *
 * 【偏差 2 —— 容器与自引用】1.0 的 CHAINED_TASK_CONFIG 是
 * Map<Integer, StreamConfig>（Java 里都是引用，自引用无妨）。C++ 不能把
 * std::map<int, StreamConfig> 直接做成 StreamConfig 的成员（成员声明处本类型
 * 还不完整），所以存 shared_ptr<StreamConfig>；StreamEdge 列表同理存
 * shared_ptr（边本身由 StreamGraph/StreamNode 共同持有）。
 *
 * 【偏差 3 —— iteration 相关】ITERATION_ID / ITERATON_WAIT 两个字段按 1.0
 * 保留（setIterationId / setIterationWaitTime），但 StreamIterationHead /
 * StreamIterationTail / BlockingQueueBroker 这条迭代作业的线还没落地，
 * 目前没有读取方。
 */
#ifndef FLINK_STREAMING_API_GRAPH_STREAMCONFIG_H
#define FLINK_STREAMING_API_GRAPH_STREAMCONFIG_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "flink/api/common/typeutils/AnySerializer.h"
#include "flink/api/java/functions/AnyKeySelector.h"
#include "flink/runtime/state/AbstractStateBackend.h"
#include "flink/streaming/api/CheckpointingMode.h"
#include "flink/streaming/api/TimeCharacteristic.h"
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

class StreamConfig {
 public:
  typedef std::shared_ptr<const flink::api::common::typeutils::AnySerializer>
      SerializerPtr;
  typedef std::shared_ptr<flink::api::java::functions::AnyKeySelector>
      KeySelectorPtr;
  typedef std::shared_ptr<flink::runtime::state::AbstractStateBackend>
      StateBackendPtr;
  typedef std::shared_ptr<flink::streaming::api::operators::StreamOperator>
      StreamOperatorPtr;
  typedef std::shared_ptr<
      flink::streaming::api::collector::selector::AnyOutputSelector>
      OutputSelectorPtr;
  typedef std::shared_ptr<StreamEdge> StreamEdgePtr;
  typedef std::vector<StreamEdgePtr> StreamEdgeList;
  typedef std::map<int, std::shared_ptr<StreamConfig> > ChainedConfigMap;

  // 1.0 DEFAULT_TIMEOUT = 100
  static const int64_t DEFAULT_TIMEOUT = 100;

  StreamConfig()
      : vertexID_(0),
        timeCharacteristic_(TimeCharacteristic::ProcessingTime),
        bufferTimeout_(DEFAULT_TIMEOUT),
        numberOfInputs_(0),
        numberOfOutputs_(0),
        checkpointingEnabled_(false),
        checkpointMode_(CheckpointingMode::EXACTLY_ONCE),
        isChainStart_(false),
        iterationWaitTime_(0) {}

  void setVertexID(int vertexID) { vertexID_ = vertexID; }
  int getVertexID() const { return vertexID_; }

  void setTimeCharacteristic(TimeCharacteristic characteristic) {
    timeCharacteristic_ = characteristic;
  }
  TimeCharacteristic getTimeCharacteristic() const {
    return timeCharacteristic_;
  }

  void setTypeSerializerIn1(SerializerPtr serializer) {
    typeSerializerIn1_ = serializer;
  }
  const SerializerPtr& getTypeSerializerIn1() const {
    return typeSerializerIn1_;
  }

  void setTypeSerializerOut(SerializerPtr serializer) {
    typeSerializerOut_ = serializer;
  }
  const SerializerPtr& getTypeSerializerOut() const {
    return typeSerializerOut_;
  }

  // 1.0 setTypeSerializerIn2 / getTypeSerializerIn2（TwoInputStreamOperator 的
  // 第二路输入）
  void setTypeSerializerIn2(SerializerPtr serializer) {
    typeSerializerIn2_ = serializer;
  }
  const SerializerPtr& getTypeSerializerIn2() const {
    return typeSerializerIn2_;
  }

  // 1.0 的 bufferTimeout：-1 表示只在缓冲区满时才 flush，0 表示每条都 flush
  void setBufferTimeout(int64_t timeout) { bufferTimeout_ = timeout; }
  int64_t getBufferTimeout() const { return bufferTimeout_; }

  void setStreamOperator(StreamOperatorPtr op) { streamOperator_ = op; }
  const StreamOperatorPtr& getStreamOperator() const { return streamOperator_; }

  void setNumberOfInputs(int numberOfInputs) {
    numberOfInputs_ = numberOfInputs;
  }
  int getNumberOfInputs() const { return numberOfInputs_; }

  void setNumberOfOutputs(int numberOfOutputs) {
    numberOfOutputs_ = numberOfOutputs;
  }
  int getNumberOfOutputs() const { return numberOfOutputs_; }

  void setCheckpointingEnabled(bool enabled) { checkpointingEnabled_ = enabled; }
  bool isCheckpointingEnabled() const { return checkpointingEnabled_; }

  void setCheckpointMode(CheckpointingMode mode) { checkpointMode_ = mode; }
  CheckpointingMode getCheckpointMode() const { return checkpointMode_; }

  void setStateBackend(StateBackendPtr backend) { stateBackend_ = backend; }
  const StateBackendPtr& getStateBackend() const { return stateBackend_; }

  // 1.0 setStatePartitioner(int input, KeySelector<?, ?>)：input 只有 0/1
  // （对应 TwoInputStreamOperator 的两路输入）
  void setStatePartitioner(int input, KeySelectorPtr partitioner) {
    statePartitioner_[input == 0 ? 0 : 1] = partitioner;
  }
  const KeySelectorPtr& getStatePartitioner(int input) const {
    return statePartitioner_[input == 0 ? 0 : 1];
  }

  void setStateKeySerializer(SerializerPtr serializer) {
    stateKeySerializer_ = serializer;
  }
  const SerializerPtr& getStateKeySerializer() const {
    return stateKeySerializer_;
  }

  void setChainStart() { isChainStart_ = true; }
  bool isChainStart() const { return isChainStart_; }

  // -------------------------- 算子链 / 边的配置 --------------------------
  // 这几项是 OperatorChain 的全部输入（1.0 OperatorChain.java:78-104）：
  //   getTransitiveChainedTaskConfigs() 给出链上每个算子的配置（按 vertexID）
  //   getOutEdgesInOrder()  决定 streamOutputs[] 的下标顺序，必须与
  //                         Environment.getWriter(index) 的编号一致
  //   getNonChainedOutputs() 本算子通向**网络**的出边
  //   getChainedOutputs()    本算子通向**链内下一个算子**的出边
  //   getOutputSelectors()   split/select 的选择器，非空则建 DirectedOutput

  // 1.0 setOutputSelectors(List<OutputSelector<?>>)
  void setOutputSelectors(const std::vector<OutputSelectorPtr>& selectors) {
    outputSelectors_ = selectors;
  }
  const std::vector<OutputSelectorPtr>& getOutputSelectors() const {
    return outputSelectors_;
  }

  // 1.0 setNonChainedOutputs(List<StreamEdge>)
  void setNonChainedOutputs(const StreamEdgeList& outputs) {
    nonChainedOutputs_ = outputs;
  }
  const StreamEdgeList& getNonChainedOutputs() const {
    return nonChainedOutputs_;
  }

  // 1.0 setChainedOutputs(List<StreamEdge>)
  void setChainedOutputs(const StreamEdgeList& chainedOutputs) {
    chainedOutputs_ = chainedOutputs;
  }
  const StreamEdgeList& getChainedOutputs() const { return chainedOutputs_; }

  // 1.0 setOutEdges(List<StreamEdge>)
  void setOutEdges(const StreamEdgeList& outEdges) { outEdges_ = outEdges; }
  const StreamEdgeList& getOutEdges() const { return outEdges_; }

  // 1.0 setInPhysicalEdges(List<StreamEdge>)
  void setInPhysicalEdges(const StreamEdgeList& inEdges) {
    inPhysicalEdges_ = inEdges;
  }
  const StreamEdgeList& getInPhysicalEdges() const { return inPhysicalEdges_; }

  // 1.0 setOutEdgesInOrder(List<StreamEdge>)
  void setOutEdgesInOrder(const StreamEdgeList& outEdgeList) {
    outEdgesInOrder_ = outEdgeList;
  }
  const StreamEdgeList& getOutEdgesInOrder() const { return outEdgesInOrder_; }

  // 1.0 setTransitiveChainedTaskConfigs(Map<Integer, StreamConfig>)
  void setTransitiveChainedTaskConfigs(const ChainedConfigMap& configs) {
    chainedTaskConfigs_ = configs;
  }
  const ChainedConfigMap& getTransitiveChainedTaskConfigs() const {
    return chainedTaskConfigs_;
  }

  // --------------------------- iteration（见偏差 3）---------------------------

  void setIterationId(const std::string& iterationId) {
    iterationId_ = iterationId;
  }
  const std::string& getIterationId() const { return iterationId_; }

  void setIterationWaitTime(int64_t time) { iterationWaitTime_ = time; }
  int64_t getIterationWaitTime() const { return iterationWaitTime_; }

 private:
  int vertexID_;
  TimeCharacteristic timeCharacteristic_;
  SerializerPtr typeSerializerIn1_;
  SerializerPtr typeSerializerIn2_;
  SerializerPtr typeSerializerOut_;
  int64_t bufferTimeout_;
  StreamOperatorPtr streamOperator_;
  int numberOfInputs_;
  int numberOfOutputs_;
  bool checkpointingEnabled_;
  CheckpointingMode checkpointMode_;
  StateBackendPtr stateBackend_;
  KeySelectorPtr statePartitioner_[2];
  SerializerPtr stateKeySerializer_;
  bool isChainStart_;
  std::vector<OutputSelectorPtr> outputSelectors_;
  StreamEdgeList nonChainedOutputs_;
  StreamEdgeList chainedOutputs_;
  StreamEdgeList outEdges_;
  StreamEdgeList inPhysicalEdges_;
  StreamEdgeList outEdgesInOrder_;
  ChainedConfigMap chainedTaskConfigs_;
  std::string iterationId_;
  int64_t iterationWaitTime_;
};

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_GRAPH_STREAMCONFIG_H
