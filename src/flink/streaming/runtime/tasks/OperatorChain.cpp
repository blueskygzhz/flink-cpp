#include "flink/streaming/runtime/tasks/OperatorChain.h"

#include <stdexcept>

#include "flink/runtime/io/network/api/CheckpointBarrier.h"
#include "flink/streaming/api/collector/selector/DirectedOutput.h"
#include "flink/streaming/runtime/tasks/ExceptionInChainedOperatorException.h"
#include "flink/streaming/runtime/tasks/StreamTask.h"

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

// 1.0 OperatorChain(StreamTask<OUT,?>, StreamOperator<OUT>,
//                   AccumulatorRegistry.Reporter)（OperatorChain.java:69）
OperatorChain::OperatorChain(StreamTask* containingTask,
                            StreamOperator* headOperator)
    : chainEntryPoint_(NULL) {
  // 1.0 还取了 userCodeClassloader（反序列化用，偏差见 StreamConfig.h 偏差 1）
  // 与 enableTimestamps = containingTask.isSerializingTimestamps()
  // （只传给 createStreamOutput 决定用哪个 StreamRecordSerializer，本实现没有
  //   序列化层，见 RecordWriterOutput.h 偏差 1，故整项省掉）
  StreamConfig& configuration = containingTask->getConfiguration();

  // 1.0: "we read the chained configs, and the order of record writer
  // registrations by output name"
  ChainedConfigs chainedConfigs;
  const StreamConfig::ChainedConfigMap& transitiveConfigs =
      configuration.getTransitiveChainedTaskConfigs();
  for (StreamConfig::ChainedConfigMap::const_iterator it =
           transitiveConfigs.begin();
       it != transitiveConfigs.end(); ++it) {
    chainedConfigs[it->first] = it->second.get();
  }
  // 1.0: chainedConfigs.put(configuration.getVertexID(), configuration)
  chainedConfigs[configuration.getVertexID()] = &configuration;

  // 1.0: "create the final output stream writers / we iterate through all the
  // out edges from this job vertex and create a stream output"
  const StreamConfig::StreamEdgeList& outEdgesInOrder =
      configuration.getOutEdgesInOrder();
  StreamOutputMap streamOutputMap;
  streamOutputs_.assign(outEdgesInOrder.size(), NULL);

  // 1.0: "from here on, we need to make sure that the output writers are shut
  // down again on failure"（1.0 用 success 标志 + finally；C++ 没有 finally，
  // 用 catch (...) 做同样的清理再重抛，语义等价）
  try {
    for (size_t i = 0; i < outEdgesInOrder.size(); ++i) {
      const StreamEdge& outEdge = *outEdgesInOrder[i];

      // 1.0: chainedConfigs.get(outEdge.getSourceId())，取不到会在
      // createStreamOutput 里 NPE；这里给一条明确的错误信息
      ChainedConfigs::const_iterator upStream =
          chainedConfigs.find(outEdge.getSourceId());
      if (upStream == chainedConfigs.end()) {
        throw std::runtime_error(
            "No chained config for source vertex of edge " +
            outEdge.getEdgeId());
      }

      RecordWriterOutput* streamOutput = createStreamOutput(
          outEdge, upStream->second, static_cast<int>(i), containingTask);

      streamOutputs_[i] = streamOutput;
      // 【偏差 4】1.0 的键是 StreamEdge（equals/hashCode 比 edgeId）
      streamOutputMap[outEdge.getEdgeId()] = streamOutput;
    }

    // 1.0: "we create the chain of operators and grab the collector that leads
    // into the chain"
    std::vector<StreamOperator*> allOps;
    chainEntryPoint_ = createOutputCollector(containingTask, &configuration,
                                             chainedConfigs, streamOutputMap,
                                             allOps);

    allOperators_ = allOps;
    // 1.0: "add the head operator to the end of the list"
    allOperators_.push_back(headOperator);
  } catch (...) {
    // 1.0: "make sure we clean up after ourselves in case of a failure after
    // acquiring the first resources"
    for (size_t i = 0; i < streamOutputs_.size(); ++i) {
      if (streamOutputs_[i] != NULL) {
        streamOutputs_[i]->close();
        streamOutputs_[i]->clearBuffers();
      }
    }
    throw;
  }
}

// 1.0 broadcastCheckpointBarrier(long id, long timestamp)（:129）
void OperatorChain::broadcastCheckpointBarrier(int64_t id, int64_t timestamp) {
  EventPtr barrier =
      std::make_shared<const flink::runtime::io::network::api::CheckpointBarrier>(
          id, timestamp);
  for (size_t i = 0; i < streamOutputs_.size(); ++i) {
    streamOutputs_[i]->broadcastEvent(barrier);
  }
}

// 1.0 flushOutputs()（:156）
void OperatorChain::flushOutputs() {
  for (size_t i = 0; i < streamOutputs_.size(); ++i) {
    streamOutputs_[i]->flush();
  }
}

// 1.0 releaseOutputs()（:169）—— try { close 全部 } finally { clearBuffers 全部 }
void OperatorChain::releaseOutputs() {
  try {
    for (size_t i = 0; i < streamOutputs_.size(); ++i) {
      streamOutputs_[i]->close();
    }
  } catch (...) {
    // 1.0: "make sure that we release the buffers in any case"
    for (size_t i = 0; i < streamOutputs_.size(); ++i) {
      streamOutputs_[i]->clearBuffers();
    }
    throw;
  }
  for (size_t i = 0; i < streamOutputs_.size(); ++i) {
    streamOutputs_[i]->clearBuffers();
  }
}

// ------------------------------------------------------------------------
//  initialization utilities（1.0 的三个 private static 工厂）
// ------------------------------------------------------------------------

// 1.0 createOutputCollector(...)（:187）
OperatorChain::Output* OperatorChain::createOutputCollector(
    StreamTask* containingTask, StreamConfig* operatorConfig,
    const ChainedConfigs& chainedConfigs, const StreamOutputMap& streamOutputs,
    std::vector<StreamOperator*>& allOperators) {
  // 1.0: new ArrayList<>(4)
  std::vector<OutputWithEdge> allOutputs;
  allOutputs.reserve(4);

  // 1.0: "create collectors for the network outputs"
  const StreamConfig::StreamEdgeList& nonChained =
      operatorConfig->getNonChainedOutputs();
  for (size_t i = 0; i < nonChained.size(); ++i) {
    const StreamEdge* outputEdge = nonChained[i].get();
    StreamOutputMap::const_iterator it =
        streamOutputs.find(outputEdge->getEdgeId());
    if (it == streamOutputs.end()) {
      // 1.0 这里 streamOutputs.get(edge) 返回 null，到 collect 时才 NPE
      throw std::runtime_error("No stream output registered for edge " +
                               outputEdge->getEdgeId());
    }
    allOutputs.push_back(OutputWithEdge(it->second, outputEdge));
  }

  // 1.0: "Create collectors for the chained outputs"
  const StreamConfig::StreamEdgeList& chained =
      operatorConfig->getChainedOutputs();
  for (size_t i = 0; i < chained.size(); ++i) {
    const StreamEdge* outputEdge = chained[i].get();
    int outputId = outputEdge->getTargetId();

    ChainedConfigs::const_iterator chainedOpConfig =
        chainedConfigs.find(outputId);
    if (chainedOpConfig == chainedConfigs.end()) {
      throw std::runtime_error("No chained config for chained output vertex " +
                               std::to_string(outputId));
    }

    Output* output =
        createChainedOperator(containingTask, chainedOpConfig->second,
                              chainedConfigs, streamOutputs, allOperators);

    allOutputs.push_back(OutputWithEdge(output, outputEdge));
  }

  // 1.0: "if there are multiple outputs, or the outputs are directed, we need
  // to wrap them as one output"
  const std::vector<StreamConfig::OutputSelectorPtr>& selectors =
      operatorConfig->getOutputSelectors();

  if (selectors.empty()) {
    // 1.0: "simple path, no selector necessary"
    if (allOutputs.size() == 1) {
      return allOutputs[0].first;
    }
    // 1.0: "send to N outputs. Note that this includes teh special case of
    // sending to zero outputs"（1.0 的 typo 原样保留）
    std::vector<Output*> asArray;
    asArray.reserve(allOutputs.size());
    for (size_t i = 0; i < allOutputs.size(); ++i) {
      asArray.push_back(allOutputs[i].first);
    }
    std::shared_ptr<Output> collector =
        std::make_shared<BroadcastingOutputCollector>(asArray);
    ownedCollectors_.push_back(collector);
    return collector.get();
  } else {
    // 1.0: "selector present, more complex routing necessary"
    std::shared_ptr<Output> directed = std::make_shared<
        flink::streaming::api::collector::selector::DirectedOutput>(selectors,
                                                                    allOutputs);
    ownedCollectors_.push_back(directed);
    return directed.get();
  }
}

// 1.0 createChainedOperator(...)（:245）
OperatorChain::Output* OperatorChain::createChainedOperator(
    StreamTask* containingTask, StreamConfig* operatorConfig,
    const ChainedConfigs& chainedConfigs, const StreamOutputMap& streamOutputs,
    std::vector<StreamOperator*>& allOperators) {
  // 1.0: "create the output that the operator writes to first. this may
  // recursively create more operators"
  Output* output = createOutputCollector(containingTask, operatorConfig,
                                         chainedConfigs, streamOutputs,
                                         allOperators);

  // 1.0: operatorConfig.getStreamOperator(userCodeClassloader) —— 一次
  // unchecked cast 到 OneInputStreamOperator（【偏差 5】这里用 dynamic_cast）
  const StreamConfig::StreamOperatorPtr& configuredOperator =
      operatorConfig->getStreamOperator();
  if (!configuredOperator) {
    throw std::runtime_error("No stream operator configured for chained vertex " +
                             std::to_string(operatorConfig->getVertexID()));
  }
  OneInputStreamOperator* chainedOperator =
      dynamic_cast<OneInputStreamOperator*>(configuredOperator.get());
  if (chainedOperator == NULL) {
    throw std::runtime_error(
        "The operator of chained vertex " +
        std::to_string(operatorConfig->getVertexID()) +
        " is not a OneInputStreamOperator");
  }

  // 1.0: "now create the operator and give it the output collector to write
  // its output to"
  chainedOperator->setup(containingTask, operatorConfig, output);

  allOperators.push_back(chainedOperator);

  // 【偏差 2】1.0 在这里按 objectReuseEnabled / isInputCopyingDisabled() 二选一，
  // 不复用对象时走 CopyingChainingOutput；本实现的记录值不可变，统一用
  // ChainingOutput
  std::shared_ptr<Output> chaining =
      std::make_shared<ChainingOutput>(chainedOperator);
  ownedCollectors_.push_back(chaining);
  return chaining.get();
}

// 1.0 createStreamOutput(...)（:272）
OperatorChain::RecordWriterOutput* OperatorChain::createStreamOutput(
    const StreamEdge& edge, StreamConfig* upStreamConfig, int outputIndex,
    StreamTask* containingTask) {
  // 1.0: outSerializer = upStreamConfig.getTypeSerializerOut(cl)
  // —— 没有序列化层，不取（见 RecordWriterOutput.h 偏差 1）

  // 1.0: StreamPartitioner<T> outputPartitioner =
  //        (StreamPartitioner<T>) edge.getPartitioner();
  StreamEdge::PartitionerPtr outputPartitioner = edge.getPartitioner();

  // 1.0: LOG.debug("Using partitioner {} for output {} of task ",
  //                outputPartitioner, outputIndex, taskName);
  // 本实现没有日志门面，故不复刻（taskName 在 1.0 里也只用于这条日志）

  StreamRecordWriter::ResultPartitionWriter* bufferWriter =
      containingTask->getEnvironment()->getWriter(outputIndex);
  if (bufferWriter == NULL) {
    throw std::runtime_error("No result partition writer for output index " +
                             std::to_string(outputIndex));
  }

  std::shared_ptr<StreamRecordWriter> recordWriter =
      std::make_shared<StreamRecordWriter>(bufferWriter, outputPartitioner,
                                           upStreamConfig->getBufferTimeout());
  // 1.0: output.setReporter(reporter) —— 【偏差 3】累加器体系未落地
  ownedRecordWriters_.push_back(recordWriter);

  std::shared_ptr<RecordWriterOutput> streamOutput =
      std::make_shared<RecordWriterOutput>(recordWriter.get());
  ownedStreamOutputs_.push_back(streamOutput);
  return streamOutput.get();
}

// ------------------------------------------------------------------------
//  Collectors for output chaining（1.0 的 private 嵌套类）
// ------------------------------------------------------------------------

// 1.0 ChainingOutput.collect(StreamRecord<T>)（:306）
void OperatorChain::ChainingOutput::collect(const StreamRecord& record) {
  try {
    operator_->setKeyContextElement1(record);
    operator_->processElement(record);
  } catch (const std::exception& e) {
    throw ExceptionInChainedOperatorException(e);
  }
}

// 1.0 ChainingOutput.emitWatermark(Watermark)（:317）
void OperatorChain::ChainingOutput::emitWatermark(const Watermark& mark) {
  try {
    operator_->processWatermark(mark);
  } catch (const std::exception& e) {
    throw ExceptionInChainedOperatorException(e);
  }
}

// 1.0 ChainingOutput.close()（:327）
void OperatorChain::ChainingOutput::close() {
  try {
    operator_->close();
  } catch (const std::exception& e) {
    throw ExceptionInChainedOperatorException(e);
  }
}

// 1.0 BroadcastingOutputCollector.emitWatermark(Watermark)（:368）
void OperatorChain::BroadcastingOutputCollector::emitWatermark(
    const Watermark& mark) {
  for (size_t i = 0; i < outputs_.size(); ++i) {
    outputs_[i]->emitWatermark(mark);
  }
}

// 1.0 BroadcastingOutputCollector.collect(StreamRecord<T>)（:375）
void OperatorChain::BroadcastingOutputCollector::collect(
    const StreamRecord& record) {
  for (size_t i = 0; i < outputs_.size(); ++i) {
    outputs_[i]->collect(record);
  }
}

// 1.0 BroadcastingOutputCollector.close()（:382）
void OperatorChain::BroadcastingOutputCollector::close() {
  for (size_t i = 0; i < outputs_.size(); ++i) {
    outputs_[i]->close();
  }
}

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
