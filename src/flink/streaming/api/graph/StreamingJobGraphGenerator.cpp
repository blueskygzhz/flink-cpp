#include "flink/streaming/api/graph/StreamingJobGraphGenerator.h"

#include <algorithm>
#include <stdexcept>

#include "flink/streaming/api/operators/StreamOperator.h"
#include "flink/streaming/runtime/partitioner/ForwardPartitioner.h"
#include "flink/streaming/runtime/partitioner/RescalePartitioner.h"

namespace flink {
namespace streaming {
namespace api {
namespace graph {

namespace {
typedef flink::runtime::jobgraph::DistributionPattern DistributionPattern;
typedef flink::runtime::jobgraph::ScheduleMode ScheduleMode;
typedef flink::runtime::io::network::partition::ResultPartitionType
    ResultPartitionType;
typedef flink::streaming::runtime::partitioner::ForwardPartitioner
    ForwardPartitioner;
typedef flink::streaming::runtime::partitioner::RescalePartitioner
    RescalePartitioner;
typedef flink::streaming::runtime::partitioner::StreamPartitioner
    StreamPartitioner;
typedef flink::streaming::api::operators::ChainingStrategy ChainingStrategy;

// 1.0 StringUtils.join(list, ", ")
std::string join(const std::vector<std::string>& parts,
                 const std::string& separator) {
  std::string result;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i != 0) result += separator;
    result += parts[i];
  }
  return result;
}

inline uint64_t rotl64(uint64_t x, int8_t r) {
  return (x << r) | (x >> (64 - r));
}

inline uint64_t fmix64(uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;
  return k;
}

const uint64_t C1 = 0x87c37b91114253d5ULL;
const uint64_t C2 = 0x4cf5ad432745937fULL;

// 小端读 8 字节（MurmurHash3_x64_128 的 getblock64）
inline uint64_t getBlock64(const unsigned char* p, int i) {
  uint64_t value = 0;
  for (int b = 0; b < 8; ++b) {
    value |= static_cast<uint64_t>(p[i * 8 + b]) << (b * 8);
  }
  return value;
}
}  // namespace

// ─────────────────── Murmur3_128Hasher（【偏差 1】）───────────────────

Murmur3_128Hasher::Murmur3_128Hasher()
    : h1_(0), h2_(0), bufferLength_(0), totalLength_(0) {
  // Guava 的 Hashing.murmur3_128(0)：seed 0，故 h1 = h2 = 0
  for (int i = 0; i < 16; ++i) buffer_[i] = 0;
}

void Murmur3_128Hasher::processBlock(const unsigned char* block) {
  uint64_t k1 = getBlock64(block, 0);
  uint64_t k2 = getBlock64(block, 1);

  k1 *= C1;
  k1 = rotl64(k1, 31);
  k1 *= C2;
  h1_ ^= k1;
  h1_ = rotl64(h1_, 27);
  h1_ += h2_;
  h1_ = h1_ * 5 + 0x52dce729ULL;

  k2 *= C2;
  k2 = rotl64(k2, 33);
  k2 *= C1;
  h2_ ^= k2;
  h2_ = rotl64(h2_, 31);
  h2_ += h1_;
  h2_ = h2_ * 5 + 0x38495ab5ULL;
}

void Murmur3_128Hasher::putByte(unsigned char byte) {
  buffer_[bufferLength_++] = byte;
  ++totalLength_;
  if (bufferLength_ == 16) {
    processBlock(buffer_);
    bufferLength_ = 0;
  }
}

// Guava Hasher.putInt(int)：小端 4 字节
void Murmur3_128Hasher::putInt(int32_t value) {
  const uint32_t bits = static_cast<uint32_t>(value);
  putByte(static_cast<unsigned char>(bits & 0xffU));
  putByte(static_cast<unsigned char>((bits >> 8) & 0xffU));
  putByte(static_cast<unsigned char>((bits >> 16) & 0xffU));
  putByte(static_cast<unsigned char>((bits >> 24) & 0xffU));
}

// Guava Hasher.putString(s, UTF-8)：写 UTF-8 原始字节
void Murmur3_128Hasher::putString(const std::string& value) {
  for (char ch : value) {
    putByte(static_cast<unsigned char>(ch));
  }
}

std::vector<unsigned char> Murmur3_128Hasher::hash() const {
  uint64_t h1 = h1_;
  uint64_t h2 = h2_;

  // tail
  uint64_t k1 = 0;
  uint64_t k2 = 0;
  for (int i = bufferLength_ - 1; i >= 8; --i) {
    k2 |= static_cast<uint64_t>(buffer_[i]) << ((i - 8) * 8);
  }
  if (bufferLength_ > 8) {
    k2 *= C2;
    k2 = rotl64(k2, 33);
    k2 *= C1;
    h2 ^= k2;
  }
  for (int i = std::min(bufferLength_, 8) - 1; i >= 0; --i) {
    k1 |= static_cast<uint64_t>(buffer_[i]) << (i * 8);
  }
  if (bufferLength_ > 0) {
    k1 *= C1;
    k1 = rotl64(k1, 31);
    k1 *= C2;
    h1 ^= k1;
  }

  // finalization
  h1 ^= static_cast<uint64_t>(totalLength_);
  h2 ^= static_cast<uint64_t>(totalLength_);
  h1 += h2;
  h2 += h1;
  h1 = fmix64(h1);
  h2 = fmix64(h2);
  h1 += h2;
  h2 += h1;

  // Guava asBytes()：两个 long 各按小端写出
  std::vector<unsigned char> bytes(16);
  for (int i = 0; i < 8; ++i) {
    bytes[i] = static_cast<unsigned char>((h1 >> (i * 8)) & 0xffU);
    bytes[8 + i] = static_cast<unsigned char>((h2 >> (i * 8)) & 0xffU);
  }
  return bytes;
}

// ─────────────────────── createJobGraph（:114）───────────────────────

std::shared_ptr<StreamingJobGraphGenerator::JobGraph>
StreamingJobGraphGenerator::createJobGraph() {
  jobGraph_.reset(new JobGraph(streamGraph_->getJobName()));

  // 1.0: "make sure that all vertices start immediately"
  jobGraph_->setScheduleMode(ScheduleMode::ALL);

  init();

  // 1.0: "Generate deterministic hashes for the nodes in order to identify
  //       them across submission iff they didn't change."
  HashMap hashes = traverseStreamGraphAndGenerateHashes();

  setChaining(hashes);

  setPhysicalEdges();

  setSlotSharing();

  configureCheckpointing();

  // 1.0 configureRestartStrategy() —— 【偏差 4】
  // 1.0 InstantiationUtil.writeObjectToConfig(executionConfig, ...) —— 【偏差 5】

  return jobGraph_;
}

// 1.0 private init()（:105）
void StreamingJobGraphGenerator::init() {
  jobVertices_.clear();
  builtVertices_.clear();
  chainedConfigs_.clear();
  vertexConfigs_.clear();
  chainedNames_.clear();
  physicalEdgesInOrder_.clear();
  slotSharingGroupPool_.clear();
  coLocationGroupPool_.clear();
}

// 1.0 private setPhysicalEdges()（:145）
void StreamingJobGraphGenerator::setPhysicalEdges() {
  std::map<int, StreamEdgeList> physicalInEdgesInOrder;

  for (const StreamEdgePtr& edge : physicalEdgesInOrder_) {
    const int target = edge->getTargetId();
    // 1.0: "create if not set"（std::map 的 operator[] 自动建空表）
    physicalInEdgesInOrder[target].push_back(edge);
  }

  for (const auto& inEdges : physicalInEdgesInOrder) {
    const int vertex = inEdges.first;
    vertexConfigs_[vertex]->setInPhysicalEdges(inEdges.second);
  }
}

// 1.0 private setChaining(hashes)（:175）：
// 「Sets up task chains from the source StreamNode instances. This will
//   recursively create all JobVertex instances.」
void StreamingJobGraphGenerator::setChaining(const HashMap& hashes) {
  for (int sourceNodeId : streamGraph_->getSourceIDs()) {
    createChain(sourceNodeId, sourceNodeId, hashes);
  }
}

// 1.0 private createChain(startNodeId, currentNodeId, hashes)（:181）
StreamingJobGraphGenerator::StreamEdgeList
StreamingJobGraphGenerator::createChain(int startNodeId, int currentNodeId,
                                        const HashMap& hashes) {
  if (builtVertices_.count(startNodeId) != 0) {
    return StreamEdgeList();
  }

  StreamEdgeList transitiveOutEdges;
  StreamEdgeList chainableOutputs;
  StreamEdgeList nonChainableOutputs;

  for (const StreamEdgePtr& outEdge :
       streamGraph_->getStreamNode(currentNodeId)->getOutEdges()) {
    if (isChainable(outEdge)) {
      chainableOutputs.push_back(outEdge);
    } else {
      nonChainableOutputs.push_back(outEdge);
    }
  }

  // chainable：startNodeId 不变，还在同一条链里
  for (const StreamEdgePtr& chainable : chainableOutputs) {
    const StreamEdgeList transitive =
        createChain(startNodeId, chainable->getTargetId(), hashes);
    transitiveOutEdges.insert(transitiveOutEdges.end(), transitive.begin(),
                              transitive.end());
  }

  // nonChainable：这条边本身就是整条链的出边，并以 target 为头开一条新链
  for (const StreamEdgePtr& nonChainable : nonChainableOutputs) {
    transitiveOutEdges.push_back(nonChainable);
    createChain(nonChainable->getTargetId(), nonChainable->getTargetId(),
                hashes);
  }

  chainedNames_[currentNodeId] =
      createChainedName(currentNodeId, chainableOutputs);

  StreamConfig* config;
  if (currentNodeId == startNodeId) {
    config = createJobVertex(startNodeId, hashes);
  } else {
    // 1.0: new StreamConfig(new Configuration())，随后在 else 分支里存进
    // chainedConfigs。这里**先占好位置**再取指针，因为 setVertexConfig 要把
    // 指针登记进 vertexConfigs_（1.0 的 StreamConfig 是 Configuration 的视图，
    // 多份视图共享底层 map，天然满足；见头文件 vertexConfigs_ 的注释）
    StreamConfig::ChainedConfigMap& chainedConfs = chainedConfigs_[startNodeId];
    std::shared_ptr<StreamConfig>& slot = chainedConfs[currentNodeId];
    slot.reset(new StreamConfig());
    config = slot.get();
  }

  setVertexConfig(currentNodeId, config, chainableOutputs,
                  nonChainableOutputs);

  if (currentNodeId == startNodeId) {
    config->setChainStart();
    config->setOutEdgesInOrder(transitiveOutEdges);
    config->setOutEdges(
        streamGraph_->getStreamNode(currentNodeId)->getOutEdges());

    for (const StreamEdgePtr& edge : transitiveOutEdges) {
      connect(startNodeId, edge);
    }

    auto it = chainedConfigs_.find(startNodeId);
    config->setTransitiveChainedTaskConfigs(
        it == chainedConfigs_.end() ? StreamConfig::ChainedConfigMap()
                                    : it->second);
  }

  return transitiveOutEdges;
}

// 1.0 private createChainedName(vertexID, chainedOutputs)（:247）
std::string StreamingJobGraphGenerator::createChainedName(
    int vertexID, const StreamEdgeList& chainedOutputs) const {
  const std::string operatorName =
      streamGraph_->getStreamNode(vertexID)->getOperatorName();
  if (chainedOutputs.size() > 1) {
    std::vector<std::string> outputChainedNames;
    for (const StreamEdgePtr& chainable : chainedOutputs) {
      auto it = chainedNames_.find(chainable->getTargetId());
      outputChainedNames.push_back(
          it == chainedNames_.end() ? std::string() : it->second);
    }
    return operatorName + " -> (" + join(outputChainedNames, ", ") + ")";
  } else if (chainedOutputs.size() == 1) {
    auto it = chainedNames_.find(chainedOutputs[0]->getTargetId());
    return operatorName + " -> " +
           (it == chainedNames_.end() ? std::string() : it->second);
  } else {
    return operatorName;
  }
}

// 1.0 private createJobVertex(streamNodeId, hashes)（:262）
StreamConfig* StreamingJobGraphGenerator::createJobVertex(
    int streamNodeId, const HashMap& hashes) {
  StreamNode* streamNode = streamGraph_->getStreamNode(streamNodeId);

  auto hashIt = hashes.find(streamNodeId);
  if (hashIt == hashes.end()) {
    throw std::logic_error(
        "Cannot find node hash. "
        "Did you generate them before calling this method?");
  }

  const JobVertexID jobVertexId(hashIt->second);

  // 【偏差 2】1.0 的 InputFormatVertex 分支省掉
  JobVertexPtr jobVertex(
      new JobVertex(chainedNames_[streamNodeId], jobVertexId));

  jobVertex->setInvokableClass(streamNode->getJobVertexClassName());

  const int parallelism = streamNode->getParallelism();
  if (parallelism > 0) {
    jobVertex->setParallelism(parallelism);
  }

  jobVertices_[streamNodeId] = jobVertex;
  builtVertices_.insert(streamNodeId);
  jobGraph_->addVertex(jobVertex);

  // 1.0: return new StreamConfig(jobVertex.getConfiguration())
  // 【偏差 1 of JobVertex.h】这里直接取那份 StreamConfig 的引用
  return &jobVertex->getConfiguration();
}

// 1.0 private setVertexConfig(...)（:310）
void StreamingJobGraphGenerator::setVertexConfig(
    int vertexID, StreamConfig* config, const StreamEdgeList& chainableOutputs,
    const StreamEdgeList& nonChainableOutputs) {
  StreamNode* vertex = streamGraph_->getStreamNode(vertexID);

  config->setVertexID(vertexID);
  config->setBufferTimeout(vertex->getBufferTimeout());

  config->setTypeSerializerIn1(vertex->getTypeSerializerIn1());
  config->setTypeSerializerIn2(vertex->getTypeSerializerIn2());
  config->setTypeSerializerOut(vertex->getTypeSerializerOut());

  config->setStreamOperator(vertex->getOperator());
  config->setOutputSelectors(vertex->getOutputSelectors());

  config->setNumberOfOutputs(static_cast<int>(nonChainableOutputs.size()));
  config->setNonChainedOutputs(nonChainableOutputs);
  config->setChainedOutputs(chainableOutputs);

  // 【偏差 3】1.0 取自 streamGraph.getEnvironment()
  config->setTimeCharacteristic(timeCharacteristic_);

  const StreamGraph::CheckpointConfig& checkpointCfg =
      streamGraph_->getCheckpointConfig();

  config->setStateBackend(streamGraph_->getStateBackend());
  config->setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
  if (checkpointCfg.isCheckpointingEnabled()) {
    config->setCheckpointMode(checkpointCfg.getCheckpointingMode());
  } else {
    // 1.0: "the 'at-least-once' input handler is slightly cheaper (in the
    //       absence of checkpoints), so we use that one if checkpointing is
    //       not enabled"
    config->setCheckpointMode(
        flink::streaming::api::CheckpointingMode::AT_LEAST_ONCE);
  }
  config->setStatePartitioner(0, vertex->getStatePartitioner1());
  config->setStatePartitioner(1, vertex->getStatePartitioner2());
  config->setStateKeySerializer(vertex->getStateKeySerializer());

  const std::string& vertexClass = vertex->getJobVertexClassName();
  if (vertexClass == StreamGraph::STREAM_ITERATION_HEAD ||
      vertexClass == StreamGraph::STREAM_ITERATION_TAIL) {
    config->setIterationId(streamGraph_->getBrokerID(vertexID));
    config->setIterationWaitTime(streamGraph_->getLoopTimeout(vertexID));
  }

  // 1.0:355-356 的 allOutputs 是死代码 —— 【偏差 6】

  vertexConfigs_[vertexID] = config;
}

// 1.0 private connect(headOfChain, edge)（:361）
void StreamingJobGraphGenerator::connect(int headOfChain,
                                         const StreamEdgePtr& edge) {
  physicalEdgesInOrder_.push_back(edge);

  const int downStreamvertexID = edge->getTargetId();

  JobVertex* headVertex = jobVertices_[headOfChain].get();
  JobVertex* downStreamVertex = jobVertices_[downStreamvertexID].get();

  StreamConfig& downStreamConfig = downStreamVertex->getConfiguration();
  downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

  // 1.0:374-393 的三个分支。前两支（ForwardPartitioner / RescalePartitioner）
  // 的函数体在 1.0 里逐字相同，但为了对照仍分开写
  StreamPartitioner* partitioner = edge->getPartitioner().get();
  if (dynamic_cast<ForwardPartitioner*>(partitioner) != NULL) {
    downStreamVertex->connectNewDataSetAsInput(
        headVertex, DistributionPattern::POINTWISE,
        ResultPartitionType::PIPELINED, true);
  } else if (dynamic_cast<RescalePartitioner*>(partitioner) != NULL) {
    downStreamVertex->connectNewDataSetAsInput(
        headVertex, DistributionPattern::POINTWISE,
        ResultPartitionType::PIPELINED, true);
  } else {
    downStreamVertex->connectNewDataSetAsInput(
        headVertex, DistributionPattern::ALL_TO_ALL,
        ResultPartitionType::PIPELINED, true);
  }
}

// 1.0 private isChainable(StreamEdge)（:401）
bool StreamingJobGraphGenerator::isChainable(const StreamEdgePtr& edge) const {
  StreamNode* upStreamVertex = edge->getSourceVertex();
  StreamNode* downStreamVertex = edge->getTargetVertex();

  const StreamGraph::StreamOperatorPtr& headOperator =
      upStreamVertex->getOperator();
  const StreamGraph::StreamOperatorPtr& outOperator =
      downStreamVertex->getOperator();

  return downStreamVertex->getInEdges().size() == 1 && outOperator &&
         headOperator &&
         upStreamVertex->isSameSlotSharingGroup(*downStreamVertex) &&
         outOperator->getChainingStrategy() == ChainingStrategy::ALWAYS &&
         (headOperator->getChainingStrategy() == ChainingStrategy::HEAD ||
          headOperator->getChainingStrategy() == ChainingStrategy::ALWAYS) &&
         dynamic_cast<ForwardPartitioner*>(edge->getPartitioner().get()) !=
             NULL &&
         upStreamVertex->getParallelism() ==
             downStreamVertex->getParallelism() &&
         streamGraph_->isChainingEnabled();
}

// 1.0 private setSlotSharing()（:420）
void StreamingJobGraphGenerator::setSlotSharing() {
  std::map<std::string,
           std::shared_ptr<flink::runtime::jobgraph::SlotSharingGroup> >
      slotSharingGroups;

  for (const auto& entry : jobVertices_) {
    const std::string slotSharingGroup =
        streamGraph_->getStreamNode(entry.first)->getSlotSharingGroup();

    auto& group = slotSharingGroups[slotSharingGroup];
    if (!group) {
      group.reset(new flink::runtime::jobgraph::SlotSharingGroup());
      slotSharingGroupPool_.push_back(group);
    }
    entry.second->setSlotSharingGroup(group.get());
  }

  for (const auto& pair : streamGraph_->getIterationSourceSinkPairs()) {
    std::shared_ptr<flink::runtime::jobgraph::CoLocationGroup> ccg(
        new flink::runtime::jobgraph::CoLocationGroup());
    coLocationGroupPool_.push_back(ccg);

    JobVertex* source = jobVertices_[pair.f0->getId()].get();
    JobVertex* sink = jobVertices_[pair.f1->getId()].get();

    ccg->addVertex(source);
    ccg->addVertex(sink);
    source->updateCoLocationGroup(ccg.get());
    sink->updateCoLocationGroup(ccg.get());
  }
}

// 1.0 private configureCheckpointing()（:451）
void StreamingJobGraphGenerator::configureCheckpointing() {
  const StreamGraph::CheckpointConfig& cfg =
      streamGraph_->getCheckpointConfig();

  if (!cfg.isCheckpointingEnabled()) {
    return;
  }

  const int64_t interval = cfg.getCheckpointInterval();
  if (interval < 1) {
    throw std::invalid_argument("The checkpoint interval must be positive");
  }

  // 1.0: "collect the vertices that receive 'trigger checkpoint' messages.
  //       currently, these are all the sources"
  std::vector<JobVertexID> triggerVertices;
  // 1.0: "collect the vertices that need to acknowledge the checkpoint
  //       currently, these are all vertices"
  std::vector<JobVertexID> ackVertices;
  // 1.0: "collect the vertices that receive 'commit checkpoint' messages
  //       currently, these are all vertices"
  std::vector<JobVertexID> commitVertices;

  for (const auto& entry : jobVertices_) {
    JobVertex* vertex = entry.second.get();
    if (vertex->isInputVertex()) {
      triggerVertices.push_back(vertex->getID());
    }
    // 1.0: TODO: add check whether the user function implements the
    //            checkpointing interface
    commitVertices.push_back(vertex->getID());
    ackVertices.push_back(vertex->getID());
  }

  jobGraph_->setSnapshotSettings(
      std::make_shared<flink::runtime::jobgraph::JobSnapshottingSettings>(
          triggerVertices, ackVertices, commitVertices, interval,
          cfg.getCheckpointTimeout(), cfg.getMinPauseBetweenCheckpoints(),
          cfg.getMaxConcurrentCheckpoints()));

  // 1.0 末尾那段默认重启策略 —— 【偏差 4】
}

// ──────────────────── 确定性 hash（:520-745）────────────────────

// 1.0 private traverseStreamGraphAndGenerateHashes()（:520）
StreamingJobGraphGenerator::HashMap
StreamingJobGraphGenerator::traverseStreamGraphAndGenerateHashes() {
  HashMap hashes;
  std::set<int> visited;
  std::vector<StreamNode*> remaining;  // 1.0 ArrayDeque 当队列
  size_t head = 0;

  // 1.0: "We need to make the source order deterministic. The source IDs are
  //       not returned in the same order, which means that submitting the same
  //       program twice might result in different traversal, which breaks the
  //       deterministic hash assignment."
  std::vector<int> sources(streamGraph_->getSourceIDs().begin(),
                           streamGraph_->getSourceIDs().end());
  std::sort(sources.begin(), sources.end());

  // 1.0: "Start with source nodes"
  for (int sourceNodeId : sources) {
    remaining.push_back(streamGraph_->getStreamNode(sourceNodeId));
    visited.insert(sourceNodeId);
  }

  // 1.0: "Traverse the graph in a breadth-first manner. Keep in mind that the
  //       graph is not a tree and multiple paths to nodes can exist."
  while (head < remaining.size()) {
    StreamNode* currentNode = remaining[head++];

    // 1.0: "Generate the hash code. Because multiple path exist to each node,
    //       we might not have all required inputs available to generate the
    //       hash code."
    if (generateNodeHash(currentNode, hashes)) {
      // 1.0: "Add the child nodes"
      for (const StreamEdgePtr& outEdge : currentNode->getOutEdges()) {
        StreamNode* child = outEdge->getTargetVertex();
        if (visited.count(child->getId()) == 0) {
          remaining.push_back(child);
          visited.insert(child->getId());
        }
      }
    } else {
      // 1.0: "We will revisit this later."
      visited.erase(currentNode->getId());
      remaining.push_back(currentNode);
    }
  }

  return hashes;
}

// 1.0 private generateNodeHash(node, hashFunction, hashes)（:587）
bool StreamingJobGraphGenerator::generateNodeHash(StreamNode* node,
                                                  HashMap& hashes) {
  // 1.0: "Check for user-specified ID"
  const std::string& userSpecifiedHash = node->getTransformationId();

  if (userSpecifiedHash.empty()) {
    // 1.0: "Check that all input nodes have their hashes computed"
    for (const StreamEdgePtr& inEdge : node->getInEdges()) {
      // 1.0: "If the input node has not been visited yet, the current node
      //       will be visited again at a later point when all input nodes
      //       have been visited and their hashes set."
      if (hashes.count(inEdge->getSourceId()) == 0) {
        return false;
      }
    }

    Murmur3_128Hasher hasher;
    const NodeHash hash = generateDeterministicHash(node, hasher, hashes);

    if (hashes.count(node->getId()) != 0) {
      // 1.0: "Sanity check"
      throw std::logic_error(
          "Unexpected state. Tried to add node hash twice. This is probably a "
          "bug in the JobGraph generator.");
    }
    hashes[node->getId()] = hash;
    return true;
  } else {
    // 1.0: "Check that this node is not part of a chain. This is currently not
    //       supported, because the runtime takes the snapshots by the operator
    //       ID of the first vertex in a chain. It's OK if the node has chained
    //       outputs."
    for (const StreamEdgePtr& inEdge : node->getInEdges()) {
      if (isChainable(inEdge)) {
        throw std::logic_error(
            "Cannot assign user-specified hash to intermediate node in chain. "
            "This will be supported in future versions of Flink. As a work "
            "around start new chain at task " + node->getOperatorName() + ".");
      }
    }

    Murmur3_128Hasher hasher;
    const NodeHash hash = generateUserSpecifiedHash(node, hasher);

    for (const auto& entry : hashes) {
      if (entry.second == hash) {
        throw std::invalid_argument(
            "Hash collision on user-specified ID. Most likely cause is a "
            "non-unique ID. Please check that all IDs specified via "
            "`uid(String)` are unique.");
      }
    }

    if (hashes.count(node->getId()) != 0) {
      throw std::logic_error(
          "Unexpected state. Tried to add node hash twice. This is probably a "
          "bug in the JobGraph generator.");
    }
    hashes[node->getId()] = hash;
    return true;
  }
}

// 1.0 private generateUserSpecifiedHash(node, hasher)（:655）
StreamingJobGraphGenerator::NodeHash
StreamingJobGraphGenerator::generateUserSpecifiedHash(
    StreamNode* node, Murmur3_128Hasher& hasher) const {
  hasher.putString(node->getTransformationId());
  return hasher.hash();
}

// 1.0 private generateDeterministicHash(node, hasher, hashes)（:665）
StreamingJobGraphGenerator::NodeHash
StreamingJobGraphGenerator::generateDeterministicHash(StreamNode* node,
                                                      Murmur3_128Hasher& hasher,
                                                      const HashMap& hashes) {
  // 1.0: "Include stream node to hash. We use the current size of the computed
  //       hashes as the ID. We cannot use the node's ID, because it is
  //       assigned from a static counter. This will result in two identical
  //       programs having different hashes."
  generateNodeLocalHash(node, hasher, static_cast<int>(hashes.size()));

  // 1.0: "Include chained nodes to hash"
  for (const StreamEdgePtr& outEdge : node->getOutEdges()) {
    if (isChainable(outEdge)) {
      StreamNode* chainedNode = outEdge->getTargetVertex();
      // 1.0: "Use the hash size again, because the nodes are chained to this
      //       node. This does not add a hash for the chained nodes."
      generateNodeLocalHash(chainedNode, hasher,
                            static_cast<int>(hashes.size()));
    }
  }

  NodeHash hash = hasher.hash();

  // 1.0: "Make sure that all input nodes have their hash set before entering
  //       this loop (calling this method)."
  for (const StreamEdgePtr& inEdge : node->getInEdges()) {
    auto it = hashes.find(inEdge->getSourceId());
    // 1.0: "Sanity check"
    if (it == hashes.end()) {
      throw std::logic_error("Missing hash for input node " +
                             inEdge->getSourceVertex()->toString() +
                             ". Cannot generate hash for " + node->toString() +
                             ".");
    }
    const NodeHash& otherHash = it->second;
    for (size_t j = 0; j < hash.size(); ++j) {
      // 1.0: hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
      hash[j] = static_cast<unsigned char>(
          static_cast<unsigned char>(hash[j] * 37) ^ otherHash[j]);
    }
  }

  return hash;
}

// 1.0 private generateNodeLocalHash(node, hasher, id)（:731）
void StreamingJobGraphGenerator::generateNodeLocalHash(
    StreamNode* node, Murmur3_128Hasher& hasher, int id) const {
  // 1.0: "This resolves conflicts for otherwise identical source nodes. BUT
  //       the generated hash codes depend on the ordering of the nodes in the
  //       stream graph."
  hasher.putInt(id);
  hasher.putInt(node->getParallelism());

  // 1.0: node.getOperator() instanceof AbstractUdfStreamOperator ->
  //      putString(UDF 的类名)
  // 【偏差】C++ 无反射取不到「UDF 类名」。AbstractUdfStreamOperator 这边
  // 提供了 getUserFunctionTypeName()（typeid().name()），语义等价：同一个
  // UDF 类型给同一个串、不同类型给不同串
  if (node->getOperator()) {
    const std::string udfTypeName =
        node->getOperator()->getUserFunctionTypeName();
    if (!udfTypeName.empty()) {
      hasher.putString(udfTypeName);
    }
  }
}

}  // namespace graph
}  // namespace api
}  // namespace streaming
}  // namespace flink
