/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator
 *
 * 1.0 abstract class AbstractUdfStreamOperator<OUT, F extends Function>
 *       extends AbstractStreamOperator<OUT>
 *       implements OutputTypeConfigurable<OUT>
 *
 *   protected final F userFunction;
 *   private transient boolean functionsClosed = false;   // 防止 close 被调两次
 *
 *   setup()    -> super.setup() + FunctionUtils.setFunctionRuntimeContext(
 *                                     userFunction, getRuntimeContext())
 *   open()     -> super.open()  + FunctionUtils.openFunction(userFunction,
 *                                     new Configuration())
 *   close()    -> super.close() + functionsClosed = true
 *                               + FunctionUtils.closeFunction(userFunction)
 *   dispose()  -> super.dispose() + 没关过才补一次 closeFunction（吞异常只记日志）
 *   snapshotOperatorState() -> super + userFunction instanceof Checkpointed
 *                              ? 把 UDF 状态存成 StateHandle 挂到 functionState
 *   restoreState()          -> super + 把 functionState 还给 UDF
 *   notifyOfCompletedCheckpoint() -> super + userFunction instanceof
 *                                            CheckpointListener 则转发
 *
 * 【偏差】1.0 还实现 OutputTypeConfigurable（让算子在 StreamGraph 定稿时拿到
 * 输出类型的 TypeSerializer，给 KryoSerializer 注册类型用）。本实现的
 * serializer 由模板在编译期取（TypeExtractor<T>），没有这个回填环节。
 *
 * 【偏差】1.0 的 openFunction(userFunction, new Configuration()) 带一个空
 * Configuration 参数（旧 batch API 的遗留）；本实现的 RichFunction.open() 无参。
 */
#ifndef FLINK_STREAMING_API_OPERATORS_ABSTRACTUDFSTREAMOPERATOR_H
#define FLINK_STREAMING_API_OPERATORS_ABSTRACTUDFSTREAMOPERATOR_H

#include <memory>
#include <stdexcept>
#include <string>

#include "flink/api/common/functions/util/FunctionUtils.h"
#include "flink/runtime/state/CheckpointListener.h"
#include "flink/streaming/api/checkpoint/Checkpointed.h"
#include "flink/streaming/api/operators/AbstractStreamOperator.h"
#include "flink/util/Preconditions.h"

namespace flink {
namespace streaming {
namespace api {
namespace operators {

template <class F>
class AbstractUdfStreamOperator : public AbstractStreamOperator {
 public:
  typedef flink::streaming::api::checkpoint::Checkpointed Checkpointed;
  typedef flink::runtime::state::CheckpointListener CheckpointListener;

  // 1.0: this.userFunction = requireNonNull(userFunction);
  explicit AbstractUdfStreamOperator(std::shared_ptr<F> userFunction)
      : userFunction_(userFunction), functionsClosed_(false) {
    if (!userFunction_) {
      throw std::invalid_argument("userFunction must not be null");
    }
  }

  // 1.0 getUserFunction()
  const std::shared_ptr<F>& getUserFunction() const { return userFunction_; }

  // ------------------------- operator life cycle -------------------------

  void setup(StreamTask* containingTask, StreamConfig* config,
             Output* output) override {
    AbstractStreamOperator::setup(containingTask, config, output);
    // 1.0: FunctionUtils.setFunctionRuntimeContext(userFunction,
    //          getRuntimeContext())
    // 【偏差】StreamingRuntimeContext 还没落地（见 AbstractStreamOperator.h
    // 偏差 2），暂时传空上下文
    flink::api::common::functions::util::FunctionUtils::
        setFunctionRuntimeContext(userFunction_, NULL);
  }

  void open() override {
    AbstractStreamOperator::open();
    flink::api::common::functions::util::FunctionUtils::openFunction(
        userFunction_);
  }

  void close() override {
    AbstractStreamOperator::close();
    functionsClosed_ = true;
    flink::api::common::functions::util::FunctionUtils::closeFunction(
        userFunction_);
  }

  void dispose() override {
    AbstractStreamOperator::dispose();
    if (!functionsClosed_) {
      functionsClosed_ = true;
      try {
        flink::api::common::functions::util::FunctionUtils::closeFunction(
            userFunction_);
      } catch (...) {
        // 1.0: "Exception while closing user function while failing or
        // canceling task" —— 只记日志，不往外抛
      }
    }
  }

  // ----------------------- checkpointing and recovery -----------------------

  StreamTaskState snapshotOperatorState(int64_t checkpointId,
                                        int64_t timestamp) override {
    StreamTaskState state =
        AbstractStreamOperator::snapshotOperatorState(checkpointId, timestamp);

    Checkpointed* chkFunction =
        dynamic_cast<Checkpointed*>(userFunction_.get());
    if (chkFunction != NULL) {
      std::string udfState;
      try {
        udfState = chkFunction->snapshotState(checkpointId, timestamp);
      } catch (const std::exception& e) {
        throw std::runtime_error(
            std::string("Failed to draw state snapshot from function: ") +
            e.what());
      }
      // 1.0: if (udfState != null)。C++ 用空字节串表达「这次没有状态」
      if (!udfState.empty()) {
        try {
          state.setFunctionState(
              std::make_shared<flink::runtime::state::StateHandle>(
                  getStateBackend()->checkpointStateSerializable(
                      udfState, checkpointId, timestamp)));
        } catch (const std::exception& e) {
          throw std::runtime_error(
              std::string("Failed to add the state snapshot of the function "
                          "to the checkpoint: ") +
              e.what());
        }
      }
    }
    return state;
  }

  void restoreState(const StreamTaskState& state,
                    int64_t recoveryTimestamp) override {
    AbstractStreamOperator::restoreState(state, recoveryTimestamp);

    const StreamTaskState::StateHandlePtr& stateHandle =
        state.getFunctionState();
    Checkpointed* chkFunction =
        dynamic_cast<Checkpointed*>(userFunction_.get());

    if (chkFunction != NULL && stateHandle) {
      const std::string& functionState = stateHandle->getState();
      if (!functionState.empty()) {
        try {
          chkFunction->restoreState(functionState);
        } catch (const std::exception& e) {
          throw std::runtime_error(
              std::string("Failed to restore state to function: ") + e.what());
        }
      }
    }
  }

  void notifyOfCompletedCheckpoint(int64_t checkpointId) override {
    AbstractStreamOperator::notifyOfCompletedCheckpoint(checkpointId);

    CheckpointListener* listener =
        dynamic_cast<CheckpointListener*>(userFunction_.get());
    if (listener != NULL) {
      listener->notifyCheckpointComplete(checkpointId);
    }
  }

 protected:
  // 1.0 的 protected final F userFunction
  std::shared_ptr<F> userFunction_;

 private:
  bool functionsClosed_;
};

}  // namespace operators
}  // namespace api
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_API_OPERATORS_ABSTRACTUDFSTREAMOPERATOR_H
