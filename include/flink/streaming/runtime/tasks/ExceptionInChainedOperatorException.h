/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException
 *
 * 1.0 原注释：「A special exception that signifies that the cause exception
 * came from a chained operator.」
 *
 * 1.0 全文（除注释）：
 *   public class ExceptionInChainedOperatorException extends RuntimeException {
 *       public ExceptionInChainedOperatorException(Throwable cause) {
 *           this("Could not forward element to next operator", cause);
 *       }
 *       public ExceptionInChainedOperatorException(String message, Throwable cause) {
 *           super(message, requireNonNull(cause));
 *       }
 *       public Throwable getOriginalCause() {
 *           Throwable ex = this;
 *           do { ex = ex.getCause(); }
 *           while (ex instanceof ExceptionInChainedOperatorException);
 *           return ex;
 *       }
 *   }
 *
 * 作用：ChainingOutput 把记录交给链上下一个算子时，若下游算子抛异常，
 * 包一层这个异常再往上扔。算子链越深就包越多层，所以 getOriginalCause()
 * 要**循环剥掉所有同类型的外层**，取到最初那个真异常。
 *
 * 【偏差】C++ 的异常没有 Java 的 cause 链（Throwable.getCause()）。
 * 这里把原异常的 what() 拼进 message，并单独记一份 originalCause_ 文本；
 * 「循环剥层」改为在包装时判断 cause 本身是不是
 * ExceptionInChainedOperatorException，若是就直接继承它的 originalCause_
 * —— 效果等同于 1.0 的 do-while 剥层，只是在包装时一次完成。
 * （C++11 的 std::throw_with_nested / rethrow_nested 能做真嵌套，但取值要靠
 *   catch 重抛，调用方 StreamTask 只打印消息，不值得。）
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_EXCEPTIONINCHAINEDOPERATOREXCEPTION_H
#define FLINK_STREAMING_RUNTIME_TASKS_EXCEPTIONINCHAINEDOPERATOREXCEPTION_H

#include <exception>
#include <stdexcept>
#include <string>

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class ExceptionInChainedOperatorException : public std::runtime_error {
 public:
  // 1.0: this("Could not forward element to next operator", cause)
  explicit ExceptionInChainedOperatorException(const std::exception& cause)
      : std::runtime_error(
            std::string("Could not forward element to next operator: ") +
            unwrap(cause)),
        originalCause_(unwrap(cause)) {}

  // 1.0: super(message, requireNonNull(cause))
  ExceptionInChainedOperatorException(const std::string& message,
                                      const std::exception& cause)
      : std::runtime_error(message + ": " + unwrap(cause)),
        originalCause_(unwrap(cause)) {}

  // 1.0 getOriginalCause()：剥掉所有 ExceptionInChainedOperatorException 外层
  const std::string& getOriginalCause() const { return originalCause_; }

 private:
  // 对应 1.0 的 do { ex = ex.getCause(); } while (ex instanceof 本类)
  static std::string unwrap(const std::exception& cause) {
    const ExceptionInChainedOperatorException* chained =
        dynamic_cast<const ExceptionInChainedOperatorException*>(&cause);
    return chained != NULL ? chained->originalCause_ : cause.what();
  }

  std::string originalCause_;
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_EXCEPTIONINCHAINEDOPERATOREXCEPTION_H
