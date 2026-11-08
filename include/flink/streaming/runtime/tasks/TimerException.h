/*
 * flink-streaming-java :
 *   org.apache.flink.streaming.runtime.tasks.TimerException
 *
 * 1.0 TimerException extends RuntimeException：定时器线程（TriggerTask.run）里
 * 抛出的异常不能直接把那个线程崩掉，于是包成 TimerException 存进
 * StreamTask.asyncException，由主线程在 checkTimerException() 里重抛
 * —— 1.0 的 StreamInputProcessor / SourceStreamTask 在每轮循环里都会调它。
 */
#ifndef FLINK_STREAMING_RUNTIME_TASKS_TIMEREXCEPTION_H
#define FLINK_STREAMING_RUNTIME_TASKS_TIMEREXCEPTION_H

#include <stdexcept>
#include <string>

namespace flink {
namespace streaming {
namespace runtime {
namespace tasks {

class TimerException : public std::runtime_error {
 public:
  explicit TimerException(const std::string& cause)
      : std::runtime_error("TimerException: " + cause) {}
};

}  // namespace tasks
}  // namespace runtime
}  // namespace streaming
}  // namespace flink
#endif  // FLINK_STREAMING_RUNTIME_TASKS_TIMEREXCEPTION_H
