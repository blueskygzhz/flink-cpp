cmake_minimum_required(VERSION 3.10)
project(flinkcpp VERSION 0.2.0 LANGUAGES CXX)

# 参考实现: Apache Flink release-1.0.0 (commit 94cd554, 2016-03-03)
# 源码路径: /data/bingochen/flink-1.0

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)

# 0.2 起不再是 single-header：有实现体的类放 src/ 下编译成静态库，
# 模板与纯接口仍留在 include/ 里（对应 Java 的「每个类一个文件」）。
set(FLINKCPP_SOURCES
  src/flink/runtime/util/MathUtils.cpp
  src/flink/runtime/util/DataInputDeserializer.cpp
  src/flink/runtime/util/DataOutputSerializer.cpp
  src/flink/runtime/state/memory/MemoryStateBackend.cpp
  src/flink/runtime/io/network/partition/consumer/SingleInputGate.cpp
  src/flink/streaming/runtime/tasks/StreamTask.cpp
)

add_library(flinkcpp STATIC ${FLINKCPP_SOURCES})
target_include_directories(flinkcpp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(flinkcpp PUBLIC Threads::Threads)

enable_testing()

# 各层的冒烟测试：对齐进度到哪里，测试就覆盖到哪里
add_executable(state_backend_test test/state_backend_test.cpp)
target_link_libraries(state_backend_test PRIVATE flinkcpp)
add_test(NAME state_backend COMMAND state_backend_test)

add_executable(operator_test test/operator_test.cpp)
target_link_libraries(operator_test PRIVATE flinkcpp)
add_test(NAME operators COMMAND operator_test)

# examples/ 下的三个程序仍是 0.1 single-header 时代的（#include
# "flinkcpp/api.hpp"），要等 streaming 层（StreamExecutionEnvironment /
# DataStream / StreamTask）按 1.0 重建完才能接回来，暂不参与构建。
