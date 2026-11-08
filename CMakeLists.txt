cmake_minimum_required(VERSION 3.10)
project(flinkcpp VERSION 0.2.0 LANGUAGES CXX)

# 参考实现: Apache Flink release-1.0.0 (commit 94cd554, 2016-03-03)
# 源码路径: /data/bingochen/flink-1.0

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)

add_library(flinkcpp INTERFACE)
target_include_directories(flinkcpp INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(flinkcpp INTERFACE Threads::Threads)

add_executable(wordcount examples/wordcount.cpp)
target_link_libraries(wordcount PRIVATE flinkcpp)

add_executable(event_time_wordcount examples/event_time_wordcount.cpp)
target_link_libraries(event_time_wordcount PRIVATE flinkcpp)

add_executable(checkpoint_restore examples/checkpoint_restore.cpp)
target_link_libraries(checkpoint_restore PRIVATE flinkcpp)

enable_testing()
add_test(NAME wordcount COMMAND wordcount)
add_test(NAME event_time_wordcount COMMAND event_time_wordcount)
add_test(NAME checkpoint_restore COMMAND checkpoint_restore)
