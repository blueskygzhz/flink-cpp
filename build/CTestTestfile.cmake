# CMake generated Testfile for 
# Source directory: /data/bingochen/flink-cpp
# Build directory: /data/bingochen/flink-cpp/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(wordcount "/data/bingochen/flink-cpp/build/wordcount")
set_tests_properties(wordcount PROPERTIES  _BACKTRACE_TRIPLES "/data/bingochen/flink-cpp/CMakeLists.txt;30;add_test;/data/bingochen/flink-cpp/CMakeLists.txt;0;")
add_test(event_time_wordcount "/data/bingochen/flink-cpp/build/event_time_wordcount")
set_tests_properties(event_time_wordcount PROPERTIES  _BACKTRACE_TRIPLES "/data/bingochen/flink-cpp/CMakeLists.txt;31;add_test;/data/bingochen/flink-cpp/CMakeLists.txt;0;")
add_test(checkpoint_restore "/data/bingochen/flink-cpp/build/checkpoint_restore")
set_tests_properties(checkpoint_restore PROPERTIES  _BACKTRACE_TRIPLES "/data/bingochen/flink-cpp/CMakeLists.txt;32;add_test;/data/bingochen/flink-cpp/CMakeLists.txt;0;")
