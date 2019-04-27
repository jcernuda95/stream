cmake_minimum_required(VERSION 3.13)
project(Stream)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads)
SET(CMAKE_CXX_FLAGS -pthread)

add_executable(Stream main.cpp MetadataManager.h MetadataManager.cpp structs.h main2.cpp stream_operator_factory.h operators/abstract_stream_operator.h operators/filter_operator.h operators/join_operator.h operators/group_operator.h operators/sum_operator.h operators/order_operator.h)

target_link_libraries(${PROJECT_NAME} pthread)
