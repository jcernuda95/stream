cmake_minimum_required(VERSION 3.13)
project(Stream)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads)
SET(CMAKE_CXX_FLAGS -pthread)

add_executable(Stream main.cpp MetadataManager.h MetadataManager.cpp structs.h main2.cpp)

target_link_libraries(${PROJECT_NAME} pthread)
