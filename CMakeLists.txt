cmake_minimum_required(VERSION 3.6)
project(code)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(mytest_SOURCE_DIR /Users/hyc/Box\ Sync/Courses/EECS277\ Database\ System/Project/project1/code)
set(PROJECT_BINARY_DIR /Users/hyc/Box\ Sync/Courses/EECS277\ Database\ System/Project/project1/code)
add_custom_target(main.out COMMAND make -C ${mytest_SOURCE_DIR}
        CLION_EXE_DIR=${PROJECT_BINARY_DIR})

