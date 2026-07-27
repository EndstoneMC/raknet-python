cmake_minimum_required(VERSION 3.15)
project(raknet_python LANGUAGES CXX)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

find_package(nanobind CONFIG REQUIRED)
find_package(raknet CONFIG REQUIRED)

nanobind_add_module(_raknet src/raknet_python.cpp)

target_link_libraries(_raknet PRIVATE raknet::raknet)

install(TARGETS _raknet DESTINATION raknet)
