cmake_minimum_required(VERSION 3.15)
project(raknet_python LANGUAGES CXX)

find_package(Python 3.12 COMPONENTS Interpreter Development.Module Development.SABIModule REQUIRED)

find_package(nanobind CONFIG REQUIRED)
find_package(raknet CONFIG REQUIRED)

nanobind_add_module(_raknet STABLE_ABI src/raknet_python.cpp)

target_link_libraries(_raknet PRIVATE raknet::raknet)

nanobind_add_stub(
    _raknet_stub
    MODULE _raknet
    OUTPUT _raknet.pyi
    PYTHON_PATH $<TARGET_FILE_DIR:_raknet>
    DEPENDS _raknet
    MARKER_FILE py.typed
)

install(TARGETS _raknet DESTINATION raknet)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/_raknet.pyi ${CMAKE_CURRENT_BINARY_DIR}/py.typed DESTINATION raknet)
