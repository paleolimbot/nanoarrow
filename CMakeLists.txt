
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.11)

project(ArrowC)

option(BUILD_SHARED_LIBS "Build using shared libraries" OFF)

include_directories(src)
add_library(arrowc src/arrowc/arrowc.c)

set_target_properties(arrowc PROPERTIES
    POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})

install(TARGETS arrowc DESTINATION lib)
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.h")
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.c")
