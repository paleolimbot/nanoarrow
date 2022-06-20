# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.14)

project(ArrowC)

option(ARROWC_BUILD_TESTS "Build tests" OFF)

option(ARROWC_CODE_COVERAGE "Enable coverage reporting" OFF)
add_library(coverage_config INTERFACE)

include_directories(src)
add_library(
    arrowc
    src/arrowc/allocator.c
    src/arrowc/error.c
    src/arrowc/metadata.c
    src/arrowc/schema.c
    src/arrowc/schema_view.c)

install(TARGETS arrowc DESTINATION lib)
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.h")
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.c")

if (ARROWC_BUILD_TESTS)
    # For testing we use GTest + Arrow C++ (both need C++11)
    include(FetchContent)

    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    find_package(Arrow REQUIRED)
    message(STATUS "Arrow version: ${ARROW_VERSION}")
    message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
    find_package(ArrowTesting REQUIRED)

    find_package(GTest REQUIRED)
    enable_testing()

    add_executable(allocator_test src/arrowc/allocator_test.cc)
    add_executable(error_test src/arrowc/error_test.cc)
    add_executable(metadata_test src/arrowc/metadata_test.cc)
    add_executable(schema_test src/arrowc/schema_test.cc)
    add_executable(schema_view_test src/arrowc/schema_view_test.cc)

    if (ARROWC_CODE_COVERAGE)
        target_compile_options(coverage_config INTERFACE -O0 -g --coverage)
        target_link_options(coverage_config INTERFACE --coverage)
        target_link_libraries(arrowc coverage_config)
    endif()

    target_link_libraries(allocator_test arrowc GTest::gtest_main arrow_shared arrow_testing_shared)
    target_link_libraries(error_test arrowc GTest::gtest_main)
    target_link_libraries(metadata_test arrowc GTest::gtest_main arrow_shared arrow_testing_shared)
    target_link_libraries(schema_test arrowc GTest::gtest_main arrow_shared arrow_testing_shared)
    target_link_libraries(schema_view_test arrowc GTest::gtest_main arrow_shared arrow_testing_shared)

    include(GoogleTest)
    gtest_discover_tests(allocator_test)
    gtest_discover_tests(error_test)
    gtest_discover_tests(metadata_test)
    gtest_discover_tests(schema_test)
    gtest_discover_tests(schema_view_test)

endif()
