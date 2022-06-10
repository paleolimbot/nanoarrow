
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
cmake_minimum_required(VERSION 3.14)

project(ArrowC)

option(ARROWC_BUILD_TESTS "Build tests" OFF)

include_directories(src)
add_library(arrowc src/arrowc/error.c src/arrowc/schema.c)

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

    add_executable(error_test src/arrowc/error_test.cc)
    add_executable(schema_test src/arrowc/schema_test.cc)

    target_link_libraries(error_test arrowc GTest::gtest_main)
    target_link_libraries(schema_test arrowc GTest::gtest_main arrow_shared arrow_testing_shared)

    include(GoogleTest)
    gtest_discover_tests(error_test)
    gtest_discover_tests(schema_test)

endif()
