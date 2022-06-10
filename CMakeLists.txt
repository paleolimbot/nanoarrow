
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(ArrowC)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

option(BUILD_EXAMPLES "Build arrowc examples" OFF)
option(BUILD_SHARED_LIBS "Build using shared libraries" OFF)

include_directories(src)
add_library(arrowc src/arrowc/arrowc.c)

set_target_properties(arrowc PROPERTIES
    POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})

if(BUILD_EXAMPLES)

endif()

install(TARGETS arrowc DESTINATION lib)
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.h")
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.c")

if(BUILD_EXAMPLES)

endif()
