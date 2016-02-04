############################################################################
#  CMakeLists.txt
#
#  Part of a simple STXXL example. See http://stxxl.sourceforge.net
#
#  Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
#
#  Distributed under the Boost Software License, Version 1.0.
#  (See accompanying file LICENSE_1_0.txt or copy at
#  http://www.boost.org/LICENSE_1_0.txt)
############################################################################

cmake_minimum_required(VERSION 2.8)

# we first give our project a name
project(extmemgraphgen)

# prohibit in-source builds
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
    message(SEND_ERROR "In-source builds are not allowed.")
endif("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")


# add third-party dependencies
SET(TMP_CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
SET(CMAKE_BUILD_TYPE "RelWithDebInfo")
add_subdirectory(libs/stxxl/)
add_subdirectory(libs/googletest/googletest)
include_directories(libs/stx-btree/include/)
SET(CMAKE_BUILD_TYPE "${TMP_CMAKE_BUILD_TYPE}")

include_directories(include/)


macro(remove_cxx_flag flag)
    string(REPLACE "${flag}" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
endmacro()

remove_cxx_flag("-O3")

# enable warnings (always good)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")

# enable C++11
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -DSTXXL_VERBOSE_LEVEL=0")

set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# Include STXXL
message(STATUS "STXXL_CXX_FLAGS: ${STXXL_CXX_FLAGS}")
message(STATUS "STXXL_INCLUDE_DIRS: ${STXXL_INCLUDE_DIRS}")
message(STATUS "STXXL_LIBRARIES: ${STXXL_LIBRARIES}")

# apply CXXFLAGS to our configuration
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftemplate-depth=1000")
include_directories(${STXXL_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DEDGE_SWAP_DEBUG_VECTOR")

# Declare the library

file(GLOB LFR_SRCS include/LFR/LFR_*.cpp)
add_library(libextmemgraphgen STATIC
    include/RandomStream.cpp
    include/PowerlawDegreeSequence.cpp
    include/EdgeSwaps/EdgeVectorCache.cpp
    include/EdgeSwaps/EdgeSwapInternalSwaps.cpp
    include/EdgeSwaps/EdgeSwapTFP.cpp
    include/HavelHakimi/HavelHakimiGenerator.cpp
    include/HavelHakimi/HavelHakimiGeneratorRLE.cpp
    include/LFR/LFR.cpp
    ${LFR_SRCS}
)

# Specify here the include directories exported
# by this library
#target_include_directories(libextmemgraphgen PUBLIC
#    ${CMAKE_CURRENT_SOURCE_DIR}
#)

# create and executable and linke with STXXL
add_executable(pa_test1 test1.cpp)
target_link_libraries(pa_test1 ${STXXL_LIBRARIES} libextmemgraphgen)

add_executable(pa_benchmark benchmark.cpp)
target_link_libraries(pa_benchmark ${STXXL_LIBRARIES} libextmemgraphgen)

add_executable(pa_lfr main_lfr.cpp)
target_link_libraries(pa_lfr libextmemgraphgen ${STXXL_LIBRARIES})

include(CMakeLocal.cmake)

add_subdirectory(test)

# Doxygen
find_package(Doxygen)

if(DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(doc
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
    )
endif(DOXYGEN_FOUND)

