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

# require cmake 2.6.4 (but please use 2.8.x)
cmake_minimum_required(VERSION 2.6.4)

# we first give our project a name
project(extmemgraphgen)

# prohibit in-source builds
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(SEND_ERROR "In-source builds are not allowed.")
endif("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")

# enable warnings (always good)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
# enable C++11
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# search for stxxl-config.cmake which contains the library's configuration
find_package(STXXL REQUIRED)

# print some info (this can be removed)
message(STATUS "STXXL_CXX_FLAGS: ${STXXL_CXX_FLAGS}")
message(STATUS "STXXL_INCLUDE_DIRS: ${STXXL_INCLUDE_DIRS}")
message(STATUS "STXXL_LIBRARIES: ${STXXL_LIBRARIES}")

# apply CXXFLAGS to our configuration
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")


# add STXXL include directory
include_directories(${STXXL_INCLUDE_DIRS})

# Declare the library
add_library(libextmemgraphgen STATIC
    RandomStream.cpp PowerlawDegreeSequence.cpp EdgeVector.cpp
)

# Specify here the include directories exported
# by this library
target_include_directories(libextmemgraphgen PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# create and executable and linke with STXXL
add_executable(test1 test1.cpp)
target_link_libraries(test1 ${STXXL_LIBRARIES} libextmemgraphgen)

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
