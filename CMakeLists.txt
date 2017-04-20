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

set(BOOST_LIBS "")
find_package(Boost 1.58.0 COMPONENTS iostreams)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -DBOOST_SUPPORT")
    message("Boost support !")
    set(BOOST_LIBS "boost_iostreams")
endif()


macro(remove_cxx_flag flag)
    string(REPLACE "${flag}" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
endmacro()

remove_cxx_flag("-O3")

# enable warnings (always good)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")

# SSE support required for crc-intrinsic
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}") #  -mtune=native -msse4")

# enable C++11
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11  -DSTXXL_VERBOSE_LEVEL=0 -DSTXXL_PARALLEL_PQ_MULTIWAY_MERGE_INTERNAL=0 -DSTXXL_PARALLEL_PQ_MULTIWAY_MERGE_EXTERNAL=0") # -DSTXXL_PARALLEL_MODE=1 causes problems?

set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# Include STXXL
message(STATUS "STXXL_CXX_FLAGS: ${STXXL_CXX_FLAGS}")
message(STATUS "STXXL_INCLUDE_DIRS: ${STXXL_INCLUDE_DIRS}")
message(STATUS "STXXL_LIBRARIES: ${STXXL_LIBRARIES}")

# apply CXXFLAGS to our configuration
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftemplate-depth=1000")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -msse4")
include_directories(${STXXL_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DEDGE_SWAP_DEBUG_VECTOR")

# Declare the library

file(GLOB LFR_SRCS include/LFR/LFR_*.cpp)
add_library(libextmemgraphgen STATIC
    include/EdgeSwaps/EdgeVectorCache.cpp
    include/EdgeSwaps/EdgeSwapInternalSwaps.cpp
    include/EdgeSwaps/EdgeSwapInternalSwapsBase.cpp
    include/EdgeSwaps/ModifiedEdgeSwapTFP.cpp
    include/EdgeSwaps/EdgeSwapTFP.cpp
    include/EdgeSwaps/SemiLoadedEdgeSwapTFP.cpp
    include/EdgeSwaps/EdgeSwapParallelTFP.cpp
    include/EdgeSwaps/IMEdgeSwap.cpp
    include/HavelHakimi/HavelHakimiGenerator.cpp
    include/HavelHakimi/HavelHakimiGeneratorRLE.cpp
    #include/LFR/LFR.cpp
    #include/LFR/GlobalRewiringSwapGenerator.cpp
    #include/LFR/CommunityEdgeRewiringSwaps.cpp
    #include/LFR/LFRCommunityAssignBenchmark.cpp
    include/IMGraph.cpp
    #include/CluewebReader.cpp
    include/ConfigurationModel/ConfigurationModel.cpp
    ${LFR_SRCS}
)



# Specify here the include directories exported
# by this library
#target_include_directories(libextmemgraphgen PUBLIC
#    ${CMAKE_CURRENT_SOURCE_DIR}
#)

# create and executable and linke with STXXL
#add_executable(pa_test1 test1.cpp)
#target_link_libraries(pa_test1 ${STXXL_LIBRARIES} libextmemgraphgen)

add_executable(pa_edge_swaps main_edge_swaps.cpp)
target_link_libraries(pa_edge_swaps libextmemgraphgen ${STXXL_LIBRARIES} ${Boost_LIBRARIES})

#add_executable(pa_benchmark benchmark.cpp)
#target_link_libraries(pa_benchmark ${STXXL_LIBRARIES} libextmemgraphgen ${BOOST_LIBS})


#add_executable(pa_lfr main_lfr.cpp)
#target_link_libraries(pa_lfr libextmemgraphgen ${STXXL_LIBRARIES})

#Hung
add_executable(data_configurationmodel cm_data.cpp)
target_link_libraries(data_configurationmodel libextmemgraphgen ${STXXL_LIBRARIES})

add_executable(hh_demo main_hh_demo.cpp)
target_link_libraries(hh_demo libextmemgraphgen ${STXXL_LIBRARIES})

#add_executable(data_hh_configurationmodel cm_hh_data_metis.cpp)
#target_link_libraries(data_hh_configurationmodel libextmemgraphgen ${STXXL_LIBRARIES})

add_executable(cm_benchmark main_cm_benchmark.cpp)
target_link_libraries(cm_benchmark libextmemgraphgen ${STXXL_LIBRARIES})

add_executable(cm_emtfp_convergence main_cm_emtfp_convergence.cpp)
target_link_libraries(cm_emtfp_convergence libextmemgraphgen ${STXXL_LIBRARIES})

add_executable(hh_graphmetrics main_hh_graphmetrics.cpp)
target_link_libraries(hh_graphmetrics libextmemgraphgen ${STXXL_LIBRARIES})

add_executable(memtfp_combined_benchmark main_memtfp_combined_benchmark.cpp)
target_link_libraries(memtfp_combined_benchmark libextmemgraphgen ${STXXL_LIBRARIES})

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

