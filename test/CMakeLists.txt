enable_testing()
find_package(GTest REQUIRED)

include_directories(${GTEST_INCLUDE_DIRS})

file(GLOB SRCS *.cpp)

add_executable(testextmemgraphgen ${SRCS})

target_link_libraries(testextmemgraphgen
    libextmemgraphgen
    ${GTEST_LIBRARIES}
    ${STXXL_LIBRARIES}
)

add_test(TestsInExtMemGraphGen testextmemgraphgen)