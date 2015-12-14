enable_testing()

message(STATUS "gtest_SOURCE_DIR: ${gtest_SOURCE_DIR}")

file(GLOB SRCS *.cpp)
add_executable(testextmemgraphgen ${SRCS})

include_directories(${gtest_SOURCE_DIR} ${gtest_SOURCE_DIR}/include)

target_link_libraries(testextmemgraphgen
    libextmemgraphgen
    gtest gtest_main
    ${STXXL_LIBRARIES}
)

add_test(TestsInExtMemGraphGen testextmemgraphgen)