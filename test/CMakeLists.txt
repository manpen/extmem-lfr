enable_testing()

file(GLOB SRCS Test*.cpp)
if(LOCAL_TEST_RESTRICTION)
    message("WARNING GOING TO BUILD RESTRICTED TEST-CASES DUE DO CMakeLocal.cmake")
    SET(SRCS ${LOCAL_TEST_RESTRICTION})
endif()
add_executable(testextmemgraphgen ${SRCS})

include_directories(${gtest_SOURCE_DIR} ${gtest_SOURCE_DIR}/include)

target_link_libraries(testextmemgraphgen
#    libextmemgraphgen
    gtest gtest_main
    ${STXXL_LIBRARIES}
)

add_executable(testrandom main_random.cpp)
target_link_libraries(testrandom ${STXXL_LIBRARIES})


add_test(TestsInExtMemGraphGen testextmemgraphgen)
add_test(TestsRandom testrandom)