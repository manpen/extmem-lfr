#include <gtest/gtest.h>


class TestSemiLoadedSwaps : public ::testing::Test {};

#ifdef EDGE_SWAP_DEBUG_VECTOR

#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <defs.h>

TEST_F(TestSemiLoadedSwaps, testOnlySemiLoadedSwaps) {
    stxxl::vector<edge_t> edge_list;
    edge_list.push_back({0, 1});
    edge_list.push_back({2, 3});
    edge_list.push_back({4, 5});
    edge_list.push_back({6, 7});

    EdgeSwapInternalSwaps algo(edge_list);

    algo.push(SemiLoadedSwapDescriptor {edge_t {0, 1}, 1, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {4, 5}, 3, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {10, 12}, 3, true}); // cannot be loaded, should be ignored
    algo.push(SemiLoadedSwapDescriptor {edge_t {0, 1}, 2, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {2, 3}, 3, true});

    algo.run();

    auto & debug = algo.debugVector();

    ASSERT_EQ(debug.size(), 4U);

    ASSERT_TRUE(debug[0].performed);
    ASSERT_TRUE(debug[1].performed);
    ASSERT_TRUE(debug[2].performed);
    ASSERT_TRUE(debug[3].performed);

    ASSERT_EQ(edge_list[0], edge_t(0, 7));
    ASSERT_EQ(edge_list[1], edge_t(1, 6));
    ASSERT_EQ(edge_list[2], edge_t(2, 5));
    ASSERT_EQ(edge_list[3], edge_t(3, 4));
}

TEST_F(TestSemiLoadedSwaps, testCombinedSwaps) {
    stxxl::vector<edge_t> edge_list;
    edge_list.push_back({0, 1});
    edge_list.push_back({2, 3});
    edge_list.push_back({4, 5});
    edge_list.push_back({6, 7});

    EdgeSwapInternalSwaps algo(edge_list);

    algo.push(SwapDescriptor {0, 1, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {4, 5}, 3, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {0, 1}, 2, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {2, 3}, 3, true});

    algo.run();

    auto & debug = algo.debugVector();

    ASSERT_TRUE(debug[0].performed);
    ASSERT_TRUE(debug[1].performed);
    ASSERT_TRUE(debug[2].performed);
    ASSERT_TRUE(debug[3].performed);

    ASSERT_EQ(edge_list[0], edge_t(0, 7));
    ASSERT_EQ(edge_list[1], edge_t(1, 6));
    ASSERT_EQ(edge_list[2], edge_t(2, 5));
    ASSERT_EQ(edge_list[3], edge_t(3, 4));
}
#else
TEST_F(TestSemiLoadedSwaps, warning) {
    EXPECT_TRUE(false) << "TestSemiLoadedSwaps Tests are disabled since built without -DEDGE_SWAP_DEBUG_VECTOR";
}
#endif