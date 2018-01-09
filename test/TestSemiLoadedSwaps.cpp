#include <gtest/gtest.h>


class TestSemiLoadedSwaps : public ::testing::Test {};

#ifdef EDGE_SWAP_DEBUG_VECTOR

#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/SemiLoadedEdgeSwapTFP.h>
#include <EdgeStream.h>
#include <defs.h>

TEST_F(TestSemiLoadedSwaps, testOnlySemiLoadedSwaps) {
    EdgeStream edge_list;
    edge_list.push({0, 1});
    edge_list.push({2, 3});
    edge_list.push({4, 5});
    edge_list.push({6, 7});
    edge_list.consume();

    EdgeSwapTFP::SemiLoadedEdgeSwapTFP algo(edge_list, 100, 8, 1llu << 30);

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

    edge_list.rewind();
    ASSERT_EQ(*edge_list, edge_t(0, 7));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(1, 6));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(2, 5));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(3, 4));
}

TEST_F(TestSemiLoadedSwaps, testCombinedSwaps) {
    EdgeStream edge_list;
    edge_list.push({0, 1});
    edge_list.push({2, 3});
    edge_list.push({4, 5});
    edge_list.push({6, 7});
    edge_list.consume();

    EdgeSwapTFP::SemiLoadedEdgeSwapTFP algo(edge_list, 100, 8, 1llu << 30);

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

    edge_list.rewind();
    ASSERT_EQ(*edge_list, edge_t(0, 7));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(1, 6));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(2, 5));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(3, 4));
}

TEST_F(TestSemiLoadedSwaps, testLoadedIsSameAsId) {
    EdgeStream edge_list;
    edge_list.push({0, 1});
    edge_list.push({2, 3});
    edge_list.push({4, 5});
    edge_list.push({6, 7});
    edge_list.consume();

    EdgeSwapTFP::SemiLoadedEdgeSwapTFP algo(edge_list, 100, 8, 1llu << 30);

    std::vector<edge_t> update_edges;
    algo.setUpdatedEdgesCallback([&](EdgeSwapTFP::SemiLoadedEdgeSwapTFP::edge_update_sorter_t &it) {
            while (!it.empty()) {
                update_edges.emplace_back(it->first, it->second);
                ++it;
            }
        });

    algo.push(SemiLoadedSwapDescriptor {edge_t {0, 1}, 0, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {2, 3}, 0, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {2, 3}, 3, true});
    algo.push(SemiLoadedSwapDescriptor {edge_t {4, 5}, 2, true});

    algo.run();

    auto & debug = algo.debugVector();

    ASSERT_EQ(debug.size(), 2u);

    ASSERT_TRUE(debug[0].performed);
    ASSERT_TRUE(debug[1].performed);

    edge_list.rewind();
    ASSERT_EQ(*edge_list, edge_t(0, 7));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(1, 2));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(3, 6));
    ++edge_list;
    ASSERT_EQ(*edge_list, edge_t(4, 5));

    ASSERT_EQ(update_edges.size(), 4u);
    ASSERT_EQ(update_edges[0], edge_t(0, 7));
    ASSERT_EQ(update_edges[1], edge_t(1, 2));
    ASSERT_EQ(update_edges[2], edge_t(3, 6));
    ASSERT_EQ(update_edges[3], edge_t(4, 5));
}
#else
TEST_F(TestSemiLoadedSwaps, warning) {
    EXPECT_TRUE(false) << "TestSemiLoadedSwaps Tests are disabled since built without -DEDGE_SWAP_DEBUG_VECTOR";
}
#endif
