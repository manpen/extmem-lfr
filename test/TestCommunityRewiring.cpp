#include <gtest/gtest.h>
#include <LFR/CommunityEdgeRewiringSwaps.h>

class TestCommunityRewiring : public ::testing::Test { };

TEST_F(TestCommunityRewiring, testTwoCommunities) {
	using edge_community_vector_t = stxxl::vector<LFR::CommunityEdge>;

	edge_community_vector_t edges;
	edges.push_back(LFR::CommunityEdge(0, edge_t(0, 1)));
	edges.push_back(LFR::CommunityEdge(1, edge_t(0, 1)));
	edges.push_back(LFR::CommunityEdge(0, edge_t(2, 3)));
	edges.push_back(LFR::CommunityEdge(1, edge_t(2, 3)));
	edges.push_back(LFR::CommunityEdge(1, edge_t(3, 4)));
	edges.push_back(LFR::CommunityEdge(0, edge_t(5, 6)));

	CommunityEdgeRewiringSwaps rewiring(edges, 200, 0);
	rewiring.run();

	LFR::CommunityEdge prev = LFR::CommunityEdge(0, edge_t::invalid());
	for (auto it = edges.cbegin(); it != edges.cend(); ++it) {
		EXPECT_NE(prev.edge, it->edge);
	}
};
