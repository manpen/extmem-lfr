#include <gtest/gtest.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <stxxl/stream>

class TestHavelHakimiIMGenerator : public ::testing::Test {
protected:
    void _check_hh(const std::vector<degree_t> & sequence, HavelHakimiIMGenerator::PushDirection dir, node_t initial_id) {
        degree_t total_degree = 0;
        HavelHakimiIMGenerator hh;

        for (auto &d : sequence) {
            total_degree += d;
            hh.push(d);
        }

        // total degree must be even
        ASSERT_EQ(total_degree & 1, 0);

        // stream must be only available after completely pushed
        ASSERT_TRUE(hh.empty());
        hh.generate(dir);
        ASSERT_FALSE(hh.empty());

        // count degrees
        std::vector<unsigned int> degrees(sequence.size());
        degree_t no_edges = 0;
        for(; !hh.empty(); ++hh, ++no_edges) {
            const edge_t & edge = *hh;

            // no self-loops
            ASSERT_NE(edge.first, edge.second);

            degrees.at(edge.first - initial_id)++;
            degrees.at(edge.second - initial_id)++;
        }

        // compare
        for(unsigned int i=0; i<sequence.size(); i++) {
            EXPECT_EQ(degrees[i], sequence[i]) << "edge: " << i;
            ASSERT_LE(degrees[i], sequence[i]) << "edge: " << i;
        }
    }
};


TEST_F(TestHavelHakimiIMGenerator, clique) {
    node_t nodes = 1000;
    std::vector<degree_t> seq(nodes, static_cast<degree_t>(nodes-1));
    this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 0);
    this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 1234);

    this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0);
    this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 1234);
}