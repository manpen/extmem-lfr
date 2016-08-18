#include <gtest/gtest.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>

#include <stxxl/stream>
#include <stxxl/bits/common/rand.h>

class TestHavelHakimiIMGenerator : public ::testing::Test {
protected:
    void _check_hh(const std::vector<degree_t> & sequence, HavelHakimiIMGenerator::PushDirection dir, node_t initial_id, bool debug = false, bool strict = false) {
        degree_t total_degree = 0;
        HavelHakimiIMGenerator hh(dir, initial_id);

        for (auto &d : sequence) {
            total_degree += d;
            hh.push(d);
        }

        // stream must be only available after completely pushed
        ASSERT_TRUE(hh.empty());
        hh.generate();
        ASSERT_FALSE(hh.empty());

        // count degrees
        std::vector<degree_t> degrees(sequence.size());
        degree_t no_edges = 0;
        edge_t previous_edge = {-1, -1};
        for(; !hh.empty(); ++hh, ++no_edges) {
            const edge_t & edge = *hh;
            if (debug)
                std::cout << "Got edge " << edge << std::endl;

            // no self-loops and ordered entries
            ASSERT_LT(edge.first, edge.second);

            // globally ordered
            ASSERT_LT(previous_edge, edge);

            ASSERT_GE(edge.first, initial_id);
            ASSERT_LE(edge.first, static_cast<node_t>(initial_id+sequence.size()-1));
            ASSERT_GE(edge.second, initial_id);
            ASSERT_LE(edge.second, static_cast<node_t>(initial_id+sequence.size()-1));

            degrees.at(edge.first - initial_id)++;
            degrees.at(edge.second - initial_id)++;

            previous_edge = edge;
        }

        // compare
        if (!strict) {
            // if errors are allowed, the degrees might not be ordered
            //std::sort(degrees.begin(), degrees.end());
        }

        for(unsigned int i=0; i<sequence.size(); i++) {
            auto required = sequence[dir == HavelHakimiIMGenerator::PushDirection::IncreasingDegree
                                       ? sequence.size()-i-1 : i];

            if (debug)
                std::cout << "Node " << i << " requested " << sequence[i] << " got " << required << std::endl;

            if (strict)
                ASSERT_EQ(degrees[i], required) << "node: " << i;

            ASSERT_LE(degrees[i], required) << "node: " << i;
        }
    }

    std::vector<degree_t> _barabasi(node_t nodes, degree_t edges_per_node) {
        unsigned int no_edges = 3 + nodes*edges_per_node;

        std::vector<node_t> graph;
        graph.reserve(2*no_edges);

        stxxl::random_number32 rand;

        // generate barabasi albert pref attachment graph with an
        // initial (edges_per_node)-start
        for(degree_t i=0; i<edges_per_node; i++) {
            graph.push_back(i+1);
            graph.push_back(0);
        }

        // preferential attachment phase
        std::set<node_t> previous_neighbors;
        for(node_t u=0; u < nodes; u++) {
            previous_neighbors.clear();
            for(degree_t e=0; e < edges_per_node; e++) {
                node_t v = u + edges_per_node + 1;
                graph.push_back(v);

                // select neighbor and ensure graph remains simple
                node_t n = v;
                while(n == v || previous_neighbors.count(n)) {
                    n = graph.at(rand(graph.size()));
                }
                previous_neighbors.insert(n);

                graph.push_back(n);
            }
        }

        // compute degrees
        std::vector<degree_t> degrees(nodes + 4);
        for(auto & u : graph) {
            degrees.at(u)++;
        }

        return degrees;
    }
};


TEST_F(TestHavelHakimiIMGenerator, clique) {
    std::vector<unsigned int> sizes({3,4,5,6,1000,1001});
    for(auto nodes : sizes) {
        std::vector<degree_t> seq(nodes, static_cast<degree_t>(nodes - 1));
        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 0, false, true);
        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 1234, false, true);

        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, false, true);
        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 1234, false, true);
    }
}

TEST_F(TestHavelHakimiIMGenerator, circle) {
    std::vector<unsigned int> sizes({3,4,5,6,1000,1001});
    for(auto nodes : sizes) {
        std::vector<degree_t> seq(nodes, static_cast<degree_t>(2));
        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 0, false, true);
        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 1234, false, true);

        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, false, true);
        this->_check_hh(seq, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 1234, false, true);
    }
}


TEST_F(TestHavelHakimiIMGenerator, barabasiAlbert) {
    unsigned int iterations = 100;
    unsigned int nodes = 1000;
    unsigned int edges_per_node = 3;

    for(unsigned int iter=0; iter < iterations; iter++) {
        auto degrees = this->_barabasi(nodes, edges_per_node);

        std::sort(degrees.begin(), degrees.end(), std::greater<degree_t>());
        this->_check_hh(degrees, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, false, true);


        std::sort(degrees.begin(), degrees.end());
        this->_check_hh(degrees, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 0, false, true);
    }
}

TEST_F(TestHavelHakimiIMGenerator, barabasiAlbertDefect) {
    unsigned int iterations = 100;
    unsigned int nodes = 1000;
    unsigned int edges_per_node = 3;

    for(unsigned int iter=0; iter < iterations; iter++) {
        auto degrees = this->_barabasi(nodes, edges_per_node);
        stxxl::random_number32 rand;

        for(unsigned int d=rand(10); d; d--)
            degrees[rand(degrees.size())] += rand(nodes/2);

        std::sort(degrees.begin(), degrees.end(), std::greater<degree_t>());
        this->_check_hh(degrees, HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, false);


        std::sort(degrees.begin(), degrees.end());
        this->_check_hh(degrees, HavelHakimiIMGenerator::PushDirection::IncreasingDegree, 0, false);
    }
}