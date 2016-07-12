#include <gtest/gtest.h>
#include <HavelHakimi/HavelHakimiRIMGenerator.h>

#include <stxxl/stream>
#include <stxxl/bits/common/rand.h>

class TestHavelHakimiRIMGenerator : public ::testing::Test {
protected:
    template <typename ValueType>
    struct PushToVector {
       std::vector<ValueType> vector;
       void push(const ValueType& v) {
          vector.push_back(v);
       }
    };

    void _check_hh(const std::vector<degree_t> & sequence, node_t initial_id, bool debug = false, bool strict = false) {
        edgeid_t total_degree = 0;
        PushToVector<edge_t> edges;
        HavelHakimiRIMGenerator<PushToVector<edge_t>> hh(edges, initial_id);

        for (auto &d : sequence) {
            total_degree += d;
            hh.push(d);
        }

        // stream must be only available after completely pushed
        hh.generate();

        // count degrees
        std::vector<degree_t> degrees(sequence.size(), 0);
        edge_t previous_edge = {-1, -1};
        for(const auto & edge : edges.vector) {
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
            degree_t required = sequence[i];

            if (debug)
                std::cout << "Node " << i << " requested " << sequence[i] << " got " << required << std::endl;

            if (strict)
                ASSERT_EQ(degrees[i], required) << "node: " << i;

            ASSERT_LE(degrees[i], required) << "node: " << i;
        }

        ASSERT_GT(static_cast<edgeid_t>(edges.vector.size()), total_degree / 3);
        ASSERT_LE(static_cast<edgeid_t>(edges.vector.size()), total_degree / 2);
    }

    std::vector<degree_t> _barabasi(node_t nodes, degree_t edges_per_node) {
        edgeid_t no_edges = 3 + nodes*edges_per_node;

        std::vector<node_t> graph;
        graph.reserve(2*no_edges);

        stxxl::random_number32 rand;

        // generate barabasi albert pref attachment graph with an
        // initial (edges_per_node)-start
        for(edgeid_t i=0; i<edges_per_node; i++) {
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


TEST_F(TestHavelHakimiRIMGenerator, clique) {
    std::vector<unsigned int> sizes({3,4,5,6,1000,1001});
    for(auto nodes : sizes) {
        std::vector<degree_t> seq(nodes, static_cast<degree_t>(nodes - 1));
        this->_check_hh(seq, 0, false, true);
        this->_check_hh(seq, 1234, false, true);

    }
}


TEST_F(TestHavelHakimiRIMGenerator, circle) {
    std::vector<unsigned int> sizes({3,4,5,6,1000,1001});
    for(auto nodes : sizes) {
        std::vector<degree_t> seq(nodes, static_cast<degree_t>(2));
        this->_check_hh(seq, 0, false, true);
        this->_check_hh(seq, 1234, false, true);
    }
}

TEST_F(TestHavelHakimiRIMGenerator, worstCase) {
    std::vector<node_t> sizes({2,4,6,8,100,1000});
    for(auto nodes : sizes) {
        std::vector<degree_t> seq(nodes);

        for(auto i=0; i < nodes; i++)
           seq[i] = 1 + i/2;


        this->_check_hh(seq, 0, false, true);
        this->_check_hh(seq, 1234, false, true);
    }
}

TEST_F(TestHavelHakimiRIMGenerator, barabasiAlbert) {
    unsigned int iterations = 100;
    unsigned int nodes = 1000;
    unsigned int edges_per_node = 3;

    for(unsigned int iter=0; iter < iterations; iter++) {
        auto degrees = this->_barabasi(nodes, edges_per_node);

        std::sort(degrees.begin(), degrees.end());
        this->_check_hh(degrees, 0, false, true);
    }
}

TEST_F(TestHavelHakimiRIMGenerator, barabasiAlbertDefect) {
    unsigned int iterations = 100;
    unsigned int nodes = 1000;
    unsigned int edges_per_node = 3;

    for(unsigned int iter=0; iter < iterations; iter++) {
        auto degrees = this->_barabasi(nodes, edges_per_node);
        stxxl::random_number32 rand;

        for(unsigned int d=rand(10); d; d--)
            degrees[rand(degrees.size())] += rand(nodes/2);

        std::sort(degrees.begin(), degrees.end());
        this->_check_hh(degrees, 0, false);
    }
}
