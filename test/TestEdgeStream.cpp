/**
 * @file
 * @brief Test cases for TupleHelpers
 * @author Manuel Penschuck
 * @copyright
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * @copyright
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gtest/gtest.h>

#include <random>
#include "EdgeStream.h"

class TestEdgeStream : public ::testing::Test { };

TEST_F(TestEdgeStream, fillReadRereadReset) {
    EdgeStream es, es1;

    constexpr node_t nodes = IntScale::M;
    constexpr unsigned int iterations = 20;

    stxxl::random_number32 rand;

    std::vector<edge_t> reference, reference1;

    auto check_against_ref= [] (EdgeStream &es, const std::vector<edge_t> & ref) {
        for(const auto & edge : ref) {
            ASSERT_FALSE(es.empty());
            ASSERT_EQ(*es, edge);
            ++es;
        }
        ASSERT_TRUE(es.empty());
    };

    for(unsigned int iter=0; iter < iterations; iter++) {
        if (iter) {
            es.clear();
        }

        reference.clear();
        reference.reserve(nodes*2);
        for(node_t u = 0; u < nodes; u++) {
            // slightly large interval, s.t. we get nodes w/o edges
            node_t v = rand(nodes*3/2);
            while(v < nodes) {
                const edge_t edge(u,v);
                es.push(edge);
                reference.push_back(edge);
                // smaller interval so we have a change to see multi-edges
                v += rand(nodes/2);
            }
        }

        es.consume();
        check_against_ref(es, reference);

        es.rewind();
        check_against_ref(es, reference);

        std::swap(es, es1);
        std::swap(reference, reference1);

        es.rewind();
        check_against_ref(es, reference);
    }
}
