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

#include <list>
#include <random>
#include <Utils/SegmentTree.h>

namespace SegmentTree {
    class TestSegmentTree : public ::testing::Test {};

    class TestSegmentTreeWriter {
    public:
        using item_type = std::pair<uint64_t, uint64_t>;
        using value_type = std::list<item_type>;

    protected:
        value_type _list;

    public:
        TestSegmentTreeWriter() {};

        void push(const std::pair<uint64_t, uint64_t> & pair) {
            _list.push_back( pair );
        }

        value_type & list() {return _list;}
    };

    TEST_F(TestSegmentTree, treeNodeContainer) {
        constexpr unsigned int iterations = 1000000;
        constexpr uint64_t maxv = (1llu << 47) - 1;

        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, maxv);


        for(unsigned int i=0; i<iterations; i++) {
            uint64_t value = distribution(generator);
            TreeNodeType leaf = static_cast<TreeNodeType>(distribution(generator) & 1);

            {
                TreeNode<stxxl::uint48> tn(leaf, value);
                ASSERT_EQ(tn.type(), leaf);
                ASSERT_EQ(tn.data(), value);
            }

            {
                value &= ((1llu<<39)-1);
                TreeNode<stxxl::uint40> tn(leaf, value);
                ASSERT_EQ(tn.type(), leaf);
                ASSERT_EQ(tn.data(), value);
            }
        }
    }

    TEST_F(TestSegmentTree, queryTokenContainer) {
        constexpr unsigned int iterations = 1000000;

        std::default_random_engine generator;
        std::uniform_int_distribution<uint64_t> distribution(0, (1llu<<48) - 1);


        for(unsigned int i=0; i<iterations; i++) {
            uint64_t weight = distribution(generator);
            uint64_t alt_weight = distribution(generator);
            uint64_t id = distribution(generator) & ((1llu<<47)-1);
            TokenType type = static_cast<TokenType>(distribution(generator) & 1);

            {
                Token<stxxl::uint48, stxxl::uint48> token(type, weight, id);
                ASSERT_EQ(token.type(), type);
                ASSERT_EQ(token.weight(), weight);
                ASSERT_EQ(token.id(), id);

                token.set_weight(alt_weight);
                ASSERT_EQ(token.type(), type);
                ASSERT_EQ(token.weight(), alt_weight);
                ASSERT_EQ(token.id(), id);
            }

            {
                weight &= ((1llu<<39)-1);
                alt_weight &= ((1llu<<39)-1);
                Token<stxxl::uint40, stxxl::uint48> token(type, weight, id);
                ASSERT_EQ(token.type(), type);
                ASSERT_EQ(token.weight(), weight);
                ASSERT_EQ(token.id(), id);

                token.set_weight(alt_weight);
                ASSERT_EQ(token.type(), type);
                ASSERT_EQ(token.weight(), alt_weight);
                ASSERT_EQ(token.id(), id);
            }

            {
                id &= ((1llu<<39)-1);
                Token<stxxl::uint40, stxxl::uint40> token(type, weight, id);
                ASSERT_EQ(token.type(), type);
                ASSERT_EQ(token.weight(), weight);
                ASSERT_EQ(token.id(), id);

                token.set_weight(alt_weight);
                ASSERT_EQ(token.type(), type);
                ASSERT_EQ(token.weight(), alt_weight);
                ASSERT_EQ(token.id(), id);
            }
        }
    }


    TEST_F(TestSegmentTree, singleSegmentQueries) {
        const unsigned int num_segments = 1024;

        TestSegmentTreeWriter writer;
        SegmentTree<TestSegmentTreeWriter, 8> segTree(writer);

        unsigned int query_id = 0;
        for (unsigned int i = 0; i < num_segments; i++) {
            segTree.add_segment(i+1, i);

            for(unsigned int j=0; j<=i; j++) {
                ASSERT_TRUE(segTree.query(0, query_id++));
            }

            ASSERT_FALSE(segTree.query(0, query_id++));
        }
        segTree.flush(true);
        auto & list = writer.list();

        ASSERT_EQ(list.size(), query_id - num_segments);

        query_id = 0;
        for (unsigned int i = 0; i < num_segments; i++) {
            for(unsigned int j=0; j<=i; j++) {
                ASSERT_FALSE(list.empty());
                ASSERT_EQ(list.front().first, i) << "query=" << query_id;
                ASSERT_EQ(list.front().second, query_id) << "query=" << query_id;
                
                list.pop_front();
                query_id++;
            }

            query_id++;
        }
    }

    TEST_F(TestSegmentTree, randomMultiSegmentQueries) {
        const unsigned int num_segments = 1 << 20;

        TestSegmentTreeWriter writer;
        SegmentTree<TestSegmentTreeWriter, 8> segTree(writer);

        std::default_random_engine generator;

        // generate segments
        unsigned int weight = 0;
        std::vector<unsigned int> sizes(num_segments);

        {
            std::uniform_int_distribution<unsigned int> distribution(1, 3);
            for (unsigned int i = 0; i < num_segments; i++) {
                auto sweight = distribution(generator);
                sizes[i] = sweight;
                segTree.add_segment(sweight, i);
                weight += sweight;
            }
        }

        segTree.flush(true);
        //segTree.toDot(std::cout);

        // generate random queries over the avaible interval
        {
            std::uniform_int_distribution<unsigned int> distribution(0, std::numeric_limits<unsigned int>::max());
            for(auto w = weight; w; --w) {
                ASSERT_TRUE(segTree.query(distribution(generator) % w, weight - w));
            }
        }

        // process all queries and check whether every query was answered
        segTree.flush(true);
        auto & list = writer.list();
        list.sort([] (const std::pair<uint64_t, uint64_t> & a, const std::pair<uint64_t, uint64_t> & b) {return a.second < b.second;});
        ASSERT_EQ(list.size(), weight);

        // since the request are randomized, we can only check, whether all segments were properly covered:
        // count the number of occurrences of every segment and later check, whether it corresponds to the expected size
        std::vector<unsigned int> hits(num_segments);
        for (unsigned int i = 0; i < weight; i++, list.pop_front()) {
            // count segment
            hits.at(list.front().first)++;

            // check query id
            ASSERT_EQ(list.front().second, i);
        }

        // compare hits to original weight
        for(unsigned int i=0; i < num_segments; i++) {
            ASSERT_EQ(hits[i], sizes[i]) << "i=" << i;
        }
    }

    TEST_F(TestSegmentTree, randomMixedQueries) {
        const unsigned int num_segments = 1 << 20;
        const unsigned int avg_seg_size = 1 << 4;

        TestSegmentTreeWriter writer;
        SegmentTree<TestSegmentTreeWriter, 8> segTree(writer);

        std::default_random_engine generator;
        std::uniform_real_distribution<float>  new_seg_prob;
        std::uniform_int_distribution<unsigned int> seg_dis(1, 2*avg_seg_size);
        std::uniform_int_distribution<unsigned int> rand_dis(0, std::numeric_limits<unsigned int>::max());


        // generate segments
        unsigned int weight = 0;
        std::vector<unsigned int> sizes(num_segments);

        unsigned int seg_id = 0;
        unsigned int qry_id = 0;
        while(seg_id < num_segments || weight) {
            bool add_segment = (!weight || new_seg_prob(generator) < 1.0/avg_seg_size) && (seg_id < num_segments);

            if (add_segment) {
                auto sweight = seg_dis(generator);
                sizes[seg_id] = sweight;
                segTree.add_segment(sweight, seg_id++);
                weight += sweight;

            } else {
                ASSERT_TRUE(segTree.query(rand_dis(generator) % weight--, qry_id++));
            }
        }
        weight = qry_id;

        // process all queries and check whether every query was answered
        segTree.flush(true);
        //segTree.toDot(std::cout);

        auto & list = writer.list();
        list.sort([] (const std::pair<uint64_t, uint64_t> & a, const std::pair<uint64_t, uint64_t> & b) {return a.second < b.second;});

        // since the request are randomized, we can only check, whether all segments were properly covered:
        // count the number of occurrences of every segment and later check, whether it corresponds to the expected size
        std::vector<unsigned int> hits(num_segments);
        for (unsigned int i = 0; i < weight; i++, list.pop_front()) {
            // count segment
            hits.at(list.front().first)++;

            // check query id
            ASSERT_EQ(list.front().second, i);
        }

        // compare hits to original weight
        for(unsigned int i=0; i < num_segments; i++) {
            EXPECT_EQ(hits[i], sizes[i]) << "i=" << i;
        }
    }

};