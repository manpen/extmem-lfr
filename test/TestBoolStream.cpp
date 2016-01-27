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
#include "BoolStream.h"

class TestBoolStream : public ::testing::Test {};

TEST_F(TestBoolStream, smallFillAndConsume) {
    for(uint i=0; i < 2; i++) {
        bool v = i;
        BoolStream bs;
        bs.push(v);
        bs.push(!v);

        bs.consume();
        ASSERT_FALSE(bs.empty());
        ASSERT_EQ(*bs, v);
        ++bs;
        ASSERT_FALSE(bs.empty());
        ASSERT_EQ(*bs, !v);
        ++bs;
        ASSERT_TRUE(bs.empty());
    }
}

TEST_F(TestBoolStream, basicFillAndConsume) {
    constexpr uint max_fills = 1000;

    for(uint i=0; i < 2; i++) {
        bool v = i;
        BoolStream bs;

        for(uint fill_size = 0; fill_size < max_fills; ++fill_size) {
            bs.push(v);
            for(uint j=fill_size; j; --j)
                bs.push(!v);
        }

        bs.consume();

        uint64_t idx = 0;

        for(uint fill_size = 0; fill_size < max_fills; ++fill_size) {
            ASSERT_FALSE(bs.empty()) << idx;
            ASSERT_EQ(*bs, v) << idx;
            ++bs; ++idx;
            for(uint j=fill_size; j; --j) {
                ASSERT_FALSE(bs.empty()) << idx;
                ASSERT_EQ(*bs, !v) << idx;
                ++bs; ++idx;
            }
        }

        ASSERT_TRUE(bs.empty());
    }
}

TEST_F(TestBoolStream, randomFillAndConsume) {
    BoolStream bs;
    stxxl::random_number32 rand;
    for(unsigned int round = 0; round < 100; round++) {
        unsigned int write_items = rand(1 << 20);
        unsigned int read_items = rand(write_items);

        std::vector<bool> reference;
        reference.reserve(write_items);

        while(write_items) {
            uint32_t w = rand(~uint32_t(0));
            for (unsigned int word = 32; write_items && word; --write_items, --word) {
                bool v = w & 1;
                w >>= 1;
                reference.push_back(v);
                bs.push(v);
            }
        }

        bs.consume();

        for(unsigned int i=0; i<read_items; i++) {
            ASSERT_FALSE(bs.empty()) << i;
            ASSERT_EQ(*bs, reference[i]) << i;
            ++bs;
        }

        bs.clear();
    }
}
