/**
 * @file
 * @brief Test cases for RandomIntervalTree
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
#include "RandomIntervalTree.h"

class TestRandomIntervalTree : public ::testing::Test {};

TEST_F(TestRandomIntervalTree, integration) {
    using T = uint64_t;
    const unsigned int max_size = 1000;
    const unsigned int iterations = 200;

    std::default_random_engine re(1);
    std::uniform_int_distribution<int> dist(0,max_size);
    std::uniform_int_distribution<int> sdist(0,13);

    unsigned int iteration = 0;
    while(iteration < iterations) {
        size_t size = dist(re);
        if (!size) continue;
        iteration++;

        // prepare data
        std::vector<unsigned int> leafes(size);
        std::vector<T> ps(size+1);
        std::generate(leafes.begin(), leafes.end(), [&] {return sdist(re);});
        std::partial_sum(leafes.begin(), leafes.end(), ps.begin()+1);
        ps[0] = 0;

        RandomIntervalTree<T> tree(leafes);

        while(ps.back()) {
            ASSERT_EQ(ps.back(), tree.total_weight());
            std::uniform_int_distribution<T> tdist(0, tree.total_weight() - 1);
            const auto r = tdist(re);
            //std::cout << r << std::endl;
            //tree.dump();

            //for(const auto & x : leafes) std::cout << x << " ";
            //std::cout << std::endl << std::endl;
            //for(const auto & x : ps) std::cout << x << " ";
            //std::cout << std::endl;

            auto leaf = tree.getLeaf(r);
            ASSERT_GT(leafes[leaf], 0u);
            ASSERT_LT(leaf, leafes.size());
            ASSERT_LE(ps[leaf], r) << ps[leaf] << " " << r << " " << ps[leaf+1];
            ASSERT_GT(ps[leaf+1], r)  << ps[leaf] << " " << r << " " << ps[leaf+1];;

            tree.decreaseLeaf(leaf);
            ASSERT_EQ(ps.back()-1, tree.total_weight());

            leafes[leaf]--;
            for(auto i=leaf+1; i<ps.size(); i++)
                ps[i]--;
        }
   }
}