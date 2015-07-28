//
// Created by michael on 27.07.15.
//

#include "TestDistributionCount.h"
#include <DistributionCount.h>
#include <stxxl/stream>
#include <PowerlawDegreeSequence.h>
#include <defs.h>

TEST_F(TestDistributionCount, testSimpleCounting) {
    std::vector<int_t> degrees({1, 1, 1, 2, 2, 3, 4, 5});
    std::vector<std::pair<int, int> > distribution = {{1, 3}, {2, 2}, {3, 1}, {4, 1}, {5, 1}};

    auto input = stxxl::stream::streamify(degrees.begin(), degrees.end());
    DistributionCount<decltype(input)> distributionCount(input);

    for (auto dist : distribution) {
        ASSERT_FALSE(distributionCount.empty());
        EXPECT_EQ(dist.first, distributionCount->value);
        EXPECT_EQ(dist.second, distributionCount->count);
        ++distributionCount;
    }

    EXPECT_TRUE(distributionCount.empty());
}

TEST_F(TestDistributionCount, testPowerlawCounting) {
    stxxl::set_seed(42);
    constexpr int_t numNodes = 10*1000*1000;
    PowerlawDegreeSequence sequence(2, 100000-1, -2, numNodes);

    // store degree sequence
    stxxl::vector<int_t> degrees(numNodes);
    stxxl::stream::materialize(sequence, degrees.begin());

    std::vector<int_t> degreeHistogram(numNodes);
    for (auto deg : degrees) {
        ++degreeHistogram[deg];
    }

    auto distriInput = stxxl::stream::streamify(degrees.begin(), degrees.end());
    DistributionCount<decltype(distriInput)> origDistri(distriInput);

    while (!origDistri.empty()) {
        EXPECT_EQ(degreeHistogram[origDistri->value], origDistri->count) << " orig distribution of degree " << origDistri->value << " is wrong.";
        ++origDistri;
    }
}

