//
// Created by michael on 27.07.15.
//

#include "TestDistributionCount.h"
#include <DistributionCount.h>
#include <vector>
#include <stxxl/stream>
#include <PowerlawDegreeSequence.h>

TEST_F(TestDistributionCount, testSimpleCounting) {
    std::vector<std::int64_t> degrees({1, 1, 1, 2, 2, 3, 4, 5});
    std::vector<std::pair<int, int> > distribution = {{1, 3}, {2, 2}, {3, 1}, {4, 1}, {5, 1}};

    using InStream = stxxl::stream::iterator2stream<std::vector<std::int64_t>::iterator>;
    InStream input(stxxl::stream::streamify(degrees.begin(), degrees.end()));
    DistributionCount<InStream> distributionCount(input);

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
    stxxl::int64 numNodes = 10*1000*1000;
    PowerlawDegreeSequence sequence(2, 100000-1, -2, numNodes);

    // store degree sequence
    stxxl::vector<stxxl::int64> degrees(numNodes);
    stxxl::stream::materialize(sequence, degrees.begin());

    std::vector<stxxl::int64> degreeHistogram(numNodes);
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

