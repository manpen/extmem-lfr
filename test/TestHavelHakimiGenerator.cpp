//
// Created by michael on 27.07.15.
//

#include <stxxl/stream>
#include <stxxl/vector>
#include "TestHavelHakimiGenerator.h"
#include <PowerlawDegreeSequence.h>
#include <DegreeDistributionCheck.h>
#include <HavelHakimiGenerator.h>


TEST_F(TestHavelHakimiGenerator, testClique) {
    std::vector<std::int64_t> degrees(10000, 9999);
    using iterator_t = std::vector<std::int64_t>::iterator;
    using InputStream = stxxl::stream::iterator2stream<iterator_t>;
    InputStream inputStream(degrees.begin(), degrees.end());
    HavelHakimiGenerator gen(inputStream);

    using result_t = stxxl::vector<HavelHakimiGenerator::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    DegreeDistributionCheck<result_t::const_iterator> check {edges.begin(), edges.end()};
    for (const auto &deg : check.getDegrees()) {
        EXPECT_EQ(9999, deg);
    }

    auto dist = check.getDistribution();

    EXPECT_EQ(9999, dist->value);
    EXPECT_EQ(10000, dist->count);

    ++dist;
    EXPECT_TRUE(dist.empty());
}

TEST_F(TestHavelHakimiGenerator, testPowerLaw) {
    stxxl::set_seed(42);
    stxxl::int64 numNodes = 100*1000;
    PowerlawDegreeSequence sequence(2, 100000-1, -2, numNodes);

    // store degree sequence
    stxxl::vector<stxxl::int64> degrees(numNodes);
    stxxl::stream::materialize(sequence, degrees.begin());


    auto inputStream = stxxl::stream::streamify(degrees.begin(), degrees.end());
    HavelHakimiGenerator gen(inputStream);

    using result_t = stxxl::vector<HavelHakimiGenerator::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    std::vector<stxxl::int64> genDegrees(numNodes);
    for (auto edge : edges) {
        ++genDegrees[edge.first];
        ++genDegrees[edge.second];
    }

    std::vector<stxxl::int64> genDegreeHistogram(numNodes);
    for (auto deg : genDegrees) {
        ++genDegreeHistogram[deg];
    }

    auto distriInput = stxxl::stream::streamify(degrees.begin(), degrees.end());
    DistributionCount<decltype(distriInput)> origDistri(distriInput);
    // EVIL! auto genDistri = DegreeDistributionCheck<result_t::iterator>(edges.begin(), edges.end()).getDistribution();
    DegreeDistributionCheck<result_t::iterator> genCheck(edges.begin(), edges.end());
    auto genDistri = genCheck.getDistribution();

    while (!origDistri.empty()) {
        ASSERT_FALSE(genDistri.empty());
        EXPECT_EQ(genDegreeHistogram[genDistri->value], genDistri->count) << " manually calculated count of degree " << genDistri->value << " does not match degreedist.";
        EXPECT_EQ(origDistri->count, genDegreeHistogram[origDistri->value]) << " generated count of degree " << origDistri->value << " does not match wanted count.";
        //EXPECT_EQ(origDistri->value, genDistri->value);
        //EXPECT_EQ(origDistri->count, genDistri->count);
        ++origDistri;
        ++genDistri;
    }
}

TEST_F(TestHavelHakimiGenerator, testFixedDegree) {
    stxxl::set_seed(42);
    stxxl::int64 numNodes = 100 * 1000;
    stxxl::int64 degree = 500;
    stxxl::vector<stxxl::int64> degrees(numNodes);
    {
        decltype(degrees)::bufwriter_type writer(degrees);
        for (stxxl::int64 u = 0; u < numNodes; ++u) {
            writer << degree;
        }
    }

    auto inputStream = stxxl::stream::streamify(degrees.begin(), degrees.end());
    HavelHakimiGenerator gen(inputStream);

    using result_t = stxxl::vector<HavelHakimiGenerator::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    std::vector<stxxl::int64> genDegrees(numNodes);
    for (auto edge : edges) {
        ++genDegrees[edge.first];
        ++genDegrees[edge.second];
    }

    for (auto d : degrees) {
        EXPECT_EQ(degree, d) << " entry in degree input vector does not match expected value of " << degree;
    }

    for (stxxl::int64 u = 0; u < numNodes; ++u) {
        EXPECT_EQ(degree, genDegrees[u]) << " generated degree of node " << u << " does not match expected degree of " << degree;
    }
}

TEST_F(TestHavelHakimiGenerator, testTwoDegrees) {
    stxxl::set_seed(42);
    stxxl::int64 numNodes = 1*100*1000;
    stxxl::int64 highDegree = 500;
    stxxl::int64 lowDegree = 2;
    stxxl::vector<stxxl::int64> degrees(numNodes);
    {
        decltype(degrees)::bufwriter_type writer(degrees);
        stxxl::int64  u = 0;
        for (; u < numNodes/2; ++u) {
            writer << lowDegree;
        }
        for (; u < numNodes; ++u) {
            writer << highDegree;
        }

    }

    auto inputStream = stxxl::stream::streamify(degrees.begin(), degrees.end());
    HavelHakimiGenerator gen(inputStream);

    using result_t = stxxl::vector<HavelHakimiGenerator::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    std::vector<stxxl::int64> genDegrees(numNodes);
    for (auto edge : edges) {
        ++genDegrees[edge.first];
        ++genDegrees[edge.second];
    }

    stxxl::int64 numLowDegNodes = 0, numHighDegNodes = 0;

    for (auto d : genDegrees) {
        if (d == lowDegree) {
            ++numLowDegNodes;
        } else if (d == highDegree) {
            ++numHighDegNodes;
        } else {
            FAIL() << " node has unexpected degree " << d;
        }
    }

    EXPECT_EQ(numNodes/2, numLowDegNodes);
    EXPECT_EQ(numNodes/2, numHighDegNodes);
}
