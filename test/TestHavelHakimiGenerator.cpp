//
// Created by michael on 27.07.15.
//
#include <gtest/gtest.h>

#include <stxxl/stream>
#include <stxxl/vector>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <DegreeDistributionCheck.h>
#include <HavelHakimi/HavelHakimiGenerator.h>
#include <defs.h>


class TestHavelHakimiGenerator : public ::testing::Test {
};


TEST_F(TestHavelHakimiGenerator, testClique) {
    constexpr int_t numNodes = 10000;
    std::vector<degree_t> degrees(numNodes, 9999);
    using iterator_t = std::vector<degree_t>::iterator;
    using InputStream = stxxl::stream::iterator2stream<iterator_t>;
    InputStream inputStream(degrees.begin(), degrees.end());

    HavelHakimiPrioQueueExt<16 * IntScale::Mi, numNodes> prio_queue;
    stxxl::STACK_GENERATOR<HavelHakimiNodeDegree, stxxl::external, stxxl::grow_shrink>::result stack;
    HavelHakimiGenerator<decltype(prio_queue), decltype(stack)> gen{prio_queue, stack, inputStream};

    using result_t = stxxl::vector<decltype(gen)::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    DegreeDistributionCheck<result_t::const_iterator> check {edges.begin(), edges.end()};
    for (const auto &deg : check.getDegrees()) {
        EXPECT_EQ(9999, deg);
    }

    auto dist = check.getDistribution();

    EXPECT_EQ(9999, dist->value);
    EXPECT_EQ(10000u, dist->count);

    ++dist;
    EXPECT_TRUE(dist.empty());
}

TEST_F(TestHavelHakimiGenerator, testPowerLaw) {
    stxxl::srandom_number32(42);
    constexpr int_t numNodes = 10 * IntScale::Mi;
    MonotonicPowerlawRandomStream<> sequence(2, 100000, -2, numNodes);

    // store degree sequence
    stxxl::vector<degree_t> degrees(numNodes);
    stxxl::stream::materialize(sequence, degrees.begin());


    auto inputStream = stxxl::stream::streamify(degrees.begin(), degrees.end());

    HavelHakimiPrioQueueExt<256 * IntScale::Mi, numNodes> prio_queue;
    stxxl::STACK_GENERATOR<HavelHakimiNodeDegree, stxxl::external, stxxl::grow_shrink>::result stack;
    HavelHakimiGenerator<decltype(prio_queue), decltype(stack)> gen{prio_queue, stack, inputStream};

    using result_t = stxxl::vector<decltype(gen)::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    std::vector<int_t> genDegrees(numNodes);
    for (auto edge : edges) {
        ++genDegrees[edge.first];
        ++genDegrees[edge.second];
    }

    std::vector<uint_t> genDegreeHistogram(numNodes);
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
    constexpr int_t numNodes = 100 * 1000;
    constexpr degree_t degree = 500;
    stxxl::vector<degree_t> degrees(numNodes);
    {
        decltype(degrees)::bufwriter_type writer(degrees);
        for (stxxl::int64 u = 0; u < numNodes; ++u) {
            writer << degree;
        }
    }

    auto inputStream = stxxl::stream::streamify(degrees.begin(), degrees.end());

    HavelHakimiPrioQueueExt<16 * IntScale::Mi, numNodes> prio_queue;
    stxxl::STACK_GENERATOR<HavelHakimiNodeDegree, stxxl::external, stxxl::grow_shrink>::result stack;
    HavelHakimiGenerator<decltype(prio_queue), decltype(stack)> gen{prio_queue, stack, inputStream};

    using result_t = stxxl::vector<decltype(gen)::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    std::vector<int_t> genDegrees(numNodes);
    for (auto edge : edges) {
        ++genDegrees[edge.first];
        ++genDegrees[edge.second];
    }

    for (auto d : degrees) {
        EXPECT_EQ(degree, d) << " entry in degree input vector does not match expected value of " << degree;
    }

    for (node_t u = 0; u < numNodes; ++u) {
        EXPECT_EQ(degree, genDegrees[u]) << " generated degree of node " << u << " does not match expected degree of " << degree;
    }
}

TEST_F(TestHavelHakimiGenerator, testTwoDegrees) {
    constexpr int_t numNodes = 1*100*1000;
    int_t highDegree = 500;
    int_t lowDegree = 2;
    stxxl::vector<degree_t> degrees(numNodes);
    {
        decltype(degrees)::bufwriter_type writer(degrees);
        int_t  u = 0;
        for (; u < numNodes/2; ++u) {
            writer << lowDegree;
        }
        for (; u < numNodes; ++u) {
            writer << highDegree;
        }

    }

    auto inputStream = stxxl::stream::streamify(degrees.begin(), degrees.end());
    HavelHakimiPrioQueueExt<16 * IntScale::Mi, numNodes> prio_queue;
    stxxl::STACK_GENERATOR<HavelHakimiNodeDegree, stxxl::external, stxxl::grow_shrink>::result stack;
    HavelHakimiGenerator<decltype(prio_queue), decltype(stack)> gen{prio_queue, stack, inputStream};

    using result_t = stxxl::vector<decltype(gen)::value_type>;
    result_t edges(gen.maxEdges());

    auto lastElement = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(lastElement - edges.begin());

    std::vector<int_t> genDegrees(numNodes);
    for (auto edge : edges) {
        ++genDegrees[edge.first];
        ++genDegrees[edge.second];
    }

    int_t numLowDegNodes = 0, numHighDegNodes = 0;

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
