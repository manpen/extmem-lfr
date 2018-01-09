/**
 * Hung 30.08.16.
 */

#include <gtest/gtest.h>
#include <ConfigurationModel/ConfigurationModelRandom.h>
#include <Utils/StreamPusher.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>

class TestConfigurationModel : public ::testing::Test {
};

TEST_F(TestConfigurationModel, hhEdge) {
	const degree_t min_deg = 5;
	const degree_t max_deg = 20;
	const node_t num_nodes = 1000;
    const degree_t threshold = max_deg / 5;
    
	HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, threshold);
	MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

	StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
	hh_gen.generate();

	for (; !hh_gen.empty(); ++hh_gen) {
		std::cout << "First Node:" << (*hh_gen).first << " with ID " << hh_gen.edge_ids().first << std::endl;
		std::cout << "Secnd Node:" << (*hh_gen).second << " with ID " << hh_gen.edge_ids().second << std::endl;
	}
}

TEST_F(TestConfigurationModel, crccomps) {
	/*
	 * CRC
	 */
	const uint32_t seed = 223224;
	// 32bit matching max_value?
	ASSERT_EQ(0xFFFFFFFFu, _mm_crc32_u32(seed, seed ^ MAX_CRCFORWARD));
	ASSERT_EQ(0xFFFFFFFFu, _mm_crc32_u32(0xFFFFFFFFu, MAX_LSB));
	// 32bit matching max_value?
	ASSERT_EQ(0x00000000u, _mm_crc32_u32(seed, seed));
	ASSERT_EQ(0x00000000u, _mm_crc32_u32(0x00000000u, MIN_LSB));

	/*
	 * MultiNodeComparator
	 */
	MultiNodeMsgComparator mnmc(seed);
	const MultiNodeMsg max = mnmc.max_value();
	const MultiNodeMsg min = mnmc.min_value();
	// 64bit matching max_value?
	ASSERT_EQ(0xFFFFFFFFFFFFFFFFu, crc64(seed, max.val()));
	// 64bit matching min_value?
	ASSERT_EQ(0x0000000000000000u, crc64(seed, min.val()));

	/*
	 * TestNodeRandomComparator
	 */
	TestNodeRandomComparator tnrc;
	const TestNodeMsg tmax = tnrc.max_value();
	const TestNodeMsg tmin = tnrc.min_value();
	ASSERT_EQ(std::numeric_limits<node_t>::max(), tmax.key);
	ASSERT_EQ(std::numeric_limits<node_t>::max(), tmax.node);
	ASSERT_EQ(std::numeric_limits<node_t>::min(), tmin.key);
	ASSERT_EQ(std::numeric_limits<node_t>::min(), tmin.node);
	ASSERT_TRUE(tnrc(tmin, tmax));
	ASSERT_FALSE(tnrc(tmin, tmin));
}

TEST_F(TestConfigurationModel, tHavelHakimi) {

    const degree_t min_deg = 10;
    const degree_t max_deg = 10000;
    const node_t num_nodes = 100000;
    const degree_t threshold = max_deg / 200;
    
    HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
    MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

    StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
    hh_gen.generate();

    ConfigurationModelCRC<HavelHakimiIMGenerator> cmhh(hh_gen, 223224, num_nodes, threshold,
                                                                hh_gen.maxDegree(), hh_gen.nodesAboveThreshold());
    cmhh.run();   

}