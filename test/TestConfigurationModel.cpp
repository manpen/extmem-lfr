/**
 * Hung 30.08.16.
 */

#include <gtest/gtest.h>
#include <iomanip>
#include <ConfigurationModel.h>
#include <string>
#include <iostream>
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
	const uint32_t maxm = max.msb();
	const uint32_t maxl = max.lsb();
	const MultiNodeMsg min = mnmc.min_value();
	const uint32_t minm = min.msb();
	const uint32_t minl = min.lsb();
	// 64bit matching max_value?
	ASSERT_EQ(0xFFFFFFFFFFFFFFFFu, crc64(seed, maxm, maxl));
	// 64bit matching min_value?
	ASSERT_EQ(0x0000000000000000u, crc64(seed, minm, minl));

	/*
	 * TestNodeRandomComparator
	 */
	TestNodeRandomComparator tnrc;
	const TestNodeMsg tmax = tnrc.max_value();
	const TestNodeMsg tmin = tnrc.min_value();
	ASSERT_EQ(std::numeric_limits<multinode_t>::max(), tmax.key);
	ASSERT_EQ(std::numeric_limits<multinode_t>::max(), tmax.node);
	ASSERT_EQ(std::numeric_limits<multinode_t>::min(), tmin.key);
	ASSERT_EQ(std::numeric_limits<multinode_t>::min(), tmin.node);
	ASSERT_TRUE(tnrc(tmin, tmax));
	ASSERT_FALSE(tnrc(tmin, tmin));
}

/*
TEST_F(TestConfigurationModel, tHavelHakimi) {
	int x = 13;

    const degree_t min_deg = 2;
    const degree_t max_deg = 100;
    const node_t num_nodes = 1000;
    
    HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
    MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

    StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
    hh_gen.generate();

    HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator> cmhh(hh_gen, 223224, 1000);
    cmhh.run();   

    std::cout << (*cmhh).first << std::endl;
}*/

TEST_F(TestConfigurationModel, reverse) {
	
	std::cout << "Value of 1u: " << 1u << std::endl;
	std::cout << "Value of reverse(1u): " << reverse(1u) << std::endl;

    ASSERT_EQ(reverse(0u), 0u);
    ASSERT_EQ(reverse(1u), pow(2, 63));
    ASSERT_EQ(reverse(2u), pow(2, 62));
    ASSERT_EQ(reverse(3u), pow(2, 63) + pow(2, 62));
}

/*
TEST_F(TestConfigurationModel, tOutput) {
	int x = 4;

    int loopCount = 0;
	int multiEdges_singleCount = 0;
	int multiEdges_multiCount = 0;

	for (uint32_t i = 0; i < 1; ++i) {
   		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
   		MonotonicPowerlawRandomStream<false> degrees(2, 1000, -2.0, 10000);
		StreamPusher<decltype(degrees), decltype(hh_gen)>(degrees, hh_gen);
	    hh_gen.generate();

	    HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator> cm(hh_gen, 223224, 1000);
		cm.run();

		ASSERT_FALSE(cm.empty());

		bool prev_multi = false;

		auto prev_edge = *cm;
		++cm;
		//std::cout << "EDGE<" << std::dec << prev_edge.first << ", " << prev_edge.second << ">" << std::endl;


		if (prev_edge.is_loop()) 
			++loopCount;

		for (; !cm.empty(); ++cm) {
			const auto & edge = *cm;
		
			if (edge.is_loop()) {
				++loopCount;
                if (prev_multi) 
                    ++multiEdges_multiCount;
                        
				prev_edge = edge;
				prev_multi = false;
				continue;
			}
			if (prev_edge == edge) {
				++multiEdges_multiCount;

				if (!prev_multi) {
					++multiEdges_singleCount;
					prev_multi = true;
				}

				prev_edge = edge;

				continue;
			}

            if (prev_multi)
                ++multiEdges_multiCount;

			prev_edge = edge;

			prev_multi = false;
		}

		// outputmessage
		std::cout << "RUN[" << i << "]:" << std::endl;
		std::cout << "         # SELF_LOOPS: " << std::dec << loopCount << std::endl;
		std::cout << "        # MULTI_EDGES: " << multiEdges_singleCount << std::endl;
		std::cout << "# EDGES_IN_MULTI_EDGE: " << multiEdges_multiCount << std::endl;
		std::cout << "=======================" << std::endl;

		ASSERT_TRUE(cm.empty());

		loopCount = 0;
		multiEdges_multiCount = 0;
		multiEdges_singleCount = 0;
	}
}

TEST_F(TestConfigurationModel, tOutputRandom) {
	int x = 4;

    int loopCount = 0;
	int multiEdges_singleCount = 0;
	int multiEdges_multiCount = 0;

	for (uint32_t i = 0; i < 1; ++i) {
   		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
   		MonotonicPowerlawRandomStream<false> degrees(2, 1000, -2.0, 10000);
		StreamPusher<decltype(degrees), decltype(hh_gen)>(degrees, hh_gen);
	    hh_gen.generate();

	    HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeComparator> cm(hh_gen, 1000);
		cm.run();

		ASSERT_FALSE(cm.empty());

		bool prev_multi = false;

		auto prev_edge = *cm;
		++cm;
		//std::cout << "EDGE<" << std::dec << prev_edge.first << ", " << prev_edge.second << ">" << std::endl;


		if (prev_edge.is_loop()) 
			++loopCount;

		for (; !cm.empty(); ++cm) {
			const auto & edge = *cm;
		
			if (edge.is_loop()) {
				++loopCount;
                if (prev_multi) 
                    ++multiEdges_multiCount;
                        
				prev_edge = edge;
				prev_multi = false;
				continue;
			}
			if (prev_edge == edge) {
				++multiEdges_multiCount;

				if (!prev_multi) {
					++multiEdges_singleCount;
					prev_multi = true;
				}

				prev_edge = edge;

				continue;
			}

            if (prev_multi)
                ++multiEdges_multiCount;

			prev_edge = edge;

			prev_multi = false;
		}

		// outputmessage
		std::cout << "RUN[" << i << "]:" << std::endl;
		std::cout << "         # SELF_LOOPS: " << std::dec << loopCount << std::endl;
		std::cout << "        # MULTI_EDGES: " << multiEdges_singleCount << std::endl;
		std::cout << "# EDGES_IN_MULTI_EDGE: " << multiEdges_multiCount << std::endl;
		std::cout << "=======================" << std::endl;

		ASSERT_TRUE(cm.empty());

		loopCount = 0;
		multiEdges_multiCount = 0;
		multiEdges_singleCount = 0;
	}
}*/