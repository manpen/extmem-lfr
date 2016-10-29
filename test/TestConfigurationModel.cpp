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

#define NDEBUG
#define CHANGECRC

class TestConfigurationModel : public ::testing::Test {
};

TEST_F(TestConfigurationModel, crc) {
	int x = 2;
	// reverse functions properly?
	ASSERT_EQ(0xFFFFFFFFFFFFFFFFu, reverse(0xFFFFFFFFFFFFFFFFu));
	ASSERT_EQ(0x0000000000000000u, reverse(0x0000000000000000u));

	const uint32_t seed = 223224;
	// 32bit matching max_value?
	ASSERT_EQ(0xFFFFFFFFu, _mm_crc32_u32(seed, seed ^ MAX_CRCFORWARD));
	ASSERT_EQ(0xFFFFFFFFu, _mm_crc32_u32(0xFFFFFFFFu, MAX_LSB));
	// 32bit matching max_value?
	ASSERT_EQ(0x00000000u, _mm_crc32_u32(seed, seed));
	ASSERT_EQ(0x00000000u, _mm_crc32_u32(0x00000000u, MIN_LSB));

	MultiNodeMsgComparator mnmc(seed);
	const uint64_t max = mnmc.max_value();
	const uint32_t maxm = static_cast<uint32_t>(max >> 32);
	const uint32_t maxl = static_cast<uint32_t>(max);
	const uint64_t min = mnmc.min_value();
	const uint32_t minm = static_cast<uint32_t>(min >> 32);
	const uint32_t minl = static_cast<uint32_t>(min);
	// 64bit matching max_value?
	ASSERT_EQ(0xFFFFFFFFFFFFFFFFu, crc64(seed, maxm, maxl));
	// 64bit matching min_value?
	ASSERT_EQ(0x0000000000000000u, crc64(seed, minm, minl));
}

TEST_F(TestConfigurationModel, tHavelHakimi) {
	int x = 11;

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
}

TEST_F(TestConfigurationModel, reverse) {
    ASSERT_EQ(reverse(static_cast<uint64_t>(0)), static_cast<uint64_t>(0));
    ASSERT_EQ(reverse(static_cast<uint64_t>(1)), static_cast<uint64_t>(1) << 63);
    ASSERT_EQ(reverse(static_cast<uint64_t>(2)), static_cast<uint64_t>(1) << 62);
    ASSERT_EQ(reverse(static_cast<uint64_t>(3)), static_cast<uint64_t>(3) << 62);
}

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

	    HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator> cm(hh_gen, 1000);
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

TEST_F(TestConfigurationModel, outputAnalysis) {
    std::cout << "aaaaaaaaaaxyz" << std::endl;

    // Test Constants
    std::cout << "STATIC CAST on MAXINT: " << static_cast<uint32_t>(0xFFFFFFFF) << std::endl;
    std::cout << "STATIC CAST on ZERO: " << static_cast<uint32_t>(0x00000000) << std::endl;
   
    // Test Conversions of constexpr
    std::cout << "MAX_LSB << 32: " << static_cast<uint64_t>(MAX_LSB) << std::endl;
    std::cout << "MIN_LSB << 32: " << static_cast<uint64_t>(MIN_LSB) << std::endl;

    int loopCount = 0;
	int multiEdges_singleCount = 0;
	int multiEdges_multiCount = 0;

	for (uint32_t i = 0; i < 1; ++i) {
		auto degrees = MonotonicPowerlawRandomStream<false>(1, 500, -2, 5000);

		ASSERT_FALSE(degrees.empty());
        ConfigurationModel<> cm(degrees, 179273927, 500000);
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

		cm.clear();

		loopCount = 0;
		multiEdges_multiCount = 0;
		multiEdges_singleCount = 0;
	}
}

