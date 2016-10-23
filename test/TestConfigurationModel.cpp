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

    CMHH<HavelHakimiIMGenerator> cmhh(hh_gen, 223224, 1000);
    cmhh.run();
}

/*
TEST_F(TestConfigurationModel, nothing) {
    nothing();
}

TEST_F(TestConfigurationModel, testNode) {
    stxxl::random_number64 rng;
    std::cout << rng(UINT64_MAX) << std::endl;
    std::cout << rng(UINT64_MAX) << std::endl;
    std::cout << rng(UINT64_MAX) << std::endl;
    std::cout << rng(UINT64_MAX) << std::endl;
    

  #ifndef NDEBUG
    auto a = TestNodeMsg{20, 20};
    std::cout << a.key << std::endl;
  #else
    auto b = MultiNodeMsg{0xFFFFFFFFFFFFFFFF};
    std::cout << "LSB of UINT64_MAX: " << std::hex << b.lsb() << std::endl;
    std::cout << "MSB of UINT64_MAX: " << std::hex << b.msb() << std::endl;
    
    uint32_t seed = (1<<16) + 1;

    MultiNodeMsgComparator mnmc{seed};
    std::cout << "Comparator_MAX: " << std::hex << mnmc.max_value() << std::endl;
    std::cout << "Comparator_MIN: " << std::hex << mnmc.min_value() << std::endl;
 
  #ifdef CHANGECRC
    MultiNodeMsg maxmsg{mnmc.max_value()};
    MultiNodeMsg minmsg{mnmc.min_value()};

    uint32_t maxhashmsb = _mm_crc32_u32(seed, maxmsg.lsb());
    uint32_t maxhashlsb = _mm_crc32_u32(maxhashmsb, maxmsg.msb());

    ASSERT_EQ(maxhashmsb, 0xFFFFFFFF);
    ASSERT_EQ(maxhashlsb, 0xFFFFFFFF);

    uint32_t minhashmsb = _mm_crc32_u32(seed, minmsg.lsb());
    uint32_t minhashlsb = _mm_crc32_u32(minhashmsb, minmsg.msb());

    ASSERT_EQ(minhashmsb, static_cast<uint32_t>(0));
    ASSERT_EQ(minhashlsb, static_cast<uint32_t>(0));
    
    ASSERT_TRUE(mnmc(minmsg, maxmsg));
  #endif
  #endif
}

TEST_F(TestConfigurationModel, testCRC) {
    uint32_t max_lsb = 0;
    uint32_t min_lsb = 0;
    
    bool found_max = false;
    bool found_min = true;

    // Test static_cast
    ASSERT_EQ(static_cast<uint32_t>(0x00000000), static_cast<uint32_t>(0));
    ASSERT_EQ(static_cast<uint32_t>(0xFFFFFFFF), UINT32_MAX);

    // Find LSB for MAX and MIN
    for (uint32_t i; i < UINT32_MAX; ++i) {
        if (_mm_crc32_u32(0xFFFFFFFF, i) == static_cast<uint32_t>(0xFFFFFFFF)) {
            max_lsb = i;
            found_max = true;
        }
        if (_mm_crc32_u32(0x00000000, i) == static_cast<uint32_t>(0x00000000)) {
            min_lsb = i;
            found_min = true;
        }
        if (found_max && found_min) 
            break;
    }

    ASSERT_TRUE(found_max);
    ASSERT_TRUE(found_min);

    // Compare current LSB constants to newly found
    ASSERT_EQ(max_lsb, MAX_LSB);
    ASSERT_EQ(min_lsb, MIN_LSB);

    // Test current LSB constants for MAX and MIN
    ASSERT_EQ(_mm_crc32_u32(0xFFFFFFFF, MAX_LSB), static_cast<uint32_t>(0xFFFFFFFF));
    ASSERT_EQ(_mm_crc32_u32(0x00000000, MIN_LSB), static_cast<uint32_t>(0x00000000));
}

TEST_F(TestConfigurationModel, findInverse) {
	bool found = false;
	for (uint32_t i = 1; i < UINT32_MAX; ++i) {
		if (_mm_crc32_u32(static_cast<uint32_t>(0), i) == 0xFFFFFFFF){
			std::cout << "POLY: " << std::hex << i << std::endl;
			found = true;
		}
	}

	ASSERT_TRUE(found);

	// WE FOUND THE CONSTANT:
	// from line 5 in intel intrinsics of _mm_crc32_u32:
	// tmp5[95:0] = 0x4546F146|00000000
	// therefore the seed XOR 0x641F6454, endianness is important here...
	// is the inverse! WE TEST:
	for (uint32_t seed = 0; seed < UINT32_MAX; ++seed) {
		uint32_t inverse_to_seed = reinterpret_cast<uint32_t>(0x641F6454 ^ seed);
		ASSERT_EQ(reinterpret_cast<uint32_t>(inverse_to_seed ^ seed), static_cast<uint32_t>(0x641F6454));
		ASSERT_EQ(_mm_crc32_u32(seed, inverse_to_seed), static_cast<uint32_t>(0xFFFFFFFF));	
	}
	// the constant for min_value is simply 0x00000000.
}

TEST_F(TestConfigurationModel, searchMax) {
	for (uint32_t i = 0; i < UINT32_MAX; ++i) {
		if (_mm_crc32_u32(0xFFFFFFFF, i) == 0xFFFFFFFF) {
			std::cout << std::hex << "MAXI" << i << std::endl;
		}
	}
}

TEST_F(TestConfigurationModel, comparator) {
	uint32_t test_Max = _mm_crc32_u32(0xFFFFFFFF, MAX_LSB);

	ASSERT_EQ(test_Max, 0xFFFFFFFF);
	
	MultiNodeMsgComparator mnmc(static_cast<uint32_t>(1<<3));
	
	auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<19));

	ConfigurationModel<> cm(degrees, static_cast<uint32_t>((1<<3)));
	cm.run();	
	
	ASSERT_FALSE(cm.empty());

	ASSERT_TRUE(true);

	auto degrees2 = MonotonicPowerlawRandomStream<false>(10, (1<<5), -2, (1<<15));

	std::vector<degree_t> ref_degrees (1<<15);

	stxxl::stream::materialize(degrees2, ref_degrees.begin());

	auto deg_stream = stxxl::stream::streamify(ref_degrees.begin(), ref_degrees.end());

	//ConfigurationModel<stxxl::stream::vector_iterator2stream<stxxl::vector<degree_t>::const_iterator>> cm2(deg_stream, static_cast<uint32_t>(1<<3));
}

TEST_F(TestConfigurationModel, algoClass) {
	// test sorter
	auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<14));
	
	ASSERT_FALSE(degrees.empty());

	ConfigurationModel<> cm(degrees, static_cast<uint32_t>((2 << 15) + 1));
	cm.run();

	ASSERT_FALSE(cm.empty());

	// test scan through, check if ordered right
	int count = 1;

	auto prev_edge = *cm;
	
	++cm;

	bool correct = prev_edge.first <= prev_edge.second;

	for(; !cm.empty(); ++count, ++cm) {
		const auto & edge = *cm;

		correct = correct & (edge.first <= edge.second) & (edge.first >= prev_edge.first);

		prev_edge = edge;
	}

	ASSERT_TRUE(correct);

	std::cout << "COUNT: " << count << std::endl;

	ASSERT_GT(count, (1<<14));

	ASSERT_TRUE(cm.empty());

	cm.clear();

	// test pusher
	auto degrees2 = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<14));

	ASSERT_FALSE(degrees2.empty());

	std::vector<degree_t> ref_degrees (1<<14);

	stxxl::stream::materialize(degrees2, ref_degrees.begin());

	ASSERT_EQ(ref_degrees.size(), static_cast<uint32_t>(1<<14));

	ASSERT_TRUE(degrees2.empty());

	auto degree_stream = stxxl::stream::streamify(ref_degrees.begin(), ref_degrees.end());

	ASSERT_FALSE(degree_stream.empty());
}

TEST_F(TestConfigurationModel, finalOutput) {
	// we do 100 Runs
	constexpr uint32_t dmin = 1;
	constexpr uint32_t dmax = (1<<9);
	constexpr uint32_t gamma = 2;
	constexpr uint32_t n = (1<<14);
	constexpr uint32_t c = 1;

	std::ostringstream stringStream;
	stringStream << "data_";
	stringStream << std::to_string(c);
	stringStream << "_";
	stringStream << std::to_string(dmin);
	stringStream << "_";
	stringStream << std::to_string(dmax);
	stringStream << "_";
	stringStream << std::to_string(gamma);
	stringStream << "_";
	stringStream << std::to_string(n);
	stringStream << ".txt";
	std::string copyOfStr = stringStream.str();

	std::ofstream outputFile(copyOfStr);
	
	std::cout << "GOING" << std::endl;
	
	for (uint32_t i = 0; i < c; ++i) {
		uint64_t loopCount = 0;
		uint64_t me_single = 0;
		uint64_t me_multi  = 0;

		auto degrees = MonotonicPowerlawRandomStream<false>(dmin, dmax, -gamma, n);

		ASSERT_FALSE(degrees.empty());

		ConfigurationModel<> cm(degrees, i);
		cm.run();

		ASSERT_FALSE(cm.empty());

		auto prev_edge = *cm;
		++cm;

		bool prev_multi = false;


		if (prev_edge.is_loop())
			++loopCount;

		for(; !cm.empty(); ++cm) {
			auto & edge = *cm;

			if (edge.is_loop()) {
				++loopCount;
                if (prev_multi)
                    ++me_multi;
                prev_edge = edge;
				prev_multi = false;
				continue;
			}
			if (prev_edge == edge) {
				++me_multi;

				if (!prev_multi) {
					++me_single;
					prev_multi = true;
				}

				prev_edge = edge;

				continue;
			}

            if (prev_multi)
                ++me_multi;

			prev_edge = edge;

			prev_multi = false;

		}
	
		cm.clear();

		std::ostringstream stringStream2;
		stringStream2 << std::to_string(loopCount);
		stringStream2 << "\t";
		stringStream2 << std::to_string(me_single);
		stringStream2 << "\t";
		stringStream2 << std::to_string(me_multi);
		stringStream2 << " #LSM\n";

		std::string lineInfo = stringStream2.str();

		std::cout << lineInfo << std::endl;

		outputFile << lineInfo;

		stringStream2.str(std::string());
	}
}
*/

TEST_F(TestConfigurationModel, reverse) {
    ASSERT_EQ(reverse(static_cast<uint64_t>(0)), static_cast<uint64_t>(0));
    ASSERT_EQ(reverse(static_cast<uint64_t>(1)), static_cast<uint64_t>(1) << 63);
    ASSERT_EQ(reverse(static_cast<uint64_t>(2)), static_cast<uint64_t>(1) << 62);
    ASSERT_EQ(reverse(static_cast<uint64_t>(3)), static_cast<uint64_t>(3) << 62);
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

    std::cout << "(1<<9): " << (1<<9) << std::endl;

	// we do 10 runs here.., with i as seed
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
		
			//std::cout << "EDGE<" << edge.first << ", " << edge.second << ">" << std::endl;


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

