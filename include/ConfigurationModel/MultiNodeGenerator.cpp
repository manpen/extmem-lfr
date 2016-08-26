#include <ConfigurationModel.h>
#include <inttypes.h>
#include <type_traits>
#include "nmmintrin.h"

void generate() {
	std::vector<uint64_t> nodes;

	std::vector<uint64_t> hash_values;

	// test hashing
	for (uint64_t i = 1; i < 10000; ++i) {
		for (uint64_t j = 1; j < 150; ++j) {
			nodes.push_back((j << 36) + i);
		}
	}
	
	uint32_t seed = 1;
	uint32_t msb = 0;
	uint32_t lsb = 0;
	uint32_t h_msb = 0;
	uint32_t h_lsb = 0;

	for (std::vector<uint64_t>::iterator it = nodes.begin(); it != nodes.end(); ++it) {
		auto & node = *it;

		msb = static_cast<uint32_t>(node >> 32);
		lsb = static_cast<uint32_t>(node);
		h_msb = _mm_crc32_u32(seed, msb);
		h_lsb = _mm_crc32_u32(h_msb, lsb);

		hash_values.push_back( static_cast<uint64_t>(h_msb) << 32 | h_lsb);

		seed = h_lsb;
	}

	sort(hash_values.begin(), hash_values.end());
	
	bool equals = false;
	uint64_t prev = 0;
	for (std::vector<uint64_t>::iterator it = hash_values.begin(); it != hash_values.end(); ++it) {
		if (prev == *it) {
			equals = true;
			break;
		}
		//printf("%" PRIu64 "\n", *it);
	}

	if (equals) printf("CRCHash duplicate found...\n");
	else printf("CRCHash clean...\n");

	// try some generated values
	std::vector<uint64_t> data;
	MultiNodeMsgSorter _multinode_sorter(MultiNodeMsgComparator{}, 64*1024*1024);
	
	auto degree_seq = MonotonicPowerlawRandomStream<false>(1, (1<<9), 2, (1<<14));

	int32_t count = 0;
	uint64_t i;
	uint64_t tmp;
	for (; !degree_seq.empty(); ++degree_seq, ++count) {
		auto & smth = *degree_seq;
		tmp = (uint32_t) smth;
		for (i = 0; i < tmp; ++i) 
			data.push_back((i << 36) | count);
			//_multinode_sorter.push(MultiNodeMsg{(i << 36) | count, tmp});
	}
	printf("Degreesize = %" PRId32 "\n", count);

	seed = 1;
	msb = 0;
	lsb = 0;
	h_msb = 0;
	h_lsb = 0;

	for (std::vector<uint64_t>::iterator it = data.begin(); it != data.end(); ++it) {
		auto & key_node  = *it;

		msb = static_cast<uint32_t>(key_node >> 32);
		lsb = static_cast<uint32_t>(key_node);
		h_msb = _mm_crc32_u32(seed, msb);
		h_lsb = _mm_crc32_u32(h_msb, lsb);

		_multinode_sorter.push({static_cast<uint64_t>(h_msb) << 32 | h_lsb, key_node});

		seed = h_lsb;
	}

	_multinode_sorter.sort();

	for (; !_multinode_sorter.empty(); ++_multinode_sorter) {
		auto & msg = *_multinode_sorter;
		printf("%" PRIu64 "\n", msg.key_node & 0x0000000FFFFFFFFF);
	}
}
