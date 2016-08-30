#pragma once
#include <limits>

#include <stxxl/vector>
#include <stxxl/sorter>

#include <defs.h>

#include "nmmintrin.h"

constexpr uint64_t NODEMASK = 0x0000000FFFFFFFFF;

using multinode_t = uint64_t;

/**
 * @typedef multinode_t
 * @brief The default signed integer to be used.
 * 
 * struct for node multiplicity
 * in the 36 lsb bits - node
 * in the 28 msb bits - key or eid
 * we expect pairwise different representations
 */
class MultiNodeMsg {
public:
	MultiNodeMsg() { }
	MultiNodeMsg(const uint64_t eid_node_) : _eid_node(eid_node_) {}

	// getters
	uint64_t eid_node() const {
		return _eid_node;
	}

	// just return the node
	uint64_t node() const {
		return _eid_node & NODEMASK;
	}

protected:
	multinode_t _eid_node;
};

// Comparator
class MultiNodeMsgComparator {	
	public:
		MultiNodeMsgComparator() {}
		MultiNodeMsgComparator(const uint32_t seed_) : _seed(seed_), _crc_max(_calculateMax(seed_)) {}
		
		// if not chained then const seed
		// make a_hash_msb, a_hash_lsb, b_hash_msb, b_hash_lsb protected?
		// invert msb's since lsb = seed then for max_value
		bool operator() (const MultiNodeMsg & a, const MultiNodeMsg & b) {
			const uint32_t a_hash_msb = _mm_crc32_u32(_seed, static_cast<uint32_t>(a.node() >> 32));
			const uint32_t a_hash_lsb = _mm_crc32_u32(~a_hash_msb, static_cast<uint32_t>(a.node()));
			const uint32_t b_hash_msb = _mm_crc32_u32(_seed, static_cast<uint32_t>(b.node() >> 32));
			const uint32_t b_hash_lsb = _mm_crc32_u32(~b_hash_msb, static_cast<uint32_t>(b.node()));

			return (static_cast<uint64_t>(a_hash_msb) << 32 | a_hash_lsb) < (static_cast<uint64_t>(b_hash_msb) << 32 | b_hash_lsb);
		}

		uint64_t max_value() const {
			return _crc_max;
		}

	protected:
		// unnecessary initialization, compiler asks for it
		const uint32_t _seed = 1;
		const uint64_t _crc_max = 0;

		uint64_t _calculateMax(const uint32_t seed_) {
			// initialization
			uint32_t max_msb = 0;
			uint32_t max_lsb = 0;
			
			uint32_t i;
			// leave it at this?
			// get MSB, iterate over all uint32_t's
			for (i = 0; i < UINT32_MAX; ++i)
				if (_mm_crc32_u32(seed_, i) == UINT32_MAX) {
					max_msb = i;
					break;
				}

			// get LSB
			for (i = 0; i < UINT32_MAX; ++i)
				if (_mm_crc32_u32(~max_msb, i) == UINT32_MAX) {
					max_lsb = i;
					break;
				}
			
			return ((static_cast<uint64_t>(max_msb) << 32) | max_lsb);
		}
	};
