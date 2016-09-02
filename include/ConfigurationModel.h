#pragma once

#include <limits>

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/common/uint_types.h>

#include <defs.h>
#include <GenericComparator.h>
#include <Utils/MonotonicPowerlawRandomStream.h>

#include "nmmintrin.h"

constexpr uint64_t NODEMASK = 0x0000000FFFFFFFFF;

using multinode_t = uint64_t;

//! Type for every (un)directed 64bit
// ommited invalid() member function
// TODOASK here the edge amountcount already?
struct edge64_t : public std::pair<multinode_t, multinode_t> {
	edge64_t() : std::pair<multinode_t, multinode_t>() {}
	edge64_t(const std::pair<multinode_t, multinode_t> & edge) : std::pair<multinode_t, multinode_t>(edge) {}
	edge64_t(const multinode_t & v1, const multinode_t & v2) : std::pair<multinode_t, multinode_t>(v1, v2) {}

	//! Enforces first<=second
	void normalize() {
		if (first > second)
			std::swap(first, second);
	}

	//! Returns true if edge represents a self-loop
	bool is_loop() const {
		return first == second;
	}
};

struct Edge64Comparator {
	bool operator()(const edge64_t &a, const edge64_t &b) const {
		if (a.first == b.first) 
			return a.second < b.second;
		else
			return a.first < b.first;
	}

	edge64_t min_value() const {
		return edge64_t(std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::min());
	}

	edge64_t max_value() const {
		return edge64_t(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
	}
};

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
		MultiNodeMsgComparator(const uint32_t seed_) : _seed(seed_) {
			_setMinMax(seed_);
		}
		
		// if not chained then const seed
		// make a_hash_msb, a_hash_lsb, b_hash_msb, b_hash_lsb protected?
		// invert msb's since lsb = seed then for max_value
		bool operator() (const MultiNodeMsg& a, const MultiNodeMsg& b) const {
			// the MSB's of hash should be enough to decide 
			const uint32_t a_hash_msb = _mm_crc32_u32(_seed, static_cast<uint32_t>(a.eid_node() >> 32));
			const uint32_t b_hash_msb = _mm_crc32_u32(_seed, static_cast<uint32_t>(b.eid_node() >> 32));

			if (UNLIKELY(a_hash_msb == b_hash_msb)) {
				const uint32_t a_hash_lsb = _mm_crc32_u32(~a_hash_msb, static_cast<uint32_t>(a.eid_node()));
				const uint32_t b_hash_lsb = _mm_crc32_u32(~b_hash_msb, static_cast<uint32_t>(b.eid_node()));
				return a_hash_lsb < b_hash_lsb;
			} else {
				return a_hash_msb < b_hash_msb;
			}
		}

		uint64_t max_value() const {
			return _crc_max;
		}

		uint64_t min_value() const {
			return _crc_min;
		}


	protected:
		// unnecessary initialization, compiler asks for it
		const uint32_t _seed = 1;
		uint64_t _crc_max = 0;
		uint64_t _crc_min = 0;

		void _setMinMax(const uint32_t seed_) {
			static_assert(sizeof(seed_) == 4, "Seed should be 4 Bytes long!");
			
			// initialization
			uint32_t max_msb = 0;
			uint32_t max_lsb = 0;
			uint32_t min_msb = 0;
			uint32_t min_lsb = 0;
			
			uint32_t i;

			bool max = false;
			bool min = false;
			// leave it at this?
			// get MSB, iterate over all uint32_t's
			for (i = 0; i < UINT32_MAX; ++i) {
				if (_mm_crc32_u32(seed_, i) == UINT32_MAX) {
					max_msb = i;
					max = true;
				}
				if (_mm_crc32_u32(seed_, i) == 0) {
					min_msb = i;
					min = true;
				}
				if (max && min) break;
			}

			assert(max);
			assert(min);

			max = false;
			min = false;

			// get LSB
			for (i = 0; i < UINT32_MAX; ++i) {
				if (_mm_crc32_u32(~max_msb, i) == UINT32_MAX) {
					max_lsb = i;
					max = true;
				}
				if (_mm_crc32_u32(~min_msb, i) == 0) {
					min_lsb = i;
					min = true;
				}
				if (max && min) break;
			}

			assert(max);
			assert(min);

			// assign
			_crc_max = (static_cast<uint64_t>(max_msb) << 32) | max_lsb;
			_crc_min = (static_cast<uint64_t>(min_msb) << 32) | min_lsb;
		}
	};

// TODO
class ConfigurationModel {
	public:
		ConfigurationModel() = delete; 

		ConfigurationModel(const ConfigurationModel&) = delete;

		using degree_buffer_t = MonotonicPowerlawRandomStream<false>;
		ConfigurationModel(degree_buffer_t &degrees, const uint32_t seed) : 
									_degrees(degrees), 
									_multinodemsg_comp(seed),
									_multinodemsg_sorter(_multinodemsg_comp, SORTER_MEM),
									_edge_sorter(Edge64Comparator(), SORTER_MEM)
		{ }
	
		// implements execution of algorithm
		void run();

		// for tests
		void run_onlyNodes() {
			_generateMultiNodes();
		}

		bool nodeNumberOdd() {
			assert(!_multinodemsg_sorter.empty());

			uint64_t count = 0;

			for (; !_multinodemsg_sorter.empty(); ++count, ++_multinodemsg_sorter) {}

			return (count & 1);
		}

//! @name STXXL Streaming Interface
//! @{
		bool empty() const {
			return _edge_sorter.empty();
		}

		const edge64_t& operator*() const {
			assert(!_edge_sorter.empty());

			return *_edge_sorter;
		}

		ConfigurationModel&operator++() {
			assert(!_edge_sorter.empty());
			
			++_edge_sorter;

			return *this;
		}
//! @}
		// for testing
		void clear() {
			_reset();
		}

	protected:
		MonotonicPowerlawRandomStream<false> _degrees;
	
		typedef stxxl::sorter<MultiNodeMsg, MultiNodeMsgComparator> MultiNodeSorter;
		MultiNodeMsgComparator _multinodemsg_comp;
		MultiNodeSorter _multinodemsg_sorter;

		using EdgeSorter = stxxl::sorter<edge64_t, Edge64Comparator>;
		EdgeSorter _edge_sorter; 

		// internal algos
		void _generateMultiNodes();
		void _generateSortedEdgeList();

		void _reset() {
			_multinodemsg_sorter.clear();
			_edge_sorter.clear();
		}
};
