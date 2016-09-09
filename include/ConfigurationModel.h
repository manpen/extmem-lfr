#pragma once

#include <limits>

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/common/uint_types.h>

#include <defs.h>
#include <GenericComparator.h>
#include <Utils/MonotonicPowerlawRandomStream.h>

#include "nmmintrin.h"

namespace ConfigurationModel {
	constexpr uint64_t NODEMASK = 0x0000000FFFFFFFFF;
	constexpr uint32_t LIMITS_LSB = 0x9BE09BAB;
	constexpr uint32_t MAX_CRCFORWARD = 0x641F6454;

	using multinode_t = uint64_t;

	//! Type for every (un)directed 64bit
	// ommited invalid() member function
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
#ifndef
	struct TestNodeMsg {
		multinode_t key;
		multinode_t eid_node;

		TestNodeMsg() { }
		TestNodeMsg(const multinode_t &key_, const multinode_t &eid_node_) : key(key_), eid_node(eid_node_) {}

		DECL_LEX_COMPARE_OS(TestNodeMsg, multinode_t, multinode_t);
	};
#endif
	/**
	 * @typedef multinode_t
	 * @brief The default signed integer to be used.
	 * 
	 * struct for node multiplicity
	 * in the 36 lsb bits - node
	 * in the 28 msb bits - key or half_edgeid
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
			MultiNodeMsgComparator(const uint32_t seed_) 
				: _seed(seed_) 
				, _limits(_setLimits(seed_))
			{
				//std::cout << "WE IN COMP CONSTRUCTOR" << std::endl;
			}
			
			// invert msb's since lsb = seed then for max_value
			bool operator() (const MultiNodeMsg& a, const MultiNodeMsg& b) const {
				// the MSB's of hash should be enough to decide 
				const uint32_t a_hash_msb = _mm_crc32_u32(_seed, static_cast<uint32_t>(a.eid_node() >> 32));
				const uint32_t b_hash_msb = _mm_crc32_u32(_seed, static_cast<uint32_t>(b.eid_node() >> 32));

				if (UNLIKELY(a_hash_msb == b_hash_msb)) {
					const uint32_t a_hash_lsb = _mm_crc32_u32(a_hash_msb, static_cast<uint32_t>(a.eid_node()));
					const uint32_t b_hash_lsb = _mm_crc32_u32(b_hash_msb, static_cast<uint32_t>(b.eid_node()));
					return a_hash_lsb < b_hash_lsb;
				} else {
					return a_hash_msb < b_hash_msb;
				}
			}

			uint64_t max_value() const {
				return _limits.first;
			}

			uint64_t min_value() const {
				return _limits.second;
			}


		protected:
			// unnecessary initialization, compiler asks for it
			const uint32_t _seed = 1;
			const std::pair<uint64_t,uint64_t> _limits;
		
			std::pair<multinode_t, multinode_t> _setLimits(const uint32_t seed_) const {
				//std::cout << "In method _setLimits..." << std::endl;
				uint64_t max_inv_msb = static_cast<uint64_t>(MAX_CRCFORWARD ^ seed_) << 32;
				uint64_t min_inv_msb = static_cast<uint64_t>(0x00000000 ^ seed_) << 32;

				return std::pair<multinode_t, multinode_t>{max_inv_msb | LIMITS_LSB, min_inv_msb | LIMITS_LSB};
			}

	};

#ifndef NDEBUG
	using TestNodeComparator = typename GenericComparatorStruct<TestNodeMsg>::Ascending;
	using TestNodeSorter = stxxl::sorter<TestNodeMsg, TestNodeComparator>;
#endif

	template <typename T = MonotonicPowerlawRandomStream<false>>
	class ConfigurationModel {
		public:
			ConfigurationModel() = delete; 

			ConfigurationModel(const ConfigurationModel&) = delete;
			ConfigurationModel(T &degrees, const uint32_t seed)
										: _degrees(degrees) 
										, _multinodemsg_comp(seed)
										, _multinodemsg_sorter(_multinodemsg_comp, SORTER_MEM)
										, _edge_sorter(Edge64Comparator(), SORTER_MEM)
#ifndef NDEBUG
										, _testnode_sorter(TestNodeComparator{}, SORTER_MEM)
#endif
			{ }
		
			// implements execution of algorithm
			void run() {
				_generateMultiNodes();

				assert(!_multinodemsg_sorter.empty());

				_generateSortedEdgeList();

				assert(!_edge_sorter.empty());
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
			T _degrees;

#ifndef NDEBUG
			TestNodeSorter _testnode_sorter;
#endif
			typedef stxxl::sorter<MultiNodeMsg, MultiNodeMsgComparator> MultiNodeSorter;
			MultiNodeMsgComparator _multinodemsg_comp;
			MultiNodeSorter _multinodemsg_sorter;

			using EdgeSorter = stxxl::sorter<edge64_t, Edge64Comparator>;
			EdgeSorter _edge_sorter; 


			// internal algos
			void _generateMultiNodes() {
				assert(!_degrees.empty());

				for (uint64_t i = 1; !_degrees.empty(); ++i, ++_degrees)
					for (degree_t j = 0; j < *_degrees; ++j)
						_multinodemsg_sorter.push(MultiNodeMsg{(static_cast<multinode_t>(j) << 36) | i});

				_multinodemsg_sorter.sort();

				assert(!_multinodemsg_sorter.empty());
			}
			
			void _generateSortedEdgeList() {
				assert(!_multinodemsg_sorter.empty());

				for(; !_multinodemsg_sorter.empty(); ) {
					auto & fst_node = *_multinodemsg_sorter;

					++_multinodemsg_sorter;

					MultiNodeMsg snd_node;

					if (!_multinodemsg_sorter.empty())
						snd_node = *_multinodemsg_sorter;
					else
						snd_node = fst_node;

					uint64_t fst_nodeid{fst_node.node()};
					uint64_t snd_nodeid{snd_node.node()};

					if (fst_nodeid < snd_nodeid)
						_edge_sorter.push(edge64_t{fst_nodeid, snd_nodeid});
					else
						_edge_sorter.push(edge64_t{snd_nodeid, fst_nodeid});

					if (!_multinodemsg_sorter.empty())
						++_multinodemsg_sorter;
					else
						break;
				}

				_edge_sorter.sort();
			}
			
			void _reset() {
				_multinodemsg_sorter.clear();
				_edge_sorter.clear();
			}
	};

}
