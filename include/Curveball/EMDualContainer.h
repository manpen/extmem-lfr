/**
 * @file EMDualContainer.h
 * @date 29. September 2017
 *
 * @author Hung Tran
 */
#pragma once

#include <atomic>
#include "defs.h"
#include <vector>
#include <parallel/algorithm>
#include <parallel/numeric>
#include <Utils/IOStatistics.h>
#include "EMMessageContainer.h"
#include "IMAdjacencyList.h"
#include "Utils/Hashfuncs.h"
#include "EMTargetInformation.h"
#include <Utils/ScopedTimer.h>
#include <Utils/IntSort.h>
#include <Utils/AlignedRNGs.h>

namespace Curveball {

	// methods that can be used in debug (checks for traded/non-traded nodes)
#ifndef NDEBUG
	static std::vector<size_t> get_zero_entries(const std::vector<int>& vec) {
		std::vector<size_t> output;
		for (size_t node = 0; node < vec.size(); node++) {
			if (vec[node] == 0)
				output.push_back(node);
		}

		return output;
	}

	static std::vector<size_t> get_non_zero_entries(const std::vector<std::atomic<int>>& vec) {
		std::vector<size_t> output;
		size_t n_count = 0;
		for (auto && t_count : vec) {
			const auto val = t_count.load();
			if (val != 0)
				output.push_back(n_count);
		}

		return output;
	}
#endif

	static node_t make_even_by_sub(const node_t n) {
		if (n % 2 == 1)
			return n - 1;
		else
			return n;
	}

	static node_t make_even_by_add(const node_t n) {
		if (n % 2 == 1)
			return n + 1;
		else
			return n;
	}

	struct ThreadBounds {
		std::vector<hnode_t> l_bounds;
		std::vector<hnode_t> u_bounds;

		ThreadBounds() = default;

		ThreadBounds(const std::vector<hnode_t> &l_bounds_,
					 const std::vector<hnode_t> &u_bounds_)
			: l_bounds(l_bounds_),
			  u_bounds(u_bounds_) {}

		ThreadBounds(const ThreadBounds &o)
			: l_bounds(o.l_bounds),
			  u_bounds(o.u_bounds) {}

		size_t size() const {
			return l_bounds.size();
		}
	};

	static node_t get_last_mc_nodes(const node_t num_nodes,
									const chunkid_t num_chunks) {
		const node_t nodes_chunks_rem =
			(num_nodes % num_chunks);

		const auto nodes_chunks_div =
			static_cast<node_t>(num_nodes / num_chunks);

		const auto even_nodes_chunks_div =
			make_even_by_sub(nodes_chunks_div);

		if (nodes_chunks_div == even_nodes_chunks_div)
			if (nodes_chunks_rem == 0)
				return even_nodes_chunks_div;
			else
				return even_nodes_chunks_div + nodes_chunks_rem;
		else
			return even_nodes_chunks_div + nodes_chunks_rem + num_chunks;
	}

	static ThreadBounds mc_get_thread_bounds(const node_t mc_even_num_nodes,
											 const chunkid_t num_microchunks,
											 const chunkid_t fanout) {
		assert(mc_even_num_nodes % 2 == 0);

		std::vector<node_t> lower_bounds;
		std::vector<node_t> upper_bounds;

		lower_bounds.reserve(static_cast<size_t>(num_microchunks * fanout));
		upper_bounds.reserve(static_cast<size_t>(num_microchunks * fanout));

		lower_bounds.push_back(0);
		for (chunkid_t i = 1; i < num_microchunks * fanout; i++) {
			const auto bound =
				make_even_by_sub(static_cast<node_t>(i * mc_even_num_nodes /
													 (num_microchunks * fanout)));
			lower_bounds.push_back(bound);
			upper_bounds.push_back(bound);
		}
		upper_bounds.push_back(mc_even_num_nodes);

		// check for empty intervals (that should not exist)
		#ifndef NDEBUG
		{
			// check for empty bound interval
			std::equal(lower_bounds.begin() + 1,
					   lower_bounds.end(),
					   upper_bounds.begin());

			for (unsigned long ix = 0; ix < lower_bounds.size(); ix++)
				assert(upper_bounds[ix] - lower_bounds[ix] > 0);
		};
		#endif

		return ThreadBounds(lower_bounds, upper_bounds);
	}

//#define BATCH_DEPS
	template<typename HashFactory, typename OutReceiver = EdgeStream>
	class EMDualContainer {
	public:
		using chunk_upperbound_vector = std::vector<hnode_t>;
		using degree_vector = std::vector<degree_t>;
		using inverse_vector = std::vector<node_t>;
		using hash_vector = std::vector<hnode_t>;
		using threadcount_vector = std::vector<std::atomic<int>>;
		using bool_vector = std::vector<int>;
		using rng_vector = std::vector<STDRandomEngine>;

	protected:
		EMMessageContainer<HashFactory, OutReceiver> _active;
		EMMessageContainer<HashFactory, OutReceiver> _pending;

		EMTargetInformation& _target_infos;

		chunk_upperbound_vector _active_upper_bounds;
		chunk_upperbound_vector _pending_upper_bounds;

		const node_t _num_nodes;
		const chunkid_t _num_chunks;

		const node_t _nodes_per_mc;
		const node_t _last_mc_nodes;

		degree_vector _mc_degs;
		inverse_vector _mc_invs;
		hash_vector _mc_hashes;
		inverse_vector _mc_clearpartner;

		degree_vector _mc_degs_psum;
		degree_vector _mc_num_inc_msgs;
		degree_vector _mc_num_inc_msgs_psum;

		msgid_t _mc_max_num_msgs;
		node_t _mc_num_loaded_nodes;

		hnode_t _b_largest_hnode;
		hnode_t _mc_largest_hnode;
		hnode_t _mc_last_largest_hnode;
		chunkid_t _current_mc_id;
		degree_t _mc_max_degree;

		node_t _g_num_processed_nodes;
		node_t _mc_last_hash_offset;
		node_t _mc_hash_offset;

		bool_vector _mc_has_traded;
		threadcount_vector _active_threads;
		std::vector<std::mutex> _node_locks;

		IMAdjacencyList _mc_adjacency_list;

		const chunkid_t _num_splits;
		const chunkid_t _num_fanout;

		const int _num_threads;

		ThreadBounds _mc_thread_bounds;

		int _b_min_mc_node;
		int _b_max_mc_node;

		RNGs<std::mt19937_64, 64> _rngs;

		std::vector<std::vector<node_t>> _t_common_neighbours;
		std::vector<std::vector<node_t>> _t_disjoint_neighbours;

		Hashfuncs<HashFactory> &_hash_funcs;

		bool _has_run;

		#ifdef BATCH_DEPS
		std::atomic<size_t> batch_dep_count;
		#endif

	public:
		EMDualContainer() = delete;

		EMDualContainer(EMDualContainer &) = delete;

		EMDualContainer(const chunk_upperbound_vector &active_upper_bounds,
						const chunk_upperbound_vector &pending_upper_bounds,
						EMTargetInformation& target_infos,
						const node_t num_nodes,
						const degree_t max_degree,
						Hashfuncs<HashFactory> &hash_funcs,
						const CurveballParams &curveball_params) :
			_active(active_upper_bounds,
					curveball_params.msg_limit,
					EMMessageContainer<HashFactory, OutReceiver>::ACTIVE,
					curveball_params.threads,
					curveball_params.insertion_buffer_size),
			_pending(pending_upper_bounds,
					 curveball_params.msg_limit,
					 EMMessageContainer<HashFactory, OutReceiver>::NEXT,
					 curveball_params.threads,
					 curveball_params.insertion_buffer_size),
			_target_infos(target_infos),
			_active_upper_bounds(active_upper_bounds),
			_pending_upper_bounds(pending_upper_bounds),
			_num_nodes(num_nodes),
			_num_chunks(curveball_params.macrochunks),
			_nodes_per_mc(make_even_by_sub(static_cast<node_t>
										   (_num_nodes / _num_chunks))),
			_last_mc_nodes(get_last_mc_nodes(_num_nodes, _num_chunks)),
			// initialize and allocate enough memory
			_mc_degs(static_cast<size_t>(_last_mc_nodes)),
			_mc_invs(static_cast<size_t>(_last_mc_nodes)),
			_mc_hashes(static_cast<size_t>(_last_mc_nodes)),
			_mc_clearpartner(static_cast<size_t>(_last_mc_nodes)),
			// initialize and allocate enough memory for prefixsum vectors
			_mc_degs_psum(static_cast<size_t>(_last_mc_nodes) + 1),
			_mc_num_inc_msgs(static_cast<size_t>(_last_mc_nodes), 0),
			_mc_num_inc_msgs_psum(static_cast<size_t>(_last_mc_nodes) + 1),
			_mc_max_num_msgs(target_infos.get_active_max_num_msgs()),
			_mc_num_loaded_nodes(0),
			_b_largest_hnode(0),
			_mc_largest_hnode(0),
			_mc_last_largest_hnode(0),
			_current_mc_id(0),
			_mc_max_degree(max_degree),
			_g_num_processed_nodes(0),
			_mc_last_hash_offset(0),
			_mc_hash_offset(0),
			_mc_has_traded(static_cast<size_t>(make_even_by_add(_last_mc_nodes)), false),
			_active_threads(static_cast<size_t>(_last_mc_nodes)),
			_node_locks(static_cast<size_t>(_last_mc_nodes)),
			_mc_adjacency_list(_last_mc_nodes, _mc_max_num_msgs),
			_num_splits(curveball_params.splits),
			_num_fanout(curveball_params.fanout),
			_num_threads(curveball_params.threads),
			_mc_thread_bounds(),
			_b_min_mc_node(0),
			_b_max_mc_node(0),
			_rngs(static_cast<size_t>(curveball_params.threads), {1}),
			_t_common_neighbours(static_cast<size_t>(curveball_params.threads)),
			_t_disjoint_neighbours(static_cast<size_t>(curveball_params.threads)),
			_hash_funcs(hash_funcs),
			_has_run(false)
		{
			// set up common and disjoint vectors for each thread
			for (int thread_id = 0; thread_id < _num_threads; thread_id++) {
				_t_common_neighbours[thread_id].reserve(static_cast<size_t>(_mc_max_degree));
				_t_disjoint_neighbours[thread_id].reserve(static_cast<size_t>(_mc_max_degree));
			}
		}

		void process_active() {
			// process macrochunk by macrochunk
			for (chunkid_t mc_id = 0; mc_id < _num_chunks; mc_id++) {
				{
					IOStatistics pre_trading_report("PreTrading");
					_current_mc_id = mc_id;

					msg_vector msgs = _active.get_messages_of(mc_id);

					std::cout << "Received " << msgs.size() << " many messages" << std::endl;

					assert(msgs.size() > 0);

					// sort messages by comparator provided by GenericComparator
					{
						ScopedTimer timer("Sorting");
						#if 0
						// parallel quick sort
						__gnu_parallel::sort(msgs.begin(),
											 msgs.end(),
											 __gnu_parallel::quicksort_tag());
						#else
						const hnode_t last_upper_bound = (mc_id > 0 ? _active_upper_bounds[mc_id - 1] : 0);
						intsort::sort(msgs,
									  [&] (const NeighbourMsg& msg)
									  {return msg.target - last_upper_bound;},
									  _active_upper_bounds[mc_id] - last_upper_bound + 1);
						// radix-sort data-structure is dealloc here
						#endif
					}

					// check if messages are sorted
					// only relevant in debug-mode
					#ifndef NDEBUG
					{
						// check if sorted
						assert(std::is_sorted(msgs.cbegin(), msgs.cend(), NeighbourMsgComparator{}));

						// check for duplicates (deprecated, neighbours do not
						// need to be sorted
						//const auto msg_iter = std::adjacent_find(msgs.begin(), msgs.end());
						//assert(msg_iter == msgs.cend());
					};
					#endif

					// =================== load informations into IM ===================

					auto insert_info = [&](node_t mc_node, const TargetMsg &info) {
						_mc_degs[mc_node] = info.degree;
						_mc_invs[mc_node] = info.inverse;
						_mc_hashes[mc_node] = info.target;
					};

					auto set_partner = [&](node_t mc_node) {
						_mc_clearpartner[mc_node - 1] = _mc_invs[mc_node];
					};

					// Loading degrees and inverses into IM
					if (LIKELY(mc_id < _num_chunks - 1)) {
						for (node_t mc_nodeid = 0;
							 mc_nodeid < _nodes_per_mc;
							 mc_nodeid++, ++_target_infos) {
							assert(static_cast<size_t>(mc_nodeid) < _mc_degs.size());

							const TargetMsg info = *_target_infos;
							insert_info(mc_nodeid, info);
							if (mc_nodeid % 2 == 1)
								set_partner(mc_nodeid);
						}

						_mc_num_loaded_nodes = _nodes_per_mc;
					} else {
						// if at last macrochunk,
						// just move the rest (might be larger than other macrochunks)
						node_t _mc_last_num_nodes = 0;

						for (node_t mc_nodeid = 0; !_target_infos.empty();
							 ++_target_infos, mc_nodeid++) {
							assert(static_cast<size_t>(mc_nodeid) < _mc_degs.size());

							const TargetMsg info = *_target_infos;
							insert_info(mc_nodeid, info);
							if (mc_nodeid % 2 == 1)
								set_partner(mc_nodeid);

							_mc_last_num_nodes++;
						}

						_mc_num_loaded_nodes = _mc_last_num_nodes;
					}

					_mc_largest_hnode = _mc_hashes[_mc_num_loaded_nodes - 1];

					assert(_mc_largest_hnode >= _g_num_processed_nodes + _mc_num_loaded_nodes - 1);

					_mc_hash_offset = _mc_largest_hnode + 1 - _mc_num_loaded_nodes - _g_num_processed_nodes;

					assert(_mc_hash_offset >= _mc_last_hash_offset);
					assert(static_cast<size_t>(_mc_num_loaded_nodes) <= static_cast<size_t>(_nodes_per_mc) + 2 * _num_chunks);

					// check if _mc_hashes is sorted and has no duplicates
					#ifndef NDEBUG
					{
						// check if _mc_hashes is increasing up to _mc_num_loaded_nodes
						std::is_sorted(_mc_hashes.begin(),
									   _mc_hashes.begin() + _mc_num_loaded_nodes);

						// check for duplicates
						const auto hashes_iter =
							std::adjacent_find(_mc_hashes.begin(),
											   _mc_hashes.begin() + _mc_num_loaded_nodes);
						assert(hashes_iter == _mc_hashes.cbegin() + _mc_num_loaded_nodes);
					};
					#endif

					// =============== initialize adjacency structure ==================

					// calc prefix sum, saves addresses, therefore start at 0
					__gnu_parallel::partial_sum(_mc_degs.cbegin(),
												_mc_degs.cbegin() + _mc_num_loaded_nodes,
												_mc_degs_psum.begin() + 1,
												std::plus<degree_t>());

					// check degree prefix sum computation
					#ifndef NDEBUG
					{
						// check prefix sum
						degree_t degree_sum = 0;
						for (node_t mc_node = 0;
							 mc_node < _mc_num_loaded_nodes;
							 mc_node++)
							degree_sum += _mc_degs[mc_node];

						assert(degree_sum == _mc_degs_psum[_mc_num_loaded_nodes]);
					};
					#endif

					// identify hash-values as indices
					_mc_thread_bounds =
						mc_get_thread_bounds(make_even_by_sub(_mc_num_loaded_nodes),
											 _num_splits * _num_threads,
											 _num_fanout);

					assert(_mc_degs_psum[_mc_num_loaded_nodes] <= _target_infos.get_active_max_num_msgs());

					// realloc adjacency list
					_mc_adjacency_list.resize(_mc_degs_psum[_mc_num_loaded_nodes]);

					// initalize
					_mc_adjacency_list.initialize(_mc_degs,
												  _mc_clearpartner,
												  _mc_num_loaded_nodes,
												  _mc_degs_psum[_mc_num_loaded_nodes]);

					// check incoming messages
					#ifndef NDEBUG
					std::vector<degree_t> _mc_seq_num_inc_msgs(static_cast<size_t>(_mc_num_loaded_nodes), 0);
					#endif

					{
						uint_t index = 0;

						for (size_t msg_ix = 0; msg_ix < msgs.size(); msg_ix++) {
							assert(msgs[msg_ix].target >= _mc_hashes[0]);
							assert(msgs[msg_ix].target <= _mc_hashes[_mc_num_loaded_nodes - 1]);

							while (msgs[msg_ix].target != _mc_hashes[index]) {
								index++;
								_mc_num_inc_msgs[index] = 0;
							}

							_mc_num_inc_msgs[index]++;
						}

						assert(index < static_cast<size_t>(_mc_num_loaded_nodes));
					}

					// check that incoming messages to node do not exceed degree
					#ifndef NDEBUG
					{
						// check that num incoming msgs does not exceed degree
						for (node_t mc_node = 0; mc_node < _mc_num_loaded_nodes; mc_node++) {
							assert(_mc_num_inc_msgs[mc_node] <= _mc_degs[mc_node]);
						}
					};
					#endif

					// compute prefix sum of number of incoming messages for
					// each node
					__gnu_parallel::partial_sum
						(_mc_num_inc_msgs.cbegin(),
						 _mc_num_inc_msgs.cbegin() + _mc_num_loaded_nodes,
						 _mc_num_inc_msgs_psum.begin() + 1);

					assert(static_cast<size_t>(_mc_num_inc_msgs_psum[_mc_num_loaded_nodes]) == msgs.size());

					#ifndef NDEBUG
					std::vector<int> is_read(msgs.size(), 0);
					#endif

					// Building adjacency structure
					#pragma omp parallel for num_threads(_num_threads)
					for (degree_t presum_id = 0;
						 presum_id < static_cast<degree_t>(_mc_thread_bounds.size());
						 presum_id++) {
						for (node_t mc_node = _mc_thread_bounds.l_bounds[presum_id];
							 mc_node < _mc_thread_bounds.u_bounds[presum_id];
							 mc_node++) {
							for (degree_t inc_msg_id = 0;
								 inc_msg_id < _mc_num_inc_msgs[mc_node];
								 inc_msg_id++) {
								// insert
								_mc_adjacency_list.insert_neighbour_at
									(mc_node,
									 msgs[_mc_num_inc_msgs_psum[mc_node]
										  + inc_msg_id].neighbour,
									 inc_msg_id);
								#ifndef NDEBUG
								is_read[_mc_num_inc_msgs_psum[mc_node] + inc_msg_id] = 1;
								#endif
							}
							// set offset
							_mc_adjacency_list.set_offset(mc_node, _mc_num_inc_msgs[mc_node]);
						} // insert neighbours
					} // insert batch of neighbours of nodes (no conflict) in parallel

					// if odd number of nodes, insert those messages too
					// these are later forwarded but not used
					if (_mc_num_loaded_nodes % 2 == 1) {
						const node_t mc_last_node = _mc_num_loaded_nodes - 1;

						for (degree_t inc_msg_id = 0;
							 inc_msg_id < _mc_num_inc_msgs[mc_last_node];
							 inc_msg_id++)
						{
							// insert
							_mc_adjacency_list.insert_neighbour_at
								(mc_last_node,
								 msgs[_mc_num_inc_msgs_psum[mc_last_node] + inc_msg_id].
									 neighbour,
								 inc_msg_id);

							#ifndef NDEBUG
							is_read[_mc_num_inc_msgs_psum[mc_last_node] + inc_msg_id] = 1;
							#endif
						}
						// set offset
						_mc_adjacency_list.set_offset(mc_last_node,
													  _mc_num_inc_msgs[mc_last_node]);
					}

					// check whether all messages have been considered
					#ifndef NDEBUG
					{
						for (msgid_t msgid = 0; msgid < static_cast<msgid_t>(msgs.size()); msgid++)
							assert(is_read[msgid] == 1);

						for (node_t sc_node_inc_msg = 0;
							 sc_node_inc_msg < make_even_by_sub(_mc_num_loaded_nodes);
							 sc_node_inc_msg++) {
							assert(_mc_num_inc_msgs[sc_node_inc_msg] <=
								   _mc_adjacency_list.received_msgs(sc_node_inc_msg));
							assert(_mc_adjacency_list.received_msgs(sc_node_inc_msg) <=
								   _mc_adjacency_list.degree_at(sc_node_inc_msg));
						}
					};
					#endif

					// check whether offsets in adjacency list are not too big
					#ifndef NDEBUG
					{
						uint_t degs_sum = 0;
						for (node_t node = 0; node < _mc_num_loaded_nodes; node++) {
							degs_sum += _mc_adjacency_list.received_msgs(node);
						}

						assert(degs_sum <= msgs.size());
					};
					#endif
				}

				IOStatistics trading_report;

				// process trades with degrees, inverses (in parallel!)
				for (uint32_t batch = 0; batch < _num_splits; batch++) {
					_b_min_mc_node = batch * _num_threads * _num_fanout;
					_b_max_mc_node = (batch + 1) * _num_threads * _num_fanout - 1;

					const node_t mc_t_max_node
						= _mc_thread_bounds.u_bounds[_b_max_mc_node];

					node_t mc_max_node;

					if (mc_id < _num_chunks - 1)
						mc_max_node = mc_t_max_node - 1;
					else {
						if (LIKELY(batch < _num_splits - 1))
							mc_max_node = mc_t_max_node - 1;
						else {
							mc_max_node = _mc_num_loaded_nodes - 1;
						}
					}

					// check whether last upper bound is set correctly
					#ifndef NDEBUG
					{
						if (mc_id == _num_chunks - 1 && batch == _num_splits - 1)
							assert(mc_max_node == _mc_num_loaded_nodes - 1);
					};
					#endif

					// last macrochunk might have odd num of nodes, we skip the last
					// but have to be able to insert into the 'useless' row
					_b_largest_hnode = _mc_hashes[mc_max_node];

					// batch largest hashed value cannot exceed macrochunk largest
					// hash value
					assert(_b_largest_hnode <= _mc_largest_hnode);

					#pragma omp parallel for num_threads(_num_threads)
					for (uint32_t bound_ix = batch * _num_threads * _num_fanout;
						 bound_ix < (batch + 1) * _num_threads * _num_fanout;
						 bound_ix++)
					{
						// iterate over own microchunk in 2-step
						for (node_t mc_node = _mc_thread_bounds.l_bounds[bound_ix];
							 mc_node < make_even_by_sub(_mc_thread_bounds.u_bounds[bound_ix]);
							 mc_node = mc_node + 2)
						{
							if (UNLIKELY(_mc_adjacency_list.has_traded(mc_node)
										 || _mc_adjacency_list.has_traded(mc_node + 1))) {
								continue;
							}

							// we subtract one, so that another thread cannot enter its workstealing phase
							if (UNLIKELY(std::atomic_fetch_sub(&_active_threads[mc_node], 1) < 0)) {
								std::atomic_fetch_add(&_active_threads[mc_node], 1);
								continue;
							}
							if (UNLIKELY(std::atomic_fetch_sub(&_active_threads[mc_node + 1], 1) < 0)) {
								std::atomic_fetch_add(&_active_threads[mc_node], 1);
								std::atomic_fetch_add(&_active_threads[mc_node + 1], 1);
								continue;
							}

							const int thread_id = omp_get_thread_num();

							// again remember:
							// we identify hash-values with the macrochunk id,
							// therefore use mc_node
							const node_t mc_node_u = mc_node;
							const node_t mc_node_v = mc_node + 1;

							assert(_mc_has_traded[mc_node_u] == _mc_adjacency_list.has_traded(mc_node_u));
							assert(_mc_has_traded[mc_node_v] == _mc_adjacency_list.has_traded(mc_node_v));
							assert(_mc_has_traded[mc_node_u] == _mc_has_traded[mc_node_v]);

							if (!_mc_adjacency_list.has_traded(mc_node_u)
								&& !_mc_adjacency_list.has_traded(mc_node_v)) {
								// not traded yet

								// check sentinel
								assert(*(_mc_adjacency_list.cbegin(mc_node_u + 1) - 1) == LISTROW_END);
								assert(*(_mc_adjacency_list.cbegin(mc_node_v + 1) - 1) == LISTROW_END);

								// ========= retrieve respective neighbours ========

								// we use mc_invs[mc_hashes[mc_node_u]] since again,
								// cleartext in neighbours

								// check number inc messages
								if (!_mc_adjacency_list.tradable(mc_node_u, mc_node_v))
								{
									std::atomic_fetch_add(&_active_threads[mc_node_u], 1);
									std::atomic_fetch_add(&_active_threads[mc_node_v], 1);

									continue;
								}

								// actual trading happens here
								// call the lambda here
								// no works-sttealing therefore false
								trade(mc_node_u,
									  mc_node_v,
									  _mc_adjacency_list.get_edge_in_partner(mc_node_u),
									  false,
									  _t_common_neighbours[thread_id],
									  _t_disjoint_neighbours[thread_id],
									  thread_id
								);
							} else {
								// if two nodes have already been traded

								std::atomic_fetch_add(&_active_threads[mc_node_u], 1);
								std::atomic_fetch_add(&_active_threads[mc_node_v], 1);
							}
						} // for-loop over nodes in microchunk
					} // omp parallel for-loop over microchunks

					#ifdef BATCH_DEPS
					std::cout << "Batch dependencies: " << batch_dep_count.load() << std::endl;
					// reset counter
					batch_dep_count.store(0);
					#endif
				} // for-loop over batches

				// sequentially process the last node
				// do not forget to send messages of possibly left out node
				// (e.g. when given odd number of nodes)
				// this can only happen at last macrochunk
				// this node must have received all messages
				// since its hash is maximal
				if (_mc_num_loaded_nodes % 2 == 1) {
					const node_t mc_last_node = _mc_num_loaded_nodes - 1;

					assert(mc_id == _num_chunks - 1);
					assert(_mc_adjacency_list.received_msgs(mc_last_node) == _mc_degs[mc_last_node]);

					// send edges directly to new round
					const hnode_t hnext_last =
						_hash_funcs.next_hash(_mc_invs[mc_last_node]);

					for (auto neigh_it = _mc_adjacency_list.begin(mc_last_node);
						 neigh_it != _mc_adjacency_list.end(mc_last_node);
						 neigh_it++)
					{
						const node_t neighbour = *neigh_it;
						const hnode_t hnext_last_neigh = _hash_funcs.next_hash(neighbour);

						if (hnext_last < hnext_last_neigh)
							_pending.push(NeighbourMsg{hnext_last, neighbour}); //sequential
						else
							_pending.push(NeighbourMsg{hnext_last_neigh,
													   _mc_invs[mc_last_node]}); //sequential
					} // for-loop over neighbours of last _odd_ node

					_mc_adjacency_list.set_traded(mc_last_node);
					//_mc_adjacency_list.reset_row(mc_last_node);
				} // sending of (odd) last nodes messages


				// force insertion of remaining messages from the next macrochunk
				if (LIKELY(mc_id + 1 < _num_chunks))
					_active.force_push(mc_id + 1);

				// reset internal data structures
				_mc_last_largest_hnode = _mc_largest_hnode;
				_mc_last_hash_offset = _mc_hash_offset;
				_g_num_processed_nodes += _mc_num_loaded_nodes;

				reset();

				trading_report.report("Trading");
			} // for-loop over macrochunks

			// check if all information was extracted
			assert(_target_infos.empty());

			_has_run = true;
		}

		void inline organize_neighbors(const node_t mc_node_u) {
			std::sort(_mc_adjacency_list.begin(mc_node_u),
					  _mc_adjacency_list.end(mc_node_u));
			//quickSort(_mc_adjacency_list.begin(mc_node_u),
			//          _mc_adjacency_list.end(mc_node_u));
		}

		// mc_node_x: mc_node_u or mc_node_v
		void send_message(const node_t mc_node_x, const node_t neighbour,
						  const int thread_id) {
			const hnode_t h_neighbour = _hash_funcs.current_hash(neighbour);

			// if neighbours hash is larger
			if (_mc_hashes[mc_node_x] < h_neighbour)
				// if hash fits into this batch => dependency
				if (UNLIKELY(h_neighbour <= _b_largest_hnode)) {
					#ifdef BATCH_DEPS
					std::atomic_fetch_add(&batch_dep_count, 1ul);
					#endif
					// the neighbour is in cleartext
					// we need its macrochunk-id,
					// which is given by rank of hash(neighbour)
					// sc_node_u is the rank of u in the macrochunk,
					// we need u as cleartext
					const auto mc_neighbour_it =
						std::lower_bound(_mc_hashes.begin() + mc_node_x + 1,
										 _mc_hashes.begin()
										 + _mc_thread_bounds.u_bounds[_b_max_mc_node],
										 h_neighbour);

					const auto mc_neighbour_signed = mc_neighbour_it - _mc_hashes.begin();

					const auto mc_neighbour = static_cast<node_t>(mc_neighbour_signed);

					// check if found element is equal to searched one
					assert(mc_neighbour_it != _mc_hashes.begin() + _mc_num_loaded_nodes);
					assert(*mc_neighbour_it == h_neighbour);
					assert(mc_node_x < mc_neighbour);
					assert(mc_node_x >= _mc_thread_bounds.l_bounds[_b_min_mc_node]);
					assert(!_mc_has_traded[mc_neighbour]);

					// we acquire the lock for mc_neighbour in order to wait for the
					// originally assigned thread to skip this node
					// when obtaining the lock we add the neighbour,
					// so no conflict can arise, or when another thread wants to send
					// a message to that node
					//_node_locks[mc_neighbour].lock();
					while (std::atomic_fetch_sub(&_active_threads[mc_neighbour], 1) < 0) {
						std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
					}

					if (mc_neighbour % 2 == 0) {
						const node_t mc_partner = mc_neighbour + 1;
						assert(!_mc_has_traded[mc_partner]);

						const degree_t received_msgs =
							_mc_adjacency_list.received_msgs(mc_neighbour);
						// not last message of this node
						if (received_msgs < _mc_degs[mc_neighbour] - 1) {
							_mc_adjacency_list.insert_neighbour_without_check_lock
								(mc_neighbour, _mc_invs[mc_node_x]);

							std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
							return;
						}

						// received_msgs == _mc_degs[mc_neighbour] - 1

						// check if partner received all necessary messages
						const bool partner_in_neighbour =
							_mc_adjacency_list.get_edge_in_partner(mc_neighbour);

						const degree_t p_received_msgs =
							_mc_adjacency_list.received_msgs(mc_partner);

						if (p_received_msgs < _mc_degs[mc_partner] - 1 - partner_in_neighbour) {
							_mc_adjacency_list.insert_neighbour_without_check
								(mc_neighbour, _mc_invs[mc_node_x]);

							std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
							return;
						}

						// get lock on the partner
						while (std::atomic_fetch_sub(&_active_threads[mc_partner], 1) < 0) {
							// this node now wants to insert its last message into the
							// adjacency row, in order to not invalidate the number of
							// inserted messages into its partner row we acquired the lock.
							// if now the partner node is currently processed as well we
							// just insert without worries, since odd partners acquire the lock
							// prior to writing (if critical, e.g. received msgs >= deg - 2)
							const degree_t updated_p_received_msgs =
								_mc_adjacency_list.received_msgs(mc_partner);

							if (updated_p_received_msgs == _mc_degs[mc_partner] - 1 - partner_in_neighbour) {
								_mc_adjacency_list.insert_neighbour_without_check_lock
									(mc_neighbour, _mc_invs[mc_node_x]);

								std::atomic_fetch_add(&_active_threads[mc_partner], 1);
								std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
								return;
							} else {
								std::atomic_fetch_add(&_active_threads[mc_partner], 1);
							}
						}

						// this value cannot change since we have the lock now
						const degree_t partner_received_msgs =
							_mc_adjacency_list.received_msgs(mc_partner);

						// received_msgs == _mc_degs[mc_neighbour] - 1
						//     and got partner lock
						// => last message to this node
						_mc_adjacency_list.insert_neighbour_without_check_lock
							(mc_neighbour, _mc_invs[mc_node_x]);

						if (partner_received_msgs
							== _mc_degs[mc_partner] - partner_in_neighbour) {
							std::vector<node_t> common_neighbours;
							std::vector<node_t> disjoint_neighbours;

							trade(mc_neighbour, mc_partner,
								  partner_in_neighbour,
								  true,
								  common_neighbours,
								  disjoint_neighbours,
								  thread_id);
						} else {
							// not worksteal tradable
							std::atomic_fetch_add(&_active_threads[mc_partner], 1);
							std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
							return;
						}
					} else {
						// mc_neighbour odd
						const node_t mc_partner = mc_neighbour - 1;
						assert(!_mc_has_traded[mc_partner]);

						const degree_t received_msgs =
							_mc_adjacency_list.received_msgs(mc_neighbour);
						const bool neighbour_in_partner =
							_mc_adjacency_list.get_edge_in_partner(mc_partner);

						// not at last message therefore we can just simply insert
						// without any workstealing attempts
						if (received_msgs < _mc_degs[mc_neighbour] - 1 - neighbour_in_partner) {
							_mc_adjacency_list.insert_neighbour_without_check_lock
								(mc_neighbour, _mc_invs[mc_node_x]);

							std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
							return;
						}

						const degree_t p_received_msgs =
							_mc_adjacency_list.received_msgs(mc_partner);

						if (p_received_msgs < _mc_degs[mc_partner] - 1) {
							_mc_adjacency_list.insert_neighbour_without_check
								(mc_neighbour, _mc_invs[mc_node_x]);

							std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
							return;
						}

						// we acquire the lock on the partner node since, we need to know,
						// if this node has been inserted into its partner row.
						// if so we need deg(v) - 1 nodes in our own row, since the edge
						// is directed into the partner row
						while (std::atomic_fetch_sub(&_active_threads[mc_partner], 1) < 0) {
							std::atomic_fetch_add(&_active_threads[mc_partner], 1);
						}

						// last message now is trying to be sent
						// we have both locks for the partner and the neighbour,
						// insert with check
						const bool tradable = _mc_adjacency_list.insert_neighbour_check
							(mc_neighbour, _mc_invs[mc_node_x]);

						if (!tradable) {
							assert(_mc_adjacency_list.received_msgs(mc_partner) != _mc_degs[mc_partner]);

							// release both locks
							std::atomic_fetch_add(&_active_threads[mc_partner], 1);
							std::atomic_fetch_add(&_active_threads[mc_neighbour], 1);
							return;
						} else {
							// tradable
							// worksteal trade
							std::vector<node_t> common_neighbours;
							std::vector<node_t> disjoint_neighbours;

							trade(mc_partner, mc_neighbour,
								  neighbour_in_partner,
								  true,
								  common_neighbours,
								  disjoint_neighbours,
								  thread_id);
						}
					}
				} else {
					if (h_neighbour <= _mc_largest_hnode) {
						// this will never be 0, since we insert at least into another batch
						// such that h_neighbour is large enough
						const node_t mc_lower = h_neighbour
												- _g_num_processed_nodes // id in mc
												- _mc_hash_offset; // current offset

						const node_t mc_upper = std::min(h_neighbour
														 - _g_num_processed_nodes // id in mc
														 - _mc_last_hash_offset // last offset
														 + 1, // strictly larger
														 _mc_num_loaded_nodes); // has to be strictly larger

						// send to other batch => no dependency
						#if 0
						const auto mc_neighbour_it =
										std::lower_bound
														(_mc_hashes.begin() +
														 _mc_thread_bounds.u_bounds[_b_max_mc_node],
														 _mc_hashes.begin() + _mc_num_loaded_nodes,
														 h_neighbour);
						#else
						const auto mc_neighbour_it =
							std::lower_bound(_mc_hashes.cbegin() + mc_lower,
											 _mc_hashes.cbegin() + mc_upper,
											 h_neighbour);
						#endif

						const auto mc_neighbour_signed = mc_neighbour_it - _mc_hashes.begin();

						const auto mc_neighbour = static_cast<node_t>(mc_neighbour_signed);

						// check if found element is equal to searched one
						assert(mc_neighbour_it != _mc_hashes.begin() + mc_upper);
						assert(*mc_neighbour_it == h_neighbour);
						// check some inequalities
						assert(mc_neighbour > _mc_thread_bounds.l_bounds[_b_max_mc_node]);
						assert(mc_neighbour >= _mc_thread_bounds.u_bounds[_b_max_mc_node]);
						assert(h_neighbour >= _g_num_processed_nodes);
						assert(_mc_hash_offset >= _mc_last_hash_offset);
						assert(mc_lower <= mc_upper);
						assert(mc_lower <= mc_neighbour);
						assert(mc_neighbour <= mc_upper);

						_mc_adjacency_list.insert_neighbour_without_check
							(mc_neighbour, _mc_invs[mc_node_x]);
					} else
						// send to other macrochunk => no dependency
						_active.push(NeighbourMsg{h_neighbour,
												  _mc_invs[mc_node_x]},
									 thread_id);
				}
			else {
				// neighbours hash is not larger, therefore send to _pending
				// we need hash(u) and hash(neighbour) of the next hash_func
				const hnode_t hnext_x = _hash_funcs.next_hash(_mc_invs[mc_node_x]);
				const hnode_t hnext_neighbour = _hash_funcs.next_hash(neighbour);

				if (hnext_x < hnext_neighbour)
					// u comes before neighbour
					_pending.push(NeighbourMsg{hnext_x, neighbour}, thread_id);
				else
					// neighbour comes before u
					_pending.push(NeighbourMsg{hnext_neighbour, _mc_invs[mc_node_x]},
								  thread_id);
			} // if edge [u, neighbour] not needed in this round
		} // end send_messages

		void trade(const node_t mc_tradenode_u, const node_t mc_tradenode_v,
				   const bool mc_v_in_mc_u,
				   const bool work_stealing_flag,
				   std::vector<node_t> &common_neighbours,
				   std::vector<node_t> &disjoint_neighbours,
				   const int thread_id) {
			assert(mc_tradenode_u == mc_tradenode_v - 1); //  we trade smaller id with smaller id + 1
			assert(!_mc_has_traded[mc_tradenode_u]); // should not trade when already traded
			assert(!_mc_has_traded[mc_tradenode_v]);

			_mc_has_traded[mc_tradenode_u] = true;
			_mc_has_traded[mc_tradenode_v] = true;

			_mc_adjacency_list.set_traded(mc_tradenode_u);
			_mc_adjacency_list.set_traded(mc_tradenode_v);

			organize_neighbors(mc_tradenode_u);
			organize_neighbors(mc_tradenode_v);

			auto u_iter_end = _mc_adjacency_list.cend(mc_tradenode_u) - mc_v_in_mc_u;
			auto v_iter_end = _mc_adjacency_list.cend(mc_tradenode_v);

			const bool shared = mc_v_in_mc_u;

			// if we worksteal, we can just use max degree of both
			if (work_stealing_flag) {
				// disjoint vector can have both degs added together
				const degree_t disjoint_alloc =
					_mc_adjacency_list.degree_at(mc_tradenode_u)
					+ _mc_adjacency_list.degree_at(mc_tradenode_v);

				// common vector can only be min of both
				const degree_t min_deg_uv =
					std::min(_mc_adjacency_list.degree_at(mc_tradenode_u),
							 _mc_adjacency_list.degree_at(mc_tradenode_v));

				common_neighbours.reserve(static_cast<size_t>(min_deg_uv));
				disjoint_neighbours.reserve(static_cast<size_t>(disjoint_alloc));
			}

			auto u_neigh_iter = _mc_adjacency_list.cbegin(mc_tradenode_u);
			auto v_neigh_iter = _mc_adjacency_list.cbegin(mc_tradenode_v);

			while ((u_neigh_iter != u_iter_end) && (v_neigh_iter != v_iter_end)) {
				assert(*u_neigh_iter != _mc_invs[mc_tradenode_v]);
				assert(*v_neigh_iter != _mc_invs[mc_tradenode_u]);

				if (*u_neigh_iter > *v_neigh_iter) {
					disjoint_neighbours.push_back(*v_neigh_iter);
					v_neigh_iter++;
					continue;
				}
				if (*u_neigh_iter < *v_neigh_iter) {
					disjoint_neighbours.push_back(*u_neigh_iter);
					u_neigh_iter++;
					continue;
				}
				// *u_neigh_iter == *v_neigh_iter
				{
					common_neighbours.push_back(*u_neigh_iter);
					u_neigh_iter++;
					v_neigh_iter++;
				}
			}
			if (u_neigh_iter == u_iter_end)
				disjoint_neighbours.insert(disjoint_neighbours.end(),
										   v_neigh_iter,
										   v_iter_end);
			else
				disjoint_neighbours.insert(disjoint_neighbours.end(),
										   u_neigh_iter,
										   u_iter_end);

			// reset both rows, not necessarily needed, sets offsets_vector to 0
			//_mc_adjacency_list.reset_row(mc_tradenode_u);
			//_mc_adjacency_list.reset_row(mc_tradenode_v);

			const degree_t u_setsize =
				static_cast<degree_t>
				(u_iter_end
				 - _mc_adjacency_list.cbegin(mc_tradenode_u)
				 - common_neighbours.size());
			const degree_t v_setsize =
				static_cast<degree_t>
				(v_iter_end
				 - _mc_adjacency_list.cbegin(mc_tradenode_v)
				 - common_neighbours.size());

			// assign first u_setsize to sc_node_u: to get edge [u, *]
			// assign  last v_setsize to sc_node_v: to get edge [v, *]
			std::shuffle(disjoint_neighbours.begin(),
						 disjoint_neighbours.end(),
						 _rngs[thread_id]);

			// distribute disjoint neighbours
			// send messages for u
			for (degree_t mc_neighbour_id = 0;
				 mc_neighbour_id < u_setsize;
				 mc_neighbour_id++)
			{
				send_message(mc_tradenode_u,
							 disjoint_neighbours[mc_neighbour_id],
							 thread_id);
			}

			// send messages for v
			for (degree_t mc_neighbour_id = u_setsize;
				 mc_neighbour_id < u_setsize + v_setsize;
				 mc_neighbour_id++)
			{
				send_message(mc_tradenode_v,
							 disjoint_neighbours[mc_neighbour_id],
							 thread_id);
			}

			// distribute common neighbours
			for (const auto common : common_neighbours) {
				send_message(mc_tradenode_u, common, thread_id);
				send_message(mc_tradenode_v, common, thread_id);
			}

			// if edge existed between u and v, send edge [u, v] to next round
			if (shared) {
				const hnode_t hnext_u = _hash_funcs.next_hash(_mc_invs[mc_tradenode_u]);
				const hnode_t hnext_v = _hash_funcs.next_hash(_mc_invs[mc_tradenode_v]);

				if (hnext_u < hnext_v)
					// u comes before neighbour
					_pending.push(NeighbourMsg{hnext_u, _mc_invs[mc_tradenode_v]},
								  thread_id);
				else
					// neighbour comes before u
					_pending.push(NeighbourMsg{hnext_v, _mc_invs[mc_tradenode_u]},
								  thread_id);
			}

			if (!work_stealing_flag) {
				common_neighbours.clear();
				disjoint_neighbours.clear();
			}

			assert(_mc_has_traded[mc_tradenode_u]);// = true;
			assert(_mc_has_traded[mc_tradenode_v]);// = true;

			std::atomic_fetch_add(&_active_threads[mc_tradenode_u], 1);
			std::atomic_fetch_add(&_active_threads[mc_tradenode_v], 1);
		} // end auto trade

		void reset() {
			// check whether threadcounter is zero for all
			#ifndef NDEBUG
			{
				size_t n_count = 0;
				for (auto && tcounter : _active_threads) {
					int val = tcounter.load();
					assert(val == 0);
					n_count++;
				}
			};
			#endif

			// check whether all nodes have been traded
			#ifndef NDEBUG
			{
				for (node_t node = 0; node < _mc_num_loaded_nodes; node++) {
					assert(_mc_adjacency_list.has_traded(node));
				}
				for (node_t node = 0; node < _mc_num_loaded_nodes; node++) {
					assert(_mc_has_traded[node]);
				}
			};
			#endif

			std::fill(_mc_has_traded.begin(), _mc_has_traded.end(), false);

			std::fill(_mc_num_inc_msgs.begin(), _mc_num_inc_msgs.end(), 0);

			// collection of not needed resetting of data structure
			//std::fill(_mc_degs_psum.begin(), _mc_degs_psum.end(), 0);
			//std::fill(_mc_num_inc_msgs_psum.begin(), _mc_num_inc_msgs_psum.end(), 0);
			//std::fill(_mc_degs.begin(), _mc_degs.end(), 0);
			//std::fill(_mc_invs.begin(), _mc_invs.end(), 0);
			//std::fill(_mc_hashes.begin(), _mc_hashes.end(), 0);
			//_b_largest_hnode = 0;
			//_mc_largest_hnode = 0;
			//_mc_num_loaded_nodes = 0;

			_mc_adjacency_list.dealloc();
		}

		void set_new_bounds(const chunk_upperbound_vector &new_bounds) {
			_pending_upper_bounds = new_bounds;
			_pending.set_new_bounds(new_bounds);
		}

		void push(const NeighbourMsg &msg) {
			_active.push(msg);
		}

		void swap() {
			// initialize first macrochunk of next round
			_pending.force_push(0);

			_active.swap_with_next(_pending);

			// reset hash offsets and num processed nodes
			_g_num_processed_nodes = 0;
			_mc_hash_offset = 0;
			_mc_last_hash_offset = 0;

			_active_upper_bounds.swap(_pending_upper_bounds);
		}

		void finalize() {
			for (chunkid_t chunk_id = 0; chunk_id < _num_chunks; chunk_id++)
				_active.force_push(chunk_id);
		}

		size_t size() {
			size_t sum = 0;
			for (chunkid_t chunkid = 0; chunkid < _num_chunks; chunkid++) {
				sum += _active.get_size_of_chunk(chunkid);
			}

			return sum;
		}

		size_t adjacency_list_size() const {
			return static_cast<size_t>(_mc_max_num_msgs);
		}

		void resize_adjacency_list(const edgeid_t degree_count) {
			_mc_adjacency_list.resize(degree_count);
		}

		#ifndef NDEBUG
		// load messages in IM, debug uses
		msg_vector load_messages() {
			msg_vector out_msgs;

			for (chunkid_t chunkid = 0; chunkid < _num_chunks; chunkid++) {
				msg_vector msg_chunk = _active.get_messages_of(chunkid);

				out_msgs.insert(out_msgs.end(), msg_chunk.begin(), msg_chunk.end());
			}

			return out_msgs;
		}
		#endif

		void get_edges(OutReceiver &out_edges) {
			_active.push_into(out_edges);
		}
	};

}