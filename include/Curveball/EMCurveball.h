/**
 * @file EMCurveball.h
 * @date 27. September 2017
 *
 * @author Hung Tran
 */

#pragma once

#include "EMDualContainer.h"
#include "EdgeStream.h"
#include "defs.h"
#include <functional>
#include <stxxl/sorter>
#include <Utils/IOStatistics.h>
#include <DegreeStream.h>
#include <GenericComparator.h>
#include "Utils/Hashfuncs.h"
#include "EMTargetInformation.h"

namespace Curveball {

	/**
	 * The EM-PGCB algorithm randomizes the incoming edge stream in a sequence
	 * of global trades provided by the HashFactory.
	 *
	 * HashFactory has to support:
	 * 	- get_random()
	 * 	- hash()
	 * 	- min_value(), see STXXL comparator requirements
	 * 	- max_value(), see STXXL comparator requirements
	 *
	 * InputStream has to support a reading streaming interface:
	 * 	- operator*()
	 * 	- operator++()
	 * 	- empty()
	 *
	 * OutputStream has to support a writing streaming interface:
	 * 	- push()
	 * 	- empty()
	 *
	 * @tparam HashFactory
	 * @tparam InputStream Incoming edges.
	 * @tparam OutReceiver Randomized edge output
	 */
	template<typename HashFactory, typename DegreeInputStream = DegreeStream, typename InputStream = EdgeStream, typename OutReceiver = EdgeStream>
	class EMCurveball {
	public:
		using NodeSorter = stxxl::sorter<node_t, NodeComparator>;
		using EdgeSorter = stxxl::sorter<edge_t, EdgeComparator>;

		/**
		 * Given the number of edges, number of nodes, number of global trade
		 * rounds, the number of threads and the size of the internal memory
		 * in Byte this class estimates (potentially) suboptimal internal
		 * parameters.
		 */
		class ParameterEstimation {
		public:
			chunkid_t num_macrochunks() const   {return std::get<0>(_param_est);}
			chunkid_t num_batches() const       {return std::get<1>(_param_est);}
			chunkid_t num_fanout() const        {return std::get<2>(_param_est);}
			size_t size_insertionbuffer() const {return std::get<3>(_param_est);}

			ParameterEstimation() = default;
			ParameterEstimation(const size_t mem, const edgeid_t num_edges, const int num_threads)
				: _param_est(_compute(mem, num_edges, num_threads))
			{}

		protected:
			using parameter_type = std::tuple<chunkid_t, chunkid_t, chunkid_t, size_t>;
			const parameter_type _param_est;

		protected:
			parameter_type _compute(const size_t mem, const edgeid_t num_edges, const int num_threads) const {
				const chunkid_t num_macrochunks = std::max(2u, static_cast<chunkid_t>(2*num_edges/mem));
				const chunkid_t num_batches = 8 * num_macrochunks * num_threads;
				const chunkid_t num_fanout = 1;
				const size_t size_insertionbuffer = std::max(32ul, static_cast<size_t>(num_threads*16));
				
				assert(num_batches < num_edges);
				
				std::cout << "Using the following estimated parameters for Curveball:\n"
						  << "num_macrochunks:     \t" << num_macrochunks << "\n"
						  << "num batches:         \t" << num_batches << "\n"
						  << "num_fanout:          \t" << num_fanout << "\n"
						  << "size_insertionbuffer:\t" << size_insertionbuffer << std::endl;
				return std::make_tuple(num_macrochunks, num_batches, num_fanout, size_insertionbuffer);
			}
		};

	protected:
		ParameterEstimation _param_est;

		InputStream &_edges;
		DegreeInputStream &_degrees;
		node_t _num_nodes;
		const tradeid_t _num_rounds;
		OutReceiver &_out_edges;
		const chunkid_t _num_chunks;
		const chunkid_t _num_splits;
		const chunkid_t _num_fanout;
		const uint_t _target_sorter_mem_size;
		const uint_t _token_sorter_mem_size;
		const msgid_t _msg_limit;
		const int _num_threads;
		const msgid_t _insertion_buffer_size;

		EdgeSorter _edge_sorter;
		const bool _sorted_output;

#ifndef NDEBUG
		NodeSorter _debug_node_tokens;

		bool display_debug = true;
#endif

	public:
		EMCurveball() = delete;

		EMCurveball(EMCurveball &) = delete;

		/**
		 * Sets every parameter of the EM-PGCB algorithm (internal ones too).
		 *
		 * @param edges Edges as stream
		 * @param degrees Degree sequence as stream
		 * @param num_nodes Number of nodes
		 * @param num_rounds Number of global trade rounds
		 * @param out_edges Output edges as stream
		 * @param num_chunks Number of macrochunks
		 * @param num_splits Number of batches
		 * @param num_fanout Multiplier per batch processing
		 * @param target_sorter_mem_size Block size for aux. info sorter in Byte
		 * @param token_sorter_mem_size Block size for final edge sorter in Byte
		 * @param msg_limit Maximum number of messages fitting into main memory
		 * @param num_threads Number of threads
		 * @param insertion_buffer_size Size of insertion buffer per thread
		 */
		EMCurveball(InputStream &edges,
					DegreeInputStream &degrees,
					const node_t num_nodes,
					const tradeid_t num_rounds,
					OutReceiver &out_edges,
					const chunkid_t num_chunks = DUMMY_CHUNKS,
					const chunkid_t num_splits = DUMMY_Z,
					const chunkid_t num_fanout = DUMMY_Z,
					const uint_t target_sorter_mem_size = DUMMY_SIZE,
					const uint_t token_sorter_mem_size = DUMMY_SIZE,
					const msgid_t msg_limit = DUMMY_LIMIT,
					const int num_threads = DUMMY_THREAD_NUM,
					const msgid_t insertion_buffer_size = DUMMY_INS_BUFFER_SIZE,
					const bool sorted_output = true
		) :
			_edges(edges),
			_degrees(degrees),
			_num_nodes(num_nodes),
			_num_rounds(num_rounds),
			_out_edges(out_edges),
			_num_chunks(num_chunks),
			_num_splits(num_splits),
			_num_fanout(num_fanout),
			_target_sorter_mem_size(target_sorter_mem_size),
			_token_sorter_mem_size(token_sorter_mem_size),
			_msg_limit(msg_limit),
			_num_threads(num_threads),
			_insertion_buffer_size(insertion_buffer_size),
			_edge_sorter(EdgeComparator{}, token_sorter_mem_size),
			_sorted_output(sorted_output)
		#ifndef NDEBUG
			, _debug_node_tokens(NodeComparator{}, token_sorter_mem_size)
		#endif
		{
			// this assert needs a rewound stream, maybe use edges.size() > 0
			assert(!_edges.empty());
			assert(num_rounds > 0);
		}

		/**
		 * Sets the essential parameters of the EM-PGCB algorithm, estimates
		 * internally used implementation aware parameters.
		 *
		 * @param edges Edges as stream
		 * @param degrees Degree sequence as stream
		 * @param num_nodes Number of nodes
		 * @param num_rounds Number of global trade rounds
		 * @param mem Size of main memory in Byte
		 * @param num_threads Number of threads
		 */
		EMCurveball(InputStream &edges,
					DegreeInputStream &degrees,
					const node_t num_nodes,
					const tradeid_t num_rounds,
					OutReceiver &out_edges,
					const int num_threads,
					const size_t mem,
					const bool sorted_output
		) :
			_param_est(mem, edges.size(), num_threads),
			_edges(edges),
			_degrees(degrees),
			_num_nodes(num_nodes),
			_num_rounds(num_rounds),
			_out_edges(out_edges),
			_num_chunks(_param_est.num_macrochunks()),
			_num_splits(_param_est.num_batches()),
			_num_fanout(_param_est.num_fanout()),
			_target_sorter_mem_size(1 * UIntScale::Gi),
			_token_sorter_mem_size(1 * UIntScale::Gi),
			_msg_limit(std::numeric_limits<msgid_t>::max()),
			_num_threads(num_threads),
			_insertion_buffer_size(_param_est.size_insertionbuffer()),
			_edge_sorter(EdgeComparator{}, 1 * UIntScale::Gi),
			_sorted_output(sorted_output)
		#ifndef NDEBUG
			, _debug_node_tokens(NodeComparator{}, 1 * UIntScale::Gi)
		#endif
		{
			// this assert needs a rewound stream, maybe use edges.size() > 0
			assert(!_edges.empty());
			assert(num_rounds > 0);
            assert(_num_chunks > 0);
            assert(_num_splits > 0);
            assert(_num_fanout > 0);
            assert(_num_splits < _edges.size());
		}

		/**
		 * Runs the algorithm.
		 * The output is put into the given output edge stream.
		 */
		void run() {
			// initialize k random hash functions and last as identity
			Hashfuncs<HashFactory> hash_funcs(_num_nodes, _num_rounds);

			EMTargetInformation target_infos(_num_chunks, _num_nodes);


			// initialize helper data structure for degree and inverse
			// of the form <h(u), deg(u), u>.
			IOStatistics first_fill_report;
			degree_t max_degree = 0;
			for (node_t node = 0; !_degrees.empty(); ++_degrees) {
                const auto degree_value = static_cast<degree_t>(*_degrees);
				// determine maximum degree while scanning degrees
				max_degree = std::max(max_degree, *_degrees);

				// in the initialization phase this can be done for both
				// the current and subsequent round
				target_infos.push_active(
					TargetMsg{hash_funcs[0].hash(node), degree_value, node}
				);

				target_infos.push_pending(
					TargetMsg{hash_funcs[1].hash(node), degree_value, node}
				);

				node++;
			}
			first_fill_report.report("FirstFill");

            assert(_num_splits < _edges.size());
			IOStatistics ds_init_report;
			// initialize message container
			EMDualContainer<HashFactory> msgs_container
				(target_infos.get_bounds_active(),
				 target_infos.get_bounds_pending(),
				 target_infos,
				 _num_nodes,
				 max_degree,
				 hash_funcs,
				 CurveballParams{_num_rounds,
								 _num_chunks,
								 _num_splits,
								 _num_fanout,
								 _token_sorter_mem_size,
								 _msg_limit,
								 _num_threads,
								 _insertion_buffer_size});


			ds_init_report.report("DualContainerInit");

			{
				IOStatistics first_msgs_push_report("InitialMessagePush");
				// push neighbour messages into data structure
				// directed according to earlier trade
				for (; !_edges.empty(); ++_edges) {
					const edge_t edge = *_edges;

					assert(edge.first != edge.second); // no self-loops
                    assert(edge.first >= 0);
                    assert(edge.first <= _num_nodes);
                    assert(edge.second >= 0);
                    assert(edge.second <= _num_nodes);

					const hnode_t h_fst = hash_funcs.current_hash(edge.first);
					const hnode_t h_snd = hash_funcs.current_hash(edge.second);

                    assert(h_fst >= 0);
                    assert(h_snd >= 0);

					// compare targets, and direct accordingly
					if (h_fst < h_snd)
						msgs_container.push(NeighbourMsg{h_fst, edge.second});
					else
						msgs_container.push(NeighbourMsg{h_snd, edge.first});
				}
			}

			// process all global trades one by one
			for (tradeid_t round = 0; round < _num_rounds; round++) {
				// process the global trade
				msgs_container.process_active();

				// reinitialize data structures
				++hash_funcs; // move to next hash-function

				// swap pending with active containers
				msgs_container.swap();
				target_infos.swap();

				// push new auxiliary info into helper
				if (round < _num_rounds - 1) {
					IOStatistics next_infos("NextInfos");

					// clear obsolete containers
					target_infos.clear_pending();

					// refill obsolete containers
					_degrees.rewind();
					for (node_t node = 0; node < _num_nodes; node++, ++_degrees) {
                        const auto degree_value = static_cast<degree_t>(*_degrees);
						target_infos.push_pending(
							TargetMsg{hash_funcs.next_hash(node),
									  degree_value,
									  node});
					}
					assert(_degrees.empty());

					// compute new bounds on the containers for msg insertion
					const auto new_bounds = target_infos.get_bounds_pending();

					msgs_container.set_new_bounds(new_bounds);
				}
			}
			// check whether all hash-functions have been processed
			assert(hash_funcs.at_last());

			// clear insertion buffers and finish up
			msgs_container.finalize();

			{
				_out_edges.clear();

				if (_sorted_output) {
					// provide randomised edge-list in a sorted order
					IOStatistics get_edges_report("PushEdgeStream");

					msgs_container.forward_unsorted_edges(_edge_sorter);

					// retrieve a sorted output
					_edge_sorter.sort();

					StreamPusher<EdgeSorter, OutReceiver>(_edge_sorter, _out_edges);
				} else {
					// provide randomised edge-list
					IOStatistics get_edges_report("PushEdgeStream");

					msgs_container.forward_unsorted_edges(_out_edges);
				}
			}
		}
	};

}
