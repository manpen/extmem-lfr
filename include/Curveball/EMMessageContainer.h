/**
 * @file EMMessageContainer.h
 * @date 28. September 2017
 *
 * @author Hung Tran
 */
#pragma once

#include "defs.h"
#include "IMMacrochunk.h"
#include "Utils/Hashfuncs.h"
#include <stx/btree_map>

namespace Curveball {

template<typename HashFactory, typename OutReceiver = EdgeStream>
class EMMessageContainer {
public:
		using chunk_vector = std::vector<IMMacrochunk<OutReceiver>>;
		using chunk_upperbound_vector = std::vector<hnode_t>;
		using bound_tree = stx::btree_map<hnode_t, chunkid_t>;
		using insertion_buffer_vector =
		std::vector<std::vector<std::vector<NeighbourMsg>>>;
		enum Mode {
				ACTIVE, NEXT
		};

protected:
		const chunkid_t _num_chunks;
		chunk_vector _macrochunks;
		bound_tree _upper_bounds;
		Mode _mode;
		const int _num_threads;
		const msgid_t _insertion_buffer_size;
		insertion_buffer_vector _insertion_buffer_vector;

public:
		EMMessageContainer() = delete;

		EMMessageContainer(const EMMessageContainer &) = delete;

		EMMessageContainer(EMMessageContainer &&other) noexcept :
			_num_chunks(other._num_chunks),
			_num_threads(other._num_threads),
			_insertion_buffer_size(other._insertion_buffer_size) {
			assert(_num_chunks == other._num_chunks);
			assert(_num_threads == other._num_threads);
			assert(_insertion_buffer_size == other._insertion_buffer_size);

			std::swap(_mode, other._mode);
			std::swap(_macrochunks, other._macrochunks);
			std::swap(_upper_bounds, other._upper_bounds);
		}

		EMMessageContainer(const chunk_upperbound_vector &upper_bounds,
											 const msgid_t msg_limit,
											 const Mode mode,
											 const int num_threads,
											 const msgid_t insertion_buffer_size)
						: _num_chunks(static_cast<chunkid_t>(upper_bounds.size())),
						  _mode(mode),
						  _num_threads(num_threads),
						  _insertion_buffer_size(insertion_buffer_size) {
			_macrochunks.reserve(_num_chunks);

			// initialize macrochunks and hashmap (target -> macrochunk_id)
			for (chunkid_t id = 0; id < _num_chunks; id++) {
				_macrochunks.emplace_back(id, msg_limit);

				_upper_bounds.insert(upper_bounds[id], id);
			}

			// initialize insertion buffers
			_insertion_buffer_vector.reserve(static_cast<size_t>(_num_threads));
			for (int thread_id = 0; thread_id < _num_threads; thread_id++) {
				_insertion_buffer_vector.emplace_back();
				_insertion_buffer_vector[thread_id].reserve(_num_chunks);
				for (chunkid_t chunk_id = 0; chunk_id < _num_chunks; chunk_id++) {
					_insertion_buffer_vector[thread_id].emplace_back();
					_insertion_buffer_vector[thread_id][chunk_id].
						reserve(static_cast<size_t>(_insertion_buffer_size));
				}
			}
		}

		void set_new_bounds(const chunk_upperbound_vector &new_bounds) {
			assert(new_bounds.size() == _num_chunks);

			_upper_bounds.clear();

			// reinitialize hashmap (target -> macrochunk_id)
			for (chunkid_t id = 0; id < _num_chunks; id++) {
				_upper_bounds.insert(new_bounds[id], id);
			}
		}

		bool empty() const {
			for (auto &&macrochunk : _macrochunks) {
				if (macrochunk.get_msg_count() == 0)
					return true;
			}

			return false;
		}

		msgid_t get_size_of_chunk(const chunkid_t chunk_id) {
			// check whether expected number of messages arrived
			#ifndef NDEBUG
			{
				msg_vector msgs;
				_macrochunks[chunk_id].load_messages(msgs);

				assert(chunk_id < _num_chunks);
				assert(_macrochunks[chunk_id].get_msg_count() == msgs.size());
			};
			#endif

			return _macrochunks[chunk_id].get_msg_count();
		}

		chunkid_t get_target_chunkid(const hnode_t target) const {
			auto chunktree_it = _upper_bounds.upper_bound(target);

			// since last element in tree is highest hash, can never be end
			assert(chunktree_it != _upper_bounds.end());

			return chunktree_it.data();
		}

		// used sequentially in the initialization phase (without locks)
		void push(const NeighbourMsg &msg) {
			const chunkid_t target_chunk = get_target_chunkid(msg.target);

			_macrochunks[target_chunk].push_sequential(msg);
		}

		// is used to force push left over msgs to restart with new macrochunk,
		// most efficient way is to just do this sequentially
		void force_push(const chunkid_t mc_id) {
			for (int thread_id = 0; thread_id < _num_threads; thread_id++) {
				// bulk push these
				_macrochunks[mc_id].bulk_push_sequential
					(_insertion_buffer_vector[thread_id][mc_id]);

				// clear buffers
				_insertion_buffer_vector[thread_id][mc_id].clear();
				_insertion_buffer_vector[thread_id][mc_id].
					reserve(static_cast<size_t>(_insertion_buffer_size));
			}
		}

		// is used while trading
		void push(const NeighbourMsg &msg, const int thread_id) {
			const chunkid_t target_chunk = get_target_chunkid(msg.target);

			if (_insertion_buffer_vector[thread_id][target_chunk].size() <
				static_cast<size_t>(_insertion_buffer_size) - 1) {
				// put into insertion buffer
				_insertion_buffer_vector[thread_id][target_chunk].push_back(msg);
			} else {
				// insert last msg, then bulk push
				_insertion_buffer_vector[thread_id][target_chunk].push_back(msg);
				_macrochunks[target_chunk].bulk_push
					(_insertion_buffer_vector[thread_id][target_chunk]);

				// clear buffer
				_insertion_buffer_vector[thread_id][target_chunk].clear();
				_insertion_buffer_vector[thread_id][target_chunk].
					reserve(static_cast<size_t>(_insertion_buffer_size));
			}
		}

		msg_vector get_messages_of(const chunkid_t chunkid) {
			msg_vector msgs;
			_macrochunks[chunkid].load_messages(msgs);

			return msgs;
		}

		void activate() {
			assert(_mode == NEXT);

			_mode == ACTIVE; // invariants, for code readability
		}

		void reset() {
			assert(_mode == ACTIVE);

			for (auto &&macrochunk : _macrochunks)
				macrochunk.reset();

			_mode == NEXT; // invariants, for code readability
		}

		// note, num_chunks is not swapped, and should be the same for each
		// it's possible to change this, remove const identifier from _num_chunks
		void swap_with_next(EMMessageContainer &other) {
			//  check if no message remained in the buffers
			#ifndef NDEBUG
			{
				// for this structures buffers
				for (const auto &thread_buffer_vec : _insertion_buffer_vector)
					for (const auto &thread_buffer : thread_buffer_vec)
						assert(thread_buffer.size() == 0);

				// and for the other structures first buffer
				// (first macrochunk is processed first)
				for (const auto &o_thread_buffer_vec : other._insertion_buffer_vector)
					assert(o_thread_buffer_vec[0].size() == 0);
			};
			#endif

			reset();
			other.activate();

			std::swap(_macrochunks, other._macrochunks);
			std::swap(_upper_bounds, other._upper_bounds);
			std::swap(_insertion_buffer_vector, other._insertion_buffer_vector);
		}

		void push_into(OutReceiver &out_edges) {
			for (chunkid_t mc_id = 0; mc_id < _num_chunks; mc_id++) {
				_macrochunks[mc_id].push_into(out_edges);
			}
		}
};

}