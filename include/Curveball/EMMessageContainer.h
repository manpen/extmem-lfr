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

	/**
	 * Data structure for a global trade.
	 *
	 * @tparam HashFactory Type of hash-functions.
	 */
	template<typename HashFactory>
	class EMMessageContainer {
	public:
		using chunk_vector = std::vector<IMMacrochunk>;
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

		/**
		 * Move constructor, needed for swap operations.
		 * @param other Other global trade container.
		 */
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

		/**
		 * Sets up the data structure for a global trade.
		 *
		 * @param upper_bounds Vector of bounds for target forwarding.
		 * @param msg_limit Maximum number of messages in a macrochunk.
		 * @param mode Active or next round.
		 * @param num_threads Number of threads.
		 * @param insertion_buffer_size Size of insertion buffer.
		 */
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

		/**
		 * Resets the bounds of the old container for the next global trade
		 * rounds. Is used to determine in which container/queue incoming
		 * messages have to be forwarded to.
		 * @param new_bounds Vector of bounds.
		 */
		void set_new_bounds(const chunk_upperbound_vector &new_bounds) {
			assert(new_bounds.size() == _num_chunks);

			_upper_bounds.clear();

			// reinitialize hashmap (target -> macrochunk_id)
			for (chunkid_t id = 0; id < _num_chunks; id++) {
				_upper_bounds.insert(new_bounds[id], id);
			}
		}

		/**
		 * Returns whether the currently held number of all messages is zero.
		 * @return Flag whether all macrochunks are empty.
		 */
		bool empty() const {
			for (auto &&macrochunk : _macrochunks) {
				if (macrochunk.get_msg_count() == 0)
					return true;
			}

			return false;
		}

		/**
		 * Returns the number of messages contained in this macrochunk,
		 * not containing left-over messages currently still kept in the
		 * insertion buffers of the processing threads.
		 * @param chunk_id Macrochunk-id.
		 * @return Number of messages currently held in the macrochunk.
		 */
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

		/**
		 * Determines for a hash-value/target in which of the macrochunks a
		 * message having the given hash-value/target belongs to.
		 * @param target Hash-value or target.
		 * @return Correct macrochunk-id for this global trade.
		 */
		chunkid_t get_target_chunkid(const hnode_t target) const {
			auto chunktree_it = _upper_bounds.upper_bound(target);

			// since last element in tree is highest hash, can never be end
			assert(chunktree_it != _upper_bounds.end());

			return chunktree_it.data();
		}

		/**
		 * Initial insertion of messages into this data structure.
		 * Can be used without locks, since initial insertion is done
		 * sequentially.
		 * @param msg Message.
		 */
		void push(const NeighbourMsg &msg) {
			const chunkid_t target_chunk = get_target_chunkid(msg.target);

			_macrochunks[target_chunk].push_sequential(msg);
		}

		/**
		 * Sequentially flushes the insertion buffers of all threads for a
		 * certain macrochunk. This is used before processing the next
		 * macrochunk since left-over messages need to be provided.
		 * @param mc_id Macrochunk-id.
		 */
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

		/**
		 * Inserts messages into the macrochunks.
		 * First the corresponding insertion buffer of the thread is filled,
		 * if its full the insertion buffer is flushed into the macrochunk.
		 * Used while trading neighbourhoods.
		 * @param msg Message to be inserted.
		 * @param thread_id Thread-id forwarding this message.
		 */
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

		/**
		 * Returns messages of the macrochunk induced by the macrochunk-id.
		 * @param chunkid Macrochunk-id.
		 * @return Messages of the macrochunk induced by the macrochunk-id.
		 */
		msg_vector get_messages_of(const chunkid_t chunkid) {
			msg_vector msgs;
			_macrochunks[chunkid].load_messages(msgs);

			return msgs;
		}

		/**
		 * Set this container as belonging to an active global trade round.
		 */
		void activate() {
			assert(_mode == NEXT);

			_mode == ACTIVE; // invariants, for code readability
		}

		/**
		 * Resets all macrochunks of this global trade round.
		 */
		void reset() {
			assert(_mode == ACTIVE);

			for (auto &&macrochunk : _macrochunks)
				macrochunk.reset();

			_mode == NEXT; // invariants, for code readability
		}

		/**
		 * Swaps this active global trade round with the next one.
		 * @param other Container struct for next global trade round.
		 */
		void swap_with_next(EMMessageContainer &other) {
			// note, num_chunks is not swapped, and should be the same for each
			// it's possible to change this, remove const identifier from _num_chunks

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

		/**
		 * Forwards messages contained in this global trade round into the
		 * given output edge stream.
		 * @tparam Receiver
		 * @param out_edges
		 */
		template <typename Receiver>
		void forward_unsorted_edges(Receiver & out_edges) {
			for (chunkid_t mc_id = 0; mc_id < _num_chunks; mc_id++) {
				_macrochunks[mc_id].forward_unsorted_edges(out_edges);
			}
		}
	};

}