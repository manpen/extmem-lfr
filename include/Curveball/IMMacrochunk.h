/**
 * @file IMMacrochunk.h
 * @date 27. September 2017
 *
 * @author Hung Tran
 */

#pragma once

#include "defs.h"
#include <vector>
#include <stxxl/sequence>
#include <parallel/algorithm>
#include <mutex>
#include <EdgeStream.h>
#include <Utils/StreamPusher.h>

namespace Curveball {

	/**
	 * Implements a macrochunk by a sequence of STXXL (works similar to queues
	 * but has better overhead).
	 */
	class IMMacrochunk {
	public:
		// messages <trade, neighbour>
		using value_type = NeighbourMsg;

		// saves bucket bounds pi(u)'s
		using bounds_vector = std::vector<node_t>;

		// extmem buffer
		using sequence_type = stxxl::sequence<value_type>;

	protected:
		// enforcing invariants
		enum Mode { PENDING, LOADED };
		Mode _mode;

		// EM data structure to store
		sequence_type _msg_sequence;

		const chunkid_t _chunkid = 0;
		const msgid_t _msg_limit = 0;

		msgid_t _msg_count = 0;

		// pushing lock
		std::mutex _pushing_lock;

	public:
		IMMacrochunk() = delete;
		IMMacrochunk(const IMMacrochunk &) = delete;
		IMMacrochunk &operator=(IMMacrochunk &&) = delete;

		/**
		 * Move constructor, used to swap global trade containers.
		 * @param other Other macrochunk.
		 */
		IMMacrochunk(IMMacrochunk &&other) noexcept
			: _mode(other._mode),
			  _chunkid(other._chunkid),
			  _msg_limit(other._msg_limit),
			  _msg_count(other._msg_count) {
			other._msg_sequence.swap(_msg_sequence);
		}

		/**
		 * Sets up the macrochunk.
		 * @param chunkid Macrochunk-id.
		 * @param msg_limit Maximum number of messages.
		 */
		IMMacrochunk(const chunkid_t chunkid, const msgid_t msg_limit)
			: _chunkid(chunkid),
			  _msg_limit(msg_limit) {
			init();
		}

		/**
		 * Sets the message count to zero.
		 */
		void init() {
			_mode = PENDING;

			_msg_count = 0;
		}

		/**
		 * @return Assigned macrochunk-id.
		 */
		chunkid_t get_chunkid() const {
			return _chunkid;
		}

		/**
		 * @return Number of messages contained in this macrochunk.
		 */
		msgid_t get_msg_count() const {
			return _msg_count;
		}

		/**
		 * Pushes all messages contained in this macrochunk into the provided
		 * vector.
		 * @param msgs_out Output message vector.
		 * @return Flag whether the macrochunk messages do not exceed the limit.
		 */
		bool load_messages(msg_vector& msgs_out) {
			//TODO: if case for when whole sequence is too big for IM
			assert(_mode == PENDING);
			auto msg_stream = _msg_sequence.get_stream();

			// all messages fit into IM
			if (msg_stream.size() <= static_cast<size_t>(_msg_limit)) {
				// load messages into IM
				msgs_out.reserve(msg_stream.size());

				while (!msg_stream.empty()) {
					msgs_out.push_back(*msg_stream);
					++msg_stream;
				}

				#ifndef NDEBUG
				_mode = PENDING;
				#else
				_mode = LOADED;
				#endif

				return true;
			}
				// the estimation fails, does not fully fit into IM
			else {
				assert(false); //TODO
				msg_vector msgs;

				//return std::make_pair<bool, msg_vector>(false, std::move(msgs));
				return false;
			}
		}

		/**
		 * Forwards a single message.
		 * Is used in the initialization phase.
		 * @param msg Message.
		 */
		void push_sequential(const value_type &msg) {
			assert(_mode == PENDING);

			_msg_sequence.push_back(msg);

			_msg_count++;
		}

		/**
		 * Pushes a whole vector of messages into this macrochunk.
		 * @param msg_bulk Vector of messages.
		 */
		void bulk_push_sequential(const std::vector<value_type> &msg_bulk) {
			assert(_mode == PENDING);

			for (const value_type msg : msg_bulk) {
				_msg_sequence.push_back(msg);
			}

			_msg_count += msg_bulk.size();
		}

		/**
		 * Pushes a whole vector of messages into this macrochunk.
		 * Used to flush insertion buffers directly, induces less synchronization
		 * overhead.
		 * @param msg_bulk Vector of messages.
		 */
		void bulk_push(const std::vector<value_type> &msg_bulk) {
			std::lock_guard<std::mutex> pushing_guard(_pushing_lock);

			assert(_mode == PENDING);

			for (const value_type msg : msg_bulk) {
				_msg_sequence.push_back(msg);
			}

			_msg_count += msg_bulk.size();
		}

		/**
		 * Resets this macrochunk by swapping it with an empty one.
		 */
		void reset() {
			sequence_type empty_sequence;
			_msg_sequence.swap(empty_sequence);

			assert(_msg_sequence.empty());

			_mode = PENDING;

			_msg_count = 0;
		}

		/**
		 * Forwards all contained messages into an output stream.
		 * @tparam Receiver
		 * @param out_edges
		 */
		template <typename Receiver>
		void forward_unsorted_edges(Receiver & out_edges) {
			auto msg_stream = _msg_sequence.get_stream();

			for (; !msg_stream.empty(); ++msg_stream) {
				const auto msg = *msg_stream;
				out_edges.push({msg.target, msg.neighbour});
			}
		}
	};

}