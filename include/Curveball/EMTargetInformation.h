/**
 * @file EMTargetInformation.h
 * @date 22. October 2017
 *
 * @author Hung Tran
 */

#pragma once

#include <defs.h>
#include <stxxl/sorter>
#include "../../libs/stxxl/include/stxxl/bits/compat/unique_ptr.h"

namespace Curveball {

	/**
	 * Data type of auxiliary information messages.
	 */
	struct TargetMsg {
		hnode_t target;
		degree_t degree;
		node_t inverse;

	DECL_LEX_COMPARE_OS(TargetMsg, target);
	};

	/**
	 * Holds all auxiliary information in a STXXL container and determines the
	 * hash-value bounds for the macrochunk message insertions.
	 */
	class EMTargetInformation {
	public:
		using value_type = TargetMsg;
		using TargetMsgComparator =
		typename GenericComparatorStruct<TargetMsg>::Ascending;
		using TargetMsgSorter = stxxl::sorter<TargetMsg, TargetMsgComparator>;

	protected:
		enum Mode { WRITING, READING };
		Mode _mode;

		std::unique_ptr<TargetMsgSorter> _active;
		std::unique_ptr<TargetMsgSorter> _pending;

		const chunkid_t _num_chunks;
		const node_t _num_nodes;
		msgid_t _active_num_messages;
		msgid_t _active_max_num_msgs;
		msgid_t _pending_num_messages;
		msgid_t _pending_max_num_msgs;

#ifndef NDEBUG
		bool display_debug = true;
#endif

	public:
		EMTargetInformation(EMTargetInformation const &) = delete;
		void operator=(EMTargetInformation const &) = delete;

		/**
		 * Sets up both auxiliary information containers for the active
		 * and next round.
		 * @param num_chunks Number of macrochunks.
		 * @param num_nodes Number of nodes.
		 */
		EMTargetInformation(const chunkid_t num_chunks, const node_t num_nodes)
			: _mode(WRITING),
			  _active(new TargetMsgSorter(TargetMsgComparator(), NODE_SORTER_MEM)),
			  _pending(new TargetMsgSorter(TargetMsgComparator(), NODE_SORTER_MEM)),
			  _num_chunks(num_chunks),
			  _num_nodes(num_nodes),
			  _active_num_messages(0),
			  _active_max_num_msgs(0),
			  _pending_num_messages(0),
			  _pending_max_num_msgs(0) {}

		/**
		 * Clears the auxiliary information for the active global trade round.
		 */
		void clear_active() {
			_active->clear();

			_mode = WRITING;
		}

		/**
		 * Clears the auxiliary information for the next global trade round.
		 */
		void clear_pending() {
			_pending->clear();

			_mode = WRITING;
		}

		// getters
#ifndef NDEBUG

		chunkid_t get_num_chunks() const {
			return _num_chunks;
		}

		node_t get_num_nodes() const {
			return _num_nodes;
		}

		msgid_t get_num_messages() const {
			return _active_num_messages;
		}

#endif

		/**
		 * Returns the maximum number of messages that occur in a macrochunk
		 * of the active global trade round.
		 * @return Maximum number of messages.
		 */
		msgid_t get_active_max_num_msgs() const {
			return _active_max_num_msgs;
		}

		/**
		 * Returns the maximum number of messages that occur in a macrochunk
		 * of the next global trade round.
		 * @return Maximum number of messages.
		 */
		msgid_t get_pending_max_num_msgs() const {
			return _pending_max_num_msgs;
		}

		/**
		 * Swaps active and next round.
		 */
		void swap() {
			_active.swap(_pending);
			_active_num_messages = _pending_num_messages;
			_active_max_num_msgs = _pending_max_num_msgs;
		}

		/**
		 * Sort the inserted auxiliary information of the active round.
		 * Enables retrieval in the processing phase.
		 */
		void sort() {
			assert(_mode == WRITING);
			_mode = READING;

			_active->sort();
		}

		/**
		 * Push auxiliary information in the active container.
		 * @param target_msg Target auxiliary information.
		 */
		void push_active(const TargetMsg &target_msg) {
			assert(_mode == WRITING);
			assert(target_msg.degree > 0);

			_active->push(target_msg);

			assert(_active->size() <= static_cast<size_t>(_num_nodes));
		}

		/**
		 * Push auxiliary information in the next container.
		 * @param target_msg Target auxiliary information.
		 */
		void push_pending(const TargetMsg &target_msg) {
			assert(_mode == WRITING);
			assert(target_msg.degree > 0);

			_pending->push(target_msg);

			assert(_pending->size() <= static_cast<size_t>(_num_nodes));
		}

		/**
		 * Rewinds the auxiliary information stream of the active round.
		 */
		void rewind() {
			assert(_mode == READING);

			_active->rewind();
		}

		/**
		 * @return Flag whether active round is completely read.
		 */
		bool empty() const {
			return _active->empty();
		}

		/**
		 * @return Current auxiliary target message.
		 */
		value_type operator*() const {
			assert(_mode == READING);

			return _active->operator*();
		}

		/**
		 * Forwards the auxiliary information stream by one.
		 * @return Reference to self.
		 */
		EMTargetInformation &operator++() {
			assert(_mode == READING);

			_active->operator++();

			return *this;
		}

#ifndef NDEBUG
		size_t size() const {
			assert(_mode == READING);

			return _active->size();
		}
#endif

		/**
		 * Returns the hash-value bounds of the active global trade which are
		 * used to determine where each message should be inserted to.
		 * @return Vector of bounds.
		 */
		std::vector<hnode_t> get_bounds_active() {
			_active->sort();

			const hnode_t t_hnodes_per_chunk = _num_nodes / _num_chunks; //_num_chunks * _num_nodes / _num_messages ;
			const hnode_t hnodes_per_chunk = (t_hnodes_per_chunk % 2 == 0 ? t_hnodes_per_chunk : t_hnodes_per_chunk - 1);

			std::vector<hnode_t> out_bounds;
			out_bounds.reserve(_num_chunks);

			// get hash value bounds for all chunks except last
			for (uint32_t i = 0; i < _num_chunks - 1; i++) {
				msgid_t tmp_degree_sum = 0;

				// iterate over all nodes except last
				for (node_t mc_node = 0;
					 mc_node < hnodes_per_chunk - 1;
					 mc_node++, _active->operator++()) {
					tmp_degree_sum += (*_active)->degree;
				}

				tmp_degree_sum += (*_active)->degree;

				out_bounds.push_back((*_active)->target + 1);

				_active->operator++();

				_active_max_num_msgs = std::max(_active_max_num_msgs, tmp_degree_sum);
			}

			// check for number of seen elements
#ifndef NDEBUG
			node_t node_count = 0;
#endif

			// determine bounds
			{
				msgid_t last_degree_sum = 0;
				hnode_t last_hnode = 0;
				for (; !_active->empty(); _active->operator++()) {
					last_degree_sum += (*_active)->degree;
					last_hnode = (*_active)->target;

					//check for number of seen elements
#ifndef NDEBUG
					node_count++;
#endif
				}

				assert(_active->empty());
				assert(node_count >= hnodes_per_chunk);

				out_bounds.push_back(last_hnode + 1);

				_active_max_num_msgs = std::max(_active_max_num_msgs, last_degree_sum);
			}

			_active->rewind();

			assert(out_bounds.size() == _num_chunks);

			_mode = READING;

			return out_bounds;
		}

		/**
		 * Returns the hash-value bounds of the next global trade which are
		 * used to determine where each message should be inserted to.
		 * @return Vector of bounds.
		 */
		std::vector<hnode_t> get_bounds_pending() {
			_pending->sort();

			const hnode_t t_hnodes_per_chunk = _num_nodes / _num_chunks; //_num_chunks * _num_nodes / _num_messages ;
			const hnode_t hnodes_per_chunk = (t_hnodes_per_chunk % 2 == 0 ? t_hnodes_per_chunk : t_hnodes_per_chunk - 1);

			std::vector<hnode_t> out_bounds;
			out_bounds.reserve(_num_chunks);

			// get hash value bounds for all chunks except last
			for (uint32_t i = 0; i < _num_chunks - 1; i++) {
				msgid_t tmp_degree_sum = 0;

				// iterate over all nodes except last
				for (node_t mc_node = 0;
					 mc_node < hnodes_per_chunk - 1;
					 mc_node++, _pending->operator++()) {
					tmp_degree_sum += (*_pending)->degree;
				}

				tmp_degree_sum += (*_pending)->degree;

				out_bounds.push_back((*_pending)->target + 1);

				_pending->operator++();

				_pending_max_num_msgs = std::max(_pending_max_num_msgs, tmp_degree_sum);
			}

			// check for number of seen elements
			#ifndef NDEBUG
			node_t node_count = 0;
			#endif

			{
				msgid_t last_degree_sum = 0;
				hnode_t last_hnode = 0;
				for (; !_pending->empty(); _pending->operator++()) {
					last_degree_sum += (*_pending)->degree;
					last_hnode = (*_pending)->target;

					// check for number of seen elements
					#ifndef NDEBUG
					node_count++;
					#endif
				}

				assert(_pending->empty());
				assert(node_count >= hnodes_per_chunk);

				out_bounds.push_back(last_hnode + 1);

				_pending_max_num_msgs = std::max(_pending_max_num_msgs,
												 last_degree_sum);
			}

			_pending->rewind();

			assert(out_bounds.size() == _num_chunks);

			_mode = READING;

			return out_bounds;
		}
	};

}