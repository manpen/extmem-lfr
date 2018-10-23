/**
 * @file IMAdjacencyList.h
 * @date 2. October 2017
 *
 * @author Hung Tran
 */

#pragma once

#include "defs.h"
#include <mutex>
#include <memory>
#include <atomic>

namespace Curveball {

	/**
	 * Data structure holding all messages/neighbours of nodes in the currently
	 * processed macrochunk. It is implemented by a vector and pointers with
	 * additional sentinels.
	 */
	class IMAdjacencyList {
	public:
		using degree_vector = std::vector<degree_t>;
		using threadcount_vector = std::vector<std::atomic<int>>;
		using neighbour_vector = std::vector<node_t>;
		using pos_vector = std::vector<edgeid_t>;
		using pos_it = pos_vector::iterator;
		using neighbour_it = neighbour_vector::iterator;
		using cneighbour_it = neighbour_vector::const_iterator;
		using nodepair_vector = std::vector<std::pair<node_t, node_t>>;

	protected:

		neighbour_vector _neighbours;
		neighbour_vector _partners;
		std::vector<int> _edge_to_partner; // use int here for thread safety
		degree_vector _offsets;
		pos_vector _begin;
		edgeid_t _degree_count;
		std::vector<std::mutex> _node_locks;

		threadcount_vector _active_threads;

		const node_t _init_num_nodes;
		msgid_t _init_num_msgs;

	public:
		IMAdjacencyList() = default;
		IMAdjacencyList(const IMAdjacencyList &) = delete;

		/**
		 * Sets maximum values for all containers.
		 * @param num_nodes Number of nodes.
		 * @param degree_count Number of messages.
		 */
		IMAdjacencyList(const node_t num_nodes, const edgeid_t degree_count) :
				_neighbours(static_cast<size_t>(degree_count) +
							static_cast<size_t>(num_nodes) + 1),
				_partners(static_cast<size_t>(num_nodes)),
				_edge_to_partner(static_cast<size_t>(num_nodes)),
				_offsets(static_cast<size_t>(num_nodes)),
				_begin(static_cast<size_t>(num_nodes) + 1),
				_degree_count(degree_count),
				_node_locks(static_cast<size_t>(num_nodes)),
				_active_threads(static_cast<size_t>(num_nodes)),
				_init_num_nodes(num_nodes),
				_init_num_msgs(degree_count) {}

		/**
		 * Resizes the vector holding the neighbours.
		 * @param degree_count
		 */
		void resize(edgeid_t degree_count);

		/**
		 * Resets the pointers setting and indicating neighbours.
		 * @param degrees Degree sequence of next macrochunk.
		 * @param partners Trading partners of nodes in the macrochunk.
		 * @param num_nodes Number of nodes in the macrochunk.
		 * @param degree_count Number of maximum messages in the macrochunk.
		 */
		void initialize(const degree_vector &degrees,
						const neighbour_vector &partners,
						node_t num_nodes,
						edgeid_t degree_count);

		/**
		 * Deallocates the vector holding the neighbours.
		 */
		void dealloc() {
			std::fill(_edge_to_partner.begin(), _edge_to_partner.end(), false);
			_neighbours = neighbour_vector();
		}

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Iterator to beginning of neighbours row of node.
		 */
		neighbour_it begin(const node_t node_id);

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Iterator to end of neighbours row of node.
		 */
		neighbour_it end(const node_t node_id);

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Const iterator to beginning of neighbours row of node.
		 */
		cneighbour_it cbegin(node_t node_id) const;

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Const iterator to end of neighbours row of node.
		 */
		cneighbour_it cend(node_t node_id) const;

		/**
		 * @return Edges contained in this subgraph.
		 */
		nodepair_vector get_edges() const;

		/**
		 * Inserts a neighbour in the desired row.
		 * @param node_id
		 * @param neighbour
		 */
		void insert_neighbour(const node_t node_id, const node_t neighbour) {
			const auto pos = begin(node_id) + _offsets[node_id];

			assert(*pos != LISTROW_END && *pos != IS_TRADED);

			*pos = neighbour;

			_offsets[node_id]++;
		}

		/**
		 * Inserts a neighbour and checks whether the final message has been
		 * inserted without synchronisation.
		 * @param node Rank of node in the macrochunk.
		 * @param neighbour Neigbour to be inserted
		 * @return Flag whether trade of node can be executed.
		 */
		bool insert_neighbour_check(const node_t node, const node_t neighbour) {
			assert(!((node % 2 == 0) && (neighbour == _partners[node])));
			insert_neighbour(node, neighbour);

			// if odd subtract one, if even add one
			const node_t partner = (node % 2 == 0 ? node + 1 : node - 1);

			auto tradable_check = [&](node_t smaller, node_t larger) {
				const bool shared_edge = !!_edge_to_partner[smaller];

				if (_offsets[smaller] != degree_at(smaller))
					return false;
				else
					return _offsets[larger] + shared_edge == degree_at(larger);
			};

			if (node % 2 == 0)
				return tradable_check(node, partner);
			else
				return tradable_check(partner, node);
		}

		/**
		 * Inserts a neighbour with synchronisation.
		 * @param node Rank of node in the macrochunk.
		 * @param neighbour Neigbour to be inserted
		 */
		void insert_neighbour_without_check(const node_t node,
											const node_t neighbour) {
			std::lock_guard<std::mutex> lock_guard(_node_locks[node]);
			const auto pos = begin(node) + _offsets[node];

			assert(*pos != LISTROW_END && *pos != IS_TRADED);

			*pos = neighbour;

			_offsets[node]++;
		}

		/**
		 * Inserts a neighbour.
		 * @param node Rank of node in the macrochunk.
		 * @param neighbour Neigbour to be inserted
		 */
		void insert_neighbour_without_check_lock(const node_t node,
												 const node_t neighbour) {
			const auto pos = begin(node) + _offsets[node];

			assert(*pos != LISTROW_END && *pos != IS_TRADED);

			*pos = neighbour;

			_offsets[node]++;
		}

		/**
		 * Directly inserts a neighbour of a node in the desired position in
		 * the adjacency list.
		 * Should not be used in a sequential setting.
		 * @param node_id Rank of node in the macrochunk.
		 * @param neighbour Neighbour of node.
		 * @param shift Position in the adjacency list of node.
		 */
		void insert_neighbour_at(const node_t node_id,
								 const node_t neighbour,
								 const degree_t shift)
		{
			const auto pos = begin(node_id) + shift;

			assert(*pos != LISTROW_END || *pos != IS_TRADED);

			if (node_id % 2 == 0) {
				if (UNLIKELY(neighbour == _partners[node_id])) {
					_edge_to_partner[node_id] = true;

					// we set the entry where v would have been to infty
					// such that sorting that row removes the occurence of v automatically
					// the existence of that edge is saved by the boolean flag
					*pos = LISTROW_END;
				} else
					*pos = neighbour;
			} else {
				*pos = neighbour;
			}
		}

		/**
		 * Sets the flag that the given node shares an edge with its partner.
		 * @param node_id Rank of node in the macrochunk.
		 */
		void set_edge_in_partner(const node_t node_id) {
			assert(node_id % 2 == 0);
			_edge_to_partner[node_id] = true;
		}

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Flag whether the given node shares an edge with its partner.
		 */
		bool get_edge_in_partner(const node_t node_id) {
			return !!_edge_to_partner[node_id];
		}

		/**
		 * Increases the number of incoming messages of a node directly.
		 * Should not be used in a sequential setting.
		 * @param node_id Rank of node in the macrochunk.
		 * @param offset Number of additional incoming messages.
		 */
		void add_offset(const node_t node_id, const degree_t offset) {
			assert(static_cast<size_t>(node_id) < _offsets.size());
			assert(offset <= degree_at(node_id));

			_offsets[node_id] = _offsets[node_id] + offset;
		}

		/**
		 * Sets the number of incoming messages of a node directly.
		 * Should not be used in a sequential setting.
		 * @param node_id Rank of node in the macrochunk.
		 * @param offset Number of incoming messages.
		 */
		void set_offset(const node_t node_id, const degree_t offset) {
			assert(static_cast<size_t>(node_id) < _offsets.size());
			assert(offset <= degree_at(node_id));

			_offsets[node_id] = offset;
		}

		/**
		 * Sets a node to traded status.
		 * @param node Rank of node in the macrochunk.
		 */
		void set_traded(const node_t node) {
			// when setting as traded,
			// those nodes should not have been traded already
			assert(_neighbours[_begin[node + 1] - 1] != IS_TRADED);

			_neighbours[_begin[node + 1] - 1] = IS_TRADED;
		}

		/**
		 * Should not be used in a sequential setting.
		 * @param node Rank of node in the macrochunk.
		 * @return Returns whether the node has been traded.
		 */
		bool has_traded(const node_t node) const {
			assert(node < static_cast<node_t>(_offsets.size()) + 1);

			if (static_cast<size_t>(node) < _offsets.size())
				return _neighbours[_begin[node + 1] - 1] == IS_TRADED;
			else
				// node == _offsets.size()
				return true;
		}

		/**
		 * @return Number of edges that can be held in this macrochunk.
		 */
		edgeid_t num_edges() const {
			return _degree_count;
		}

		/**
		 * Sorts row of node with the given rank.
		 * @param node_id Rank of node in the macrochunk.
		 */
		void sort_row(const node_t node_id) {
			// this should be in-place
			std::sort(begin(node_id), end(node_id));
		}

		/**
		 * Resets the row of node with the given rank.
		 * @param node_id Rank of node in the macrochunk.
		 */
		void reset_row(const node_t node_id) {
			// can only reset rows that are already traded
			assert(has_traded(node_id));
			assert(static_cast<size_t>(node_id) < _offsets.size());

			_offsets[node_id] = 0;
		}

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Degree of node.
		 */
		degree_t degree_at(const node_t node_id) const {
			assert(node_id < static_cast<node_t>(_offsets.size()) + 1);

			// if the last node wants to check whether it is tradable
			// its check will always fail
			if (static_cast<size_t>(node_id) != _offsets.size())
				return static_cast<degree_t>(_begin[node_id + 1] - _begin[node_id] - 1);
			else
				return INVALID_NODE;

		}

		/**
		 * @param node_id Rank of node in the macrochunk.
		 * @return Number of incoming messages of node.
		 */
		degree_t received_msgs(const node_t node_id) const {
			return _offsets[node_id];
		}

		/**
		 * Returns whether trade between two nodes can be executed.
		 * @param smaller Trading partner with smaller rank.
		 * @param larger Trading partner with larger rank.
		 * @return Flag whether trade between trading partners can be executed.
		 */
		bool tradable(const node_t smaller, const node_t larger) const {
			assert(smaller < larger);

			const bool shared_edge = !!_edge_to_partner[smaller];

			if (_offsets[smaller] != degree_at(smaller))
				return false;
			else
				return _offsets[larger] + shared_edge == degree_at(larger);
		}

		#ifndef NDEBUG
		/**
		 * Check whether the offset values are monotonically increasing.
		 */
		void check_offsets(node_t num_nodes) {
			for (node_t node = 0; node < num_nodes - 1; node++)
				assert(_offsets[node] <= _offsets[node + 1]);
		}
		#endif

	};
}