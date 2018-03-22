/**
 * @file IMAdjacencyList.cpp
 * @date 2. October 2017
 *
 * @author Hung Tran
 */

#include "IMAdjacencyList.h"

namespace Curveball {

	using neighbour_vector = std::vector<node_t>;
	using degree_vector = std::vector<degree_t>;
	using degree_it = std::vector<degree_t>::const_iterator;
	using pos_vector = std::vector<edgeid_t>;
	using neighbour_it = neighbour_vector::iterator;
	using cneighbour_it = neighbour_vector::const_iterator;
	using nodepair_vector = std::vector<std::pair<node_t, node_t> >;

/**
 * @brief Initialize method
 *
 */
//TODO parallelize
	void IMAdjacencyList::initialize(const degree_vector &degrees,
									 const neighbour_vector &partners,
									 const node_t num_nodes,
									 const edgeid_t degree_count) {
		_partners = partners;
		// check bounds computed by EMDegreeHelper
		assert(degree_count <= _init_num_msgs);
		assert(num_nodes <= _init_num_nodes);

		std::fill(_offsets.begin(), _offsets.end(), 0);
		std::fill(_neighbours.begin(), _neighbours.end(), 0);
		for (auto &thread_count : _active_threads) {
			thread_count = 0;
		}

		_degree_count = degree_count;

		degree_t sum = 0;
		for (degree_t node_id = 0; node_id < num_nodes; node_id++) {
			_begin[node_id] = sum;

			// no isolated nodes allowed
			assert(degrees[node_id] > 0);

			sum += degrees[node_id];
			_neighbours[sum] = LISTROW_END;

			sum += 1;
		}
		_neighbours[sum] = LISTROW_END;
		_begin[num_nodes] = sum;

		assert(sum == static_cast<degree_t>(degree_count + num_nodes));
	}

	void IMAdjacencyList::resize(const edgeid_t degree_count) {
		_neighbours.resize(degree_count + _offsets.size() + 1),
			_init_num_msgs = degree_count;
	}

	neighbour_it IMAdjacencyList::begin(const node_t node_id) {
		return _neighbours.begin() + _begin[node_id];
	}

	neighbour_it IMAdjacencyList::end(const node_t node_id) {
		return _neighbours.begin() + _begin[node_id] + _offsets[node_id];
	}

	cneighbour_it IMAdjacencyList::cbegin(const node_t node_id) const {
		return _neighbours.cbegin() + _begin[node_id];
	}

	cneighbour_it IMAdjacencyList::cend(const node_t node_id) const {
		return _neighbours.cbegin() + _begin[node_id] + _offsets[node_id];
	}

	nodepair_vector IMAdjacencyList::get_edges() const {
		nodepair_vector edges;
		edges.reserve(static_cast<size_t>(_degree_count));

		for (node_t nodeid = 0;
			 nodeid < static_cast<node_t>(_offsets.size()); nodeid++) {
			for (auto it = cbegin(nodeid); it != cend(nodeid); it++) {
				edges.emplace_back(nodeid, *it);
			}
		}

		return edges;
	}

}