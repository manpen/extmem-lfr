#pragma once

#include <cassert>

#include <stxxl/sorter>

#include <GenericComparator.h>

// code by Hung Tran
template <typename EdgeReader>
class ConfigurationModelRandom {
public:
    using value_type = edge_t;

    ConfigurationModelRandom() = delete;
    ConfigurationModelRandom(const ConfigurationModelRandom&) = delete;

    ConfigurationModelRandom(EdgeReader &edges, unsigned int seed = 1)
        : _edges(edges)
        , _random_gen(seed)
        , _nodemsg_sorter(NodeMsgComparator{}, SORTER_MEM)
        , _edge_sorter(EdgeComparator{}, SORTER_MEM)
    {}

    // implements execution of algorithm
    void run() {
        assert(!_edges.empty());

        _generateMultiNodes();

        assert(!_nodemsg_sorter.empty());

        _generateSortedEdgeList();

        assert(!_edge_sorter.empty());
    }

//! @name STXXL Streaming Interface
//! @{
    bool empty() const {
        return _edge_sorter.empty();
    }

    const value_type& operator*() const {
        assert(!_edge_sorter.empty());

        return *_edge_sorter;
    }

    ConfigurationModelRandom& operator++() {
        assert(!_edge_sorter.empty());

        ++_edge_sorter;

        return *this;
    }
//! @}

    edgeid_t size() const {
        return _edge_sorter.size();
    }

protected:
    struct NodeMsg {
        node_t key;
        node_t node;

        NodeMsg() { }
        NodeMsg(const node_t &key_, const node_t &node_) : key(key_), node(node_) {}

        DECL_LEX_COMPARE(NodeMsg, key, node);
    };

    class NodeMsgComparator {
    public:
        NodeMsgComparator() { }

        bool operator() (const NodeMsg& a, const NodeMsg& b) const {
            return std::tie(a.key, a.node) < std::tie(b.key, b.node);
        }

        NodeMsg max_value() const {
            return NodeMsg(std::numeric_limits<node_t>::max(), std::numeric_limits<node_t>::max());
        }

        NodeMsg min_value() const {
            return NodeMsg(std::numeric_limits<node_t>::min(), std::numeric_limits<node_t>::min());
        }
    };

    using NodeMsgSorter = stxxl::sorter<NodeMsg, NodeMsgComparator>;
    using EdgeComparator = GenericComparator<edge_t>::Ascending;
    using EdgeSorter = stxxl::sorter<value_type, EdgeComparator>;


    EdgeReader _edges;
    std::mt19937_64 _random_gen;

    NodeMsgSorter _nodemsg_sorter;

    EdgeSorter _edge_sorter;
    
    // internal algos
    void generate_multi_nodes() {
        assert(!_edges.empty());
        std::uniform_int_distribution<node_t> dis;

        for (; !_edges.empty(); ++_edges) {
            _nodemsg_sorter.push( {dis(_random_gen), (*_edges).first } );
            _nodemsg_sorter.push( {dis(_random_gen), (*_edges).second} );
        }

        _nodemsg_sorter.sort();
    }

    void generate_sorted_edgelist() {
        assert(!_nodemsg_sorter.empty());

        for(; !_nodemsg_sorter.empty(); ++_nodemsg_sorter) {
            const node_t fst_node = (*_nodemsg_sorter).node;

            ++_nodemsg_sorter;
            assert(!_nodemsg_sorter.empty());
            const node_t snd_node = (*_nodemsg_sorter).node;

            if (fst_node < snd_node)
                _edge_sorter.push({fst_node, snd_node});
            else
                _edge_sorter.push({snd_node, fst_node});
        }

        _edge_sorter.sort();
    }
};

template <typename EdgeReader>
class ConfigurationModelCRC {
public:
	ConfigurationModelCRC() = delete;
	ConfigurationModelCRC(const ConfigurationModelCRC&) = delete;
	ConfigurationModelCRC(
		EdgeReader & edge_reader_in,
		const uint32_t seed,
		const uint64_t node_upperbound,
		const degree_t threshold,
		const degree_t max_degree,
		const node_t   nodes_above_threshold)
		: _edges(edge_reader_in)
		, _seed(seed)
		, _node_upperbound(node_upperbound)
		, _shift_upperbound(std::min(node_upperbound, _maxShiftBound(node_upperbound)))
		, _threshold(threshold)
		, _max_degree(max_degree)
		, _nodes_above_threshold(nodes_above_threshold)
		, _high_degree_shift_bounds(_highDegreeShiftBounds(node_upperbound, nodes_above_threshold))
		, _multinodemsg_comp(seed)
		, _multinodemsg_sorter(_multinodemsg_comp, SORTER_MEM)
		, _edge_sorter(EdgeComparator(), SORTER_MEM)
	{ }

	using value_type = edge_t;

	void run() {
		assert(!_edges.empty());

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

	const value_type& operator*() const {
		//assert(!_edge_sorter.empty());

		return *_edge_sorter;
	}

	ConfigurationModelCRC&operator++() {
		assert(!_edge_sorter.empty());

		++_edge_sorter;

		return *this;
	}
//! @}

	void clear() {
		_multinodemsg_sorter.clear();
		_edge_sorter.clear();
	}

	uint64_t size(){
		return _edge_sorter.size();
	}

protected:
	EdgeReader _edges;

	const uint32_t _seed;
	const uint64_t _node_upperbound;
	const uint64_t _shift_upperbound;
	const degree_t _threshold;
	const degree_t _max_degree;
	const node_t   _nodes_above_threshold;
	const std::pair<node_t, node_t> _high_degree_shift_bounds;

	typedef stxxl::sorter<MultiNodeMsg, MultiNodeMsgComparator> MultiNodeSorter;
	MultiNodeMsgComparator _multinodemsg_comp;
	MultiNodeSorter _multinodemsg_sorter;

	using EdgeSorter = stxxl::sorter<value_type, EdgeComparator>;
	EdgeSorter _edge_sorter;

	void _generateMultiNodes() {
		assert(!_edges.empty());

		//stxxl::random_number<> rand;
		std::random_device rd;
		// random noise
		std::mt19937_64 gen64(rd());
		std::uniform_int_distribution<node_t> dis64;

		// shift multiplier for high degree nodes
		std::uniform_int_distribution<node_t> disShift(_high_degree_shift_bounds.first, _high_degree_shift_bounds.second);

		//std::cout << "MaxDegree: " << _max_degree << std::endl;
		//std::cout << "Threshold: " << _threshold << std::endl;
		//std::cout << "NAT: " << _nodes_above_threshold << std::endl;

		// do first problematic nodes
		for (node_t count_threshold = 0; (count_threshold < _nodes_above_threshold) && (!_edges.empty()); ++count_threshold) {
			// new code
			// prevent sorter out of bounds
			//std::cout << "CT: " << count_threshold << std::endl;

			if (_threshold > 0) {
				while((static_cast<node_t>((*_edges).second) < _nodes_above_threshold) && !_edges.empty()) {
					const node_t random_noise = dis64(gen64);
					const node_t fst_node = _node_upperbound + disShift(gen64) * _nodes_above_threshold + static_cast<node_t>((*_edges).first);
					const node_t snd_node = _node_upperbound + disShift(gen64) * _nodes_above_threshold + static_cast<node_t>((*_edges).second);

					_multinodemsg_sorter.push(
						MultiNodeMsg{ (random_noise & (node_t) 0xFFFFFFF000000000) | fst_node });

					_multinodemsg_sorter.push(
						MultiNodeMsg{ (random_noise << 36) | snd_node });

					++_edges;

					if ((*_edges).first != count_threshold)
						break;
				}

				while ((static_cast<node_t>((*_edges).first) == count_threshold) && !_edges.empty()) {
					//std::cout << "Only Second Problematic" << *_edges << std::endl;
					const node_t random_noise = dis64(gen64);

					const node_t fst_node = _node_upperbound + disShift(gen64) * _nodes_above_threshold + static_cast<node_t>((*_edges).first);

					_multinodemsg_sorter.push(
						MultiNodeMsg{ (random_noise & (node_t) 0xFFFFFFF000000000) | fst_node });
					_multinodemsg_sorter.push(
						MultiNodeMsg{ (random_noise << 36) | static_cast<node_t>((*_edges).second) });

					++_edges;
				}
			} else
				break;
		}

		// not so problematic
		for (; !_edges.empty(); ++_edges) {
			const node_t random_noise = dis64(gen64);

			_multinodemsg_sorter.push(
				MultiNodeMsg{ (random_noise & (node_t) 0xFFFFFFF000000000) | static_cast<node_t>((*_edges).first)});
			_multinodemsg_sorter.push(
				MultiNodeMsg{ (random_noise << 36) | static_cast<node_t>((*_edges).second)});
		}

		_multinodemsg_sorter.sort();

		assert(!_multinodemsg_sorter.empty());
	}

	/**
	 * HavelHakimi gives us a graphical sequence, therefore no need to randomize an "half-edge" for the last node.
	**/
	void _generateSortedEdgeList() {
		assert(!_multinodemsg_sorter.empty());

		for(; !_multinodemsg_sorter.empty(); ++_multinodemsg_sorter) {
			auto const & fst_node = *_multinodemsg_sorter;

			const node_t fst_entry = ( fst_node.node() <= (node_t) _node_upperbound ? fst_node.node() : (fst_node.node() - _node_upperbound) % _nodes_above_threshold);

			++_multinodemsg_sorter;

			auto const & snd_node = *_multinodemsg_sorter;

			const node_t snd_entry = ( snd_node.node() <= (node_t) _node_upperbound ? snd_node.node() : (snd_node.node() - _node_upperbound) % _nodes_above_threshold);

			if (fst_entry < snd_entry)
				_edge_sorter.push(edge_t{fst_entry, snd_entry});
			else
				_edge_sorter.push(edge_t{snd_entry, fst_entry});
		}

		_edge_sorter.sort();
	}

	uint64_t _maxShiftBound(uint64_t n) const {
		return 27 - static_cast<uint64_t>(log2(n));
	}

	std::pair<node_t, node_t> _highDegreeShiftBounds(uint64_t node_upperbound, node_t nodes_above_threshold) const {
		return std::pair<node_t, node_t>{(pow(2, 32) - node_upperbound) / nodes_above_threshold,
																		 (pow(2, 36) - node_upperbound) / nodes_above_threshold - 1};
	}

};