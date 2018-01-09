#pragma once

#include <defs.h>
#include <cassert>

#include <stxxl/sorter>

#include <GenericComparator.h>
#include <TupleHelper.h>

#include <random>

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

        generate_multinodes();

        assert(!_nodemsg_sorter.empty());

        generate_sorted_edgelist();

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
    void generate_multinodes() {
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