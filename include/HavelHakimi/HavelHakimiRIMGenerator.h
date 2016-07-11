#pragma once
#include <iostream>
#include <vector>
#include <assert.h>
#include <stxxl/bits/common/utils.h>
#include "defs.h"


template <class PushTarget>
class HavelHakimiRIMGenerator {
public:
    //! Type of stream interface
    using value_type = edge_t;

protected:
    // Data keeping
    struct Block {
        degree_t degree;
        node_t node_lower; // node id range covered by this block
        node_t node_upper; // both side inclusive

        Block() {}
        Block(degree_t degree_, node_t node_lower_, node_t noder_upper_)
            : degree(degree_), node_lower(node_lower_), node_upper(noder_upper_)
        {}

        node_t size() const {
            return node_upper - node_lower + 1;
        }
    };

    //! Stores block of nodes with same degree sorted by degree desc,
    //! i.e. _blocks.front().degree is smallest entry and has largest
    //! nodes. The ordering is established only after calling generate()
    std::vector<Block> _blocks;

    //! Push phase
    const node_t _initial_node;
    node_t _push_current_node;
    edgeid_t _max_number_of_edges;

    //! Generation phase
    PushTarget& _push_target;

    /**
     * Moves the block with highest degree from queue into stack and
     * updates degree information. If remaining_neighbors indicates
     * a future partial consumption, the block is automatically split
     */
    void _checkout_block() {
        if (_blocks.empty()) {
            std::cerr << "HH: Cannot materialize degree sequence. "
                    "Node " << _current_edge.first << " requires "
            << _remaining_neighbors << " more neighbors.\n";
            _remaining_neighbors = 0;

            _fetch_next_edge();

            return;
        }

        auto block = _blocks.back();
        if (block.size() > _remaining_neighbors) {
            // partially consume block, i.e. keep lower ids at higher degree
            _blocks.back().node_upper -= _remaining_neighbors;
            block.node_lower = _blocks.back().node_upper+1;

            // move unused block to stack
            _blocks_checkedout.push(_blocks.back());
            _blocks.pop_back();

        } else {
            // completely consume block, i.e. remove it from queue
            // (may be reinserted with lower degree from stack)
            _blocks.pop_back();
        }

        --block.degree;
        _blocks_checkedout.push(block);

        _current_edge.second = block.node_lower;
        --_remaining_neighbors;
    }

    /**
     * Returns blocks buffered in stack into queue and tries
     * to merge neighboring blocks if degree and boundaries match
     */
    void _restore_blocks() {
        // blocks with degree zero are kept in stack to avoid
        // special treatment; they however can be destroyed
        for (; !_blocks_checkedout.empty() && !_blocks_checkedout.top().degree; _blocks_checkedout.pop() );


        // if stack is empty nothing is to be done
        if (UNLIKELY(_blocks_checkedout.empty()))
            return;

        // make sure there is at least one block in the deque
        // to avoid checking for corner cases in the for loop
        if (UNLIKELY(_blocks.empty())) {
            _blocks.push_back(_blocks_checkedout.top());
            _blocks_checkedout.pop();
        }

        for(; !_blocks_checkedout.empty(); _blocks_checkedout.pop()) {
            const auto & stack_block = _blocks_checkedout.top();
            auto & deque_block = _blocks.back();

            // test whether stack's block can be merged
            if (UNLIKELY(
                (stack_block.degree == deque_block.degree) &&
                (stack_block.node_upper+1 == deque_block.node_lower)
            )) {
                deque_block.node_lower = stack_block.node_lower;
            } else {
                _blocks.push_back(stack_block);
            }
        }
    }

    void _fetch_next_edge() {
        assert(!_empty);

        if (LIKELY(_remaining_neighbors)) {
            // advance to next neighbor
            _current_edge.second++;

            // if this new edge is not contained in the current block being consumed
            // we have to fetch a new one
            if (UNLIKELY(_blocks_checkedout.top().node_upper < _current_edge.second)) {
                // will get correct id and also decrement _remaining_neighbors
                _checkout_block();

            } else {
                --_remaining_neighbors;
            }

        } else {
            // move blocks previously "parked" on stack back to queue
            _restore_blocks();
            _verify_deque_invariants();

            if (UNLIKELY(_blocks.empty())) {
                _empty = true;
                return;
            }

            // take out first vertex from next block
            auto & block = _blocks.back();
            _current_edge.first = block.node_lower++;
            _remaining_neighbors = block.degree;

            // if this block contained only a single vertex, we can destroy it
            if (block.node_lower > block.node_upper)
                _blocks.pop_back();

            _checkout_block();
        }
    }

    void _verify_block_invariants() {
#ifndef NDEBUG
        if (_blocks.empty())
            return;

        assert(_blocks.front().node_lower  >= _initial_node);
        assert(_blocks.back().node_upper   <= _push_current_node-1);

        auto previous_block = _blocks.cbegin();
        for (auto it = _blocks.cbegin()+1; it != _blocks.cend(); ++it) {
            auto & block = *it;

            assert(block.degree < previous_block.degree);
            assert(block.node_lower <= block.node_upper);
            assert(block.node_lower == previous_block.node_upper+1);

            previous_block++;
        }
#endif
    }

public:
    HavelHakimiRIMGenerator(PushTarget& push_target, node_t initial_node = 0) :
        _mode(Push),
        _initial_node(initial_node),
        _push_current_node(initial_node),
        _max_number_of_edges(0),
        _push_target(push_target)
    {}

    //! Push a new vertex -represented by its degree- into degree sequence
    void push(degree_t deg) {
        assert(_mode == Push);
        assert(deg > 0);

        if (UNLIKELY(_blocks.empty())) {
           _blocks.push_back(Block(deg, _push_current_node, _push_current_node));
        } else {
        if (LIKELY(_blocks.back().degree == deg)) {
           _blocks.back().node_upper++;
        } else {
           assert(deg < _blocks.back().degree());
           _blocks.push_back(Block(deg, _push_current_node, _push_current_node));
        }

        ++_push_current_node;
        _max_number_of_edges += deg;
    }

    //! Switch to generation mode; the streaming interface become available
    void generate() {
        assert(_mode == Push);

        _max_number_of_edges /= 2;

        std::cout << "HH Queue size: " << _blocks.size() << " for " << (_push_current_node - _initial_node) << " nodes\n";

        // padding to allow safe writes in case of shifts w/o boundary checks
        _verify_block_invariants();

        // There are the following update cases:
        //  - no split: simply reduce degree by one
        //  - split w/ deg-1 existing: split and merge
        //  - split w/o deg-1 existing: insert new block and shift all remaining towards the end



        auto begin = _blocks.begin();

        auto decrement_degrees = [&] (_blocks::iterator it) {
            while(it != _blocks.end()) {
               it->degree--;
               ++it;
            }
        };

        auto insert_and_decrement = [&] (_blocks::iterator it, const Block & new_block) {
            auto tmp = new_block;
            while(it != _blocks.end()) {
               std::swap(tmp, *it);
               tmp.degree--;
               ++it;
            }
            _blocks.push_back(tmp);
        };


        while(!_blocks.empty()) {
            _verify_block_invariants();

            // extract current_node and remove first block if now empty
            auto current_node = begin.node_lower++;
            auto degree_remain = begin.degree;
            if (UNLIKELY(begin_it.node_lower >= begin_it.node_upper))
               ++begin;

            while(degree_remain) {
               if (UNLIKELY(last->size() > degree_remain)) {



            }
            
        }
            

    }

    edgeid_t maxEdges() const {
        assert(_mode == Generate);
        return _max_number_of_edges;
    }
};
