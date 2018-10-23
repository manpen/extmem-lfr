#pragma once

#include <iostream>
#include <deque>
#include <stack>
#include <assert.h>
#include <stxxl/bits/common/utils.h>
#include <stxxl/sequence>
#include <DegreeStream.h>
#include "defs.h"

namespace detail {

    template <bool DegreesOut, bool DeficitsOut = false>
    class HavelHakimiIMGenerator_impl {
    public:
        //! Direction of Input Stream arriving
        enum PushDirection {
            IncreasingDegree, DecreasingDegree
        };

        //! Type of stream interface
        using value_type = edge_t;

    protected:
        // Operation mode
        enum Mode {
            Push, Generate
        };
        Mode _mode;

        // Data keeping
        struct Block {
            degree_t degree;
            node_t node_lower; // node id range covered by this block
            node_t node_upper; // both side inclusive

            Block() {}

            Block(degree_t degree_, node_t node_lower_, node_t noder_upper_)
                    : degree(degree_), node_lower(node_lower_),
                      node_upper(noder_upper_) {}

            node_t size() const {
                return node_upper - node_lower + 1;
            }
        };

        //! Stores block of nodes with same degree sorted by degree asc,
        //! i.e. _blocks.front().degree is smallest entry and has largest
        //! nodes. The ordering is established only after calling generate()
        std::deque<Block> _blocks;

        //! While producing an edge we remove blocks from the deque and
        //! buffer them in the stack
        std::stack<Block> _blocks_checkedout;

        //! Push phase
        const PushDirection _push_direction;
        const node_t _initial_node;
        node_t _push_current_node;
        edgeid_t _max_number_of_edges;

        //! Generation phase
        bool _empty;            //!< The last generation step failed (or in Push-Mode)
        edge_t _current_edge;   //!< The current edge available via operator*
        degree_t _remaining_neighbors; //!< The number of neighbors not satisfies for
        //!< for current vertex (_current_edge.first).

        edgeid_t _unsatisfied_degree;
        node_t _unsatisfied_nodes;

        //! Degree outputs
        using degree_count_t = std::pair<degree_t, uint32_t>;
        std::vector<degree_count_t> _input_degree_blocks;
        std::vector<degree_count_t>::const_iterator _input_degree_blocks_it;
        degree_t _input_degree_counter = 0;

        std::vector<std::pair<node_t, degree_t>> _node_degree_deficits;
        degree_t _unsatisfied_neighbors = 0;

        IMGroupedDegreeStream _output_degrees;
        bool _skipped_first = false;

        /**
		 * Moves the block with highest degree from queue into stack and
		 * updates degree information. If remaining_neighbors indicates
		 * a future partial consumption, the block is automatically split
		 */
        void _checkout_block() {
            if (_blocks.empty()) {
                _unsatisfied_neighbors = _remaining_neighbors;

                _unsatisfied_nodes++;
                _unsatisfied_degree += _remaining_neighbors;

                //std::cerr << "HH: Cannot materialize degree sequence. "
                //        "Node " << _current_edge.first << " requires "
                //<< _remaining_neighbors << " more neighbors.\n";
                _remaining_neighbors = 0;

                _fetch_next_edge();

                return;
            }

            auto block = _blocks.back();
            if (block.size() > _remaining_neighbors) {
                // partially consume block, i.e. keep lower ids at higher degree
                _blocks.back().node_upper -= _remaining_neighbors;
                block.node_lower = _blocks.back().node_upper + 1;

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
            for (; !_blocks_checkedout.empty() &&
                   !_blocks_checkedout.top().degree; _blocks_checkedout.pop()) {
            }


            // if stack is empty nothing is to be done
            if (UNLIKELY(_blocks_checkedout.empty()))
                return;

            // make sure there is at least one block in the deque
            // to avoid checking for corner cases in the for loop
            if (UNLIKELY(_blocks.empty())) {
                _blocks.push_back(_blocks_checkedout.top());
                _blocks_checkedout.pop();
            }

            for (; !_blocks_checkedout.empty(); _blocks_checkedout.pop()) {
                const auto &stack_block = _blocks_checkedout.top();
                auto &deque_block = _blocks.back();

                // test whether stack's block can be merged
                if (UNLIKELY(
                        (stack_block.degree == deque_block.degree) &&
                        (stack_block.node_upper + 1 == deque_block.node_lower)
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
                if (UNLIKELY(
                        _blocks_checkedout.top().node_upper < _current_edge.second)) {
                    // will get correct id and also decrement _remaining_neighbors
                    _checkout_block();

                } else {
                    --_remaining_neighbors;
                }

            } else {
                if (DegreesOut || DeficitsOut) {
                    if (LIKELY(_skipped_first)) {
                        if (DegreesOut) {
                            _output_degrees.push((*_input_degree_blocks_it).first - _unsatisfied_neighbors);
                        }

                        if (DeficitsOut)
                            if (_unsatisfied_neighbors > 0)
                                _node_degree_deficits.emplace_back(_current_edge.first, _unsatisfied_neighbors);

                        // increase count of used nodes with that degree, if all have been used up move to the next
                        // degree block
                        _input_degree_counter++;
                        if (_input_degree_counter == (*_input_degree_blocks_it).second) {
                            _input_degree_counter = 0;
                            ++_input_degree_blocks_it;
                        }

                        _unsatisfied_neighbors = 0;
                    } else
                        _skipped_first = true;
                }

                // move blocks previously "parked" on stack back to queue
                _restore_blocks();
                _verify_deque_invariants();

                if (UNLIKELY(_blocks.empty())) {
                    std::cout << "HH generate sequence with "
                              << (_push_current_node - _initial_node)
                              << " nodes and " << _max_number_of_edges << " req. edges; "
                              << " reqs for " << _unsatisfied_nodes << " nodes and "
                              << _unsatisfied_degree << " edges unsatisfied"
                              << std::endl;
                    _empty = true;
                    return;
                }

                // take out first vertex from next block
                auto &block = _blocks.back();
                _current_edge.first = block.node_lower++;
                _remaining_neighbors = block.degree;

                // if this block contained only a single vertex, we can destroy it
                if (block.node_lower > block.node_upper)
                    _blocks.pop_back();

                _checkout_block();
            }
        }

        void _verify_deque_invariants() {
#ifndef NDEBUG
            if (_blocks.empty())
                return;

            assert(_blocks.back().node_lower >= _initial_node);
            assert(_blocks.front().node_upper <= _push_current_node - 1);

            auto previous_block = _blocks.front();
            for (auto it = _blocks.cbegin() + 1; it != _blocks.cend(); ++it) {
                auto &block = *it;

                assert(block.degree > previous_block.degree);
                assert(block.node_lower <= block.node_upper);
                assert(block.node_lower < previous_block.node_upper);
            }
#endif
        }

    public:
        HavelHakimiIMGenerator_impl(PushDirection push_direction = IncreasingDegree,
                                    node_t initial_node = 0) :
                _mode(Push),
                _push_direction(push_direction),
                _initial_node(initial_node),
                _push_current_node(initial_node),
                _max_number_of_edges(0),
                _unsatisfied_degree(0),
                _unsatisfied_nodes(0)
        {}

        //! Push a new vertex -represented by its degree- into degree sequence
        void push(degree_t deg) {
            assert(_mode == Push);
            assert(deg > 0);

            if (UNLIKELY(_blocks.empty())) {
                _blocks.push_back(Block(deg, _push_current_node, _push_current_node));
            }

            if (_push_direction == IncreasingDegree) {
                if (LIKELY(_blocks.back().degree == deg)) {
                    _blocks.back().node_upper = _push_current_node;
                } else {
                    assert(deg > _blocks.back().degree);
                    _blocks.push_back(Block(deg, _push_current_node, _push_current_node));
                }
            } else {
                if (LIKELY(_blocks.front().degree == deg)) {
                    _blocks.front().node_upper = _push_current_node;

                } else {
                    assert(deg < _blocks.back().degree);
                    _blocks.push_front(Block(deg, _push_current_node, _push_current_node));
                }
            }

            ++_push_current_node;
            _max_number_of_edges += deg;

            if (DegreesOut || DeficitsOut) {
                if (_input_degree_blocks.empty())
                    _input_degree_blocks.emplace_back(deg, 1);
                else {
                    auto & last_degree_block = _input_degree_blocks.back();
                    const auto last_degree = last_degree_block.first;

                    if (last_degree == deg)
                        last_degree_block.second++;
                    else
                        _input_degree_blocks.emplace_back(deg, 1);
                }
            }
        }

        //! Switch to generation mode; the streaming interface become available
        void generate() {
            if (DegreesOut || DeficitsOut) {
                _input_degree_blocks_it = _input_degree_blocks.cbegin();
                _input_degree_counter = 0;
            }

            assert(_mode == Push);

            _max_number_of_edges /= 2;

            // If the degree sequence was provided in increasing order, we have to
            // reverse the node ids
            if (_push_direction == IncreasingDegree) {
                for (auto &block : _blocks) {
                    auto tmp = _push_current_node - 1 - block.node_lower + _initial_node;
                    block.node_lower =
                            _push_current_node - 1 - block.node_upper + _initial_node;
                    block.node_upper = tmp;
                }
            }

            std::cout << "HH Queue size: " << _blocks.size() << " for "
                      << (_push_current_node - _initial_node) << " nodes\n";

            #ifndef NDEBUG
            {
                // assert node range is properly covered and that deque invariants hold
                assert(_blocks.back().node_lower == _initial_node);
                assert(_blocks.front().node_upper == _push_current_node - 1);

            }
            #endif

            // Switch
            _mode = Generate;

            _empty = _blocks.empty();
            _remaining_neighbors = 0;
            _fetch_next_edge();
        }

        //! Push rest of degrees into output degree stream
        void finalize() {
            if (DegreesOut) {
                if (_input_degree_blocks_it == _input_degree_blocks.cend())
                    return;

                // push remaining degrees that are unprocessed in the degree block
                for (; _input_degree_counter < (*_input_degree_blocks_it).second; _input_degree_counter++)
                    _output_degrees.push((*_input_degree_blocks_it).first);

                // move on to the rest of the degrees
                ++_input_degree_blocks_it;
                for (; _input_degree_blocks_it != _input_degree_blocks.cend(); ++_input_degree_blocks_it)
                    _output_degrees.push_group(*_input_degree_blocks_it);

                assert(_output_degrees.size() == static_cast<size_t>(_push_current_node - _initial_node));
            }
        }

        std::vector<std::pair<node_t, degree_t>> & get_deficits() {
            return _node_degree_deficits;
        }

        IMGroupedDegreeStream & get_degree_stream() {
            return _output_degrees;
        }

        HavelHakimiIMGenerator_impl &operator++() {
            assert(_mode == Generate);
            _fetch_next_edge();
            return *this;
        }

        const value_type &operator*() const {
            assert(_mode == Generate);
            return _current_edge;
        }

        const value_type *operator->() const {
            assert(_mode == Generate);
            return &_current_edge;
        }

        bool empty() const {
            return _empty;
        }

        edgeid_t maxEdges() const {
            assert(_mode == Generate);
            return _max_number_of_edges;
        }

        edgeid_t unsatisfiedDegree() const {
            return _unsatisfied_degree;
        }

        node_t unsatisfiedNodes() const {
            return _unsatisfied_nodes;
        }
    };

}

using HavelHakimiIMGenerator = detail::HavelHakimiIMGenerator_impl<false>;
using HavelHakimiIMGeneratorWithDegrees = detail::HavelHakimiIMGenerator_impl<true, false>;
using HavelHakimiIMGeneratorWithDeficits = detail::HavelHakimiIMGenerator_impl<false, true>;