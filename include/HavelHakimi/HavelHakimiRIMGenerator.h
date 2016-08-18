#pragma once
#include <iostream>
#include <vector>
#include <list>
#include <assert.h>
#include <stxxl/bits/common/utils.h>
#include "defs.h"
#include <functional>

class HavelHakimiRIMGenerator {
public:
    //! Type of stream interface
    using value_type = edge_t;

    static constexpr bool debug = false;

    //! Direction of Input Stream arriving
    enum Direction {
        IncreasingDegree, DecreasingDegree
    };

protected:
    const Direction _push_direction;

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
            return node_upper - node_lower;
        }
    };

    //! Stores block of nodes with same degree sorted by degree desc,
    //! i.e. _blocks.front().degree is smallest entry and has largest
    //! nodes. The ordering is established only after calling generate()
    using BlockContainer = std::list<Block>;
    BlockContainer _blocks;

    //! Push phase
    const node_t _initial_node;
    node_t _push_current_node;
    edgeid_t _degree_sum;

    //! Generation phase
    edgeid_t _max_number_of_edges;

    void _print_blocks() {
        if (debug) {
           std::cerr << "[HH-RIM] blocks:";
           for(const auto &b : _blocks)
              std::cerr << " [L:" << b.node_lower << " U:" << b.node_upper << " D:" << b.degree << "] ";
           std::cerr << std::endl;
        }
    }



    void _verify_block_invariants() {
#ifndef NDEBUG
        _print_blocks();

        assert(!_blocks.empty());

        assert(_blocks.front().node_lower  >= _initial_node);
        assert(_blocks.back().node_upper   <= _push_current_node);

        auto previous_block = _blocks.begin();

        for (auto it = std::next(previous_block); it != _blocks.cend(); ++it) {
            auto & block = *it;

            assert(block.degree > previous_block->degree);
            assert(block.node_lower <= block.node_upper);
            assert(block.node_lower == previous_block->node_upper);

            previous_block++;
        }
#endif
    }

public:
    HavelHakimiRIMGenerator(Direction push_direction = IncreasingDegree, node_t initial_node = 0) :
        _push_direction(push_direction),
        _initial_node(initial_node),
        _push_current_node(initial_node),
        _degree_sum(0),
        _max_number_of_edges(0)
    { }

    //! Push a new vertex -represented by its degree- into degree sequence
    void push(const degree_t & deg, const node_t nodes = 1) {
        assert(nodes > 0);
        assert(deg > 0);

        if (UNLIKELY(_blocks.empty())) {
           _blocks.push_back(Block(deg, _push_current_node, _push_current_node+nodes));
        } else {
            if (LIKELY(_blocks.back().degree == deg)) {
                _blocks.back().node_upper += nodes;
            } else {
                assert(!(_push_direction == IncreasingDegree && deg < _blocks.back().degree));
                assert(!(_push_direction == DecreasingDegree && deg > _blocks.back().degree));
                _blocks.push_back(Block(deg, _push_current_node, _push_current_node + nodes));
            }
        }

        _push_current_node += nodes;
        _degree_sum += deg;
    }

    //! Switch to generation mode; the streaming interface become available
    void generate(std::function<void(const edge_t &)>&& push_target) {
        if (_push_direction == DecreasingDegree) {
            BlockContainer rev_blocks;

            node_t nid = _initial_node;
            while(!_blocks.empty()) {
                const Block& b = _blocks.back();
                rev_blocks.emplace_back(b.degree, nid, nid + b.size());
                nid += b.size();
                _blocks.pop_back();
            }

            std::swap(rev_blocks, _blocks);
            _verify_block_invariants();
        }


        _max_number_of_edges = _degree_sum / 2;

        std::cout << "[HH-RIM] No. Blocks: " << _blocks.size() << ". No Nodes: " << (_push_current_node - _initial_node) << std::endl;
        assert(_blocks.size());

        edgeid_t edges_produced = 0;
#ifndef NDEBUG
        edge_t reader_edge = {0,0};
#endif
        auto emit_edges = [&] (const node_t& source, const node_t from, const degree_t no) {
           for(node_t i = from; i < from + no; i++) {
              push_target({(source), (i)});
              if (debug)
                 std::cerr << "[HH-RIM] push edge (" << source << ", " << i << ")" << std::endl;
           }

           if (debug) std::cerr << std::endl;

           edges_produced += no;
#ifndef NDEBUG
           edge_t this_edge{source, from + no - 1};
           assert(this_edge.first < this_edge.second);
           assert(this_edge.first >= _initial_node);
           assert(this_edge.second < _push_current_node);
           assert(reader_edge < this_edge);           
           reader_edge = this_edge;
#endif
        };
#ifndef NDEBUG
        auto last_total_degree = std::accumulate(_blocks.begin(), _blocks.end(), edgeid_t{0},
             [] (const edgeid_t& i, const Block& blk) {return i+blk.size()*blk.degree;});
#endif
        while(!_blocks.empty()) {
            _verify_block_invariants();
            
            // extract current_node and remove first block if now empty
            auto current_node = _blocks.front().node_lower++;
            const auto degree = _blocks.front().degree;
            
            if (UNLIKELY(!_blocks.front().size())) {
               _blocks.pop_front();
            }

            assert(degree);

            degree_t degree_remain = degree;
            edges_produced = 0;
            auto reader = std::prev(_blocks.end());
            while(degree_remain) {
               if (UNLIKELY(!reader->degree)) {
                  break;
               }

               if (LIKELY(reader->size() <= degree_remain)) {
                  if (debug)
                     std::cout << "Red Deg" << std::endl;
                  
                  // we just decrease the block's degree and (later) connect to
                  // vertices contained
                  reader->degree--;
                  degree_remain -= reader->size();
                  reader--;

               } else {
                  // we have to split the block
                  if (reader != _blocks.begin() && std::prev(reader)->degree+1 == reader->degree) {
                     if (debug)
                        std::cout << "Split Merge Prev " << std::endl;

                     // but we can merge the split block with the previous one
                     auto prev = std::prev(reader);
                     emit_edges(current_node, prev->node_upper, degree_remain);
                     prev->node_upper += degree_remain;
                     reader->node_lower = prev->node_upper;

                  } else if(std::next(reader) != _blocks.end() && (reader->degree == std::next(reader)->degree)) {
                     if (debug)
                        std::cout << "Split Merge Next" << std::endl;
                     // but we can merge the remaining part of the split with the next block
                     auto next = std::next(reader);

                     emit_edges(current_node, reader->node_lower, degree_remain);
                     emit_edges(current_node, next->node_lower, next->size());

                     reader->degree--;
                     reader->node_upper = reader->node_lower + degree_remain;
                     next->node_lower = reader->node_upper;

                     reader = next;

                  } else {
                     if (debug)
                        std::cout << "Split" << std::endl;
                     // but we cannot merge and have to shift all blocks
                     emit_edges(current_node, reader->node_lower, degree_remain);

                     reader = std::next(_blocks.insert(reader, {reader->degree-1, reader->node_lower, reader->node_lower + degree_remain}));
                     // reduce size of unused block
                     reader->node_lower += degree_remain;
                  }  
                  degree_remain = 0;
               }

               _print_blocks();
            }

            {
               auto prev = reader;
               reader++;
               if (reader != _blocks.end()) {
                  emit_edges(current_node, reader->node_lower, _blocks.back().node_upper - reader->node_lower);

                  // merge with previous ?
                  if (UNLIKELY(_blocks.size() > 1 &&  prev->degree == reader->degree)) {
                     if (debug) {
                        std::cout << "Merge" << std::endl;
                        _print_blocks();
                     }

                     prev->node_upper = reader->node_upper;
                     _blocks.erase(reader);
                  }   
               }
            }

            assert(edges_produced == degree - degree_remain);

            if (UNLIKELY(degree_remain)) {
               std::cerr << "[HH-RIM] Node " << current_node << " has unsatisfied degree of " << degree_remain << ".\n"; 
            }

            while(UNLIKELY(!_blocks.empty() && !_blocks.front().degree))
               _blocks.pop_front();

#ifndef NDEBUG
            auto total_degree = std::accumulate(_blocks.begin(), _blocks.end(), edgeid_t{0},
                  [] (const edgeid_t& i, const Block& blk) {return i+blk.size()*blk.degree;});
            assert(last_total_degree - total_degree == 2*edges_produced);
            last_total_degree = total_degree;
#endif
        }

        if (debug)
           std::cerr << "[HH-RIM] Blocks remaining in buffer: " << _blocks.size() << std::endl;

        _blocks.clear();
    }

    // Value is valid after \generate was called
    edgeid_t maxEdges() const {
        return _max_number_of_edges;
    }
};
