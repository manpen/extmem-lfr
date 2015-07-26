#pragma once

#include <stxxl/types>
#include <stxxl/priority_queue>
#include <stxxl/stack>
#include <utility>

template <typename InputStream>
class HavelHakimiGeneratorRLE {
public:
    typedef std::pair<stxxl::uint64, stxxl::uint64> value_type;

private:
    using node_block_type = typename InputStream::value_type;
    struct ComparatorLess
    {
        bool operator () (const node_block_type& a, const node_block_type & b) const { 
            // basic lexicographical ordering
            return (a.value < b.value) || (a.value == b.value && a.index < b.index); 
        }
        
        node_block_type min_value() const { 
            return {
                .value = std::numeric_limits<typename node_block_type::value_type>::min(),
                .count = 0,
                .index = 0
            };
        }
    };

    using pqueue_type = typename stxxl::PRIORITY_QUEUE_GENERATOR<node_block_type, ComparatorLess, 16*1024*1024, 1024*1024>::result;
    using pq_block_type = typename pqueue_type::block_type;
    stxxl::read_write_pool<pq_block_type> _pool;
    pqueue_type _prioQueue;

    using node_degree_stack_type = typename stxxl::STACK_GENERATOR<node_block_type, stxxl::external, stxxl::grow_shrink>::result;
    node_degree_stack_type _buffer;

    stxxl::int64 _current_node;
    stxxl::int64 _current_node_degree;
    stxxl::int64 _current_partner_block_nodes_left;
    stxxl::int64 _current_partner_node;
    
    value_type _current_edge;

    stxxl::uint64 _num_edges;
    stxxl::uint64 _edge_id;

    bool _empty;
    

    // Moves elements from the buffer back into the priority queue
    // If two neighbouring blocks of the same degree with sequential indicies are found,
    // they are automatically merge in order to compensate for fragmentation
    void _write_buffer_back() {
#if 0
        // variant that allows for fragmentation
        while(!_buffer.empty()) {
            _prioQueue.push(_buffer.top());
            _buffer.pop();
        }
        
#else        
        if (_buffer.empty())
            return;
        
        node_block_type last_block;
        if (_prioQueue.empty()) {
            last_block = _buffer.top();
            _buffer.pop();
        } else {
            last_block = _prioQueue.top();
            node_block_type block = _buffer.top();

            if ( (last_block.value == block.value) && (block.index - block.count == last_block.index) ) {
                _prioQueue.pop();
            } else {
                last_block = block;
                _buffer.pop();
            }
        }
            
        while(!_buffer.empty()) {
            node_block_type block = _buffer.top();
            _buffer.pop();
            
            // TODO: Change order of block/last_block if buffer is a FIFO
            if ( (last_block.value == block.value) && (block.index - block.count == last_block.index) ) {
                // we can merge those blocks !
                block.count += last_block.count;
                
            } else {
                _prioQueue.push(last_block);
                
            }
            
            last_block = block;
        }
        
        _prioQueue.push(last_block);
#endif        
    }
    
    // helper function that will reduce the degree of up to #nodes_to_consume of the block
    // provided by one and then buffer this block (or the resulting two blocks of different degrees)
    void _consume_and_buffer_block(node_block_type & block, stxxl::uint64 nodes_to_consume) {
        assert(block.index >= block.count);
        
        if (!block.count)
            return;
        
        if (nodes_to_consume >= block.count) {
            // whole block will be consumed, i.e. reduce degree of each node
            if (--block.value)
                _buffer.push(block);

            // set partner information
            _current_partner_node = block.index;
            _current_partner_block_nodes_left = block.count;
            
        } else {
            // split block and consume second half
            node_block_type new_block = {
                .value = block.value - 1,
                .count = nodes_to_consume,
                .index = block.index - block.count + nodes_to_consume
            };
            block.count -= nodes_to_consume;

            _buffer.push(block);
            
            if (new_block.value)
                _buffer.push(new_block);

            // set partner information
            _current_partner_node = new_block.index;
            _current_partner_block_nodes_left = new_block.count;            
        }
    }
    
    void _generate_next_edge() {
        if ( UNLIKELY(!_current_node_degree) ) {
            // we're done with the current node: write buffer back and (try to) fetch next node
            _write_buffer_back();
        
            if ( UNLIKELY(_prioQueue.empty()) ) {
                // all blocks consumed: nothing left to do 
                _empty = true;
                return;
            }
        }
        
        if (!_current_node_degree) {
            auto block = _prioQueue.top();
            _prioQueue.pop();

            // we remove the first node from the block, so the block's index and size have to be adapted
            _current_node = block.index--;
            block.count--;
            
            _current_node_degree = block.value;
            _consume_and_buffer_block(block, _current_node_degree);
            
            if (_edge_id >= _num_edges)
                std::cout << "Started with node " << _current_node << " and requested degree of " << _current_node_degree << std::endl;
            
            assert(_current_node_degree);
        }
        
        if (!_current_partner_block_nodes_left) {
            if ( UNLIKELY(_prioQueue.empty()) ) {
                STXXL_ERRMSG(
                    "Degree sequence not realizable, node " << _current_node <<
                    " should have gotten " << _current_node_degree << " more neighbors"
                );
                
                // invalid degree sequence; let's reduce this nodes degree and move to the next node
                _current_node_degree = 0;
                _generate_next_edge();
                return;
                
            } else {
                auto block = _prioQueue.top();
                _prioQueue.pop();
                _consume_and_buffer_block(block, _current_node_degree);
            }
        }
        
        if (_edge_id >= _num_edges) {
            std::cout << _prioQueue.size() << " " << _current_node_degree << " " << _current_node_degree << std::endl;
        }
            
            
        
        _current_edge = {_current_node, _current_partner_node--};
        _current_node_degree--;
        _current_partner_block_nodes_left--;
        _edge_id++;
    }
    
    
public:
    HavelHakimiGeneratorRLE(InputStream &input) 
        : _pool(static_cast<size_t>(8*1024*1024/pq_block_type::raw_size), static_cast<size_t>(8*1024*1024/pq_block_type::raw_size))
        , _prioQueue(_pool)
        , _edge_id(0)
        , _empty(false)
    {
        _num_edges = 0;

        for (; !input.empty(); ++input) {
            auto block = *input;
            _prioQueue.push(block);
            _num_edges += block.value * block.count;
        }

        _num_edges /= 2;
        
        // state that will fetch the "next" (i.e. the first) node properly from the prio queue
        _current_node_degree = 0;
        _current_partner_block_nodes_left = 0;
        _generate_next_edge();
    }

    const value_type & operator * () const { return _current_edge; };
    const value_type * operator -> () const { return &_current_edge; };
    
    HavelHakimiGeneratorRLE & operator++ () {
        _generate_next_edge();
        return *this;
    }
    
    bool empty() const { return _empty; };

    stxxl::uint64 maxEdges() const { return _num_edges; };
};
