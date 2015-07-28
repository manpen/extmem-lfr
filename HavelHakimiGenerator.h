#pragma once

#include <vector>
#include <queue>
#include <utility>

#include <stxxl/priority_queue>
#include <stxxl/stack>

#include "defs.h"

struct HavelHakimiNodeDegree {
    stxxl::int64 node;
    stxxl::int64 degree;

    bool operator < (const HavelHakimiNodeDegree & o) const { return std::tie(degree, node) < std::tie(o.degree, o.node); }
    
    struct ComparatorLess {
        bool operator () (const HavelHakimiNodeDegree& a, const HavelHakimiNodeDegree & b) const { return a < b; }
        HavelHakimiNodeDegree min_value() const { return {0LL, std::numeric_limits<stxxl::int64>::min()}; }
    };
};

inline std::ostream &operator<<(std::ostream &os, HavelHakimiNodeDegree const &m) {
    return os << "{node:" << m.node << ", degree:" << m.degree << "}";
}

template <uint InternalMemory, uint MaxElements>
struct HavelHakimiPrioQueueExt {
    using pqueue_type = typename stxxl::PRIORITY_QUEUE_GENERATOR<
                                     HavelHakimiNodeDegree,
                                     HavelHakimiNodeDegree::ComparatorLess,
                                     InternalMemory,
                                     MaxElements
                                 >::result;
        
    using block_type  = typename pqueue_type::block_type;
    
    stxxl::read_write_pool<block_type> pool;
    pqueue_type prioQueue;
    
    HavelHakimiPrioQueueExt(size_t memory_for_pools = InternalMemory)
        : pool(static_cast<size_t>(memory_for_pools/2/block_type::raw_size),
               static_cast<size_t>(memory_for_pools/2/block_type::raw_size))
        , prioQueue(pool)
    {}
    
    ~HavelHakimiPrioQueueExt() = default;
    
    pqueue_type & queue() {
        return prioQueue;
    }
};

struct HavelHakimiPrioQueueInt {
    using pqueue_type = std::priority_queue<
                            HavelHakimiNodeDegree,
                            std::vector<HavelHakimiNodeDegree>,
                            HavelHakimiNodeDegree::ComparatorLess
                        >;
    
    pqueue_type prioQueue;
    
    HavelHakimiPrioQueueInt()
        : prioQueue()
    {}
    
    ~HavelHakimiPrioQueueInt() = default;    
    
    pqueue_type & queue() {
        return prioQueue;
    }    
};

template <typename PrioQueue, typename Stack>
class HavelHakimiGenerator {
public:
    using value_type = edge_t;

private:
    // configured containters have to be supplied via constructor
    using pqueue_type = typename PrioQueue::pqueue_type;
    pqueue_type & prioQueue;
    Stack & stack;

    // HavelHakimi state
    HavelHakimiNodeDegree current_node_degree;
    value_type current;

    stxxl::int64 numEdges;
    bool is_empty;
    
public:
    template <typename InputStream>
    HavelHakimiGenerator(PrioQueue & queue, Stack & stack, InputStream &degrees) 
        : prioQueue(queue.queue())
        , stack(stack)
        , is_empty(false) 
    {
        numEdges = 0;

        stxxl::int64 u = 0;
        for (; !degrees.empty(); ++degrees, ++u) {
            if (*degrees > 0) {
                prioQueue.push({u, *degrees});
            }
            numEdges += *degrees;
        }

        current_node_degree = prioQueue.top();
        prioQueue.pop();

        numEdges /= 2;
        ++(*this);
    }
     
    HavelHakimiGenerator<PrioQueue, Stack>& operator++() {
        if (prioQueue.empty() && current_node_degree.degree > 0) {
            STXXL_ERRMSG("Degree sequence not realizable, node " << current_node_degree.node << " should have got " << current_node_degree.degree << " more neighbors");
        }

        // if no more edges need to be generated anymore for the current node or cannot be generated anymore (PQ empty)
        if (current_node_degree.degree <= 0 || (prioQueue.empty() && !stack.empty())) {
            // remove nodes from stack and put them back into the PQ
            while (!stack.empty()) {
                prioQueue.push(stack.top());
                stack.pop();
            }

            // new source node
            if (!prioQueue.empty()) {
                current_node_degree = prioQueue.top();
                prioQueue.pop();
            }
        }

        // if the current node needs edges and we can find targets for edges in the PQ
        if (current_node_degree.degree > 0 && !prioQueue.empty()) {
            // target node is node with highest degree from PQ
            HavelHakimiNodeDegree partner = prioQueue.top();
            prioQueue.pop();
            // set current edge
            current = {current_node_degree.node, partner.node};
            // decrease degrees of both nodes
            --partner.degree;
            --current_node_degree.degree;
            // if the target node needs to get more edges add it to the stack so it gets re-added to the PQ
            if (partner.degree > 0) {
                stack.push(partner);
            }
        } else {
        // as in the first part we already tried to make edge generation possible not being able to create edges
        // here means that we cannot generated any edges anymore
            if (current_node_degree.degree > 0) {
                STXXL_ERRMSG("Degree sequence not realizable, node " << current_node_degree.node << " should have got " << current_node_degree.degree << " more neighbors");
            }

            is_empty = true;
        }

        return *this;
    }
    
    const value_type & operator * () const {
        return current;
    };
    
    const value_type * operator -> () const {
        return &current;
    };
    
    bool empty() {
        return is_empty;
    };

    stxxl::int64 maxEdges() const {
        return numEdges;
    };
};


