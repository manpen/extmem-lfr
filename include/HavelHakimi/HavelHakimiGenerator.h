/**
 * @file
 * @brief  Implements an external memory Havel Hakimi Edge generator on top of STXXL's streaming interface.
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */
#pragma once

#include <vector>
#include <queue>
#include <utility>
#include <stack>

#include <stxxl/priority_queue>
#include <stxxl/stack>

#include "defs.h"

/**
 * @class HavelHakimiNodeDegree
 * @brief Representation of a node including its degree for the HavelHakimi algorithm
 * @see HavelHakimiNodeDegree::ComparatorLess
 */
struct HavelHakimiNodeDegree {
    stxxl::int64 node;    ///< node index
    stxxl::int64 degree;  ///< node degree

    bool operator < (const HavelHakimiNodeDegree & o) const { return std::tie(degree, node) < std::tie(o.degree, o.node); }
    
    /***
     * @class ComparatorLess
     * @brief Implements a strict total order over the nodes' degrees
     * 
     * Indented to be used with STXXL::priority_queue.
     * The default operator is return iff a.degree < b.degree
     */
    struct ComparatorLess {
        bool operator () (const HavelHakimiNodeDegree& a, const HavelHakimiNodeDegree & b) const { return a < b; }
        HavelHakimiNodeDegree min_value() const { return {0LL, std::numeric_limits<stxxl::int64>::min()}; }
    };
};

inline std::ostream &operator<<(std::ostream &os, HavelHakimiNodeDegree const &m) {
    return os << "{node:" << m.node << ", degree:" << m.degree << "}";
}

/**
 * @class HavelHakimiPrioQueueExt
 * @brief A wrapper for the STXXL::priority_queue that also instantiates the memory pool
 * 
 * A minimalistic example:
 * @code{.cpp}
 *  using hh_prio_queue = HavelHakimiPrioQueueExt<16 * IntScale::Mi, IntScale::Mi>;
 *  hh_prio_queue prio_queue;
 *  
 *  using hh_stack = stxxl::STACK_GENERATOR<HavelHakimiNodeDegree, stxxl::external, stxxl::grow_shrink>::result;
 *  hh_stack stack;
 *  
 *  HavelHakimiGenerator<hh_prio_queue, hh_stack> hhgenerator{prio_queue, stack, degreeSequence};
 * @endcode
 */
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

/**
 * @class HavelHakimiPrioQueueInt
 * @brief A wrapper for the STL::priority_queue to be used with HavelHakimiGenerator
 * 
 * A minimalistic example:
 * @code{.cpp}
 *  HavelHakimiPrioQueueInt prio_queue;
 *  std::stack<HavelHakimiNodeDegree> stack;
 *  HavelHakimiGenerator<HavelHakimiPrioQueueInt, std::stack<HavelHakimiNodeDegree>> hhgenerator{prio_queue, stack, degreeSequence};
 * @endcode
 */
struct HavelHakimiPrioQueueInt {
    /// Type of the priority queue managed by this class
    using pqueue_type = std::priority_queue<
                            HavelHakimiNodeDegree,
                            std::vector<HavelHakimiNodeDegree>,
                            HavelHakimiNodeDegree::ComparatorLess
                        >;
    
    pqueue_type prioQueue; ///< PrioQueue managed by this class
    
    HavelHakimiPrioQueueInt()
        : prioQueue()
    {}
    
    ~HavelHakimiPrioQueueInt() = default;    
    
    /**
     * @brief Accessed by the algorithm using the priority queue.
     */
    pqueue_type & queue() {
        return prioQueue;
    }    
};

/**
 * @class HavelHakimiGenerator
 * @brief Implementation of the Havel Hakimi algorithm using STXXL's streaming interface
 * 
 * Upon construction, the whole input stream (an integer stream is expected. Each entry correspondes
 * to the degree of one node in the output) is consumed and stored in the priority queue
 * supplied via the constructor. The streaming interface then can be used to query all edges.
 * 
 * For instanciation examples (including the auxilary data structures see HavelHakimiPrioQueueInt
 * and HavelHakimiPrioQueueExt).
 * 
 * @code{cpp}
 *  HavelHakimiGenerator<...> hhg(prio_queue, stack, input_degree_sequeunce);
 * 
 *  for(; !hhg.empty(); ++hhg) {
 *      edge_t edge = *hhg;
 *      std::cout << i++ << ": " << edge.first << ", " << edge.second << std::endl;
 *  }
 * @endcode
 * 
 * @tparam PrioQueue    The type of the priority queue reference provided to the constructor.
 *                      The user has to ensure, that the queue can store as many items as provided by
 *                      the input degree sequeuce.
 * @tparam Stack        The type of the stack reference provided to the constructor.
 *                      It has to accommodate upto M elements, where M=max(elements of input sequence)
 */
template <typename PrioQueue, typename Stack>
class HavelHakimiGenerator {
public:
    /**
     * @typedef value_type
     * @brief Output type used by operator* / operator->
     */
    using value_type = edge_t; 

private:
    using pqueue_type = typename PrioQueue::pqueue_type;
    pqueue_type & prioQueue;    ///< reference to externally managed priority queue
    Stack & stack;              ///< reference to externally managed stack

    // HavelHakimi state
    HavelHakimiNodeDegree current_node_degree; ///< Description of the node current selected by the HH algorithm
                                               ///  The node will remain selected until its degree is zero
                                               ///  or there are no more node to become neighbors
    
    value_type current;                        ///< Edge generated by ++operator to be access by */-> operators

    stxxl::int64 numEdges; ///< Upper limit for the number of edges produced
    
    bool is_empty;  ///< Is set, if there are no more edges avaible
    
public:
    /**
     * @param[in,out] prio_queue Reference to the priority queue wrapper. 
     *                           The wrapper has to have a member function queue() referencing the 
     *                           actual priority queue (see class description for capacity requirements). 
     *                           Container must be initiallly empty.
     * @param[in,out] stack      Reference to the stack to be used (see class description for capacity 
     *                           requirements). Container must be empty.
     * @param[in] degrees        Degree sequence supplied via STXXL stream interface.
     */
    template <typename InputStream>
    HavelHakimiGenerator(PrioQueue & prio_queue, Stack & stack, InputStream &degrees) 
        : prioQueue(prio_queue.queue())
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
    
    ~HavelHakimiGenerator() = default;
    
    /**
     * @brief Advance to next edge (if available)
     * @warning Allowed only if !\link empty()
     */
    HavelHakimiGenerator<PrioQueue, Stack>& operator++() {
        assert(!empty());
        
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

    /**
     * @brief A reference to the current edge
     * @warning Valid only if !\link empty()
     */
    const value_type & operator * () const {
        assert(!empty());
        return current;
    };
    
    /**
     * @brief A pointer to the current edge
     * @warning Valid only if !\link empty()
     */
    const value_type * operator -> () const {
        assert(!empty());
        return &current;
    };

    /**
     * @brief Indicates end of stream
     * 
     * Indicates whether the current element accessible via operator*() and operator->() is valid
     * and whether a call to operator++() is allowed;
     */
    bool empty() const {
        return is_empty;
    };

    /**
     * @brief Upper limit for the number of edge that can be produced by the stream.
     * Iif the input degree sequeunce is realizable, the estimation is exact.
     */
    stxxl::int64 maxEdges() const {
        return numEdges;
    };
};

extern template class HavelHakimiGenerator<HavelHakimiPrioQueueInt, std::stack<HavelHakimiNodeDegree>>;