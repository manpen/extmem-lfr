//
// Created by michael on 09.07.15.
//

#ifndef MYPROJECT_HAVELHAKIMIGENERATOR_H
#define MYPROJECT_HAVELHAKIMIGENERATOR_H


#include <stxxl/types>
#include <stxxl/priority_queue>
#include <stxxl/stack>
#include <utility>

class HavelHakimiGenerator {
public:
    typedef std::pair<stxxl::int64, stxxl::int64> value_type;
    struct node_degree {
        stxxl::int64 node;
        stxxl::int64 degree;
    };

private:
    struct ComparatorLess
    {
        bool operator () (const node_degree& a, const node_degree & b) const { return a.degree < b.degree; }
        node_degree min_value() const { return {0LL, std::numeric_limits<stxxl::int64>::min()}; }
    };

    typedef stxxl::PRIORITY_QUEUE_GENERATOR<node_degree, ComparatorLess, 16*1024*1024, 1024*1024>::result pqueue_type;
    typedef pqueue_type::block_type block_type;
    stxxl::read_write_pool<block_type> pool;
    pqueue_type prioQueue;

    typedef stxxl::STACK_GENERATOR<node_degree, stxxl::external, stxxl::grow_shrink>::result node_degree_stack_type;
    node_degree_stack_type stack;

    node_degree current_node_degree;

    value_type current;

    stxxl::int64 numEdges;
public:
    template <typename InputStream>
    HavelHakimiGenerator(InputStream &degrees) : pool(static_cast<stxxl::read_write_pool<block_type>::size_type>(8*1024*1024/block_type::raw_size),
                                                      static_cast<stxxl::read_write_pool<block_type>::size_type>(8*1024*1024/block_type::raw_size)),
                                                 prioQueue(pool) {
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

    const value_type & operator * () const { return current; };
    const value_type * operator -> () const { return &current; };
    HavelHakimiGenerator & operator++ ();
    bool empty() { return prioQueue.empty() && stack.empty(); };

    stxxl::int64 maxEdges() const { return numEdges; };
};


inline std::ostream &operator<<(std::ostream &os, HavelHakimiGenerator::node_degree const &m) {
    return os << "{node:" << m.node << ", degree:" << m.degree << "}";
}


#endif //MYPROJECT_HAVELHAKIMIGENERATOR_H
