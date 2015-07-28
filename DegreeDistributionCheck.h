#pragma once

#include <algorithm>

#include <stxxl/vector>
#include <stxxl/sorter>
#include "DistributionCount.h"


/*
 * Implements two features:
 *  1.) Provided an (unsorted) list of edges, it computes the degree of every node
 *  2.) Offers an DistributionCount-based streaming interface to query the degree distribution
 */
template <typename InputIterator, typename EdgeType = typename InputIterator::value_type, typename T = stxxl::int64>
class DegreeDistributionCheck {
public:
    struct DegreeDistributionCheckIntComp {
        bool operator()(const T &a, const T &b) const {return a < b;}
        T min_value() const {return std::numeric_limits<T>::min();}
        T max_value() const {return std::numeric_limits<T>::max();}
    };
    
    using edge_type = EdgeType;
    using degree_vector_type = typename stxxl::VECTOR_GENERATOR<T>::result;
    using degree_sorter_type = typename stxxl::sorter<T, DegreeDistributionCheckIntComp>;
    using distribution_type = DistributionCount<degree_sorter_type, T>;
    
private:    
    degree_vector_type _degrees;
    degree_sorter_type _sorter; 
    
    /* Computes the degree of each node (assuming the edge list represents an undirected (multi)graph).
     * To acchive sort-complexity the following algorithm is used:
     *  0.) Initialise a vector with n integers, where n = max( {u,v} for {u,v} in edges )
     *  1.) Sort by the first node of each edge
     *  2.) Scan over the edge list and degree vector in parallel, increasing the latter of each node in the former one
     * 
     *  3.) Repeat steps 1 and 2 with the second entry of each edge
     */
    void _compute_node_degrees_from_edges(
        InputIterator edges_begin, InputIterator edges_end
    ) {
        struct my_comparator {
            bool operator()(const edge_type &a, const edge_type &b) const {return a.first < b.first;}
            edge_type min_value() const {return {std::numeric_limits<T>::min(),std::numeric_limits<T>::min()};}
            edge_type max_value() const {return {std::numeric_limits<T>::max(),std::numeric_limits<T>::max()};}
        };    
        
        
        T max_node_id = 0;
        
        for(unsigned int phase=0; phase < 2; phase++) {
            // in the first phase we deal with the edge's first node, in the second phase with the second one
            stxxl::sorter<edge_type, my_comparator> edge_sorter(my_comparator(), 256*1024*1024);
            
            for (InputIterator it = edges_begin; it != edges_end; ++it) {
                edge_type edge = *it;
                if (phase) {
                    std::swap(edge.first, edge.second);
                } else {
                    max_node_id = std::max(max_node_id, std::max<T>(edge.first, edge.second));
                }
                edge_sorter.push(edge);
            }
                
            edge_sorter.sort();
            
            if (!phase)
                _degrees.resize(max_node_id + 1);
            
            bool first = true;
            T last_node, counter = 0;
            while(!edge_sorter.empty()) {
                auto edge = *edge_sorter;
                auto node = edge.first;
                
                if (last_node != node || first) {
                    if (LIKELY (!first)) {
                        _degrees[last_node] += counter;
                    } else {
                        first = false;
                    }
                    
                    counter = 0;
                    last_node = node;
                }
                
                counter++;
                ++edge_sorter;
            }
            
            _degrees[last_node] += counter;
        }
    }
    
public:
    // Construct object and compute node degrees (expensive)
    DegreeDistributionCheck(InputIterator edges_begin, InputIterator edges_end)
        : _sorter(DegreeDistributionCheckIntComp(), 256*1024*1024)
    {
        _compute_node_degrees_from_edges(edges_begin, edges_end);
    }
 
    degree_vector_type & getDegrees() {
        return _degrees;
    }

    const degree_vector_type & getDegrees() const {
        return _degrees;
    }

    // Construct and instance of DistributionCount to query the degree distribution
    distribution_type getDistribution() {
        _sorter.clear();
        
        for(const auto d : _degrees)
            _sorter.push(d);
        
        _sorter.sort();
        
        return {_sorter};
    }
};
    
    
    