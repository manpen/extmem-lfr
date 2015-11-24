#pragma once

#include <algorithm>

#include <stxxl/vector>
#include <stxxl/sorter>
#include "DistributionCount.h"

#include "defs.h"

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
    
    void _compute_node_degrees_from_edges(
        InputIterator edges_begin, InputIterator edges_end
    ) {
        T max_node_id = 0;
        
        stxxl::sorter<T, DegreeDistributionCheckIntComp> 
            node_sorter(DegreeDistributionCheckIntComp(), 256*1024*1024);

        // copy nodes of edges into sorter
#ifndef NDEBUG            
        T sum_push = 0;
        T num_push = 0;
#endif        
        for (InputIterator it = edges_begin; it != edges_end; ++it) {
            edge_type edge = *it;
            max_node_id = std::max(max_node_id, std::max<T>(edge.first, edge.second));
            node_sorter.push(edge.first);
            node_sorter.push(edge.second);

#ifndef NDEBUG            
            sum_push += edge.first + edge.second;
            num_push += 2;
#endif        
        }
        node_sorter.sort();
        
        _degrees.resize(max_node_id + 1);
        for(T i=0; i <= max_node_id; i++)
            _degrees[i] = 0;
        
        bool first = true;
        T last_node = 0; // prevent warning
        T counter = 0;
        
#ifndef NDEBUG            
        T sum_consume = 0;
#endif
        while(!node_sorter.empty()) {
            auto node = *node_sorter;
#ifndef NDEBUG            
            sum_consume += node;
#endif
            
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
            ++node_sorter;
        }

        
        _degrees[last_node] += counter;

#ifndef NDEBUG            
        T num_ref = std::accumulate(_degrees.begin(), _degrees.end(), 0);
        std::cout << "Pushed: "  << sum_push << " Consumed: " << sum_consume << std::endl;
        std::cout << "Num Pushed: " << num_push << " Generated: " << num_ref << std::endl;
        assert(sum_push == sum_consume);
        assert(num_push == num_ref);
#endif
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
    
    
    