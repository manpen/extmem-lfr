#pragma once

#include <defs.h>
#include "TupleHelper.h"

#include <thread>
#include <SyncWorker.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <stxxl/sorter>
#include <stxxl/vector>
#include <EdgeStream.h>

//#define LFR_TESTING

namespace LFR {
class NodeDegreeMembership {
    degree_t _degree;
    community_t _memberships;
    bool _ceil; // FIXME: This eats away 4 bytes


public:
    // second parameter can be used to support non-equal distribution
    // eg to get rid of rounding induced quantization
    degree_t intraCommunityDegree(double mixing, community_t com) const {
        assert(_memberships > 0);
        auto intraDegree = totalInternalDegree(mixing);
        return intraDegree/_memberships + ((intraDegree%_memberships) > com);
    }

    degree_t totalInternalDegree(double mixing) const {
        return degree() - externalDegree(mixing);
    }

    degree_t externalDegree(double mixing) const {
        return static_cast<degree_t>(degree() * mixing) + _ceil;
    }

    NodeDegreeMembership() {}
    NodeDegreeMembership(const degree_t & degree_, const community_t & memberships_, const bool ceil = false) :
        _degree(degree_), _memberships(memberships_), _ceil(ceil) {}

    const degree_t& degree() const {return _degree;}
    const community_t& memberships() const {return _memberships;}
    const bool& ceil() const {return _ceil;}

    DECL_LEX_COMPARE_OS(NodeDegreeMembership, _degree, _memberships);
};

class NodeDegreeMembershipInternalDegComparator {
    const double _mixing;

public:
    NodeDegreeMembershipInternalDegComparator(double m) : _mixing(m) {}

    bool operator()(const NodeDegreeMembership &a, const NodeDegreeMembership &b) const {
        auto ai = a.intraCommunityDegree(_mixing,0);
        auto bi = b.intraCommunityDegree(_mixing,0);
        return std::tie(ai, a.degree(), a.ceil()) > std::tie(bi, b.degree(), b.ceil());
    }

    NodeDegreeMembership max_value() const {
        return {std::numeric_limits<degree_t>::min(),
                std::numeric_limits<community_t>::max(), true};
    }

    NodeDegreeMembership min_value() const {
        return {std::numeric_limits<degree_t>::max(), 1, false};
    }
};

struct CommunityAssignment {
    community_t community_id; //!< community id
    degree_t degree; //!< intra-community degree of this node
    node_t node_id; //!< node id

    CommunityAssignment() {}
    CommunityAssignment(const community_t& community_id_, const node_t degree_, const node_t node_id_)
        : community_id(community_id_), degree(degree_), node_id(node_id_) {}


    bool operator< (const CommunityAssignment& o) const {
        return     (community_id <  o.community_id)
               || ((community_id == o.community_id) && (degree >  o.degree))
               || ((community_id == o.community_id) && (degree == o.degree) && (node_id < o.node_id));
    }

    DECL_TO_TUPLE(community_id, degree, node_id);
    DECL_TUPLE_OS(CommunityAssignment);
};

struct CommunityEdge {
    community_t community_id;
    edge_t edge;

    CommunityEdge() {}
    CommunityEdge(const community_t& community_id, const edge_t& edge) : community_id(community_id), edge(edge) {}

    DECL_LEX_COMPARE_OS(CommunityEdge, edge, community_id);
};

struct OverlapConfigConstDegree {
    community_t multiCommunityDegree;
    node_t overlappingNodes;
};

struct OverlapConfigGeometric {
    degree_t maxDegreeIntraDegree;
};

union OverlapConfig {
    OverlapConfigConstDegree constDegree;
    OverlapConfigGeometric geometric;
};

enum OverlapMethod {
    constDegree, geometric
};

class LFR {
    friend class LFRCommunityAssignBenchmark;

public:
    using NodeDegreeDistribution = MonotonicPowerlawRandomStream<false>;
    using CommunityDistribution = MonotonicPowerlawRandomStream<false>;


protected:
    using WorkerType = SyncWorker;

    // model parameters
    const node_t _number_of_nodes;
    NodeDegreeDistribution::Parameters _degree_distribution_params;
    const community_t _number_of_communities;
    CommunityDistribution::Parameters _community_distribution_params;
    const double _mixing;
    
    OverlapMethod _overlap_method;
    OverlapConfig _overlap_config;

    community_t _overlap_max_memberships;

    uint_t _max_memory_usage;
    uint_t _degree_sum;

    double _community_rewiring_random {0.0};

    // model materialization
    stxxl::sorter<NodeDegreeMembership, NodeDegreeMembershipInternalDegComparator> _node_sorter;

    /**
     * The i-th entry contains the sum of sizes of communities 0 to i-1. It, hence,
     * corresponds to the index of the first assignment of this community in _community_assignments.
     * The last entry corresponds to no community and contains the total sum for easier accessing
     */
    std::vector<node_t> _community_cumulative_sizes;

    /**
     * Community assignments sorted by community (asc), degree (desc), node (asc)
     * refer to _community_cumulative_sizes[k] to get the index of the first entry
     * of community k */
    stxxl::vector<CommunityAssignment> _community_assignments;

    EdgeStream _intra_community_edges;
    EdgeStream _inter_community_edges;
    EdgeStream _edges;


    /// Get community size based on _community_cumulative_sizes
    node_t _community_size(community_t com) const {
        assert(size_t(com+1) < _community_cumulative_sizes.size());
        return _community_cumulative_sizes[com+1] - _community_cumulative_sizes[com];
    }

    void _compute_node_distributions();
    void _compute_community_size();
    void _compute_community_assignments();
    void _correct_community_sizes();
    template <bool is_disjoint>
    void _generate_community_graphs();
    void _generate_global_graph(int_t swaps_per_iteration);
    void _merge_community_and_global_graph();

    void _verify_assignment();
    void _verify_result_graph();

public:
    LFR(const NodeDegreeDistribution::Parameters & node_degree_dist,
        const NodeDegreeDistribution::Parameters & community_degree_dist,
        double mixing_parameter,
        uint_t max_memory_usage
    ) :
        _number_of_nodes(node_degree_dist.numberOfNodes),
        _degree_distribution_params(node_degree_dist),
        _number_of_communities((community_t)community_degree_dist.numberOfNodes),
        _community_distribution_params(community_degree_dist),
        _mixing(mixing_parameter),
        _max_memory_usage(max_memory_usage),
        _node_sorter(NodeDegreeMembershipInternalDegComparator(_mixing), SORTER_MEM)
    {
        _overlap_method = geometric;
        _overlap_config.geometric.maxDegreeIntraDegree = (uint_t)node_degree_dist.maxDegree;

        if (_max_memory_usage < 4 * SORTER_MEM + 1.3 * SORTER_MEM * omp_get_max_threads() + sizeof(node_t) * _number_of_communities * 4) {
            throw std::runtime_error("Not enough memory given, need at least memory for a sorter and a bit more per thread, four global sorters and several values per community.");
        }

        _max_memory_usage -= SORTER_MEM; // for _node_sorter FIXME see if we really need it constantly...
        _max_memory_usage -= _number_of_communities * sizeof(node_t); // for _community_cumulative_sizes
    }

    LFR(const LFR& other)
          : LFR(other._degree_distribution_params, other._community_distribution_params, other._mixing, other._max_memory_usage)
    {
        setOverlap(other._overlap_method, other._overlap_config);
    }

    void setOverlap(OverlapMethod method, const OverlapConfig & config) {
        _overlap_method = method;
        _overlap_config = config;
    }

    EdgeStream & get_edges() {
        return _edges;
    }

    void setCommunityRewiringRandom(const double& v) {
        assert(v >= 0);
        _community_rewiring_random = v;
    }

    /**
     * This exports the community assignments such that in every line a node id and its community/communities are written (separated by space).
     * Node ids are 1-based.
     */
    template <typename ostream_t>
    void export_community_assignment(ostream_t & os) {
        using node_community_t = std::tuple<node_t, community_t>;
        using nc_comp_t = GenericComparatorTuple<node_community_t>::Ascending;

        stxxl::sorter<node_community_t, nc_comp_t> output_sorter(nc_comp_t(), SORTER_MEM);

        for (const auto& ca : _community_assignments) {
            output_sorter.push(std::make_tuple(ca.node_id, ca.community_id));
        }

        output_sorter.sort();

        node_t u = std::get<0>(*output_sorter);
        os << u;
        for (; !output_sorter.empty(); ++output_sorter) {
            if (std::get<0>(*output_sorter) != u) {
                u = std::get<0>(*output_sorter);
                os << std::endl << u;
            }

            os << " " << std::get<1>(*output_sorter);
        }
    }

    /**
     * This exports the community assignments such that in every line a node id and its community/communities are written (separated by space).
     * Node ids are 1-based.
     */
    template <typename ostream_t>
    void export_community_assignment_binary(ostream_t & os) {
        for (const auto& ca : _community_assignments) {
            static_assert(std::is_same<int32_t, decltype(ca.node_id)>::value, "node id is not longer int32_t");
            static_assert(std::is_same<int32_t, decltype(ca.community_id)>::value, "node id is not longer int32_t");
            os.write(reinterpret_cast<const char*>(&ca.node_id), 4);
            os.write(reinterpret_cast<const char*>(&ca.community_id), 4);
        }
    }

    void run();
};


}
