#pragma once

#include <defs.h>
#include "TupleHelper.h"

#include <thread>
#include <SyncWorker.h>
#include <PowerlawDegreeSequence.h>
#include <stxxl/sorter>
#include <stxxl/vector>

//#define LFR_TESTING

namespace LFR {
struct NodeDegreeMembership {
    degree_t degree;
    community_t memberships;

    // second parameter can be used to support non-equal distribution
    // eg to get rid of rounding induced quantization
    degree_t intraCommunityDegree(double mixing, community_t com) const {
        assert(memberships >= 0);
        auto intraDegree = totalInternalDegree(mixing);
        return intraDegree/memberships + ((intraDegree%memberships) > com);
    }

    degree_t totalInternalDegree(double mixing) const {
        return degree - externalDegree(mixing);
    }

    degree_t externalDegree(double mixing) const {
        return static_cast<degree_t>(degree * mixing);
    }

    NodeDegreeMembership() {}
    NodeDegreeMembership(const degree_t & degree_, const community_t & memberships_) :
        degree(degree_), memberships(memberships_) {}

    DECL_LEX_COMPARE_OS(NodeDegreeMembership, degree, memberships);
};

class NodeDegreeMembershipInternalDegComparator {
    const double _mixing;

public:
    NodeDegreeMembershipInternalDegComparator(double m) : _mixing(m) {}

    bool operator()(const NodeDegreeMembership &a, const NodeDegreeMembership &b) const {
        auto ai = a.intraCommunityDegree(_mixing,0);
        auto bi = b.intraCommunityDegree(_mixing,0);
        return std::tie(ai, a.degree) > std::tie(bi, b.degree);
    }

    NodeDegreeMembership max_value() const {
        return {std::numeric_limits<degree_t>::min(),
                std::numeric_limits<community_t>::max()};
    }

    NodeDegreeMembership min_value() const {
        return {std::numeric_limits<degree_t>::max(), 1};
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


class LFR {
public:
    using NodeDegreeDistribution = PowerlawDegreeSequence;
    using CommunityDistribution = PowerlawDegreeSequence;


protected:
    using WorkerType = SyncWorker;

    // model parameters
    const node_t _number_of_nodes;
    NodeDegreeDistribution::Parameters _degree_distribution_params;
    degree_t _max_degree_within_community;
    const community_t _number_of_communities;
    CommunityDistribution::Parameters _community_distribution_params;
    const double _mixing;

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

    /// Get community size based on _community_cumulative_sizes
    node_t _community_size(community_t com) const {
        assert(size_t(com+1) < _community_cumulative_sizes.size());
        return _community_cumulative_sizes[com+1] - _community_cumulative_sizes[com];
    }

    void _compute_node_distributions();
    void _compute_community_size();
    void _compute_community_assignments();

public:
    LFR(const NodeDegreeDistribution::Parameters & node_degree_dist,
        const NodeDegreeDistribution::Parameters & community_degree_dist,
        double mixing_parameter) :
        _number_of_nodes(node_degree_dist.numberOfNodes),
        _degree_distribution_params(node_degree_dist),
        _max_degree_within_community((uint_t)node_degree_dist.maxDegree),
        _number_of_communities((community_t)community_degree_dist.numberOfNodes),
        _community_distribution_params(community_degree_dist),
        _mixing(mixing_parameter),
        _node_sorter(NodeDegreeMembershipInternalDegComparator(_mixing), SORTER_MEM)
    {}

    void setMaxDegreeWithinCommunity(degree_t v) {
        assert(v >= _degree_distribution_params.minDegree);
        _max_degree_within_community = v;
    }

    void run();
};


}
