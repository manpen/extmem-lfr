#pragma once

#include <defs.h>
#include "TupleHelper.h"

#include <thread>
#include <SyncWorker.h>
#include <PowerlawDegreeSequence.h>
#include <stxxl/sorter>


namespace LFR {
struct NodeDegreeMembership {
    degree_t degree;
    community_t memberships;

    degree_t intraCommunityDegree(double mixing) const {
        assert(memberships >= 0);
        return static_cast<degree_t>((1.0-mixing) * degree / memberships);
    }

    degree_t totalInternalDegree(double mixing) const {
        return intraCommunityDegree(mixing);
    }

    degree_t externalDegree(double mixing) const {
        return degree - totalInternalDegree(mixing);
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
        auto ai = a.intraCommunityDegree(_mixing);
        auto bi = b.intraCommunityDegree(_mixing);
        return std::tie(ai, a.degree) > std::tie(bi, b.degree);
    }

    NodeDegreeMembership min_value() const {
        return {std::numeric_limits<degree_t>::min(), std::numeric_limits<community_t>::max()};
    }

    NodeDegreeMembership max_value() const {
        return {std::numeric_limits<degree_t>::max(), 1};
    }
};

struct CommunityAssignment {
    community_t community;
    node_t node;

    CommunityAssignment() {}
    CommunityAssignment(const community_t& community_, const node_t node_)
        : community(community_), node(node_) {}

    DECL_LEX_COMPARE_OS(CommunityAssignment, community, node);
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
    std::vector<node_t> _community_sizes;
    stxxl::vector<CommunityAssignment> _community_assignments;


    void _compute_node_distributions();
    void _compute_community_size();
    void _compute_community_assignments();

public:
    LFR(node_t number_of_nodes, const NodeDegreeDistribution::Parameters & node_degree_dist,
        community_t number_of_communities, const NodeDegreeDistribution::Parameters & community_degree_dist,
        double mixing_parameter) :
        _number_of_nodes(number_of_nodes),
        _degree_distribution_params(node_degree_dist),
        _max_degree_within_community((uint_t)node_degree_dist.maxDegree),
        _number_of_communities(number_of_communities),
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
