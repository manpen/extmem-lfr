#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <tuple>
#include <set>
#include <vector>
#include <stack>
#include <functional>
#include "EdgeSwapBase.h"
#include "GenericComparator.h"
#include "TupleHelper.h"
#include <algorithm>
#include <iterator>
#include <functional>
#include <array>

class EdgeSwapInternalSwapsBase : public EdgeSwapBase {
public:
protected:
#ifdef EDGE_SWAP_DEBUG_VECTOR
    typename debug_vector::bufwriter_type _debug_vector_writer;
#endif

private:
    struct edge_existence_request_t {
        edge_t e;
        int_t sid;
        bool forward_only; // if this requests is only for generating the correct forwaring information but no existence information is needed
        DECL_TO_TUPLE(e, sid, forward_only);
        bool operator< (const edge_existence_request_t& o) const {
            return (e < o.e || (e == o.e && (sid > o.sid || (sid == o.sid && forward_only < o.forward_only))));
        }
    };

    stxxl::sorter<edge_existence_request_t, typename GenericComparatorStruct<edge_existence_request_t>::Ascending> _query_sorter; // Query of possible conflict edges. This may be large (too large...)

    struct edge_existence_answer_t {
        int_t sid;
        edge_t e;
        int_t numExistences;
        DECL_LEX_COMPARE(edge_existence_answer_t, sid, e);
    };

    std::vector<edge_existence_answer_t> _edge_existence_pq;

    struct edge_existence_successor_t {
        int_t from_sid;
        edge_t e;
        int_t to_sid;
        DECL_LEX_COMPARE(edge_existence_successor_t, from_sid, e);
    };

    std::vector<edge_existence_successor_t> _edge_existence_successors;

    void simulateSwapsAndGenerateEdgeExistenceQuery(const std::vector<swap_descriptor> &swaps, const std::vector<edge_t> &edges, const std::array<std::vector<bool>, 2> &swap_has_successor);
    template <typename EdgeReader>
    void loadEdgeExistenceInformation(EdgeReader &edgeReader);
    void performSwaps(const std::vector<swap_descriptor>& swaps, std::vector<edge_t> &edges);

protected:
    template <typename EdgeReader>
    void executeSwaps(const std::vector< EdgeSwapBase::swap_descriptor > &swaps, std::vector< edge_t > &edges, const std::array< std::vector< bool >, 2> &swap_has_successor, EdgeReader &edgeReader);

public:
    EdgeSwapInternalSwapsBase(const EdgeSwapInternalSwapsBase &) = delete;

    EdgeSwapInternalSwapsBase() :
        EdgeSwapBase()
#ifdef EDGE_SWAP_DEBUG_VECTOR
        , _debug_vector_writer(_result)
#endif
        , _query_sorter(typename GenericComparatorStruct<edge_existence_request_t>::Ascending(), SORTER_MEM)
    {
    }

};