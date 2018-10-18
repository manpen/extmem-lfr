#pragma once

#include <stxxl/vector>
#include <defs.h>
#include "LFR.h"
#include <Swaps.h>
#include <EdgeSwaps/EdgeSwapInternalSwapsBase.h>
#include <array>

#ifndef SEQPAR
    #if 1
        #include <parallel/algorithm>
        #define SEQPAR __gnu_parallel
    #else
        #define SEQPAR std
    #endif
#endif

class CommunityEdgeRewiringSwaps : public EdgeSwapInternalSwapsBase {
private:
    using edge_community_t = LFR::CommunityEdge;
    using edge_community_vector_t = stxxl::vector<edge_community_t>;
    edge_community_vector_t &_community_edges;
    size_t _max_swaps;

    struct community_swap_edges_t {
        community_t community_id;
        edge_t duplicate_edge;
        edge_t partner_edge;
    };

    struct edge_community_swap_t {
        edge_t e;
        community_t community_id;
        uint_t sid;
        unsigned char spos;

        DECL_LEX_COMPARE(edge_community_swap_t, e, community_id, sid, spos);
    };

    struct random_edge_t {
        edgeid_t id;
        edge_t edge;

#ifndef NDEBUG
        community_t comm;

        random_edge_t() : id(-1), edge(edge_t::invalid()), comm(-1) {}

        random_edge_t(const edgeid_t& i_, const edge_t& e_, const community_t& c_)
                : id(i_), edge(e_), comm(c_)
        {}

#else
        random_edge_t()  {}

        random_edge_t(const edgeid_t& i_, const edge_t& e_, const community_t&)
                : id(i_), edge(e_)
        {}
#endif
    };

    std::vector<SwapDescriptor> _current_swaps;
    std::vector<edge_t> _edges_in_current_swaps;
    std::vector<edgeid_t> _edge_ids_in_current_swaps;
    std::vector<community_t> _community_of_current_edge;
    std::array<std::vector<bool>, 2> _swap_has_successor;

    const double _random_edge_ratio;

    template <typename Callback>
    void loadAndStoreEdges(Callback callback);

    void deleteDuplicates();

    class EdgeReaderWrapper {
    private:
        stxxl::vector<edge_community_t>::bufreader_type _reader;
    public:
        EdgeReaderWrapper(edge_community_vector_t& intra_edges) : _reader(intra_edges) { };

        EdgeReaderWrapper& operator++() {
#ifndef NDEBUG
            auto previous = *_reader;
#endif
            ++_reader;
#ifndef NDEBUG
            assert(_reader.empty() || previous != *_reader);
#endif
            return *this;
        };

        void rewind() {
            _reader.rewind();
        };

        const edge_t& operator*() const {
            return _reader->edge;
        };

        const edge_t* operator->() const {
            return &(_reader->edge);
        };

        bool empty() const {
            return _reader.empty();
        };

        stxxl::vector<edge_community_t>::size_type size() const {
            return _reader.size();
        };
    };

public:
    CommunityEdgeRewiringSwaps(stxxl::vector<edge_community_t> &intra_edges, const size_t& max_swaps, const double& random_edge_ratio)
            : _community_edges(intra_edges)
            , _max_swaps(max_swaps)
            , _random_edge_ratio(random_edge_ratio)
    {};

    void run();
};
