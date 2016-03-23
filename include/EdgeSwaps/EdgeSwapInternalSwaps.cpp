#include "EdgeSwapInternalSwaps.h"
#include <EdgeSwaps/EdgeSwapInternalSwapsBase_impl.h>

#if 1
    #include <parallel/algorithm>
    #define SEQPAR __gnu_parallel
#else
    #define SEQPAR std
#endif

void EdgeSwapInternalSwaps::updateEdgesAndLoadSwapsWithEdgesAndSuccessors() {
    // stores load requests with information who requested the edge
    struct edgeid_swap_t {
        edgeid_t eid;
        internal_swapid_t sid;
        unsigned char spos;

        DECL_LEX_COMPARE(edgeid_swap_t, eid, sid, spos);
    };

    struct edge_swap_t {
        edge_t e;
        internal_swapid_t sid;
        unsigned char spos;

        DECL_LEX_COMPARE(edge_swap_t, e, sid, spos);
    };


    // if we have no swaps to load and no edges to write back, do nothing (might happen by calling flush several times)
    if (_current_swaps.empty() && _current_semiloaded_swaps.empty() && _edge_ids_in_current_swaps.empty()) return;

    // load edge endpoints for edges in the swap set
    std::vector<edgeid_swap_t> edgeIdLoadRequests;
    edgeIdLoadRequests.reserve(_current_swaps.size() * 2 + _current_semiloaded_swaps.size());

    std::vector<edge_swap_t> edgeLoadRequests;
    edgeLoadRequests.reserve(_current_semiloaded_swaps.size());

    uint_t numMaxSwaps = _current_semiloaded_swaps.size() + _current_swaps.size();

    // copy old edge ids for writing back
    std::vector<edgeid_t> old_edge_ids;
    old_edge_ids.swap(_edge_ids_in_current_swaps);
    _edge_ids_in_current_swaps.reserve(numMaxSwaps * 2);

    // copy updated edges for writing back
    std::vector<edge_t> updated_edges;
    updated_edges.swap(_edges_in_current_swaps);
    _edges_in_current_swaps.reserve(numMaxSwaps * 2);

    _swap_has_successor[0].clear();
    _swap_has_successor[0].resize(numMaxSwaps);
    _swap_has_successor[1].clear();
    _swap_has_successor[1].resize(numMaxSwaps);

    for (internal_swapid_t i = 0; i < _current_swaps.size(); ++i) {
        const auto & swap = _current_swaps[i];
        edgeIdLoadRequests.push_back(edgeid_swap_t {swap.edges()[0], i, 0});
        edgeIdLoadRequests.push_back(edgeid_swap_t {swap.edges()[1], i, 1});
    }

    internal_swapid_t semiLoadedOffset = _current_swaps.size();

    for (internal_swapid_t i = 0; i < _current_semiloaded_swaps.size(); ++i) {
        const auto & swap = _current_semiloaded_swaps[i];
        edgeLoadRequests.push_back(edge_swap_t {swap.edge(), i + semiLoadedOffset, 0});
        edgeIdLoadRequests.push_back(edgeid_swap_t {swap.eid(), i + semiLoadedOffset, 1});
        _current_swaps.push_back(SwapDescriptor(-1, swap.eid(), swap.direction()));
    }

    std::cout << "Requesting " << edgeIdLoadRequests.size()  + edgeLoadRequests.size() << " non-unique edges for internal swaps" << std::endl;
    SEQPAR::sort(edgeIdLoadRequests.begin(), edgeIdLoadRequests.end());
    SEQPAR::sort(edgeLoadRequests.begin(), edgeLoadRequests.end());


    { // load edges from EM. Generates successor information and swap_edges information (for the first edge in the chain).
        int_t int_eid = 0;
        edgeid_t id = 0;

        typename edge_vector::bufreader_type edge_reader(_edges);
        auto request_it = edgeIdLoadRequests.begin();
        auto semi_loaded_request_it = edgeLoadRequests.begin();

        auto use_edge = [&] (const edge_t & cur_e) {
            internal_swapid_t sid;
            unsigned char spos;

            auto match_request = [&]() {
                if (request_it != edgeIdLoadRequests.end() && request_it->eid == id && !(semi_loaded_request_it != edgeLoadRequests.end() && semi_loaded_request_it->e == cur_e && semi_loaded_request_it->sid < request_it->sid)) {
                    sid = request_it->sid;
                    spos = request_it->spos;
                    ++request_it;
                    return true;
                } else if (semi_loaded_request_it != edgeLoadRequests.end() && semi_loaded_request_it->e == cur_e) {
                    sid = semi_loaded_request_it->sid;
                    spos = semi_loaded_request_it->spos;
                    ++semi_loaded_request_it;
                    return true;
                } else {
                    return false;
                }
            };

            if (match_request()) {
                _edge_ids_in_current_swaps.push_back(id);
                _edges_in_current_swaps.push_back(cur_e);
                assert(static_cast<uint_t>(int_eid) == _edges_in_current_swaps.size() - 1);

                // set edge id to internal edge id
                _current_swaps[sid].edges()[spos] = int_eid;

                auto lastSpos = spos;
                auto lastSid = sid;

                // further requests for the same swap - store successor information
                while (match_request()) {
                    // set edge id to internal edge id
                    _current_swaps[sid].edges()[spos] = int_eid;
                    _swap_has_successor[lastSpos][lastSid] = true;
                    lastSpos = spos;
                    lastSid = sid;
                }

                ++int_eid;
            }
        };

        if (updated_edges.empty()) {
            // just read edges
            for (; !edge_reader.empty(); ++id, ++edge_reader) {
                use_edge(*edge_reader);
            }

        } else {
            // read old edge vector and merge in updates, write out result
            edge_vector output_vector;
            output_vector.reserve(_edges.size());
            typename edge_vector::bufwriter_type writer(output_vector);

            auto old_e = old_edge_ids.begin();
            auto new_e = updated_edges.begin();

            int_t read_id = 0;
            edge_t cur_e;

            for (; !edge_reader.empty() || new_e != updated_edges.end(); ++id) {
                // Skip old edges
                while (old_e != old_edge_ids.end() && *old_e == read_id) {
                    ++edge_reader;
                    ++read_id;
                    ++old_e;
                }

                // merge update edges and read edges
                if (new_e != updated_edges.end() && (edge_reader.empty() || *new_e < *edge_reader)) {
                    cur_e = *new_e;
                    writer << cur_e;
                    ++new_e;
                } else {
                    if (edge_reader.empty()) { // due to the previous while loop both could be empty now
                        break; // abort the loop as we do not have any edges to process anymore.
                    }

                    cur_e = *edge_reader;
                    writer << cur_e;
                    ++read_id;
                    ++edge_reader;
                }

                use_edge(cur_e);
            }

            writer.finish();
            _edges.swap(output_vector);
        }
    }
};



void EdgeSwapInternalSwaps::process_buffer() {
    bool show_stats = true;

    if (_current_swaps.empty() && _current_semiloaded_swaps.empty())
        return;

    _start_stats(show_stats);

    updateEdgesAndLoadSwapsWithEdgesAndSuccessors();

    _report_stats("load swaps", show_stats);

    typename edge_vector::bufreader_type edgeReader(_edges);

    executeSwaps(_current_swaps, _edges_in_current_swaps, _swap_has_successor, edgeReader);

    SEQPAR::sort(_edges_in_current_swaps.begin(), _edges_in_current_swaps.end());

    if (_updated_edges_callback) {
        _updated_edges_callback(_edges_in_current_swaps);
    }

    _current_swaps.clear();
    _current_swaps.reserve(_num_swaps_per_iteration);

    _current_semiloaded_swaps.clear();
}
