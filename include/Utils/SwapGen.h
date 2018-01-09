//
// Created by hung on 15.05.17.
//

#pragma once
#include <EdgeStream.h>
#include <SwapStream.h>
#include <random>


template <typename EdgeVector, typename EdgeStream, typename EdgeSwapMsgSorter, typename SwapDirectionStream>
class SwapGen {
    EdgeVector & _edges;
    EdgeStream & _redirect;
    EdgeSwapMsgSorter & _swaps_out;
    SwapDirectionStream & _swaps_dir;
    const edgeid_t _num_edges;
    edgeid_t _swap_count;

public:
    // _swaps will usually be an empty SwapStream
    SwapGen(EdgeVector& edges, EdgeStream& redirect, EdgeSwapMsgSorter& swaps, SwapDirectionStream& swaps_dir, edgeid_t num_edges)
        : _edges(edges), _redirect(redirect), _swaps_out(swaps), _swaps_dir(swaps_dir), _num_edges(num_edges), _swap_count(0)
    {

    }

    // We are using mt19937_64 here, since edgeid_t is of type int_t = std::int64_t
    bool process() {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<edgeid_t> dist(0, _num_edges - 1);
        std::bernoulli_distribution ber(0.5);

        swapid_t swap_id = 0;

        auto prev = *_edges;
        ++_edges;
        _redirect.push(prev);

        if (prev.is_loop()) {
            edgeid_t swap_partner;
            do {
                swap_partner = dist(gen);
            } while (UNLIKELY(swap_partner == 0));
            _swaps_out->push({0, swap_id++});
            _swaps_out->push({swap_partner, swap_id++});
            _swaps_dir.push(ber(gen));

        }

        // We start from 1 because, first edge has been checked already
        // In this implementation when we have several copies of edges
        // [u, v], [u, v], [u, v], ...
        // we don't generate a random swap constituent for the first occurrence of the edge.
        for (edgeid_t count = 1; !_edges.empty(); ++_edges, ++count) {
            auto curr = *_edges;
            _redirect.push(curr);

            if (curr == prev || curr.is_loop()) {
                edgeid_t swap_partner;

                do {
                    swap_partner = dist(gen);
                } while (UNLIKELY(swap_partner == count));

                if (count < swap_partner) {
                    _swaps_out->push({count, swap_id++});
                    _swaps_out->push({swap_partner, swap_id++});
                    _swaps_dir.push(ber(gen));
                } else {
                    _swaps_out->push({swap_partner, swap_id++});
                    _swaps_out->push({count, swap_id++});
                    _swaps_dir.push(ber(gen));
                }
            }

            prev = curr;
        }

        return swap_id > 0;
    }

    edgeid_t swapCount() const {
        return _swap_count;
    }
};