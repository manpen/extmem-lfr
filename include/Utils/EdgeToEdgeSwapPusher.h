#pragma once
#include <random>

#include <defs.h>
#include <Swaps.h>

template <class EdgesIn, class EdgesOut, class SwapsOut>
class EdgeToEdgeSwapPusher {
    const seed_t _seed;
    const edgeid_t _numberOfEdges;


    EdgesIn & _input;
    EdgesOut & _edge_output;
    SwapsOut & _output;

public:
    EdgeToEdgeSwapPusher(EdgesIn& input, EdgesOut& edge_output, SwapsOut& output, seed_t seed = stxxl::get_next_seed(), bool start_now = true)
            : EdgeToEdgeSwapPusher(input, input.size(), edge_output, output, seed, start_now)
    {}


    EdgeToEdgeSwapPusher(EdgesIn& input, edgeid_t numberOfEdges, EdgesOut& edge_output, SwapsOut& output, seed_t seed = stxxl::get_next_seed(), bool start_now = true)
            : _seed(seed), _numberOfEdges(numberOfEdges),
              _input(input), _edge_output(edge_output), _output(output)
    {
        if (start_now)
            process();
    }

    void process() {
        std::mt19937_64 _randomGen(_seed);
        std::uniform_int_distribution<edgeid_t> _randomDistr(0, _numberOfEdges-1);

        auto prev = edge_t::invalid();

        for (edgeid_t count = 0; !_input.empty(); ++_input, ++count) {
            const auto& curr = *_input;
            _edge_output.push(curr);

            if (UNLIKELY(curr == prev || curr.is_loop())) {
                edgeid_t random_partner;

                do {
                    random_partner = _randomDistr(_randomGen);
                } while (UNLIKELY(random_partner == count));

                if (count < random_partner)
                    _output.push({count, random_partner, _randomGen() & 1});
                else
                    _output.push({random_partner, count, _randomGen() & 1});
            }

            prev = curr;
        }
    }
};
