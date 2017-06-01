#pragma once
#include <random>

/**
 * Connects a producer implemented as a stream to an consumer which accepts
 * elements using a push() method.
 */
template <class InStream, class OutReceiver>
class StreamPusher {
    InStream & _input;
    OutReceiver & _output;

public:
    StreamPusher(InStream& input, OutReceiver& output, bool start_now = true)
        : _input(input), _output(output)
    {
        if (start_now)
            process();
    }

    void process() {
        for(; !_input.empty(); ++_input)
            _output.push(*_input);
    }
};

template <class EdgesIn, class SwapsOut>
class EdgeSwapGenPusher {
    EdgesIn & _input;
    SwapsOut & _output;
    edgeid_t _input_Num;

public:
    EdgeSwapGenPusher(EdgesIn& input, SwapsOut& output, bool start_now = true)
            : _input(input), _output(output), _input_Num(input.size())
    {
        if (start_now)
            process();
    }

    void process() {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<node_t> dis(0, _input_Num - 1);
        std::bernoulli_distribution ber(0.5);

        auto prev = *_input;
        ++_input;
        if (prev.is_loop())
            _output.push({0, dis(gen), ber(gen)});

        for (edgeid_t count = 1; !_input.empty(); ++_input, ++count) {
            auto curr = *_input;
            if (curr == prev || curr.is_loop()) {
                auto random_partner = dis(gen);
                if (LIKELY(random_partner != count))
                    _output.push({count, random_partner, ber(gen)});
            }
        }

    }
};


