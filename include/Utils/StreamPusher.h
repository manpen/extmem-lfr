#pragma once

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
            _output(*_input);
    }
};
