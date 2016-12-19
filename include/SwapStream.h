#pragma once

#include <defs.h>
#include <stxxl/sequence>
#include <memory>

class SwapStream {
public:
    using value_type = SwapDescriptor;

protected:
    using em_buffer_t = stxxl::sequence<SwapDescriptor>;
    using em_reader_t = typename em_buffer_t::stream;

    std::unique_ptr<em_buffer_t> _em_buffer;
    std::unique_ptr<em_reader_t> _em_reader;

    enum Mode {WRITING, READING};
    Mode _mode;

    // WRITING
    swapid_t _number_of_swaps;

    // READING
    value_type _current;
    bool _empty;


public:
    SwapStream()
    { clear(); }

    SwapStream(const SwapStream &) = delete;

    ~SwapStream() {
        _em_reader.reset(nullptr);
        _em_buffer.reset(nullptr);
    }

    SwapStream(SwapStream&&) = default;

    SwapStream& operator=(SwapStream&&) = default;

    // Write interface
    void push(const SwapDescriptor swap) {
        assert(_mode == WRITING);

        em_buffer_t & em_buffer = *_em_buffer;

        em_buffer.push_back(swap);
        ++_number_of_swaps;
    }

    void consume() {rewind();}

    void rewind() {
        _mode = READING;
        _em_reader.reset(new em_reader_t(*_em_buffer));
        // Dummy value
        _current = {0, 1, 0};
        _empty = _em_reader->empty();

        if (!empty())
            ++(*this);
    }

    void clear() {
        _mode = WRITING;
        _number_of_swaps = 0;
        _em_reader.reset(nullptr);
        _em_buffer.reset(new em_buffer_t(16, 16));
    }

    size_t size() const {
        return static_cast<size_t>(_number_of_swaps);
    }

// Consume interface
    //! return true when in write mode or if edge list is empty
    bool empty() const {
        return _empty;
    }

    const value_type& operator*() const {
        assert(READING == _mode);
        return _current;
    }

    const value_type* operator->() const {
        assert(READING == _mode);
        return &_current;
    }


    SwapStream& operator++() {
        assert(READING == _mode);
        assert(!_empty);

        em_reader_t& reader = *_em_reader;

        // handle end of stream
        _empty = reader.empty();
        if (UNLIKELY(_empty))
            return *this;

        _current = *reader;

        ++reader;

        return *this;
    }
};