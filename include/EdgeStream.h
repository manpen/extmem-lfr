#pragma once

#include <defs.h>
#include <stxxl/sequence>
#include <memory>

class EdgeStream {
public:
    using value_type = edge_t;

protected:
    using em_buffer_t = stxxl::sequence<node_t>;
    using em_reader_t = typename em_buffer_t::stream;

    std::unique_ptr<em_buffer_t> _em_buffer;
    std::unique_ptr<em_reader_t> _em_reader;

    enum Mode {WRITING, READING};
    Mode _mode;

    // WRITING
    node_t _current_out_node;
    edgeid_t _number_of_edges;

    // READING
    value_type _current;
    bool _empty;

public:
    EdgeStream() {clear();}

    EdgeStream(const EdgeStream &) = delete;

    ~EdgeStream() {
        // in this order ;)
        _em_reader.reset(nullptr);
        _em_buffer.reset(nullptr);
    }

    EdgeStream(EdgeStream&&) = default;
    EdgeStream& operator=(EdgeStream&&) = default;

// Write interface
    void push(const edge_t& edge) {
        assert(_mode == WRITING);

        em_buffer_t & em_buffer = *_em_buffer;

        // we need a non-decreasing out-node sequence
        assert(_current_out_node <= edge.first);

        while(UNLIKELY(_current_out_node < edge.first)) {
            em_buffer.push_back(INVALID_NODE);
            _current_out_node++;
        }

        em_buffer.push_back(edge.second);
        _number_of_edges++;
    }

    //! see rewind
    void consume() {rewind();}

    //! switches to read mode and resets the stream
    void rewind() {
        _mode = READING;
        _em_reader.reset(new em_reader_t(*_em_buffer));
        _current = {0, 0};
        _empty = _em_reader->empty();

        if (!empty())
            ++(*this);
    }

    // returns back to writing mode on an empty stream
    void clear() {
        _mode = WRITING;
        _current_out_node = 0;
        _number_of_edges = 0;
        _em_reader.reset(nullptr);
        _em_buffer.reset(new em_buffer_t(16, 16));
    }

    //! Number of edges available if rewind was called
    size_t size() const {
        return static_cast<size_t>(_number_of_edges);
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

    EdgeStream& operator++() {
        assert(READING == _mode);
        assert(!_empty);

        em_reader_t& reader = *_em_reader;

        // handle end of stream
        _empty = reader.empty();
        if (UNLIKELY(_empty))
            return *this;


        // increment out-node in case we see invalid
        for(; UNLIKELY(*reader == INVALID_NODE); ++reader, ++_current.first) {
            // it is illegal for a sequence to end with an _op_node
            // since it does not represent a new edge and hence cannot
            // have been written
            assert(!reader.empty());
        }

        _current.second = *reader;
        ++reader;

        return *this;
    }
};
