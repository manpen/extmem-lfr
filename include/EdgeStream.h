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

    bool _allow_multi_edges;
    bool _allow_loops;


    // WRITING
    node_t _current_out_node;
    external_size_t _number_of_edges;
    
    edgeid_t _number_of_selfloops;
    edgeid_t _number_of_multiedges;

    // READING
    value_type _current;
    bool _empty;

public:
    EdgeStream(bool multi_edges = true, bool loops = true)
        : _allow_multi_edges(multi_edges)
        , _allow_loops(loops)
        , _current(edge_t::invalid())
    {clear();}

    EdgeStream(const EdgeStream &) = delete; // ; , bool multi_edges = false, bool loops = false) = delete;

    ~EdgeStream() {
        // in this order ;)
        _em_reader.reset(nullptr);
        _em_buffer.reset(nullptr);
    }

    EdgeStream(EdgeStream&&) = default;
    
    EdgeStream& operator=(EdgeStream&&) = default;

    // Hung enable multi-edges and loops
    void enableModifiedTFP() {
        _allow_multi_edges = true;
        _allow_loops = true;
    }

// Write interface
    void push(const edge_t& edge) {
        assert(_mode == WRITING);

        // count selfloops and fail if they are illegal
        {
            const bool selfloop = (edge.first == edge.second);
            _number_of_selfloops += selfloop;
            assert(_allow_loops || !selfloop);
        }

        // count multiedges and fail if they are illegal
        {
            const bool multiedge = (edge == _current);
            _number_of_multiedges += multiedge;
            assert(_allow_multi_edges || !multiedge);
        }

        // ensure order
        assert(!_number_of_edges || _current <= edge);

        em_buffer_t & em_buffer = *_em_buffer;

        while(UNLIKELY(_current_out_node < edge.first)) {
            em_buffer.push_back(INVALID_NODE);
            _current_out_node++;
        }

        em_buffer.push_back(edge.second);
        _number_of_edges++;

        _current = edge;
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
        _number_of_multiedges = 0;
        _number_of_selfloops = 0;
        _em_reader.reset(nullptr);
        _em_buffer.reset(new em_buffer_t(16, 16));
    }

    //! Number of edges available if rewind was called
    const external_size_t& size() const {
        return _number_of_edges;
    }

    const edgeid_t& selfloops() const {
        return _number_of_selfloops;
    }

    const edgeid_t& multiedges() const {
        return _number_of_multiedges;
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
            // it is illegal for a sequence to end with an "invalid"
            // since it does not represent a new edge and hence cannot
            // have been written
            assert(!reader.empty());
        }

        _current.second = *reader;
        ++reader;

        return *this;
    }
};
