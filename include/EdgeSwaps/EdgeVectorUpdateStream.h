#pragma once
#include <EdgeStream.h>

/**
 * @file
 * @brief  Bufreader to edge vector with merger of updated edges
 * @author Manuel Penschuck
 * @copyright to be decided
 */

/**
 * @brief Bufreader to edge vector with merger of updated edges
 *
 * This class expects two primary input sources (provided via constructor):
 *  - A sorted edge vector. The vector is consumed in conjunction with the
 *    EdgeValidStream. A false in the latter indicates that the edge has to
 *    be skipped. Both source have to have the same cardinality.
 *
 *  - A stream providing sorted edge updates
 *
 * The number of "false" in the EdgeValidStream is assumed to match the number
 * of elements in the UpdatedEdgeStream.
 */
template <typename EdgeVector, typename EdgeValidStream, typename UpdatedEdgeStream>
class EdgeVectorUpdateStream {
public:
    using value_type = edge_t;

protected:

    // read port
    EdgeVector& _edges;
    typename EdgeVector::bufreader_type _edge_reader;
    EdgeValidStream& _edge_valid_stream;

    // write port
    EdgeVector _edges_write_vector;
#ifndef NDEBUG
    edgeid_t  _writer_eid;
#endif
    typename EdgeVector::bufwriter_type _edge_writer;

    // updates
    UpdatedEdgeStream& _updated_edges;

    // STXXL's streaming interface
    edge_t _current;
    bool _empty;

    void _skip_invalid_edges() {
        while (UNLIKELY(!_edge_reader.empty() && !(*_edge_valid_stream))) {
            assert(!_edge_valid_stream.empty());
            //std::cout << "Skip " << (_edges.size() - _edge_valid_stream.size()) << " " << *_edge_reader << std::endl;
            ++_edge_reader;
            ++_edge_valid_stream;
        }
        assert(_edge_reader.empty() == _edge_valid_stream.empty());
    }

public:
    //! The edge vector remains unaltered until finish is called.
    EdgeVectorUpdateStream(EdgeVector& edges, EdgeValidStream& valid_stream, UpdatedEdgeStream& updated_edges)
        : _edges(edges),
          _edge_reader(_edges),
          _edge_valid_stream(valid_stream),
          _edges_write_vector(),
#ifndef NDEBUG
          _writer_eid(0),
#endif
          _edge_writer(_edges_write_vector),
          _updated_edges(updated_edges)
    {
        _edges_write_vector.reserve(_edges.size());
        _skip_invalid_edges();

        _empty = _edge_reader.empty() && _updated_edges.empty();

        if (!_empty)
            operator++();
    }

//! @name STXXL Streaming Interface
//! @{
    //! If either input stream is empty, the next value from the non-empty is retrieved.
    //! If case both streams are available, the smaller (based on the bigger-comparator
    //! of the PQ) is chosen.
    EdgeVectorUpdateStream& operator++() {
        assert(!empty());

        _empty = _edge_reader.empty() && _updated_edges.empty();

        if (UNLIKELY(_empty)) return *this;

        /*{
            std::cout << "UpdateStream vector: ";
            if (_edge_reader.empty())
                std::cout << "empty";
            else
                std::cout << *_edge_reader << " " << *_edge_valid_stream;

            std::cout << " stream: ";
            if (_updated_edges.empty())
                std::cout << "empty";
            else
                std::cout << *_updated_edges;
        } */

        if (_edge_reader.empty()) {
            _current = *_updated_edges;
            ++_updated_edges;

        } else if(_updated_edges.empty() || *_updated_edges > *_edge_reader) {
            assert(!_edge_valid_stream.empty() && *_edge_valid_stream);
            _current = *_edge_reader;
            ++_edge_reader;
            ++_edge_valid_stream;
            _skip_invalid_edges();

        } else {
            assert(!_edge_valid_stream.empty() && *_edge_valid_stream);
            // check for parallel edges
            #ifndef NDEBUG
            // Hung: removed this if since multi-edges allowed
            //if (*_updated_edges == *_edge_reader) {
            //    std::cout << "Duplicate detected before writing eid " << _writer_eid << " values " << *_edge_reader << " / " << *_updated_edges << std::endl;
            //    assert(*_updated_edges != *_edge_reader);
            //}
            #endif

            _current = *_updated_edges;
            ++_updated_edges;
        }

        //std::cout << " current: " << _current << std::endl;

        _edge_writer << _current;
#ifndef NDEBUG
        ++_writer_eid;
#endif

        return *this;
    }

    const edge_t& operator*() const {
        return _current;
    }

    bool empty() const {
        return _empty;
    }
//! @}

    //! Continues stream until end and flushes writer buffers.
    //! The edge vector provided to the constructor gets updated.
    void finish() {
        // consume stream and flush buffers
        while(!empty()) {
            operator++();
        }
        assert(_edge_valid_stream.empty());
        assert(empty());
        _edge_writer.finish();

        // switch roles of _edges and _edges_write_vector
        assert(_edges.size() == _edges_write_vector.size());
        _edges.clear();
        _edges.swap(_edges_write_vector);
    }
};

template <typename EdgeValidStream, typename UpdatedEdgeStream>
class EdgeVectorUpdateStream<EdgeStream, EdgeValidStream, UpdatedEdgeStream> {
public:
    using value_type = edge_t;

protected:
    // read port
    EdgeStream& _edges;
    EdgeValidStream& _edge_valid_stream;

    // write port
    EdgeStream _edges_new;
#ifndef NDEBUG
    edgeid_t  _writer_eid;
#endif

    // updates
    UpdatedEdgeStream& _updated_edges;

    // STXXL's streaming interface
    edge_t _current;
    bool _empty;

    void _skip_invalid_edges() {
        while (UNLIKELY(!_edges.empty() && !(*_edge_valid_stream))) {
            assert(!_edge_valid_stream.empty());
            ++_edges;
            ++_edge_valid_stream;
        }
        assert(_edges.empty() == _edge_valid_stream.empty());
    }

public:
    //! Expects a rewinded ege stream
    EdgeVectorUpdateStream(EdgeStream& edges, EdgeValidStream& valid_stream, UpdatedEdgeStream& updated_edges)
            : _edges(edges),
              _edge_valid_stream(valid_stream),
#ifndef NDEBUG
              _writer_eid(0),
#endif
              _updated_edges(updated_edges)
    {
        _skip_invalid_edges();

        _empty = _edges.empty() && _updated_edges.empty();

        if (!_empty)
            operator++();
    }

//! @name STXXL Streaming Interface
//! @{
    //! If either input stream is empty, the next value from the non-empty is retrieved.
    //! If case both streams are available, the smaller (based on the bigger-comparator
    //! of the PQ) is chosen.
    EdgeVectorUpdateStream& operator++() {
        assert(!empty());

        _empty = _edges.empty() && _updated_edges.empty();

        if (UNLIKELY(_empty)) return *this;

        if (_edges.empty()) {
            _current = *_updated_edges;
            ++_updated_edges;

        } else if(_updated_edges.empty() || *_updated_edges > *_edges) {
            assert(!_edge_valid_stream.empty() && *_edge_valid_stream);
            _current = *_edges;
            ++_edges;
            ++_edge_valid_stream;
            _skip_invalid_edges();

        } else {
            assert(!_edge_valid_stream.empty() && *_edge_valid_stream);
            // check for parallel edges
#ifndef NDEBUG
            /* Hung: no need for this, since multi-edges are allowed
            if (*_updated_edges == *_edges) {
                std::cout << "Duplicate detected before writing eid " << _writer_eid << " values " << *_edges << " / " << *_updated_edges << std::endl;
                // assert(*_updated_edges != *_edges);
            }*/
#endif

            _current = *_updated_edges;
            ++_updated_edges;
        }

        _edges_new.push(_current);
#ifndef NDEBUG
        ++_writer_eid;
#endif

        return *this;
    }

    const edge_t& operator*() const {
        return _current;
    }

    bool empty() const {
        return _empty;
    }
//! @}

    //! Continues stream until end and flushes writer buffers.
    //! The edge stream provided to the constructor gets updated and is rewinded.
    void finish() {
        // consume stream and flush buffers
        while(!empty()) {
            operator++();
        }
        assert(_edge_valid_stream.empty());
        assert(empty());

        // switch roles of _edges and _edges_write_vector
        assert(_edges.size() == _edges_new.size());
        _edges.clear();
        std::swap(_edges, _edges_new);
    }
};
