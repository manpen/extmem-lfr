#pragma once
/**
 * @file
 * @brief  Bufreader to edge vector with merger of updated edges
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#include <memory>
#include <stxxl/vector>

class BoolStream : stxxl::noncopyable {
    using T = std::uint64_t;
    using em_buffer_t = stxxl::vector<T>;
    using reader_t    = typename em_buffer_t::bufreader_type;
    using writer_t    = typename em_buffer_t::bufwriter_type;


    enum Mode {WRITING, READING};
    Mode _mode;

    // the big picture
    em_buffer_t _em_buffer;
    std::unique_ptr<writer_t> _writer;
    std::unique_ptr<reader_t> _reader;
    uint64_t _items_stored;

    uint64_t _items_consumable;

    // word-wise buffer
    T _buffered_word;
    uint _remaining_bits;
    constexpr static uint _max_bits_buffer = sizeof(T) * 8;
    constexpr static T _msb = T(1) << (_max_bits_buffer - 1);

    void _fetch_word() {
        reader_t & reader = *_reader;

        assert(!reader.empty());

        _buffered_word = *reader;
        ++reader;

        _remaining_bits = _max_bits_buffer;
    }

    void _ensure_writer() {
        if (LIKELY(bool(_writer))) return;
        _writer.reset(new writer_t(_em_buffer.end()));
    }

public:
    // Let's satisfy the big 5 ;)
    BoolStream() {
        clear();
    }
    ~BoolStream() {
        _writer.release();
        _reader.release();
    }

    BoolStream(const BoolStream&) = delete;

    BoolStream(BoolStream&& other) {
        swap(other);
    }

    BoolStream& operator=(BoolStream&& other) {
        swap(other);
        return *this;
    }

    void swap(BoolStream& other) {
        // clear writer buffers
        if (_writer) {
            _writer->finish();
            _writer.release();
        }

        if (other._writer) {
            other._writer->finish();
            other._writer.release();
        }

        // compute position with in readers
        auto pos  = (READING == _mode) ? _em_buffer.size() - _reader->size() : 0;
        auto opos = (READING == other._mode) ? other._em_buffer.size() - other._reader->size() : 0;

        std::swap(_mode, other._mode);
        _em_buffer.swap(other._em_buffer);

        // reconstruct readers if necessary
        if (_mode == READING)
            _reader.reset(new reader_t(_em_buffer.begin() + opos, _em_buffer.end()));

        if (other._mode == READING)
            other._reader.reset(new reader_t(other._em_buffer.begin()+pos, other._em_buffer.end()));

        std::swap(_items_stored, other._items_stored);
        std::swap(_items_consumable, other._items_consumable);
        std::swap(_buffered_word, other._buffered_word);
        std::swap(_remaining_bits, other._remaining_bits);
    }


    //! Clear structure and switch into write mode
    void clear() {
        _em_buffer.clear();
        _writer.release();
        _reader.release();

        _mode = WRITING;
        _remaining_bits = _max_bits_buffer;
        _items_consumable = 0;
    }

    //! Add a new data item
    void push(bool v) {
        assert(_mode == WRITING);

        _buffered_word = (_buffered_word << 1) | v;
        _remaining_bits--;
        _items_consumable++;

        if (!_remaining_bits) {
            _ensure_writer();
            (*_writer) << _buffered_word;
            _remaining_bits = _max_bits_buffer;
        }
    }

    //! Switch to reading mode, make the stream interface available
    void consume() {
        assert(_mode == WRITING);

        _ensure_writer();
        auto & writer = *_writer;
        if (_remaining_bits != _max_bits_buffer) {
            writer << (_buffered_word << _remaining_bits);
        }
        writer.finish();

        _items_stored = _items_consumable;

        _mode = READING;
        rewind();
    }


    void rewind() {
        assert(_mode == READING);

        _reader.reset(new reader_t(_em_buffer));

        _items_consumable = _items_stored;
        if (_items_consumable)
            _fetch_word();
    }

//! @name STXXL Streaming Interface
//! @{
    BoolStream& operator++() {
        assert(_mode == READING);
        assert(!empty());

        _buffered_word <<= 1;
        --_remaining_bits;
        --_items_consumable;

        if (UNLIKELY(!_remaining_bits && _items_consumable))
            _fetch_word();

        return *this;
    }

    bool operator*() const {
        assert(_mode == READING);
        assert(!empty());

        return _buffered_word & _msb;
    }

    bool empty() const {
        assert(_mode == READING);
        return !_items_consumable;
    }
//! @}

    //! Returns the number of bits currently available
    //! (if stream were in consume mode)
    size_t size() const {
        return _items_consumable;
    }
};
