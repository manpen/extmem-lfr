#pragma once
/**
 * @file
 * @brief  Bufreader to edge vector with merger of updated edges
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#include <memory>
#include <stxxl/sequence>

class BoolStream  {
    using T = std::uint64_t;

    using em_buffer_t = stxxl::sequence<T>;
    using reader_t    = typename em_buffer_t::stream;

    enum Mode {WRITING, READING};
    Mode _mode;

    // the big picture
    std::unique_ptr<em_buffer_t> _em_buffer;
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

public:
    // Let's satisfy the big 5 ;)
    BoolStream() {
        clear();
    }
    ~BoolStream() {
        _reader.release();
        _em_buffer.release();
    }

    BoolStream(BoolStream&& other) = default;
    BoolStream& operator=(BoolStream&& other) = default;

    BoolStream(const BoolStream&) = delete;

    //! Clear structure and switch into write mode
    void clear() {
        _em_buffer.reset(new em_buffer_t);
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
            _em_buffer->push_back(_buffered_word);
            _remaining_bits = _max_bits_buffer;
        }
    }

    //! Switch to reading mode, make the stream interface available
    void consume() {
        assert(_mode == WRITING);

        if (_remaining_bits != _max_bits_buffer) {
            _em_buffer->push_back(_buffered_word << _remaining_bits);
        }

        _items_stored = _items_consumable;

        _mode = READING;
        rewind();
    }

    void rewind() {
        assert(_mode == READING);

        _reader.reset(new reader_t(*_em_buffer));

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
