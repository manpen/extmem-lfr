#pragma once
/**
 * @file
 * @brief  Bufreader to edge vector with merger of updated edges
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#include <memory>
#include <stxxl/sequence>

class BoolStream : stxxl::noncopyable {
    using T = std::uint64_t;
    using seq_t    = stxxl::sequence<T>;
    using reader_t = stxxl::sequence<T>::stream;

    enum Mode {WRITING, READING};
    Mode _mode;

    // the big picture
    seq_t _sequence;
    std::unique_ptr<reader_t> _reader;
    uint64_t _items_stored;

    uint64_t _items_consumable;

    // word-wise buffer
    T _buffered_word;
    uint _remaining_bits;
    constexpr static uint _max_bits_buffer = sizeof(T) * 8;
    constexpr static T _msb = T(1) << (_max_bits_buffer - 1);

    void _fetch_word() {
        assert(!_sequence.empty());

        auto & reader = *_reader;

        _buffered_word = *reader;
        ++reader;

        _remaining_bits = _max_bits_buffer;
    }

public:
    // Let's satisfy the big 5 ;)
    BoolStream() {clear();}
    ~BoolStream() = default;
    BoolStream(const BoolStream&) = delete;
    BoolStream(BoolStream&& other) {
        swap(other);
    }

    BoolStream& operator=(BoolStream&& other) {
        swap(other);
        return *this;
    }

    void swap(BoolStream& other) {
        std::swap(_mode, other._mode);
        _sequence.swap(other._sequence);
        std::swap(_reader, other._reader);

        std::swap(_items_stored, other._items_stored);
        std::swap(_items_consumable, other._items_consumable);
        std::swap(_buffered_word, other._buffered_word);
        std::swap(_remaining_bits, other._remaining_bits);
    }


    //! Clear structure and switch into write mode
    void clear() {
        seq_t new_seq;
        _reader.release();
        _sequence.swap(new_seq);

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
            _sequence.push_back(_buffered_word);
            _remaining_bits = _max_bits_buffer;
        }
    }

    //! Switch to reading mode, make the stream interface available
    void consume() {
        assert(_mode == WRITING);

        if (_remaining_bits != _max_bits_buffer) {
            _sequence.push_back(_buffered_word << _remaining_bits);
        }

        _items_stored = _items_consumable;

        _mode = READING;
        rewind();
    }


    void rewind() {
        assert(_mode == READING);

        _reader.reset(new reader_t(_sequence));

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