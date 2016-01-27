#pragma once
/**
 * @file
 * @brief  Bufreader to edge vector with merger of updated edges
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#include <stxxl/queue>

class BoolStream : stxxl::noncopyable {
    using T = std::uint64_t;

    enum Mode {WRITING, READING};
    Mode _mode;

    // the big picture
    stxxl::queue<T> _queue;
    uint64_t _items_stored;

    // word-wise buffer
    T _buffered_word;
    uint _remaining_bits;
    constexpr static uint _max_bits_buffer = sizeof(T) * 8;
    constexpr static T _msb = T(1) << (_max_bits_buffer - 1);

    void _fetch_word() {
        assert(!_queue.empty());

        _buffered_word = _queue.front();
        _queue.pop();

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
        _queue.swap(other._queue);
        std::swap(_items_stored, other._items_stored);
        std::swap(_buffered_word, other._buffered_word);
        std::swap(_remaining_bits, other._remaining_bits);
    }


    //! Clear structure and switch into write mode
    void clear() {
        while(!_queue.empty())
            _queue.pop();

        _mode = WRITING;
        _remaining_bits = _max_bits_buffer;
        _items_stored = 0;
    }

    //! Add a new data item
    void push(bool v) {
        assert(_mode == WRITING);

        _buffered_word = (_buffered_word << 1) | v;
        _remaining_bits--;
        _items_stored++;

        if (!_remaining_bits) {
            _queue.push(_buffered_word);
            _remaining_bits = _max_bits_buffer;
        }
    }

    //! Switch to reading mode, make the stream interface available
    void consume() {
        if (_remaining_bits != _max_bits_buffer) {
            _queue.push(_buffered_word << _remaining_bits);
        }

        _mode = READING;

        if (_items_stored)
            _fetch_word();
    }

//! @name STXXL Streaming Interface
//! @{
    BoolStream& operator++() {
        assert(_mode == READING);
        assert(!empty());

        _buffered_word <<= 1;
        --_remaining_bits;
        --_items_stored;

        if (UNLIKELY(!_remaining_bits && _items_stored))
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
        return !_items_stored;
    }
//! @}

    //! Returns the number of bits currently available
    //! (if stream were in consume mode)
    size_t size() const {
        return _items_stored;
    }
};