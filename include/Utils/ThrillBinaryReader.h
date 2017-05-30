#pragma once
#include <fstream>


class ThrillBinaryReader {
public:
    using value_type = edge_t;

    ThrillBinaryReader(const std::string& filename = "")
    {
        if (!filename.empty())
            open(filename);
    }

    void open(const std::string& filename) {
        _is.close();

        _current = {0, 0};
        _edges_read = 0;
        _remaining_degree = 0;
        _empty = false;


        _is.open(filename);
        _advance();
    }

    ThrillBinaryReader& operator++() {
        _advance();
        return *this;
    }

    const value_type & operator*() const {
        assert(!empty());
        return _current;
    }


    bool empty() const {
        return _empty;
    }

    const edgeid_t& edges_read() {
        return _edges_read;
    }

private:
    std::ifstream _is;

    value_type _current;
    edgeid_t _edges_read;
    degree_t _remaining_degree;
    bool _empty;

    void _advance() {
        assert(!_empty);

        if (!_is.good()) {
            assert(!_remaining_degree);
            _empty = true;
        }

        while(UNLIKELY(!_remaining_degree)) {
            if (!_is.good()) {
                _empty = true;
                return;
            }

            _remaining_degree = _get_varint();
            _current.first++;
        }


        std::uint32_t v;
        if (!_is.read(reinterpret_cast<char*>(&v), 4)) {
            throw std::runtime_error("I/O error while reading next neighbor");
        }
        _current.second = static_cast<node_t>(v);

        _remaining_degree--;
        _edges_read++;
    }


    uint64_t _get_varint() {
        auto get_byte = [&]() -> uint8_t {
            uint8_t result;
            assert(_is.good());
            _is.read(reinterpret_cast<char*>(&result), 1);
            return result;
        };

        uint64_t u, v = get_byte();
        if (!(v & 0x80)) return v;
        v &= 0x7F;
        u = get_byte(), v |= (u & 0x7F) << 7;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 14;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 21;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 28;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 35;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 42;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 49;
        if (!(u & 0x80)) return v;
        u = get_byte(), v |= (u & 0x7F) << 56;
        if (!(u & 0x80)) return v;
        u = get_byte();
        if (u & 0xFE)
            throw std::overflow_error("Overflow during varint64 decoding.");
        v |= (u & 0x7F) << 63;
        return v;
    }
};
