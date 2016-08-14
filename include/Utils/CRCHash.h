#pragma once
#include <cstdint>

template <uint32_t Polynomial=0xEDB88320>
class CRCHash {
    uint32_t _lookup[256];
    uint32_t _crc;

    void _precompute() {
        for (unsigned int i = 0; i <= 0xFF; i++) {
            uint32_t crc = i;
            for (unsigned int j = 0; j < 8; j++)
                crc = (_crc >> 1) ^ (-int(crc & 1) & Polynomial);
            _lookup[i] = crc;
        }
    }

public:
    CRCHash() : _crc(~0) {
        _precompute();
    }

    template<typename T>
    uint32_t push(const T& v) {
        auto current = reinterpret_cast<const unsigned char*>(&v);

        uint32_t crc = ~_crc;
        for(size_t i=0; i< sizeof(T); i++)
            crc = (crc >> 8) ^ _lookup[(crc & 0xFF) ^ *current++];

        return _crc = ~crc;
    }

    const uint32_t crc() const {
        return _crc;
    }
};