#pragma once
#include <random>
#include <mutex>

class RandomSeed {
public:
    void seed(unsigned int seed) {
        _seed = 16807*seed;
        if (!_seed) _seed = 0x1234567890 ;
        _re.seed(_seed % 2147483647);
    }

    unsigned int seed() const {
        return _seed;
    }

    // get a fixed seed
    unsigned int get_seed(uint32_t id=1234) const {
        return  (_seed * id * 0x492559f6) % 2147483647;
    }

    // get next element in seed sequence
    unsigned int get_next_seed() {
        std::unique_lock<std::mutex> lock(_mutex);
        return _re();
    }


    static RandomSeed& get_instance() {
        return *_instance;
    }

protected:
    uint64_t _seed{1};

    std::default_random_engine _re;
    std::mutex _mutex;


    static RandomSeed* _instance;

};
