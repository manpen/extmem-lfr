#pragma once

#include <limits>
#include <array>
#include <vector>
#include <tuple>
#include <algorithm>
#include <type_traits>
#include <cassert>
#include <omp.h>

namespace intsort {
namespace internal {

template<typename T>
uint32_t ilog2(T x) {
    if (!x) return 0;

    const typename std::make_unsigned<T>::type input = x;

    int i = 0;
    while (x >>= 1) i++;

    i += (input > (1llu << i));

    assert((1llu << i) >= input);
    assert((1llu << i) / 2 < input);

    return i;
}

template <typename T1, typename T2>
auto idiv_ceil(T1 a, T2 b) {
    return static_cast<T1>((static_cast<unsigned long long>(a)+b-1) / b);
};


template<typename Iter, typename KeyExtract, typename Key, size_t RADIX_WIDTH=8,
    typename T = typename std::iterator_traits<Iter>::value_type >
inline bool intsort_impl(const Iter begin, std::vector<T>& buffer,
                         KeyExtract key_extract,
                         const Key max_key = std::numeric_limits<Key>::max()) {

    const size_t n = buffer.size();
    if (n < 2 || max_key < 1)
        return false; // in these cases the input is trivially sorted

    // compute mask and shifts to later compute the queue index
    static_assert(RADIX_WIDTH >= 1, "Radix has to be at least 2");
    static_assert(RADIX_WIDTH <= 8 * sizeof(T), "Radix is not allowed to exceed numer of bits in T");

    constexpr size_t no_queues = 1 << RADIX_WIDTH;

    const auto max_threads = std::min<int>(omp_get_max_threads(),
                                           idiv_ceil(n, 1 << 17));
    using IndexArray = std::array<size_t, no_queues>;

    // compute how many iterations we need to sort numbers [0, ..., max_key], i.e. log(max_key, base=RADIX_WIDTH)
    const size_t max_bits = ilog2(max_key);

    std::array<size_t, no_queues + 1> splitter;
    splitter[no_queues] = n;

    const size_t msb_radix_width = std::min(max_bits, RADIX_WIDTH);
    const size_t msb_radix = 1 << msb_radix_width;
    const size_t msb_shift = max_bits - msb_radix_width;

    const size_t lsb_remaining_width = max_bits - msb_radix_width;
    const int no_iters = idiv_ceil(std::max<size_t>(1, lsb_remaining_width),
                                   RADIX_WIDTH);
    const size_t adaptive_width = idiv_ceil(lsb_remaining_width, no_iters);
    const size_t adaptive_no_queues = 1 << adaptive_width;

    #ifndef NDEBUG
    {
        size_t i = 0;
        for (auto it = begin; it != begin + n; ++it, ++i) {
            if (key_extract(*it) > max_key) {
                std::cerr << "key_extract(*it)=" << key_extract(*it)
                          << " > max_key=" << max_key
                          << " at position i=" << i << std::endl;
                abort();
            }
        }
    }
    #endif

    // perform the first round as MSB radix sort which will yield indendent
    // chunks which then can be sorted pleasingly parallel. We add some padding
    // to thread_counter to avoid false sharing
    std::vector< std::array<size_t, no_queues + 64 / sizeof(size_t)> >
        thread_counters(max_threads);

    #pragma omp parallel num_threads(max_threads)
    {
        // symmetry breaking
        const auto tid = omp_get_thread_num();
        const auto no_threads = omp_get_num_threads();

        // figure out workload for each thread
        const size_t chunk_size = idiv_ceil(n, no_threads);
        const auto chunk = std::make_pair(chunk_size * tid,
                                          std::min(chunk_size * (tid + 1),
                                                   n));

        // thread-local counters and iterators
        IndexArray queue_pointer;

        {
            const auto input_begin = begin + chunk.first;
            const auto input_end = begin + chunk.second;

            auto &counters = thread_counters[tid];
            counters.fill(0);
            for (auto it = input_begin; it != input_end; ++it) {
                counters[key_extract(*it) >> msb_shift]++;
            }

            if (no_threads > 1) {
                #pragma omp barrier
            }

            {
                size_t index = 0;
                size_t tmp = 0; // avoid warnings
                for (size_t qid = 0; qid != no_queues; ++qid) {
                    for (int ttid = 0; ttid < no_threads; ttid++) {
                        if (ttid == tid) tmp = index;
                        index += thread_counters[ttid][qid];
                    }
                    queue_pointer[qid] = tmp;
                }

                // store splitters which will be processed pleasingly parallel
                if (0 == tid) {
                    std::copy(queue_pointer.cbegin(), queue_pointer.cend(),
                              splitter.begin());
                }
            }

            for (auto it = input_begin; it != input_end; ++it) {
                const auto key = key_extract(*it);
                const auto shifted = key >> msb_shift;
                const auto index = queue_pointer[shifted]++;
                buffer[index] = std::move(*it);
            }
        }

        if (lsb_remaining_width) {
            if (no_threads > 1) {
                #pragma omp barrier
            }


            IndexArray counters;

            const Key mask = (Key(1) << adaptive_width) - 1;
            auto get_queue_index = [&](Key key, int iteration) {
              return (key >> (iteration * adaptive_width)) & mask;
            };


            auto move_to_queues =
                [adaptive_no_queues, key_extract, get_queue_index]
                    (const auto begin, const auto end,
                     const auto buffer_base, auto& counters,
                     int iteration, const bool count
                    ) {
                  IndexArray pointers;
                  pointers[0] = 0;
                  std::partial_sum(counters.cbegin(),
                                   counters.cbegin() + (adaptive_no_queues - 1),
                                   pointers.begin() + 1);

                  if (count)
                      std::fill_n(counters.begin(), adaptive_no_queues, 0);

                  for (auto it = begin; it != end; ++it) {
                      const Key key = key_extract(*it);
                      const auto index =
                          pointers[get_queue_index(key, iteration)]++;

                      if (count)
                          counters[get_queue_index(key, iteration+1)]++;

                      buffer_base[index] = std::move(*it);
                  }
                };


            // Now solve parts independently
            #pragma omp for nowait
            for (size_t i = 0; i < msb_radix; ++i) {
                if (splitter[i] == splitter[i + 1]) continue;

                auto input_base = buffer.begin() + splitter[i];
                auto buffer_base = begin + splitter[i];
                const size_t size = splitter[i + 1] - splitter[i];

                // iteration 0
                {
                    const auto input_begin = input_base;
                    const auto input_end = input_base + size;

                    // in the first round we have to count the
                    // elements for each queue; later we do it while
                    // moving elements
                    std::fill_n(counters.begin(), adaptive_no_queues, 0);
                    for (auto it = input_begin;
                         it != input_end; ++it) {
                        counters[get_queue_index(key_extract(*it), 0)]++;
                    }

                    move_to_queues(input_base, input_base + size,
                                   buffer_base, counters, 0, no_iters != 1);

                    std::swap(input_base, buffer_base);
                }

                // iterations 1 to no_iters-2
                for(int iteration = 1; iteration < no_iters-1; iteration++) {
                    move_to_queues(input_base, input_base + size,
                                   buffer_base, counters, iteration, true);

                    std::swap(input_base, buffer_base);
                }

                // last iteration (no_iters - 1)
                if (no_iters > 1)
                    move_to_queues(input_base, input_base + size,
                                   buffer_base, counters, no_iters-1, false);
            }
        }
    }

    return !lsb_remaining_width || !(no_iters % 2);
}
} // namespace: internal


template<typename Iter, typename KeyExtract, typename Key, size_t RADIX_WIDTH = 8,
    typename T = typename std::iterator_traits<Iter>::value_type>
inline void sort(const Iter begin, const Iter end, KeyExtract key_extract,
                 const Key max_key = std::numeric_limits<Key>::max()) {

    const size_t n = std::distance(begin, end);
    std::vector<T> buffer(n);

    bool need_buffer =
        internal::intsort_impl<Iter, KeyExtract, Key, RADIX_WIDTH, T>
            (begin, buffer, key_extract, max_key);

    if (need_buffer)
        std::copy(buffer.cbegin(), buffer.cend(), begin);
}

template<typename T, typename KeyExtract, typename Key, size_t RADIX_WIDTH = 8>
inline void sort(std::vector<T> &input, KeyExtract key_extract,
                 const Key max_key = std::numeric_limits<Key>::max()) {
    auto begin = input.begin();
    auto end = input.end();

    const size_t n = std::distance(begin, end);
    std::vector<T> buffer(n);

    bool need_buffer =
        internal::intsort_impl<decltype(begin), KeyExtract, Key, RADIX_WIDTH, T>
            (begin, buffer, key_extract, max_key);

    if (need_buffer) input.swap(buffer);
}

} // namespace: intsort

