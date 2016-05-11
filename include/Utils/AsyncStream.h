#pragma once

#include <stxxl/bits/common/utils.h>

#include <thread>
#include <future>
#include <chrono>
#include <vector>
#include <iostream>

#define ASYNC_STREAM_STATS

template <typename StreamIn, typename T = typename StreamIn::value_type>
class AsyncStream {
public:
    using value_type = T;

protected:
    using BufferType = std::vector<T>;
    using Iterator = typename BufferType::const_iterator;

    constexpr static double initial_batch_time_factor = 0.2;
    constexpr static double dampening_factor = 8.0;

    // producer port
    StreamIn &  _producing_stream;
    BufferType _produce_buffer;

    const double _target_batch_time;
    double _batch_time;
    double _last_rate;
    std::future<std::pair<Iterator, double>> _producer_future;

    // consume port
    BufferType _consume_buffer;
    Iterator _consume_iter;
    Iterator _consume_end;
    bool _consume_empty;

#ifdef ASYNC_STREAM_STATS
    uint64_t _stat_wait_for_future;
    uint64_t _stat_received_buffers;
    uint64_t _stat_received_elements;
#endif


    // only directly called in the constructor; otherwise invoked in _receive_buffer
    void _start_producing() {
       if (UNLIKELY(_producing_stream.empty())) {
          _consume_empty = true;
          return;
       }

       size_t elements = std::min<size_t>(1 + _last_rate * _batch_time, _produce_buffer.size());
       _batch_time = _target_batch_time;
       //std::cout << "Request " << elements << " elements to be produced async" << std::endl;

       _producer_future =  std::async(std::launch::async, [] (StreamIn& stream, BufferType & buf, size_t num) {
           auto begin = std::chrono::high_resolution_clock::now();

           auto it = buf.begin();
           for(auto counter=num; counter && !stream.empty(); --counter, ++stream, ++it)
              *it = *stream;

           auto end = std::chrono::high_resolution_clock::now();


           return std::make_pair(Iterator(it), double(num) * 1.0e9 / std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count());
       }, std::ref(_producing_stream), std::ref(_produce_buffer), elements);
    }

    void _receive_buffer() {
       if (UNLIKELY(_consume_empty))
          return;

       double rate;
       {
         #ifdef ASYNC_STREAM_STATS
            auto begin = std::chrono::high_resolution_clock::now();
         #endif

          // wait for produer
          std::tie(_consume_end, rate) = _producer_future.get();

          #ifdef ASYNC_STREAM_STATS
             auto end = std::chrono::high_resolution_clock::now();
             _stat_wait_for_future += std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count();

            _stat_received_buffers++;
            _stat_received_elements += (_consume_end - _produce_buffer.begin());
          #endif
       }



       _last_rate = ((dampening_factor-1) * _last_rate + rate) / dampening_factor;
       //std::cout << "Got buffer produced at rate " << _last_rate << std::endl;

       // swap buffers and begin reading the consume side
       std::swap(_consume_buffer, _produce_buffer);
       _consume_iter = _consume_buffer.cbegin();

       // request the generation of the next buffer
       _start_producing();
    }

public:
    //! The block size of every async computation is defined by batch_time in seconds.
    //! We try to slowly approach it, but a resonably good estimation should be provided.
    //! The memory for the two blocks is allocated in the constructor and is by default 
    //! twice the size requiered to fill a buffer at estimated_rate.
    AsyncStream(StreamIn& stream, bool auto_acquire = true, double estimated_rate = 1.0e7, double batch_time = 0.5, size_t max_buffer_size = 0)
            : _producing_stream(stream)
            , _target_batch_time(batch_time)
            , _batch_time(initial_batch_time_factor * batch_time)
            , _last_rate(estimated_rate)
            , _consume_empty(false)
    {
       if (!max_buffer_size)
          max_buffer_size = 2 *  estimated_rate * batch_time;

       _consume_buffer.resize(max_buffer_size);
       _produce_buffer.resize(max_buffer_size);

       _start_producing();
       if (auto_acquire)
          acquire();
    }

    ~AsyncStream() {
       if (_producer_future.valid())
          _producer_future.wait();
    }

    //! Needs to be called before the first access to the streaming interface
    void acquire() {
       _receive_buffer();
    }

    //! Intended to be use when input stream run empty and was restarted externally
    //! Behavior only define if empty() == true. Then, the stream is restarted.
    //! Otherwise it is stopped somewhere and restarted.
    //! @warning You have to call acquire again
    void restart(bool auto_acquire) {
       if (_producer_future.valid())
          _producer_future.wait();

       _batch_time = _target_batch_time * initial_batch_time_factor;
       _start_producing();
       _consume_empty = false;


       if (auto_acquire)
          acquire();
    }

    AsyncStream& operator++() {
       ++_consume_iter;

       if (UNLIKELY(_consume_iter == _consume_end)) {
          _receive_buffer();
       }

       return *this;
    }

    const value_type & operator*() const {
       assert(!empty());

       return *_consume_iter;
    }

    bool empty() const {
       return _consume_empty && _consume_iter == _consume_end;
    }

    void report_stats(const std::string & name) {
#ifdef ASYNC_STREAM_STATS
        std::cout << name << ": ";
        report_stats();
#endif
    }

    void report_stats() {
#ifdef ASYNC_STREAM_STATS
        std::cout << "AsyncStream received " << _stat_received_elements << " elements in "
                  << _stat_received_buffers << " buffers. Waited for future " << _stat_wait_for_future << "ms. "
                     "Last rate: " << _last_rate
        << std::endl;
#endif
    }
};

      
