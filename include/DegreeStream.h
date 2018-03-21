/**
 * @file DegreeStream.h
 * @date 29. November 2017
 *
 * @author Hung Tran
 */

#pragma once

#include <defs.h>
#include <stxxl/sequence>
#include <memory>

class DegreeStream {
public:
	using value_type = degree_t;

protected:
	using em_buffer_t = stxxl::sequence<degree_t>;
	using em_reader_t = typename em_buffer_t::stream;

	std::unique_ptr<em_buffer_t> _em_buffer;
	std::unique_ptr<em_reader_t> _em_reader;

	enum Mode {
			WRITING, READING
	};
	Mode _mode;

	size_t _size = 0;

public:
	DegreeStream() {
		clear();
	}

	DegreeStream(const DegreeStream &) = delete;

	~DegreeStream() {
		// in this order ;)
		_em_reader.reset(nullptr);
		_em_buffer.reset(nullptr);
	}

	DegreeStream(DegreeStream &&) = default;

	DegreeStream &operator=(DegreeStream &&) = default;

// Write interface
	void push(const degree_t degree) {
		assert(_mode == WRITING);

		em_buffer_t &em_buffer = *_em_buffer;

		em_buffer.push_back(degree);

		_size++;
	}

	//! switches to read mode and resets the stream
	void rewind() {
		_mode = READING;
		_em_reader.reset(new em_reader_t(*_em_buffer));
	}

	// returns back to writing mode on an empty stream
	void clear() {
		_mode = WRITING;
		_em_reader.reset(nullptr);
		_em_buffer.reset(new em_buffer_t(16, 16));
	}

	size_t size() const {
		return _size;
	}

// Consume interface
	//! return true when in write mode or if edge list is empty
	bool empty() const {
		return _em_reader->empty();
	}

	const value_type &operator*() const {
		assert(READING == _mode);
		return _em_reader->operator*();
	}

	DegreeStream &operator++() {
		assert(READING == _mode);

		em_reader_t &reader = *_em_reader;

		if (UNLIKELY(reader.empty()))
			return *this;

		++reader;

		return *this;
	}
};

class DegreeBiStream {
public:
  using value_type = degree_t;

protected:
  using em_buffer_t = stxxl::sequence<degree_t>;
  using em_reader_t = typename em_buffer_t::stream;

  std::unique_ptr<em_buffer_t> _em_forward_buffer;
  std::unique_ptr<em_buffer_t> _em_reverse_buffer;
  std::unique_ptr<em_reader_t> _em_reader;

  enum Mode {
	  WRITING, FORWARD, REVERSE
  };
  Mode _mode;

  size_t _size = 0;
  const size_t _offset;

public:
  DegreeBiStream(size_t offset = 0) : _offset(offset) {
	  clear();
  }

  DegreeBiStream(const DegreeBiStream &) = delete;

  ~DegreeBiStream() {
	  // in this order ;)
	  _em_reader.reset(nullptr);
	  _em_forward_buffer.reset(nullptr);
	  _em_reverse_buffer.reset(nullptr);
  }

  DegreeBiStream(DegreeBiStream &&) = default;

  DegreeBiStream &operator=(DegreeBiStream &&) = default;

// Write interface
  void push_forward(const degree_t degree) {
	  assert(_mode == WRITING);

	  em_buffer_t &em_buffer = *_em_forward_buffer;

	  em_buffer.push_back(degree);

	  _size++;
  }

  void push_reverse(const degree_t degree) {
	  assert(_mode == WRITING);

	  em_buffer_t &em_buffer = *_em_reverse_buffer;

	  em_buffer.push_front(degree);

	  _size++;
  }

  //! switches to read mode and resets the stream
  void rewind() {
	  _mode = FORWARD;
	  _em_reader.reset(new em_reader_t(*_em_forward_buffer));

	  for (size_t count = 0; count < _offset; ++count)
		  _em_reader->operator++();
  }

  // returns back to writing mode on an empty stream
  void clear() {
	  _mode = WRITING;
	  _em_reader.reset(nullptr);
	  _em_forward_buffer.reset(new em_buffer_t(16, 16));
	  _em_reverse_buffer.reset(new em_buffer_t(16, 16));
  }

  size_t size() const {
	  return _size - _offset;
  }

// Consume interface
  //! return true when in write mode or if edge list is empty
  bool empty() const {
	  return _em_reader->empty();
  }

  const value_type &operator*() const {
	  assert(FORWARD == _mode || REVERSE == _mode);
	  return _em_reader->operator*();
  }

  DegreeBiStream &operator++() {
	  assert(FORWARD == _mode || REVERSE == _mode);

	  em_reader_t &reader = *_em_reader;

	  if (UNLIKELY(reader.empty()))
		  return *this;

	  ++reader;
	  if (UNLIKELY(reader.empty()))
		  if (FORWARD == _mode) {
			  _mode = REVERSE;
			  _em_reader.reset(new em_reader_t(*_em_reverse_buffer));
		  }


	  return *this;
  }
};
