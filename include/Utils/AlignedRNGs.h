#pragma once

#include <random>
#include <memory>
#include <cassert>

template <typename RNG, size_t ALIGN=64>
class RNGs {
public:
  template <typename T>
  RNGs(size_t size, std::initializer_list<T> ss)
	  : size_(size)
  {
	  size_t tmp_space = sizeof(Item) * size + ALIGN;
	  raw_data_ = new uint8_t[tmp_space];

	  void* tmp_ptr = raw_data_;
	  std::align(ALIGN, sizeof(Item), tmp_ptr, tmp_space);
	  data_ = reinterpret_cast<Item*>(tmp_ptr);

	  assert(reinterpret_cast<uintptr_t>(data_) % ALIGN == 0);

	  std::seed_seq seq(ss);

	  for(size_t i=0; i<size; ++i) {
		  new (&data_[i]) Item(seq);
	  }
  }

  ~RNGs() {
	  for(size_t i=0; i<size_; ++i)
		  data_[i].~Item();

	  delete[] (raw_data_);
  }

  RNG& operator[] (const size_t i) {
	  assert(i < size_);
	  return data_[i].rng_;
  }

  const RNG& operator[] (const size_t i) const {
	  assert(i < size_);
	  return data_[i].rng_;
  }

private:
  struct Item : private std::array<uint8_t, (ALIGN - (sizeof(RNG) % ALIGN)) % ALIGN> {
	Item(std::seed_seq& q) : rng_(q) {}
	~Item() = default;
	RNG rng_;
  };

  static_assert(sizeof(Item) % ALIGN == 0, "Padding does not work");
  static_assert(sizeof(RNG) + ALIGN > sizeof(Item), "Padding is too large");

  uint8_t* raw_data_;
  Item* data_;
  const size_t size_;
};
