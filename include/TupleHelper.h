/**
 * @file
 * @brief Generic comparator and ostream-casting for tuple types
 * @author Manuel Penschuck
 * @copyright
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * @copyright
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <iostream>
#include <sstream>
#include <tuple>
#include <type_traits>

/**
 * @brief Generic comparator to be used in conjunction with STXXL's sorting
 * for tuple with std::numeric-limits compatible types.
 */
template <typename TupleT>
class GenericComparatorTuple{
// helper to compute min/max values
   template <unsigned int i, typename ...Args>
   struct NLimit {
      static TupleT max(Args ...args) {
         using type = typename std::remove_reference<decltype(std::get<i-1>(TupleT()))>::type;
         type val =  std::numeric_limits<type>::max();
         return NLimit<i-1, type, Args...>::max(val, args...);
      }
      static TupleT min(Args ...args) {
         using type = typename std::remove_reference<decltype(std::get<i-1>(TupleT()))>::type;
         type val =  std::numeric_limits<type>::min();
         return NLimit<i-1, type, Args...>::min(val, args...);
      }
   };

   template <typename ...Args>
   struct NLimit<0, Args...> {
      static TupleT max(Args ...args) {
         return std::tuple<Args...>(args...);
      }
      static TupleT min(Args ...args) {
         return std::tuple<Args...>(args...);
      }
   };

public:
   struct Ascending {
      Ascending() {}
      bool operator()(const TupleT &a, const TupleT &b) const { return a < b; }
      TupleT min_value() const { return NLimit<std::tuple_size<TupleT>::value>::min(); }
      TupleT max_value() const { return NLimit<std::tuple_size<TupleT>::value>::max(); }
   };

   struct Descending {
      Descending() {}
      bool operator()(const TupleT &a, const TupleT &b) const { return b < a; }
      TupleT min_value() const { return NLimit<std::tuple_size<TupleT>::value>::max(); }
      TupleT max_value() const { return NLimit<std::tuple_size<TupleT>::value>::min(); }
   };
};

//! @brief Helper class to push a tuple into a ostringstream
template <class Stream, class Tuple, unsigned int i>
struct TuplePrint {
   static Stream & print(Stream & ss, const Tuple & t) {
      ss << std::get<std::tuple_size<Tuple>::value - i>(t) << ", ";
      TuplePrint<Stream, Tuple, i-1>::print(ss, t);
      return ss;
   }
};

//! @brief Helper class to push a tuple into a ostringstream
template <class Stream, class Tuple>
struct TuplePrint<Stream, Tuple, 1> {
   static Stream & print(Stream & ss, const Tuple & t) {
      ss << std::get<std::tuple_size<Tuple>::value - 1>(t);
      return ss;
   }
};

//! @brief Print tuples (required to avoid STXXL_MSG related issues)
//! Requires that there exists a ostream::operator<< overload for all
//! types in the tuple
template <typename ...Args>
inline std::ostream &operator<<(std::ostream &os, std::tuple<Args...> t) {
   // we first buffer the output to avoid race-conditions in a multi-threaded enviroment
   std::ostringstream oss;

   // use helper to print the tuple into the string buffer
   using Tuple = decltype(t);
   oss << "tuple(";
   TuplePrint<decltype(oss), Tuple, std::tuple_size<Tuple>::value>::print(oss, t);
   oss << ")";

   // copy text into output stream (thread-safe, hence no race-conditions)
   os << oss.str();
   return os;
}
