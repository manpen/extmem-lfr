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
#include <limits>
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

template <typename T,
          typename TupleT = decltype(std::declval<T>().to_tuple())>
class GenericComparatorStruct {
   template < typename... Ts>
   using tuple_with_removed_refs = std::tuple<typename std::remove_reference<Ts>::type...>;

   template <typename... Ts>
   static tuple_with_removed_refs<Ts...> remove_ref_from_tuple_members(std::tuple<Ts...> const&) {
      return tuple_with_removed_refs<Ts...> { };
   }

public:
   struct Ascending {
      Ascending() {}

      bool operator()(const T &a, const T &b) const {
         return a < b;
      }

      T min_value() const {
         T result;
         using TupleNoRefT = decltype(remove_ref_from_tuple_members(std::declval<T>().to_tuple()));
         typename GenericComparatorTuple<TupleNoRefT>::Ascending comp;
         result.to_tuple() = comp.min_value();
         return result;
      }

      T max_value() const {
         T result;
         using TupleNoRefT = decltype(remove_ref_from_tuple_members(std::declval<T>().to_tuple()));
         typename GenericComparatorTuple<TupleNoRefT>::Ascending comp;
         result.to_tuple() = comp.max_value();
         return result;
      }
   };

   struct Descending {
      Descending() {}

      bool operator()(const T &a, const T &b) const {
         return b < a;
      }

      T min_value() const {
         T result;
         using TupleNoRefT = decltype(remove_ref_from_tuple_members(std::declval<T>().to_tuple()));
         typename GenericComparatorTuple<TupleNoRefT>::Descending comp;
         result.to_tuple() = comp.min_value();
         return result;
      }

      T max_value() const {
         T result;
         using TupleNoRefT = decltype(remove_ref_from_tuple_members(std::declval<T>().to_tuple()));
         typename GenericComparatorTuple<TupleNoRefT>::Descending comp;
         result.to_tuple() = comp.max_value();
         return result;
      }
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
inline std::ostream& operator<<(std::ostream& os, const std::tuple<Args...> & t) {
   // we first buffer the output to avoid race-conditions in a multi-threaded enviroment
   std::ostringstream oss;

   // use helper to print the tuple into the string buffer
   using Tuple = typename std::decay<decltype(t)>::type;
   oss << "(";
   TuplePrint<decltype(oss), Tuple, std::tuple_size<Tuple>::value>::print(oss, t);
   oss << ")";

   // copy text into output stream (thread-safe, hence no race-conditions)
   os << oss.str();
   return os;
}

//! @brief Print tuples (required to avoid STXXL_MSG related issues)
//! Requires that there exists a ostream::operator<< overload for all
//! types in the tuple
template <typename T1, typename T2>
inline std::ostream& operator<<(std::ostream& os, const std::pair<T1, T2> & t) {
   // we first buffer the output to avoid race-conditions in a multi-threaded enviroment
   std::ostringstream oss;

   // use helper to print the tuple into the string buffer
   oss << "(" << t.first << ", " << t.second << ")";

   // copy text into output stream (thread-safe, hence no race-conditions)
   os << oss.str();
   return os;
}

struct TupleSortable {
   virtual void streamify(std::ostream &) {}
};

#define DECL_TO_TUPLE(...) \
   auto to_tuple() -> decltype(std::tie(__VA_ARGS__)) {return std::tie(__VA_ARGS__);} \
   auto to_tuple() const -> const decltype(std::make_tuple(__VA_ARGS__)) {return std::make_tuple(__VA_ARGS__);}
#define DECL_LEX_COMPARE(name, ...) \
   DECL_TO_TUPLE(__VA_ARGS__) \
   bool operator< (const name& o) const {return to_tuple() <  o.to_tuple(); } \
   bool operator> (const name& o) const {return to_tuple() >  o.to_tuple(); } \
   bool operator<=(const name& o) const {return to_tuple() <= o.to_tuple(); } \
   bool operator>=(const name& o) const {return to_tuple() >= o.to_tuple(); } \
   bool operator==(const name& o) const {return to_tuple() == o.to_tuple(); } \
   bool operator!=(const name& o) const {return to_tuple() != o.to_tuple(); } \

#define DECL_LEX_COMPARE_OS(name, ...) \
   DECL_LEX_COMPARE(name, __VA_ARGS__) \
   DECL_TUPLE_OS(name) \

#define DECL_TUPLE_OS(name) \
   }; \
   inline std::ostream& operator<<(std::ostream &os, const name & t) {\
       std::ostringstream oss; \
       const auto & tu = t.to_tuple();\
       using Tuple = typename std::decay<decltype(tu)>::type; \
       oss << "" #name "[";\
       TuplePrint<decltype(oss), Tuple, std::tuple_size<Tuple>::value>::print(oss, tu);\
       oss << "]";\
       os << oss.str();\
       return os;\


inline std::ostream &operator<<(std::ostream &os, TupleSortable t) {
   t.streamify(os);
   return os;
}
