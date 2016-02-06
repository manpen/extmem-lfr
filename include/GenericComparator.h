/**
 * @file
 * @brief Generic comparator to be used in conjunction with STXXL's sorting.
 *
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

#include <limits>
#include <utility>

/**
 * @brief Generic comparator to be used in conjunction with STXXL's sorting.
 * @warning The template type T has be supported by the std::numeric_limits trait
 *          and requires the less operator.
 */
template <typename T>
struct GenericComparator{
   struct Ascending {
      Ascending() {}
      bool operator()(const T &a, const T &b) const { return a < b; }
      T min_value() const { return std::numeric_limits<T>::min(); }
      T max_value() const { return std::numeric_limits<T>::max(); }
   };

   struct Descending {
      Descending() {}
      bool operator() (const T& a, const T& b) const {return a > b;}
      T min_value() const {return std::numeric_limits<T>::max();}
      T max_value() const {return std::numeric_limits<T>::min();}
   };
};

namespace std {
   template<typename T1, typename T2>
   class numeric_limits<std::pair<T1, T2>> {
      using T = std::pair<T1, T2>;
   public:
      static T min() { return {numeric_limits<T1>::min(), numeric_limits<T2>::min()}; }

      static T max() { return {numeric_limits<T1>::max(), numeric_limits<T2>::max()}; }
   };
}