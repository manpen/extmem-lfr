/**
 * @file
 * @brief Test cases for TupleHelpers
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
#include <gtest/gtest.h>

#include <TupleHelper.h>

class TestTupleHelper : public ::testing::Test {
protected:
   template <typename T>
   std::string _string_stream(const T& t) {
      std::ostringstream oss;
      oss << std::noboolalpha;
      oss << t;
      return oss.str();
   }
};

TEST_F(TestTupleHelper, streamer) {
   ASSERT_STREQ(this->_string_stream(1).c_str(), "1");
   ASSERT_STREQ(this->_string_stream(std::make_tuple(1, 2)).c_str(), "tuple(1, 2)");
   ASSERT_STREQ(this->_string_stream(std::make_tuple(1, "test")).c_str(), "tuple(1, test)");
   ASSERT_STREQ(this->_string_stream(std::make_tuple(1, "test", false)).c_str(), "tuple(1, test, 0)");
   ASSERT_STREQ(this->_string_stream(std::make_tuple(1, 2, 3, 4, -5)).c_str(), "tuple(1, 2, 3, 4, -5)");
}

TEST_F(TestTupleHelper, compareAsc1) {
   using T = std::tuple<unsigned int>;

   GenericComparatorTuple<T>::Ascending comp;
   auto max = comp.max_value();
   auto min = comp.min_value();

   ASSERT_EQ(std::get<0>(max), std::numeric_limits<unsigned int>::max());
   ASSERT_EQ(std::get<0>(min), 0u);

   ASSERT_TRUE(comp(min, max));
   ASSERT_FALSE(comp(max, min));

   T t1(1);
   T t2(2);

   ASSERT_TRUE(comp(t1, t2));
   ASSERT_FALSE(comp(t2, t1));
   ASSERT_TRUE(comp(min, t1));
   ASSERT_TRUE(comp(min, t2));
   ASSERT_TRUE(comp(t1, max));
   ASSERT_TRUE(comp(t2, max));
}

TEST_F(TestTupleHelper, compareDesc1) {
   using T = std::tuple<unsigned int>;

   GenericComparatorTuple<T>::Descending comp;
   auto max = comp.max_value();
   auto min = comp.min_value();

   ASSERT_EQ(std::get<0>(min), std::numeric_limits<unsigned int>::max());
   ASSERT_EQ(std::get<0>(max), 0u);

   ASSERT_TRUE(comp(min, max));
   ASSERT_FALSE(comp(max, min));

   T t2(1);
   T t1(2);

   ASSERT_TRUE(comp(t1, t2));
   ASSERT_FALSE(comp(t2, t1));
   ASSERT_TRUE(comp(min, t1));
   ASSERT_TRUE(comp(min, t2));
   ASSERT_TRUE(comp(t1, max));
   ASSERT_TRUE(comp(t2, max));
}


TEST_F(TestTupleHelper, compareAsc2) {
   using T = std::tuple<unsigned int, bool>;

   GenericComparatorTuple<T>::Ascending comp;
   auto max = comp.max_value();
   auto min = comp.min_value();

   ASSERT_EQ(std::get<0>(max), std::numeric_limits<unsigned int>::max());
   ASSERT_EQ(std::get<0>(min), 0u);

   ASSERT_EQ(std::get<1>(max), true);
   ASSERT_EQ(std::get<1>(min), false);

   ASSERT_TRUE(comp(min, max));
   ASSERT_FALSE(comp(max, min));

   T t1(1, false);
   T t2(1, true);

   ASSERT_TRUE(comp(t1, t2));
   ASSERT_FALSE(comp(t2, t1));
   ASSERT_TRUE(comp(min, t1));
   ASSERT_TRUE(comp(min, t2));
   ASSERT_TRUE(comp(t1, max));
   ASSERT_TRUE(comp(t2, max));
}

TEST_F(TestTupleHelper, compareDesc2) {
   using T = std::tuple<unsigned int, bool>;

   GenericComparatorTuple<T>::Descending comp;
   auto max = comp.max_value();
   auto min = comp.min_value();

   ASSERT_EQ(std::get<0>(min), std::numeric_limits<unsigned int>::max());
   ASSERT_EQ(std::get<0>(max), 0u);

   ASSERT_EQ(std::get<1>(min), true);
   ASSERT_EQ(std::get<1>(max), false);

   ASSERT_TRUE(comp(min, max));
   ASSERT_FALSE(comp(max, min));

   T t2(1, false);
   T t1(1, true);

   ASSERT_TRUE(comp(t1, t2));
   ASSERT_FALSE(comp(t2, t1));
   ASSERT_TRUE(comp(min, t1));
   ASSERT_TRUE(comp(min, t2));
   ASSERT_TRUE(comp(t1, max));
   ASSERT_TRUE(comp(t2, max));
}