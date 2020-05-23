#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>


#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "table.h"
#include "proto/table.pb.h"


namespace pydb {
namespace {

TEST(BinarySearch, UpperBoundNoDups) {
  std::vector<int64_t> times = {0, 1, 2, 3, 4, 5, 6, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_upper_bound(start, end , -100), 0);
  EXPECT_EQ(c_upper_bound(start, end , 0), 1);
  EXPECT_EQ(c_upper_bound(start, end , 5), 6);
  EXPECT_EQ(c_upper_bound(start, end , 6), 7);
  EXPECT_EQ(c_upper_bound(start, end , 7), 8);
  EXPECT_EQ(c_upper_bound(start, end , 100), 8);
}

TEST(BinarySearch, UpperBoundDups) {
  std::vector<int64_t> times = {0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_upper_bound(start, end , -100), 0);
  EXPECT_EQ(c_upper_bound(start, end , 0), 2);
  EXPECT_EQ(c_upper_bound(start, end , 3), 5);
  EXPECT_EQ(c_upper_bound(start, end , 8), 11);
}

TEST(BinarySearch, LowerBoundNoDups) {
  std::vector<int64_t> times = {0, 1, 2, 3, 4, 5, 6, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_lower_bound(start, end , -100), 0);
  EXPECT_EQ(c_lower_bound(start, end , 0), 0);
  EXPECT_EQ(c_lower_bound(start, end , 5), 5);
  EXPECT_EQ(c_lower_bound(start, end , 6), 6);
  EXPECT_EQ(c_lower_bound(start, end , 7), 7);
  EXPECT_EQ(c_lower_bound(start, end , 8), 8);
  EXPECT_EQ(c_lower_bound(start, end , 100), 8);
}

TEST(BinarySearch, LowerBoundDups) {
  std::vector<int64_t> times = {0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_lower_bound(start, end , -100), 0);
  EXPECT_EQ(c_lower_bound(start, end , 0), 0);
  EXPECT_EQ(c_lower_bound(start, end , 3), 4);
  EXPECT_EQ(c_lower_bound(start, end , 8), 11);
}

TEST(CharBuffer, CheckAlignment) {
  char* buffer = new char[1000];
  EXPECT_EQ(alignof(buffer) % 8, 0);
  buffer[0] = 11;
  buffer[8] = 11;
  auto* view = reinterpret_cast<int64_t*>(buffer + 1);
  view[0] = 1;
  EXPECT_EQ(buffer[0], 11);
  EXPECT_NE(buffer[8], 11);
  delete[] buffer;
}

TEST(CPP, Version) {
  EXPECT_EQ(__cplusplus, 201703);
}

template <typename T>
RawColumnData RawColumnDataFromVector(std::vector<T>* vec) {
  return {
    .data = reinterpret_cast<char*>(vec->data()),
    .size = sizeof(T)*vec->size(),
  };
}

TEST(SubTable, Basics) {
  pydb::proto::Table t;
  auto* schema = t.mutable_schema();
  schema->add_tag_column("s");
  schema->set_time_column("t");
  auto* vc = schema->add_value_column();
  vc->set_name("c");
  vc->set_type(pydb::proto::ColumnSchema::BYTE);
  auto* sub_id = t.add_sub_table_id();
  sub_id->set_id("first");
  (*sub_id->mutable_tag())["s"] = 11;

  pydb::SubTable k("/Users/piter/CLionProjects/PyDB/mock/table_x", t, t.sub_table_id(0));

  size_t rows = 1000;
  std::vector<int64_t> time(rows, 12);
  std::vector<char> feature(rows, 11);

//  EXPECT_FALSE(k.Query({}).has_value());
  absl::flat_hash_map<std::string, RawColumnData> column_data;
  column_data["t"] = RawColumnDataFromVector(&time);
  column_data["c"] = RawColumnDataFromVector(&feature);

  k.AppendData(column_data);

  proto::Selector selector;
  selector.add_columns("t");
  selector.add_columns("c");
  selector.add_columns("s");
  auto* time_selector = selector.mutable_time_selector();
//  time_selector->set_start(2);
//  time_selector->set_include_start(true);

  auto result = k.Query(selector);
  EXPECT_TRUE(result.has_value());

  //  EXPECT_EQ(result->at("c").size, 10000000);

//  EXPECT_THAT()


}


}  // namespace
}  // pydb