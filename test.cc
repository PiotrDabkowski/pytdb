#include <stdio.h>
#include <filesystem>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "table.h"
#include "proto/table.pb.h"

namespace pydb {
namespace {

using ::testing::UnorderedElementsAre;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Pair;

TEST(BinarySearch, UpperBoundNoDups) {
  std::vector<int64_t> times = {0, 1, 2, 3, 4, 5, 6, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_upper_bound(start, end, -100), 0);
  EXPECT_EQ(c_upper_bound(start, end, 0), 1);
  EXPECT_EQ(c_upper_bound(start, end, 5), 6);
  EXPECT_EQ(c_upper_bound(start, end, 6), 7);
  EXPECT_EQ(c_upper_bound(start, end, 7), 8);
  EXPECT_EQ(c_upper_bound(start, end, 100), 8);
}

TEST(BinarySearch, UpperBoundDups) {
  std::vector<int64_t> times = {0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_upper_bound(start, end, -100), 0);
  EXPECT_EQ(c_upper_bound(start, end, 0), 2);
  EXPECT_EQ(c_upper_bound(start, end, 3), 5);
  EXPECT_EQ(c_upper_bound(start, end, 8), 11);
}

TEST(BinarySearch, LowerBoundNoDups) {
  std::vector<int64_t> times = {0, 1, 2, 3, 4, 5, 6, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_lower_bound(start, end, -100), 0);
  EXPECT_EQ(c_lower_bound(start, end, 0), 0);
  EXPECT_EQ(c_lower_bound(start, end, 5), 5);
  EXPECT_EQ(c_lower_bound(start, end, 6), 6);
  EXPECT_EQ(c_lower_bound(start, end, 7), 7);
  EXPECT_EQ(c_lower_bound(start, end, 8), 8);
  EXPECT_EQ(c_lower_bound(start, end, 100), 8);
}

TEST(BinarySearch, LowerBoundDups) {
  std::vector<int64_t> times = {0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 7};
  auto* start = times.data();
  auto* end = start + times.size();
  EXPECT_EQ(c_lower_bound(start, end, -100), 0);
  EXPECT_EQ(c_lower_bound(start, end, 0), 0);
  EXPECT_EQ(c_lower_bound(start, end, 3), 4);
  EXPECT_EQ(c_lower_bound(start, end, 8), 11);
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

template<typename T>
RawColumnData FromVector(std::vector<T>* vec) {
  return {
      .data = reinterpret_cast<char*>(vec->data()),
      .size = sizeof(T) * vec->size(),
  };
}

// Converts to vector and invalidates RawColumnData. Also does some basic checks on the validity of the data for
// the given return type (such as size of data is a multiple of type size).
template<typename T>
std::vector<T> ToVector(RawColumnData* raw_column) {
  EXPECT_EQ(alignof(raw_column) % 8, 0);
  auto* data = reinterpret_cast<T*>(raw_column->data);
  EXPECT_EQ(raw_column->size % sizeof(T), 0);
  size_t num_items = raw_column->size / sizeof(T);
  std::vector<T> result;
  result.reserve(num_items);
  for (int i = 0; i < num_items; ++i) {
    result.push_back(data[i]);
  }
  delete[] raw_column->data;
  return result;
}

class TableTest : public testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::path tmp_dir_path{std::filesystem::temp_directory_path() /= std::tmpnam(nullptr)};
    EXPECT_TRUE(std::filesystem::create_directories(tmp_dir_path));
    root_dir_ = tmp_dir_path.string();
    root_dir_ = "/Users/piter/CLionProjects/PyDB/mock";
    std::filesystem::remove_all(root_dir_);
  }

  void TearDown() override {
//    std::filesystem::remove_all(root_dir_);
  }

  template <typename T>
  void CheckColumn(RawColumns& columns, const std::string& column_name, const std::vector<T>& expected) {
    EXPECT_TRUE(columns.contains(column_name)) << column_name;
    auto actual_vec = ToVector<T>(&columns.at(column_name));
    EXPECT_EQ(actual_vec.size(), expected.size()) << column_name;
    EXPECT_THAT(actual_vec, ElementsAreArray(expected.begin(), expected.end())) << column_name;
  }

  std::string root_dir_;

};

template <typename T>
std::vector<T> Range(int32_t start, int32_t end) {
  std::vector<T> res;
  EXPECT_GE(end, start);
  for (int32_t i = start; i < end; ++i) {
    res.push_back(i);
  }
  return res;
}

TEST_F(TableTest, SubTableWorkflow) {
  pydb::proto::Table config;
  config.set_index_density(256);

  auto* schema = config.mutable_schema();
  schema->set_time_column("t");
  schema->add_tag_column("s");
  auto* vc = schema->add_value_column();
  vc->set_name("v");
  vc->set_type(pydb::proto::ColumnSchema::FLOAT);

  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  EXPECT_EQ(sub_id.id(), "sub,s=GOOG");
  EXPECT_EQ(sub_id.tag().at("s"), 11);
  EXPECT_EQ(sub_id.str_tag(0), "GOOG");

  SubTable sub(root_dir_, config, sub_id);

  proto::Selector sel;
  sel.add_column("t");
  sel.add_column("s");
  EXPECT_FALSE(sub.Query(sel));

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);

  // Throw on out of order data.
  EXPECT_THROW(sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}}), std::invalid_argument);

  auto result = sub.Query(sel);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);
  CheckColumn(*result, "t", times);
  CheckColumn(*result, "s", std::vector<uint32_t>(rows, 11));

  size_t new_rows = 1000;
  auto times2 = Range<int64_t>(rows, rows + new_rows);
  auto values2 = Range<float>(rows, rows + new_rows);
  rows += new_rows;

  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}});
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024, 1280, 1536, 1792));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);

  result = sub.Query(sel);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);
  CheckColumn(*result, "t", Range<int64_t>(0, rows));
  CheckColumn(*result, "s", std::vector<uint32_t>(rows, 11));

  // Read the table from scratch and check it still works.
  SubTable sub2(root_dir_, config, sub_id);
  EXPECT_THAT(sub2.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024, 1280, 1536, 1792));
  EXPECT_EQ(sub2.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub2.GetIndex().num_rows, rows);

  sel.add_column("v");

  auto verify = [this, &sub2](const proto::Selector& selector, int32_t start, int32_t end) {
    spdlog::info("Verifying query {}...", selector.DebugString());
    auto q_res = sub2.Query(selector);
    if (start == end) {
      EXPECT_FALSE(q_res);
      return;
    }
    EXPECT_TRUE(q_res);
    EXPECT_EQ(q_res->size(), 3);
    this->CheckColumn(*q_res, "t", Range<int64_t>(start, end));
    this->CheckColumn(*q_res, "s", std::vector<uint32_t>(end - start, 11));
    this->CheckColumn(*q_res, "v", Range<float>(start, end));
  };

  verify(sel, 0, 2000);

  auto* t_sel = sel.mutable_time_selector();

  t_sel->set_start(11);
  verify(sel, 11, 2000);

  t_sel->set_start(-11);
  verify(sel, 0, 2000);

  t_sel->set_start(2000);
  verify(sel, 0, 0);

  t_sel->set_start(1200);
  verify(sel, 1200, 2000);

  t_sel->set_start(1999);
  verify(sel, 1999, 2000);

  t_sel->set_include_start(false);
  verify(sel, 0, 0);

  t_sel->set_start(-1);
  verify(sel, 0, 2000);

  t_sel->set_start(0);
  verify(sel, 1, 2000);

  t_sel->set_start(1200);
  verify(sel, 1201, 2000);

  t_sel->set_include_start(true);

  t_sel->set_start(1200);
  t_sel->set_end(1205);
  verify(sel, 1200, 1205);

  t_sel->set_start(900);
  t_sel->set_end(1705);
  verify(sel, 900, 1705);

  t_sel->set_start(1990);
  t_sel->set_end(1995);
  verify(sel, 1990, 1995);

  t_sel->set_start(1000);
  t_sel->set_end(20040);
  verify(sel, 1000, 2000);

  t_sel->set_start(1);
  t_sel->set_end(5);
  verify(sel, 1, 5);

  t_sel->set_include_end(true);
  t_sel->set_start(1);
  t_sel->set_end(5);
  verify(sel, 1, 6);

  t_sel->set_start(1);
  t_sel->set_end(1);
  verify(sel, 1, 2);

  t_sel->set_start(0);
  t_sel->set_end(1999);
  verify(sel, 0, 2000);

  t_sel->set_include_start(false);
  t_sel->set_start(0);
  t_sel->set_end(1999);
  t_sel->set_include_end(false);
  verify(sel, 1, 1999);

  t_sel->set_include_start(false);
  t_sel->set_start(5);
  t_sel->set_end(5);
  t_sel->set_include_end(true);
  verify(sel, 0, 0);

  t_sel->set_include_start(true);
  t_sel->set_start(5);
  t_sel->set_end(5);
  t_sel->set_include_end(false);
  verify(sel, 0, 0);

  t_sel->set_start(50);
  t_sel->set_end(5);
  verify(sel, 0, 0);

  t_sel->set_start(2000);
  t_sel->set_end(2001);
  verify(sel, 0, 0);

  t_sel->set_start(-10);
  t_sel->set_end(0);
  verify(sel, 0, 0);

  t_sel->set_start(-10);
  t_sel->set_end(0);
  t_sel->set_include_end(true);
  verify(sel, 0, 1);

  t_sel->set_start(0);
  t_sel->set_end(0);
  verify(sel, 0, 1);

  t_sel->set_start(1);
  t_sel->set_end(0);
  verify(sel, 0, 0);

  t_sel->set_start(1600);
  t_sel->set_end(2010);
  verify(sel, 1600, 2000);
}


}  // namespace
}  // pydb