#include <stdio.h>
#include <filesystem>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "table.h"
#include "proto/table.pb.h"

namespace pytdb {
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

template<typename T>
std::vector<T> Range(int32_t start, int32_t end) {
  std::vector<T> res;
  EXPECT_GE(end, start);
  for (int32_t i = start; i < end; ++i) {
    res.push_back(i);
  }
  return res;
}

class TableTest : public testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::path
        tmp_dir_path{std::filesystem::temp_directory_path() /= std::tmpnam(nullptr)};
    EXPECT_TRUE(std::filesystem::create_directories(tmp_dir_path));
    root_dir_ = tmp_dir_path.string();
    std::filesystem::remove_all(root_dir_);
  }

  proto::Table GetTSVConfig(size_t index_density = 256) {
    proto::Table config;
    config.set_index_density(index_density);

    auto* schema = config.mutable_schema();
    schema->set_time_column("t");
    schema->add_tag_column("s");
    auto* vc = schema->add_value_column();
    vc->set_name("v");
    vc->set_type(proto::ColumnSchema::FLOAT);
    return config;
  }

  void CheckTSVQuery(const SubTable& tsv_sub_table,
                     const proto::Selector& selector,
                     int32_t start,
                     int32_t end,
                     uint32_t sub_table_id = 1) {
    auto q_res = tsv_sub_table.Query(selector);
    if (start >= end) {
      EXPECT_FALSE(q_res);
      return;
    }
    EXPECT_TRUE(q_res);
    EXPECT_EQ(q_res->size(), 3);
    CheckColumn(*q_res, "t", Range<int64_t>(start, end));
    CheckColumn(*q_res, "s", std::vector<uint32_t>(end - start, sub_table_id));
    CheckColumn(*q_res, "v", Range<float>(start, end));
  }

  void TearDown() override {
    std::filesystem::remove_all(root_dir_);
  }

  template<typename T>
  void CheckColumn(RawColumns& columns,
                   const std::string& column_name,
                   const std::vector<T>& expected) {
    EXPECT_TRUE(columns.contains(column_name)) << column_name;
    auto actual_vec = ToVector<T>(&columns.at(column_name));
    EXPECT_EQ(actual_vec.size(), expected.size()) << column_name;
    EXPECT_THAT(actual_vec, ElementsAreArray(expected.begin(), expected.end())) << column_name;
  }

  std::string root_dir_;

};



TEST_F(TableTest, SubTableId) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  EXPECT_EQ(sub_id.id(), "sub,s=GOOG");
  EXPECT_EQ(sub_id.tag().at("s"), 1);
  EXPECT_EQ(sub_id.str_tag(0), "GOOG");
}

TEST_F(TableTest, SingleAppendDataAndIndex) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  SubTable sub(root_dir_, config, table.MakeSubTableId({{"s", "GOOG"}}));

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);

  // Throw on out of order data.
  EXPECT_THROW(sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}}),
               std::invalid_argument);
}

TEST_F(TableTest, AppendPersistence) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, sub_id);

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});

  SubTable sub2(root_dir_, config, sub_id);
  EXPECT_THAT(sub2.GetIndex().pos, ElementsAre(0, 256, 512, 768));
  EXPECT_EQ(sub2.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub2.GetIndex().num_rows, rows);
}

TEST_F(TableTest, RejectOutOfOrder) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  SubTable sub(root_dir_, config, table.MakeSubTableId({{"s", "GOOG"}}));

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});

  // Throw on out of order data.
  EXPECT_THROW(sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}}),
               std::invalid_argument);

  // Data unchanged.
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);
}

TEST_F(TableTest, RejectOutOfOrderFullRollback) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, sub_id);

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);
  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});

  size_t new_rows = 5000;
  auto times2 = Range<int64_t>(rows, rows + new_rows);
  auto values2 = Range<float>(rows, rows + new_rows);
  // Insert a single OOO item at the end.
  times2.push_back(times2.back() - 1);
  values2.push_back(times2.back() + 1);

  // Throw on out of order data.
  EXPECT_THROW(sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}}),
               std::invalid_argument);

  // Data unchanged, this case is tricky as we needed to do the full rollback because data until the last row has
  // been already inserted.
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);

  // Check if nothing has been committed to the filesystem.
  SubTable sub2(root_dir_, config, sub_id);
  EXPECT_THAT(sub2.GetIndex().pos, ElementsAre(0, 256, 512, 768));
  EXPECT_EQ(sub2.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub2.GetIndex().num_rows, rows);
}

TEST_F(TableTest, MultipleAppends) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  SubTable sub(root_dir_, config, table.MakeSubTableId({{"s", "GOOG"}}));

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);
  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});
  // Throw on out of order data.
  EXPECT_THROW(sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}}),
               std::invalid_argument);

  size_t new_rows = 900;
  auto times2 = Range<int64_t>(rows, rows + new_rows);
  auto values2 = Range<float>(rows, rows + new_rows);
  rows += new_rows;

  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}});
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024, 1280, 1536, 1792));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);
}

TEST_F(TableTest, AppendTruncateExistingMode) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, sub_id);

  auto times = Range<int64_t>(0, 1000);
  auto values = Range<float>(0, 1000);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});


  auto times2 = Range<int64_t>(500, 1025);
  auto values2 = Range<float>(500, 1025);
  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}}, AppendDataMode::kTruncateExisting);

  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512));
  EXPECT_EQ(sub.GetIndex().last_ts, 1024);
  EXPECT_EQ(sub.GetIndex().num_rows, 525);

  proto::Selector sel;
  sel.add_column("t");
  sel.add_column("s");
  sel.add_column("v");
  CheckTSVQuery(sub, sel, 500, 1025);

  // Double check if everything has been committed.
  SubTable sub2(root_dir_, config, sub_id);
  CheckTSVQuery(sub2, sel, 500, 1025);
  EXPECT_THAT(sub2.GetIndex().pos, ElementsAre(0, 256, 512));
  EXPECT_EQ(sub2.GetIndex().last_ts, 1024);
  EXPECT_EQ(sub2.GetIndex().num_rows, 525);
}

TEST_F(TableTest, AppendSkipOverlapMode) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, sub_id);

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});

  size_t overlap = 500;
  size_t rows2 = 525;
  size_t total_rows = rows + rows2 - overlap;
  auto times2 = Range<int64_t>(rows - overlap, total_rows);
  auto values2 = Range<float>(rows - overlap, total_rows);
  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}}, AppendDataMode::kSkipOverlap);

  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024));
  EXPECT_EQ(sub.GetIndex().last_ts, total_rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, total_rows);

  proto::Selector sel;
  sel.add_column("t");
  sel.add_column("s");
  sel.add_column("v");
  CheckTSVQuery(sub, sel, 0, 1025);

  // Double check if everything has been committed.
  SubTable sub2(root_dir_, config, sub_id);
  CheckTSVQuery(sub2, sel, 0, 1025);
  EXPECT_THAT(sub2.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024));
  EXPECT_EQ(sub2.GetIndex().last_ts, total_rows - 1);
  EXPECT_EQ(sub2.GetIndex().num_rows, total_rows);
}

TEST_F(TableTest, AppendTruncateExistingOverlapMode) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, sub_id);

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});

  size_t overlap = 500;
  size_t rows2 = 525;
  size_t total_rows = rows + rows2 - overlap;
  auto times2 = Range<int64_t>(rows - overlap, total_rows);
  auto values2 = Range<float>(rows - overlap, total_rows);
  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}}, AppendDataMode::kTruncateExistingOverlap);

  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024));
  EXPECT_EQ(sub.GetIndex().last_ts, total_rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, total_rows);

  proto::Selector sel;
  sel.add_column("t");
  sel.add_column("s");
  sel.add_column("v");
  CheckTSVQuery(sub, sel, 0, 1025);

  // Double check if everything has been committed.
  SubTable sub2(root_dir_, config, sub_id);
  CheckTSVQuery(sub2, sel, 0, 1025);
  EXPECT_THAT(sub2.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024));
  EXPECT_EQ(sub2.GetIndex().last_ts, total_rows - 1);
  EXPECT_EQ(sub2.GetIndex().num_rows, total_rows);
}

TEST_F(TableTest, Querying) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, sub_id);

  proto::Selector sel;
  sel.add_column("t");
  sel.add_column("s");
  // Empty table.
  EXPECT_FALSE(sub.Query(sel));

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);

  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});

  auto result = sub.Query(sel);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);
  CheckColumn(*result, "t", times);
  CheckColumn(*result, "s", std::vector<uint32_t>(rows, 1));

  size_t new_rows = 1000;
  auto times2 = Range<int64_t>(rows, rows + new_rows);
  auto values2 = Range<float>(rows, rows + new_rows);
  rows += new_rows;
  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}});

  result = sub.Query(sel);
  EXPECT_TRUE(result);
  EXPECT_EQ(result->size(), 2);
  CheckColumn(*result, "t", Range<int64_t>(0, rows));
  CheckColumn(*result, "s", std::vector<uint32_t>(rows, 1));

  // Read the table from scratch and check it still works.
  SubTable sub2(root_dir_, config, sub_id);
  sel.add_column("v");

  CheckTSVQuery(sub2, sel, 0, 2000);

  auto* t_sel = sel.mutable_time_selector();

  t_sel->set_start(11);
  CheckTSVQuery(sub2, sel, 11, 2000);

  t_sel->set_start(-11);
  CheckTSVQuery(sub2, sel, 0, 2000);

  t_sel->set_start(2000);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(1200);
  CheckTSVQuery(sub2, sel, 1200, 2000);

  t_sel->set_start(1999);
  CheckTSVQuery(sub2, sel, 1999, 2000);

  t_sel->set_include_start(false);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(-1);
  CheckTSVQuery(sub2, sel, 0, 2000);

  t_sel->set_start(0);
  CheckTSVQuery(sub2, sel, 1, 2000);

  t_sel->set_start(1200);
  CheckTSVQuery(sub2, sel, 1201, 2000);

  t_sel->set_include_start(true);

  t_sel->set_start(1200);
  t_sel->set_end(1205);
  CheckTSVQuery(sub2, sel, 1200, 1205);

  t_sel->set_start(900);
  t_sel->set_end(1705);
  CheckTSVQuery(sub2, sel, 900, 1705);

  t_sel->set_start(1990);
  t_sel->set_end(1995);
  CheckTSVQuery(sub2, sel, 1990, 1995);

  t_sel->set_start(1000);
  t_sel->set_end(20040);
  CheckTSVQuery(sub2, sel, 1000, 2000);

  t_sel->set_start(1);
  t_sel->set_end(5);
  CheckTSVQuery(sub2, sel, 1, 5);

  t_sel->set_include_end(true);
  t_sel->set_start(1);
  t_sel->set_end(5);
  CheckTSVQuery(sub2, sel, 1, 6);

  t_sel->set_start(1);
  t_sel->set_end(1);
  CheckTSVQuery(sub2, sel, 1, 2);

  t_sel->set_start(0);
  t_sel->set_end(1999);
  CheckTSVQuery(sub2, sel, 0, 2000);

  t_sel->set_include_start(false);
  t_sel->set_start(0);
  t_sel->set_end(1999);
  t_sel->set_include_end(false);
  CheckTSVQuery(sub2, sel, 1, 1999);

  t_sel->set_include_start(false);
  t_sel->set_start(5);
  t_sel->set_end(5);
  t_sel->set_include_end(true);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_include_start(true);
  t_sel->set_start(5);
  t_sel->set_end(5);
  t_sel->set_include_end(false);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(50);
  t_sel->set_end(5);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(2000);
  t_sel->set_end(2001);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(-10);
  t_sel->set_end(0);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(-10);
  t_sel->set_end(0);
  t_sel->set_include_end(true);
  CheckTSVQuery(sub2, sel, 0, 1);

  t_sel->set_start(0);
  t_sel->set_end(0);
  CheckTSVQuery(sub2, sel, 0, 1);

  t_sel->set_start(1);
  t_sel->set_end(0);
  CheckTSVQuery(sub2, sel, 0, 0);

  t_sel->set_start(1600);
  t_sel->set_end(2010);
  CheckTSVQuery(sub2, sel, 1600, 2000);

  t_sel->set_last_n(11);
  CheckTSVQuery(sub2, sel, 2000 - 11, 2000);

  t_sel->set_last_n(110000);
  CheckTSVQuery(sub2, sel, 0, 2000);
}

TEST_F(TableTest, AppendSkipOverlap) {
  proto::Table config = GetTSVConfig();
  Table table(root_dir_, config);
  auto sub_id = table.MakeSubTableId({{"s", "GOOG"}});
  SubTable sub(root_dir_, config, table.MakeSubTableId({{"s", "GOOG"}}));

  size_t rows = 1000;
  auto times = Range<int64_t>(0, rows);
  auto values = Range<float>(0, rows);
  sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}});
  // Throw on out of order data.
  EXPECT_THROW(sub.AppendData({{"t", FromVector(&times)}, {"v", FromVector(&values)}}),
               std::invalid_argument);

  size_t new_rows = 900;
  auto times2 = Range<int64_t>(rows, rows + new_rows);
  auto values2 = Range<float>(rows, rows + new_rows);
  rows += new_rows;

  sub.AppendData({{"t", FromVector(&times2)}, {"v", FromVector(&values2)}});
  EXPECT_THAT(sub.GetIndex().pos, ElementsAre(0, 256, 512, 768, 1024, 1280, 1536, 1792));
  EXPECT_EQ(sub.GetIndex().last_ts, rows - 1);
  EXPECT_EQ(sub.GetIndex().num_rows, rows);
}

TEST_F(TableTest, TableWorkflow) {
  proto::Table config;
  config.set_index_density(256);

  auto* schema = config.mutable_schema();
  schema->set_time_column("t");
  schema->add_tag_column("s");
  schema->add_tag_column("c");
  auto* vc = schema->add_value_column();
  vc->set_name("v");
  vc->set_type(proto::ColumnSchema::FLOAT);

  Table tmp_table(root_dir_, config);
  EXPECT_EQ(tmp_table.GetMeta().SerializeAsString(), config.SerializeAsString());
  Table tmp_table2(root_dir_);
  EXPECT_EQ(tmp_table2.GetMeta().SerializeAsString(), tmp_table.GetMeta().SerializeAsString());
  tmp_table2.MintStringRefs({"hello", "a world", "this"});
  EXPECT_EQ(*tmp_table2.ResolveStringRefs({1})[0], "hello");

  tmp_table2.MintStringRefs({"is", "hello", "a world", "great"});
  EXPECT_THAT(tmp_table2
                  .MintStringRefs({"is", "great", "hello", "xx", "is", "hello", "GOOG", "PL"}),
              testing::ElementsAre(4, 5, 1, 6, 4, 1, 7, 8));
  EXPECT_EQ(*tmp_table2.ResolveStringRefs({5})[0], "great");

  Table tmp_table3(root_dir_);
  EXPECT_EQ(*tmp_table3.ResolveStringRefs({5, 1})[0], "great");
  EXPECT_EQ(*tmp_table3.ResolveStringRefs({5, 1})[1], "hello");
  EXPECT_THAT(tmp_table3.MintStringRefs({"is", "great", "hello", "xx", "is", "hello", "FB"}),
              testing::ElementsAre(4, 5, 1, 6, 4, 1, 9));
  struct TableData {
    std::vector<int64_t> t;
    std::vector<StrRef> s;
    std::vector<StrRef> c;
    std::vector<float> v;
  };

  auto to_cols = [](TableData* d) {
    return RawColumns({
                          {"t", FromVector(&d->t)},
                          {"s", FromVector(&d->s)},
                          {"c", FromVector(&d->c)},
                          {"v", FromVector(&d->v)},
                      });
  };
  auto check_res = [this](std::optional<RawColumns> q_res, std::optional<TableData> d) {
    if (!d) {
      EXPECT_FALSE(q_res);
      return;
    }
    EXPECT_TRUE(q_res);
    EXPECT_EQ(q_res->size(), 4);
    this->CheckColumn(*q_res, "t", d->t);
    this->CheckColumn(*q_res, "v", d->v);
    this->CheckColumn(*q_res, "s", d->s);
    this->CheckColumn(*q_res, "c", d->c);

  };
  TableData sub1_part1 = {
      .t = {1, 2, 3},
      .s = {1, 1, 1},
      .c = {2, 2, 2},
      .v = {3, 2, 1},
  };
  tmp_table3.AppendData(to_cols(&sub1_part1));

  Table tmp_table4(root_dir_);
  proto::Selector sel;
  sel.mutable_sub_table_selector()->add_tag_order("s");
  sel.add_column("t");
  sel.add_column("v");
  sel.add_column("s");
  sel.add_column("c");
  check_res(tmp_table4.Query(sel), sub1_part1);

  TableData sub1_part2 = {
      .t = {4, 5, 6},
      .s = {1, 1, 1},
      .c = {2, 2, 2},
      .v = {3, 5, 1},
  };
  tmp_table4.AppendData(to_cols(&sub1_part2));
  check_res(tmp_table4.Query(sel), TableData{
      .t = {1, 2, 3, 4, 5, 6},
      .s = {1, 1, 1, 1, 1, 1},
      .c = {2, 2, 2, 2, 2, 2},
      .v = {3, 2, 1, 3, 5, 1},
  });

  TableData sub12_part31 = {
      .t = {11, 33, 44},
      .s = {1, 2, 2},
      .c = {2, 3, 3},
      .v = {44, 55, 66},
  };

  tmp_table4.AppendData(to_cols(&sub12_part31));
  check_res(tmp_table4.Query(sel), TableData{
      .t = {33, 44, 1, 2, 3, 4, 5, 6, 11},
      .s = {2, 2, 1, 1, 1, 1, 1, 1, 1},
      .c = {3, 3, 2, 2, 2, 2, 2, 2, 2},
      .v = {55, 66, 3, 2, 1, 3, 5, 1, 44},
  });

  Table tmp_table5(root_dir_);
  check_res(tmp_table5.Query(sel), TableData{
      .t = {33, 44, 1, 2, 3, 4, 5, 6, 11},
      .s = {2, 2, 1, 1, 1, 1, 1, 1, 1},
      .c = {3, 3, 2, 2, 2, 2, 2, 2, 2},
      .v = {55, 66, 3, 2, 1, 3, 5, 1, 44},
  });
  auto* tag_sel = sel.mutable_sub_table_selector()->add_tag_selector();
  tag_sel->set_name("s");
  tag_sel->add_value("a world");
  check_res(tmp_table5.Query(sel), TableData{
      .t = {33, 44},
      .s = {2, 2},
      .c = {3, 3},
      .v = {55, 66},
  });
  tag_sel->add_value("giga");
  check_res(tmp_table5.Query(sel), TableData{
      .t = {33, 44},
      .s = {2, 2},
      .c = {3, 3},
      .v = {55, 66},
  });
  tag_sel->add_value("hello");
  check_res(tmp_table5.Query(sel), TableData{
      .t = {33, 44, 1, 2, 3, 4, 5, 6, 11},
      .s = {2, 2, 1, 1, 1, 1, 1, 1, 1},
      .c = {3, 3, 2, 2, 2, 2, 2, 2, 2},
      .v = {55, 66, 3, 2, 1, 3, 5, 1, 44},
  });

  auto* time_sel = sel.mutable_time_selector();
  time_sel->set_start(2);
  time_sel->set_end(4);
  time_sel->set_include_end(true);

  check_res(tmp_table5.Query(sel), TableData{
      .t = {2, 3, 4},
      .s = {1, 1, 1},
      .c = {2, 2, 2},
      .v = {2, 1, 3},
  });

}

}  // namespace
}  // pytdb