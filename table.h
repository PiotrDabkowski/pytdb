#pragma once

#include <string>
#include <utility>
#include <vector>
#include <optional>

#include <iostream>
#include <fstream>
#include <filesystem>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_format.h"
#include "spdlog/spdlog.h"
#include "proto/table.pb.h"

namespace pydb {

//class MemoryTable {
//
//};
//

bool MaybeCreateDir(const std::string &path);

struct Index {
  std::vector<int64_t> value = {};
  // Must start with position 0!!!
  std::vector<size_t> pos = {};
  // Actual values that are always in sync with the current table state.
  int64_t last_ts = std::numeric_limits<int64_t>::min();
  size_t num_rows = 0;

};

//template<typename T>
//struct ColumnData {
//  T *data;
//  // Note: total number of elements is num_rows * width. Most of the time the width will be 1...
//  size_t num_rows;
//  proto::ColumnSchema::Type type;
//  size_t width;
//};

constexpr uint32_t kInvalidStrRef = -1;

struct ColumnMeta {
  std::string path;
  proto::ColumnSchema::Type type;
  size_t width;
  size_t type_size;
  size_t row_size;
  // If path is empty, this one will be provided for tag columns.
  uint32_t tag_str_ref = kInvalidStrRef;
};

struct RawColumnData {
  char *data;
  size_t size;
};

using RawColumns = absl::flat_hash_map<std::string, RawColumnData>;
using Span = std::pair<size_t, size_t>;
const Span kEmptySpan = {0, 0};

// First greater element, if any, otherwise returns size.
size_t c_upper_bound(const int64_t *start, const int64_t *end, int64_t value);

// First greater or equal element, if any, otherwise returns size.
size_t c_lower_bound(const int64_t *start, const int64_t *end, int64_t value);

size_t GetTypeSize(proto::ColumnSchema::Type type);

class SubTable {
 public:

  SubTable(const std::string &root_table_dir, const proto::Table &table_meta, const proto::SubTableId &sub_table_id)
      : table_meta_(table_meta),
        sub_table_dir_(absl::StrCat(root_table_dir, "/", sub_table_id.id())),
        meta_path_(absl::StrCat(sub_table_dir_, "/", "META.pb")) {
    InitSubTable(sub_table_id);
    ExtractColumnMeta();
  };

  void InitMeta(const proto::SubTableId &sub_table_id) {
    std::ifstream in_meta_file(meta_path_, std::ifstream::in | std::ifstream::binary);
    if (in_meta_file) {
      GOOGLE_CHECK(meta_.ParseFromIstream(&in_meta_file)) << "Could not parse table meta at: " << meta_path_;
      GOOGLE_CHECK_EQ(meta_.id().id(), sub_table_id.id()) << "Inconsistent table id. weird...";
      return;
    }
    // Initialize the table - meta file does not exist yet.
    *meta_.mutable_id() = sub_table_id;
    WriteMeta();
    std::filesystem::temp_directory_path();
  }

  void InitIndex() {
    if (!meta_.has_index()) {
      // Default instance.
      index_ = {};
      return;
    }
    const auto &index = meta_.index();
    index_ = Index{
        .last_ts = index.last_ts(),
        .num_rows = index.num_rows(),
    };
    index_.value = {index.value().begin(), index.value().end()};
    index_.pos = {index.pos().begin(), index.pos().end()};
  }

  Index GetIndex() {
    return index_;
  }

  void UpdateMeta() {
    auto *index = meta_.mutable_index();
    index->set_num_rows(index_.num_rows);
    index->set_last_ts(index_.last_ts);
    *index->mutable_pos() = {index_.pos.begin(), index_.pos.end()};
    *index->mutable_value() = {index_.value.begin(), index_.value.end()};
    WriteMeta();
  }

  void WriteMeta() {
    std::ofstream out_meta_file(meta_path_, std::ofstream::out | std::ofstream::binary);
    GOOGLE_CHECK(meta_.SerializePartialToOstream(&out_meta_file)) << "Could not write to: " << meta_path_;
  }

  const proto::SubTableId &GetId() {
    return meta_.id();
  }

  void InitSubTable(const proto::SubTableId &sub_table_id) {
    GOOGLE_CHECK(MaybeCreateDir(sub_table_dir_)) << "Failed to create sub table dir " << sub_table_dir_;
    for (const auto&[column, column_meta] : column_meta_) {
      if (column_meta.path.empty()) {
        // No need to init. Column is the same for the sub table (tag column).
        continue;
      }
      // Just to init the table, this will append nothing, but initialize the file if needed.
      AppendRawColumnData(column_meta, RawColumnData{.data=nullptr, .size=0});
    }
    InitMeta(sub_table_id);
    InitIndex();
  }

  std::string GetColumnPath(const std::string &name) {
    return absl::StrCat(sub_table_dir_, "/", name, ".bin");
  }
  void ExtractColumnMeta() {
    for (const auto &value_column : table_meta_.schema().value_column()) {
      column_meta_[value_column.name()] = ColumnMeta{
          .path=GetColumnPath(value_column.name()),
          .type=value_column.type(),
          .width=value_column.width(),
      };
    }
    for (const auto &tag_column : table_meta_.schema().tag_column()) {
      GOOGLE_CHECK(meta_.id().tag().contains(tag_column))
              << "Tag for column not found in sub table id specification: " << tag_column << " "
              << meta_.id().DebugString();
      column_meta_[tag_column] = ColumnMeta{
          .type=proto::ColumnSchema::STRING_REF,
          .width=1,
          .tag_str_ref = meta_.id().tag().at(tag_column),
      };
    }
    column_meta_[table_meta_.schema().time_column()] = ColumnMeta{
        .path=GetColumnPath(table_meta_.schema().time_column()),
        .type=proto::ColumnSchema::INT64,
        .width=1,
    };

    for (auto&[name, column_meta] : column_meta_) {
      column_meta.type_size = GetTypeSize(column_meta.type);
      column_meta.row_size = column_meta.type_size * column_meta.width;
    }
  }

  absl::optional<RawColumns> Query(const proto::Selector &selector) {
    spdlog::info("New query arrived...");
    absl::flat_hash_set<std::string> columns_to_query;
    for (const auto &column_name : selector.column()) {
      columns_to_query.insert(column_name);
    }
    const std::string &time_column_name = table_meta_.schema().time_column();
    bool return_time_column = columns_to_query.contains(time_column_name);
    if (return_time_column) {
      // It will be queried anyway for the position selection.
      columns_to_query.erase(time_column_name);
    }
    Span query_span;
    RawColumnData time_column_data;

    std::tie(query_span, time_column_data) = QueryTimeSpan(selector.time_selector(), return_time_column);
    spdlog::info("query_span is {}->{}", query_span.first, query_span.second);

    if (query_span == kEmptySpan) {
      // No results for this query.
      return {};
    }
    RawColumns result;
    for (const auto &column_name : columns_to_query) {
      result[column_name] = ReadRawColumnSingle(column_meta_.at(column_name), query_span);
    }
    if (return_time_column) {
      result[time_column_name] = time_column_data;
    }
    spdlog::info("Done, query had results.");
    return result;
  }

  std::pair<Span, RawColumnData> QueryTimeSpan(const proto::TimeSelector &time_selector, bool return_time_column) {
    Span coarse_span = QueryCoarseTimeSpanFromIndex(time_selector);
    spdlog::info("coarse query_span is {}->{}", coarse_span.first, coarse_span.second);

    if (coarse_span.first >= coarse_span.second) {
      return {kEmptySpan, {}};
    }
    RawColumnData
        raw_coarse_time = ReadRawColumnSingle(column_meta_.at(table_meta_.schema().time_column()), coarse_span);
    auto *coarse_time = reinterpret_cast<int64_t *>(raw_coarse_time.data);
    const size_t num_rows = coarse_span.second - coarse_span.first;
    GOOGLE_CHECK_EQ(raw_coarse_time.size, sizeof(int64_t) * num_rows);

    ssize_t start;
    if (time_selector.has_start()) {
      if (time_selector.include_start()) {
        start = c_lower_bound(coarse_time, coarse_time + num_rows, time_selector.start());
      } else {
        start = c_upper_bound(coarse_time, coarse_time + num_rows, time_selector.start());
      }
      if (start == num_rows) {
        delete[] raw_coarse_time.data;
        return {kEmptySpan, {}};
      }
      start += coarse_span.first;
    } else {
      spdlog::error("No query start!");
      start = 0;
    }

    ssize_t end;
    if (time_selector.has_end()) {
      if (time_selector.include_end()) {
        end = c_upper_bound(coarse_time, coarse_time + num_rows, time_selector.end());
      } else {
        end = c_lower_bound(coarse_time, coarse_time + num_rows, time_selector.end());
      }
      end += coarse_span.first;
    } else {
      end = index_.num_rows;
    }
    const ssize_t num_return_rows = end - start;
    if (num_return_rows <= 0) {
      delete[] raw_coarse_time.data;
      return {kEmptySpan, {}};
    }

    RawColumnData time_column = {};
    if (return_time_column) {
      const size_t num_return_bytes = num_return_rows * sizeof(int64_t);
      char *return_buffer = new char[num_return_bytes];
      std::memcpy(return_buffer,
                  raw_coarse_time.data + (start - coarse_span.first) * sizeof(int64_t),
                  num_return_bytes);
      time_column = {
          .data = return_buffer,
          .size = num_return_bytes,
      };
    }
    delete[] raw_coarse_time.data;
    return {
        Span{start, end},
        time_column
    };
  }

  // Returns a coarse span where the data is guaranteed to be contained. The more precise the better as it avoids
  // reading actual time data from memory.
  Span QueryCoarseTimeSpanFromIndex(const proto::TimeSelector &time_selector) {
    if (index_.num_rows == 0) {
      spdlog::info("No rows in table, query result will be empty.");
      return kEmptySpan;
    }
    if (time_selector.has_end() && index_.value.at(0) > time_selector.end()) {
      return kEmptySpan;
    }
    if (time_selector.has_start() && index_.last_ts < time_selector.start()) {
      return kEmptySpan;
    }
    Span result;
    if (time_selector.has_start()) {
      // First greater or equal index position
      auto start_it = std::lower_bound(index_.value.begin(), index_.value.end(), time_selector.start());
      // We need to take the first one smaller than start position, so the -1 will do.
      result.first = index_.pos[std::max(std::distance(index_.value.begin(), start_it) - 1, 0L)];
    } else {
      result.first = 0;
    }
    if (time_selector.has_end()) {
      // First greater position.
      auto end_it = std::upper_bound(index_.value.begin(), index_.value.end(), time_selector.end());
      if (end_it == index_.value.end()) {
        // Need to read everything until the end, no greater index element found.
        result.second = index_.num_rows;
      } else {
        result.second = index_.pos[std::distance(index_.value.begin(), end_it)];
      }
    } else {
      result.second = index_.num_rows;
    }
    return result;
  }

  RawColumnData ReadRawColumnSingle(const ColumnMeta &column_meta, const Span &span) {
    return ReadRawColumn(column_meta, {span});
  }

  RawColumnData ReadRawColumn(const ColumnMeta &column_meta, const std::vector<Span> &spans) {
    size_t num_rows = 0;
    for (const auto &span : spans) {
      num_rows += span.second - span.first;
    }
    size_t num_bytes = num_rows * column_meta.row_size;
    char *buffer = new char[num_bytes];
    GOOGLE_CHECK_EQ(alignof(buffer) % 8, 0) << "Bad alignment!";
    if (!column_meta.path.empty()) {
      size_t buffer_pos = 0;
      std::ifstream f(column_meta.path, std::ifstream::in | std::ifstream::binary);
      GOOGLE_CHECK(f.is_open()) << "Could not open file for read at: " << column_meta.path;
      for (const auto &span : spans) {
        size_t start = column_meta.row_size * span.first;
        size_t to_read = column_meta.row_size * span.second - start;
        f.seekg(start);
        f.read((buffer + buffer_pos), to_read);
        buffer_pos += to_read;
      }
      GOOGLE_CHECK(f.good()) << "Something went wrong when reading: " << column_meta.path << " EOF: " << f.eof();
    } else {
      GOOGLE_CHECK_NE(column_meta.tag_str_ref, kInvalidStrRef)
              << "Columns without a path must be tag columns with a valid str_ref.";
      GOOGLE_CHECK_EQ(column_meta.width, 1) << "tag columns must be of width 1.";
      auto *refs = reinterpret_cast<uint32_t *>(buffer);
      for (size_t i = 0; i < num_rows; ++i) {
        refs[i] = column_meta.tag_str_ref;
      }
    }

    return {
        .data=buffer,
        .size=num_bytes,
    };
  }

  void AppendData(const RawColumns &column_data) {
    // This condition should be checked at the Table level and not check-fail if not satisfied (but rather throw).
    GOOGLE_CHECK_EQ(column_data.size(), column_meta_.size() - table_meta_.schema().tag_column_size())
            << "Must specify data for all non-tag coulumns!";
    RawColumnData time_column = column_data.at(table_meta_.schema().time_column());
    const size_t num_extra_rows = time_column.size / sizeof(int64_t);
    if (num_extra_rows == 0) {
      return;
    }
    auto *time = reinterpret_cast<int64_t *>(time_column.data);
    int64_t index_density = table_meta_.index_density();
    for (size_t i = 0; i < num_extra_rows; ++i) {
      if (time[i] < index_.last_ts) {
        throw std::invalid_argument(absl::StrFormat(
            "Out of range timestamp for sub table %s, latest inserted was %d, tried to insert %d",
            meta_.id().id(),
            index_.last_ts,
            time[i]));
      }
      index_.last_ts = time[i];
      if (index_.num_rows % index_density == 0) {
        // Add entry to the index.
        index_.value.push_back(index_.last_ts);
        index_.pos.push_back(index_.num_rows);
      }
      index_.num_rows++;
    }

    for (const auto&[column_name, raw_column_data] : column_data) {
      const ColumnMeta &column_meta = column_meta_.at(column_name);
      GOOGLE_CHECK_EQ(column_meta.row_size * num_extra_rows, raw_column_data.size)
              << "Bad data size for column: " << column_name;
      AppendRawColumnData(column_meta, raw_column_data);
    }
    UpdateMeta();
  }

  void AppendRawColumnData(const ColumnMeta &column_meta, const RawColumnData &raw_column_data) {
    std::ofstream f(column_meta.path, std::ofstream::out | std::ofstream::binary | std::ofstream::app);
    GOOGLE_CHECK(f.is_open()) << "Could not open file for write at: " << column_meta.path;
    if (raw_column_data.size > 0) {
      f.write(raw_column_data.data, raw_column_data.size);
    }
    GOOGLE_CHECK(f.good()) << "Something went wrong when writing: " << column_meta.path;
  }

 private:
  proto::SubTable meta_;
  const proto::Table &table_meta_;

  const std::string sub_table_dir_;
  const std::string meta_path_;

  absl::flat_hash_map<std::string, ColumnMeta> column_meta_;

  Index index_;

};

class Table {
 public:
  Table(std::string table_root_dir, proto::Table table_meta)
      : table_root_dir_(std::move(table_root_dir)), meta_(std::move(table_meta)) {
    sub_tables_.reserve(meta_.sub_table_id_size());

    for (const auto &sub_table_id : meta_.sub_table_id()) {
      AddSubTable(sub_table_id);
    }
    non_tag_columns_ = {meta_.schema().time_column()};
    for (const auto &value_column : meta_.schema().value_column()) {
      non_tag_columns_.push_back(value_column.name());
    }
  }

  absl::optional<RawColumns> Query(const proto::Selector &selector) {
    std::vector<SubTable *> sub_tables = GetSelectedSubTables(selector.sub_table_selector());
    if (sub_tables.empty()) {
      return {};
    }
    absl::flat_hash_map<std::string, std::vector<RawColumnData>> sub_results;
    size_t result_size = 0;
    for (SubTable *sub_table : sub_tables) {
      if (result_size > (1UL << 30UL)) {
        // Bail out if the result is over 1GB, bad query? TODO: Improve error handling in general.
        break;
      }
      auto sub_query_result = sub_table->Query(selector);
      if (!sub_query_result) {
        continue;
      }
      for (const auto&[column, sub_column_result] : *sub_query_result) {
        sub_results[column].push_back(sub_column_result);
        result_size += sub_column_result.size;
      }
    }
    if (result_size == 0) {
      return {};
    }
    RawColumns results;
    for (const auto&[column, column_sub_results] : sub_results) {
      results[column] = MergeColumnSubResults(column_sub_results);
    }
    return results;
  }

  bool MaybeAddSubTable(const absl::flat_hash_map<std::string, std::string> &str_tags) {
    GOOGLE_CHECK_EQ(str_tags.size(), meta_.schema().tag_column_size()) << "Specify all tags for a sub table";
    const proto::SubTableId id = MakeSubTableId(str_tags);
    if (sub_table_unique_index_.contains(GetUniqueIndexEntry(id))) {
      return false;
    }
    AddSubTable(id);
    return true;
  }

  void AppendData(const RawColumns &column_data) {
    std::vector<uint32_t> sub_table_selector(meta_.schema().tag_column_size(), kInvalidStrRef);

  }

  void AppendRowSpanToSubTable(const RawColumns &column_data, const Span &row_span, SubTable *sub_table) {
    RawColumns sub_column_data;
    const size_t total_rows = column_data.at(meta_.schema().time_column()).size / sizeof(int64_t);
    for (const auto &column_name : non_tag_columns_) {
      const RawColumnData &full_column = column_data.at(column_name);
      GOOGLE_CHECK_EQ(full_column.size % total_rows, 0);
      size_t item_size = full_column.size / total_rows;
      GOOGLE_CHECK_EQ(item_size % 8, 0) << "Some bad element size...";
      sub_column_data[column_name] = {
          .data = full_column.data + row_span.first * item_size,
          .size = (row_span.second - row_span.first) * item_size,
      };
    }
    sub_table->AppendData(sub_column_data);
  }

  RawColumnData MergeColumnSubResults(const std::vector<RawColumnData> &column_sub_results) {
    size_t merged_byte_size = 0;
    for (const auto &column_sub_result : column_sub_results) {
      merged_byte_size += column_sub_result.size;
    }
    GOOGLE_CHECK_GE(merged_byte_size, 0) << "No results to merge.";
    char *buffer = new char[merged_byte_size];
    size_t buffer_pos = 0;
    for (const auto &column_sub_result : column_sub_results) {
      std::memcpy(buffer + buffer_pos, column_sub_result.data, column_sub_result.size);
      buffer_pos += column_sub_result.size;
      delete[] column_sub_result.data;
    }
    return {
        .data = buffer,
        .size = merged_byte_size,
    };
  }

  std::vector<int32_t> MintStringRefs(const std::vector<std::string> &strings) {
    std::vector<int32_t> result;
    for (const auto &str : strings) {
      if (inv_string_ref_map_.contains(str)) {
        result.push_back(inv_string_ref_map_[str]);
      } else {
        int32_t mint = string_ref_map_.size() + 11;
        result.push_back(mint);
        inv_string_ref_map_[str] = mint;
        string_ref_map_[mint] = str;
      }
    }
    return result;
  }

  proto::SubTableId MakeSubTableId(const absl::flat_hash_map<std::string, std::string> &str_tags) {
    proto::SubTableId sub_id;
    std::string id = "sub";
    for (const auto &tag_column : meta_.schema().tag_column()) {
      absl::StrAppend(&id, ",", tag_column, "=", str_tags.at(tag_column));
      (*sub_id.mutable_tag())[tag_column] = MintStringRefs({str_tags.at(tag_column)}).at(0);
      sub_id.add_str_tag(str_tags.at(tag_column));
    }
    sub_id.set_id(id);
    return sub_id;
  }

 private:

  void AddSubTable(const proto::SubTableId &id) {
    sub_tables_.push_back(absl::make_unique<SubTable>(table_root_dir_, meta_, id));
    IndexSubTable(sub_tables_.back().get());
  }

  absl::flat_hash_map<std::string, uint32_t> ConvertStrTags(const absl::flat_hash_map<std::string,
                                                                                      std::string> &str_tags) {
    absl::flat_hash_map<std::string, uint32_t> result;
    // Cheap enough to do it stupidly rather than in batch.
    for (const auto&[tag, value] : str_tags) {
      result[tag] = MintStringRefs({tag}).at(0);
    }
    return result;
  }

  std::string GetUniqueIndexEntry(const proto::SubTableId &sub_table_id) {
    std::vector<uint32_t> tag_str_refs;
    for (const auto &tag_column : meta_.schema().tag_column()) {
      tag_str_refs.push_back(sub_table_id.tag().at(tag_column));
    }
    return GetUniqueIndexEntry(tag_str_refs);
  }

  std::string GetUniqueIndexEntry(const std::vector<uint32_t> &tag_str_refs) {
    std::string entry;
    for (uint32_t str_ref : tag_str_refs) {
      GOOGLE_CHECK_NE(str_ref, kInvalidStrRef);
      absl::StrAppend(&entry, ",", str_ref);
    }
    return entry;
  }

  std::vector<SubTable *> GetSelectedSubTables(const proto::SubTableSelector &selector) {
    if (selector.tag_selector().empty()) {
      // All sub tables.
      std::vector<SubTable *> all_sub_tables;
      for (auto &sub_table : sub_tables_) {
        all_sub_tables.push_back(sub_table.get());
      }
      return all_sub_tables;
    }
    GOOGLE_CHECK(false) << "Selecting sub tables not implemented yet.";
    return {};
  }

  void IndexSubTable(SubTable *sub_table) {
    const std::string entry = GetUniqueIndexEntry(sub_table->GetId());
    GOOGLE_CHECK(!sub_table_unique_index_.contains(entry))
            << "Indexed table already exists: " << sub_table->GetId().id();
    sub_table_unique_index_[entry] = sub_table;

    for (int i = 0; i < meta_.schema().tag_column_size(); ++i) {
      sub_table_index_[{meta_.schema().tag_column(i), sub_table->GetId().str_tag(i)}].push_back(sub_table);
    }
  }

  const std::string table_root_dir_;
  proto::Table meta_;

  std::vector<std::unique_ptr<SubTable>> sub_tables_;
  absl::flat_hash_map<int32_t, std::string> string_ref_map_;
  absl::flat_hash_map<std::string, int32_t> inv_string_ref_map_;
  std::vector<std::string> non_tag_columns_;

  // Index by {tag_column_name, tag_value}  -> sub tables .
  absl::flat_hash_map<std::pair<std::string, std::string>, std::vector<SubTable *>> sub_table_index_;
  // Index by uint32 comma separated tags (in order of tag columns)  -> sub table
  // A bit stupid, but only relevant for writes.
  absl::flat_hash_map<std::string, SubTable *> sub_table_unique_index_;

//  std::vector<SubTable> sub_tables_;

};

} // namespace pydb


