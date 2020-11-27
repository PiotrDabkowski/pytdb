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
#include "absl/container/node_hash_map.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "proto/table.pb.h"

namespace pytdb {

class TableWrap;

bool MaybeCreateDir(const std::string& path);

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

using StrRef = uint32_t;
constexpr StrRef kInvalidStrRef = -1;

struct ColumnMeta {
  std::string path;
  proto::ColumnSchema::Type type;
  size_t width;
  size_t type_size;
  size_t row_size;
  // If path is empty, this one will be provided for tag columns.
  StrRef tag_str_ref = kInvalidStrRef;
};

struct RawColumnData {
  char* data;
  size_t size;
};

// Do not remove check-fails from write routines, if they fail then we are in inconsistent state, so better to crash...
template<typename T>
void WriteProto(const std::string& path, const T& proto) noexcept {
  std::ofstream out_file(path, std::ofstream::out | std::ofstream::binary);
  GOOGLE_CHECK(out_file) << "Could not open path: " << path;
  GOOGLE_CHECK(proto.SerializePartialToOstream(&out_file))
          << "Could not write meta to: " << path;
}

template<typename T>
bool ReadProto(const std::string& path, T* proto) {
  std::ifstream in_meta_file(path, std::ifstream::in | std::ifstream::binary);
  if (!in_meta_file) {
    return false;
  }
  GOOGLE_CHECK(proto->ParseFromIstream(&in_meta_file))
          << "Could not parse table meta at: " << path;
  return true;
}
// In general, if .lock file exists then .back file definitely exists and is good to read.
// If lock does not exist then the main file is good to read.
// Read:
// OK -> just read main
// LOCK -> read backup
// Write:
// OK:
//   1. Copy main to back
//   2. Write lock
//   3. Write main
//   4. Remove lock, remove back
// LOCK:
//   1. Do not touch bak, it is good, just write main
//   2. remove lock, remove back
template<typename T>
bool ReadProtoSafe(const std::string& path, T* proto) {
  if (std::filesystem::exists(absl::StrCat(path, ".lock"))) {
//    spdlog::error(absl::StrCat("[Read] File %s is corrupted, reading backup.", path));
    return ReadProto(absl::StrCat(path, ".back"), proto);
  }
  return ReadProto(path, proto);
}

template<typename T>
void WriteProtoSafe(const std::string& path, const T& proto) noexcept {
  try {
    if (std::filesystem::exists(absl::StrCat(path, ".lock"))) {
//      spdlog::error(absl::StrCat("[Write] File %s is corrupted, recovering.", path));
      WriteProto(path, proto);
    } else {
      WriteProto(absl::StrCat(path, ".back"), proto);
      proto::Dummy dummy;
      dummy.set_dummy("lock");
      WriteProto(absl::StrCat(path, ".lock"), dummy);
      WriteProto(path, proto);
    }
    // Both lock and back must exist at this point.
    GOOGLE_CHECK(std::filesystem::remove(absl::StrCat(path, ".lock")));
    GOOGLE_CHECK(std::filesystem::remove(absl::StrCat(path, ".back")));
  } catch (...) {
    GOOGLE_CHECK(false) << "Write routine failed. Cannot recover.";
  }
}

using RawColumns = absl::flat_hash_map<std::string, RawColumnData>;
using Span = std::pair<size_t, size_t>;
const Span kEmptySpan = {0, 0};

// First greater element, if any, otherwise returns size.
size_t c_upper_bound(const int64_t* start, const int64_t* end, int64_t value);

// First greater or equal element, if any, otherwise returns size.
size_t c_lower_bound(const int64_t* start, const int64_t* end, int64_t value);

size_t GetTypeSize(proto::ColumnSchema::Type type);

enum AppendDataMode {
  kAppend,
  kTruncateExisting,
  kTruncateExistingOverlap,
  kSkipOverlap,
};

class SubTable {
 public:
  SubTable(const std::string& root_table_dir,
           const proto::Table& table_meta,
           const proto::SubTableId& sub_table_id)
      : table_meta_(table_meta),
        sub_table_dir_(absl::StrCat(root_table_dir, "/", sub_table_id.id())),
        meta_path_(absl::StrCat(sub_table_dir_, "/", "META.pb")),
        lock_path_(absl::StrCat(sub_table_dir_, "/", "mess.lock")) {
    InitSubTable(sub_table_id);
    ExtractColumnMeta();
  };

  const Index& GetIndex() const {
    return index_;
  }

  const proto::SubTableId& GetId() const {
    return meta_.id();
  }

  std::optional<RawColumns> Query(const proto::Selector& selector) const {
    absl::ReaderMutexLock lock(&mu_);
    absl::flat_hash_set<std::string> columns_to_query;
    for (const auto& column_name : selector.column()) {
      columns_to_query.insert(column_name);
    }
    const std::string& time_column_name = table_meta_.schema().time_column();
    bool return_time_column = columns_to_query.contains(time_column_name);
    if (return_time_column) {
      // It will be queried anyway for the position selection.
      columns_to_query.erase(time_column_name);
    }
    Span query_span;
    RawColumnData time_column_data{};

    std::tie(query_span, time_column_data) =
        QueryTimeSpan(selector.time_selector(), return_time_column);

    if (query_span == kEmptySpan) {
      // No results for this query.
      return {};
    }
    RawColumns result;
    for (const auto& column_name : columns_to_query) {
      result[column_name] = ReadRawColumnSingle(column_meta_.at(column_name), query_span);
    }
    if (return_time_column) {
      result[time_column_name] = time_column_data;
    }
    return result;
  }

  void AppendData(const RawColumns& column_data, AppendDataMode append_mode = kAppend) {
    absl::WriterMutexLock lock(&mu_);
    // This condition should be checked at the Table level and not check-fail if not satisfied (but rather throw).
    GOOGLE_CHECK_EQ(column_data.size(),
                    column_meta_.size() - table_meta_.schema().tag_column_size())
            << "Must specify data for all non-tag columns!";

    if (std::filesystem::exists(lock_path_)) {
      // Bring back the consistent state. The loaded meta is consistent.
      RecoverMess();
    }

    // Here handle extra truncation...
    if (append_mode == kTruncateExisting) {
      SafeTruncateTable(0);
    }
    RawColumnData time_column = column_data.at(table_meta_.schema().time_column());
    const size_t num_extra_rows = time_column.size / sizeof(int64_t);
    if (num_extra_rows == 0) {
      return;
    }
    auto* time = reinterpret_cast<int64_t*>(time_column.data);

    if (append_mode == kTruncateExistingOverlap && time[0] >= index_.last_ts) {
      proto::TimeSelector time_selector;
      // Get the position of the element equal to the first time value of the appended column_data.
      time_selector.set_start(time[0]);
      size_t
          no_overlap_len = QueryTimeSpan(time_selector, /*return_time_column=*/false).first.first;
      // Truncate to this element.
      SafeTruncateTable(no_overlap_len);
    }
    size_t num_accepted_rows = 0;

    int64_t index_density = table_meta_.index_density();
    for (size_t i = 0; i < num_extra_rows; ++i) {
      if (time[i] < index_.last_ts) {
        if (append_mode == kSkipOverlap) {
          if (num_accepted_rows) {
            throw std::invalid_argument(absl::StrFormat(
                "Overlap was skipped, accepted %d rows so far, but it seems like there are out of order rows afterwards at position %d",
                num_accepted_rows,
                i));
          }
          continue;
        }
        // Restore the index.
        InitIndex();
        throw std::invalid_argument(absl::StrFormat(
            "Out of range timestamp for sub table %s, latest inserted was %d, tried to insert %d (append mode: %d)",
            meta_.id().id(),
            index_.last_ts,
            time[i],
            append_mode));
      }
      index_.last_ts = time[i];
      if (index_.num_rows % index_density == 0) {
        // Add entry to the index.
        index_.value.push_back(index_.last_ts);
        index_.pos.push_back(index_.num_rows);
      }
      index_.num_rows++;
      num_accepted_rows++;
    }
    if (num_accepted_rows == 0) {
      return;
    }
    WriteProto(lock_path_, proto::Dummy());
    size_t row_skip = num_extra_rows - num_accepted_rows;
    if (append_mode != kSkipOverlap) {
      GOOGLE_CHECK_EQ(row_skip, 0);
    }
    try {
      for (const auto&[column_name, raw_column_data] : column_data) {
        const ColumnMeta& column_meta = column_meta_.at(column_name);
        GOOGLE_CHECK_EQ(column_meta.row_size * num_extra_rows, raw_column_data.size)
                << "Bad data size for column: " << column_name;
        size_t byte_skip = row_skip * column_meta.row_size;
        GOOGLE_CHECK_GT(raw_column_data.size, byte_skip);
        AppendRawColumnData(column_meta, RawColumnData{
            .data=raw_column_data.data + byte_skip,
            .size=raw_column_data.size - byte_skip,
        });
      }

    } catch (...) {
      // Restore the index from serialized representation in meta (this will not throw). Brings back the index
      // consistency.
      InitIndex();
      // Recover columns based on currently serialized meta.
      RecoverMess();
      // Brought back the consistent state, we can remove the mess lock.
      GOOGLE_CHECK(std::filesystem::remove(lock_path_));
      // Still throw to notify that append did not work out.
      throw;
    }
    // This will either work or crash, if it works then data is in. Otherwise lock_path will be seen on the next write
    // (after restart with reloaded meta) and the mess will be cleaned.
    UpdateMeta();
    WriteMeta();
    GOOGLE_CHECK(std::filesystem::remove(lock_path_));
  }

  void TruncateAll() {
    mu_.AssertNotHeld();
    absl::WriterMutexLock lock(&mu_);
    SafeTruncateTable(0);
  }

 private:
  void SafeTruncateTable(size_t num_rows) {
    mu_.AssertHeld();
    TruncateIndex(num_rows);
    UpdateMeta();
    WriteProto(lock_path_, proto::Dummy());
    WriteMeta();
    // At this point the table is truncated, even if below fails.
    TruncateData(num_rows);
    GOOGLE_CHECK_EQ(index_.num_rows, num_rows);
    GOOGLE_CHECK(std::filesystem::remove(lock_path_));
  }

  // Truncates the tables on disk, ensure to first truncate and write index and create a mess lock.
  void TruncateData(size_t num_rows) {
    mu_.AssertHeld();
    for (const auto&[column_name, column_meta] : column_meta_) {
      if (column_meta.path.empty()) {
        // Not stored.
        continue;
      }
      size_t expected_size = num_rows * column_meta.row_size;
      if (expected_size == 0 && !std::filesystem::exists(column_meta.path)) {
        // Does not exist, satisfies the requirement.
        continue;
      }
      // The size is guaranteed to be at least expected size, we will never extend (unless there is a bug ;) ).
      // Truncate the file size to the one that is consistent with expectation, this will remove bad, partially
      // added data.
      std::filesystem::resize_file(column_meta.path, expected_size);
    }
  }

  // Truncates index_, does not write the updated index to disk.
  void TruncateIndex(size_t num_rows) {
    mu_.AssertHeld();
    if (num_rows == 0) {
      index_ = {};
      return;
    }
    index_.num_rows = num_rows;
    size_t truncate_to = 0;
    for (size_t pos : index_.pos) {
      if (pos > num_rows) {
        break;
      }
      ++truncate_to;
    }
    index_.pos.resize(truncate_to);
    index_.value.resize(truncate_to);
    RawColumnData last_time =
        ReadRawColumnSingle(column_meta_.at(table_meta_.schema().time_column()),
                            {num_rows - 1, num_rows});
    auto* last_time_arr = reinterpret_cast<int64_t*>(last_time.data);
    index_.last_ts = last_time_arr[0];
    delete[] last_time.data;
  }

  void RecoverMess() {
    TruncateData(index_.num_rows);
  }

  void InitMeta(const proto::SubTableId& sub_table_id) {
    std::ifstream in_meta_file(meta_path_, std::ifstream::in | std::ifstream::binary);
    if (ReadProtoSafe(meta_path_, &meta_)) {
      GOOGLE_CHECK_EQ(meta_.id().id(), sub_table_id.id()) << "Inconsistent table id. weird...";
      return;
    }
    // Initialize the table - meta file does not exist yet.
    *meta_.mutable_id() = sub_table_id;
    WriteMeta();
  }

  void InitIndex() noexcept {
    if (!meta_.has_index()) {
      // Default instance.
      index_ = {};
      return;
    }
    const auto& index = meta_.index();
    index_ = Index{
        .last_ts = index.last_ts(),
        .num_rows = index.num_rows(),
    };
    index_.value = {index.value().begin(), index.value().end()};
    index_.pos = {index.pos().begin(), index.pos().end()};
  }

  void UpdateMeta() noexcept {
    auto* index = meta_.mutable_index();
    index->set_num_rows(index_.num_rows);
    index->set_last_ts(index_.last_ts);
    *index->mutable_pos() = {index_.pos.begin(), index_.pos.end()};
    *index->mutable_value() = {index_.value.begin(), index_.value.end()};
  }

  void WriteMeta() {
    WriteProtoSafe(meta_path_, meta_);
  }

  void InitSubTable(const proto::SubTableId& sub_table_id) {
    GOOGLE_CHECK(MaybeCreateDir(sub_table_dir_))
            << "Failed to create sub table dir " << sub_table_dir_;
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

  std::string GetColumnPath(const std::string& name) {
    return absl::StrCat(sub_table_dir_, "/", name, ".bin");
  }

  void ExtractColumnMeta() {
    for (const auto& value_column : table_meta_.schema().value_column()) {
      column_meta_[value_column.name()] = ColumnMeta{
          .path=GetColumnPath(value_column.name()),
          .type=value_column.type(),
          .width=value_column.width(),
      };
    }

    for (const auto& tag_column : table_meta_.schema().tag_column()) {
      GOOGLE_CHECK(meta_.id().tag().count(tag_column) > 0)
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

  std::pair<Span, RawColumnData> QueryTimeSpan(const proto::TimeSelector& time_selector,
                                               bool return_time_column) const {
    const std::string& time_column_name = table_meta_.schema().time_column();
    if (time_selector.has_last_n()) {
      const Span selected_span =
          {index_.num_rows - std::min(static_cast<size_t>(time_selector.last_n()), index_.num_rows),
           index_.num_rows};
      return {
          selected_span,
          ReadRawColumnSingle(column_meta_.at(time_column_name), selected_span)
      };
    }

    Span coarse_span = QueryCoarseTimeSpanFromIndex(time_selector);

    if (coarse_span.first >= coarse_span.second) {
      return {kEmptySpan, {}};
    }
    RawColumnData
        raw_coarse_time = ReadRawColumnSingle(column_meta_.at(time_column_name), coarse_span);
    auto* coarse_time = reinterpret_cast<int64_t*>(raw_coarse_time.data);
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
      char* return_buffer = new char[num_return_bytes];
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
  Span QueryCoarseTimeSpanFromIndex(const proto::TimeSelector& time_selector) const {
    if (index_.num_rows == 0) {
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
      auto start_it =
          std::lower_bound(index_.value.begin(), index_.value.end(), time_selector.start());
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

  RawColumnData ReadRawColumnSingle(const ColumnMeta& column_meta, const Span& span) const {
    return ReadRawColumn(column_meta, {span});
  }

  RawColumnData ReadRawColumn(const ColumnMeta& column_meta, const std::vector<Span>& spans) const {
    size_t num_rows = 0;
    for (const auto& span : spans) {
      num_rows += span.second - span.first;
    }
    size_t num_bytes = num_rows * column_meta.row_size;
    char* buffer = new char[num_bytes];
    GOOGLE_CHECK_EQ(alignof(buffer) % 8, 0) << "Bad alignment!";
    if (!column_meta.path.empty()) {
      size_t buffer_pos = 0;
      std::ifstream f(column_meta.path, std::ifstream::in | std::ifstream::binary);
      GOOGLE_CHECK(f.is_open()) << "Could not open file for read at: " << column_meta.path;
      for (const auto& span : spans) {
        size_t start = column_meta.row_size * span.first;
        size_t to_read = column_meta.row_size * span.second - start;
        f.seekg(start);
        f.read((buffer + buffer_pos), to_read);
        buffer_pos += to_read;
      }
      GOOGLE_CHECK(f.good())
              << "Something went wrong when reading: " << column_meta.path << " EOF: " << f.eof();
    } else {
      GOOGLE_CHECK_NE(column_meta.tag_str_ref, kInvalidStrRef)
              << "Columns without a path must be tag columns with a valid str_ref.";
      GOOGLE_CHECK_EQ(column_meta.width, 1) << "tag columns must be of width 1.";
      auto* refs = reinterpret_cast<StrRef*>(buffer);
      for (size_t i = 0; i < num_rows; ++i) {
        refs[i] = column_meta.tag_str_ref;
      }
    }

    return {
        .data=buffer,
        .size=num_bytes,
    };
  }

  void AppendRawColumnData(const ColumnMeta& column_meta, const RawColumnData& raw_column_data) {
    std::ofstream
        f(column_meta.path, std::ofstream::out | std::ofstream::binary | std::ofstream::app);
    GOOGLE_CHECK(f.is_open()) << "Could not open file for write at: " << column_meta.path;
    if (raw_column_data.size > 0) {
      f.write(raw_column_data.data, raw_column_data.size);
    }
    GOOGLE_CHECK(f.good()) << "Something went wrong when writing: " << column_meta.path;
  }

  mutable absl::Mutex mu_;

  proto::SubTable meta_;
  const proto::Table& table_meta_;

  const std::string sub_table_dir_;
  const std::string meta_path_;
  const std::string lock_path_;

  absl::flat_hash_map<std::string, ColumnMeta> column_meta_;

  Index index_;

};

class Table {
 public:
  explicit Table(std::string table_root_dir) : Table(table_root_dir, {}) {};

  Table(std::string table_root_dir, const std::optional<proto::Table>& table_meta)
      : table_root_dir_(std::move(table_root_dir)),
        meta_path_(absl::StrCat(table_root_dir_, "/", "META.pb")),
        str_ref_path_(absl::StrCat(table_root_dir_, "/", "STR_REF.pb")) {
    MaybeCreateDir(table_root_dir_);
    sub_tables_.reserve(meta_.sub_table_id_size());
    if (table_meta) {
      meta_ = *table_meta;
      bool table_exists = ReadProtoSafe(meta_path_, &meta_);
      if (table_exists) {
        if (table_meta->schema().SerializeAsString() != GetMeta().schema().SerializeAsString()) {
          throw std::invalid_argument(absl::StrFormat(
              "Table at %s already exists, but the provided table schema is not consistent: \nexisting: '%s'\nprovided: '%s'",
              table_root_dir_,
              GetMeta().schema().DebugString(),
              table_meta->schema().DebugString()));
        }

      } else {
        WriteProtoSafe(meta_path_, meta_);
      }
    } else {
      if (!ReadProtoSafe(meta_path_, &meta_)) {
        throw std::invalid_argument(absl::StrFormat(
            "Table at %s does not exist yet, you need to provide the metadata.",
            meta_path_));
      }
    }

    LoadStringRefs();
    for (const auto& sub_table_id : meta_.sub_table_id()) {
      MountSubTable(sub_table_id);
    }
    non_tag_columns_ = {meta_.schema().time_column()};
    for (const auto& value_column : meta_.schema().value_column()) {
      non_tag_columns_.push_back(value_column.name());
    }
    for (size_t i = 0; i < meta_.schema().tag_column_size(); ++i) {
      tag_column_to_order_[meta_.schema().tag_column(i)] = i;
    }
  }

  const proto::Table& GetMeta() {
    return meta_;
  }

  std::optional<RawColumns> Query(const proto::Selector& selector) {
    std::vector<SubTable*> sub_tables = GetSelectedSubTables(selector.sub_table_selector());
    if (sub_tables.empty()) {
      return {};
    }
    absl::flat_hash_map<std::string, std::vector<RawColumnData>> sub_results;
    size_t result_size = 0;
    for (SubTable* sub_table : sub_tables) {
      if (result_size > (1UL << 30UL)) {
        // Bail out if the result is over 1GB, bad query? TODO: Improve error handling in general.
//        spdlog::error("Query result too large, returning first 1GB...");
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

  SubTable* GetOrCreateSubTable(std::vector<StrRef> sub_table_selector) {
    absl::WriterMutexLock lock(&mu_subtable_);
    GOOGLE_CHECK_EQ(sub_table_selector.size(), meta_.schema().tag_column_size())
            << "Bad selector, must be equal to number of tag columns in the same order."
            << sub_table_selector.size();
    const std::vector<const std::string*> resolved_selector = ResolveStringRefs(sub_table_selector);
    GOOGLE_CHECK_EQ(resolved_selector.size(), sub_table_selector.size());
    absl::flat_hash_map<std::string, std::string> str_tags;
    for (int i = 0; i < sub_table_selector.size(); ++i) {
      GOOGLE_CHECK(resolved_selector[i])
              << "Reference to unknown string (did you forget to mint it?): "
              << sub_table_selector[i];
      str_tags[meta_.schema().tag_column(i)] = *resolved_selector[i];
    }
    const proto::SubTableId id = MakeSubTableId(str_tags);
    const auto index_entry = GetUniqueIndexEntry(id);
    if (!sub_table_unique_index_.contains(index_entry)) {
      // Lock is held so this is safe.
      *meta_.add_sub_table_id() = id;
      MountSubTable(id);
      GOOGLE_CHECK(sub_table_unique_index_.contains(index_entry));
      WriteProtoSafe(meta_path_, meta_);
    }
    return sub_table_unique_index_[index_entry];
  }

  void AppendData(const RawColumns& column_data, AppendDataMode append_data_mode = kAppend) {
    // Thread safe, sub tables handle the thread safety independently.
    const size_t total_rows = column_data.at(meta_.schema().time_column()).size / sizeof(int64_t);
    if (!total_rows) {
      return;
    }
    std::vector<const StrRef*> tag_cols;
    std::vector<StrRef> last_sub_table_selector;
    size_t last_start = 0;
    auto append_span =
        [this, append_data_mode, &column_data](const std::vector<StrRef>& selector,
                                               const Span& row_span) {
          SubTable* sub_table = this->GetOrCreateSubTable(selector);
          GOOGLE_CHECK(sub_table);
          this->AppendRowSpanToSubTable(column_data, row_span, append_data_mode, sub_table);
        };

    for (const auto& tag_column_name : meta_.schema().tag_column()) {
      const auto& col_data = column_data.at(tag_column_name);
      GOOGLE_CHECK_EQ(col_data.size, sizeof(StrRef) * total_rows)
              << "Unexpected number of rows for tag column.";
      tag_cols.push_back(reinterpret_cast<StrRef*>(col_data.data));
      last_sub_table_selector.push_back(tag_cols.back()[0]);
    }
    GOOGLE_CHECK_EQ(last_sub_table_selector.size(), tag_cols.size());
    std::vector<StrRef> sub_table_selector(tag_cols.size(), kInvalidStrRef);
    for (size_t row = 0; row < total_rows; ++row) {
      for (size_t tag_column_id = 0; tag_column_id < tag_cols.size(); ++tag_column_id) {
        sub_table_selector[tag_column_id] = tag_cols[tag_column_id][row];
      }
      if (sub_table_selector != last_sub_table_selector) {
        append_span(last_sub_table_selector, {last_start, row});
        last_sub_table_selector = sub_table_selector;
        last_start = row;
      }
    }
    if (last_start < total_rows) {
      append_span(last_sub_table_selector, {last_start, total_rows});
    }
  }

  void AppendRowSpanToSubTable(const RawColumns& column_data,
                               const Span& row_span,
                               AppendDataMode append_data_mode,
                               SubTable* sub_table) {
    RawColumns sub_column_data;
    const size_t total_rows = column_data.at(meta_.schema().time_column()).size / sizeof(int64_t);
    for (const auto& column_name : non_tag_columns_) {
      const RawColumnData& full_column = column_data.at(column_name);
      GOOGLE_CHECK_EQ(full_column.size % total_rows, 0);
      size_t item_size = full_column.size / total_rows;
      GOOGLE_CHECK_EQ(item_size % 4, 0) << absl::StrFormat(
          "Bad element size (%d) column %s, columns size %d, total_rows %d",
          item_size,
          column_name,
          full_column.size,
          total_rows);
      sub_column_data[column_name] = {
          .data = full_column.data + row_span.first * item_size,
          .size = (row_span.second - row_span.first) * item_size,
      };
    }
    sub_table->AppendData(sub_column_data, append_data_mode);
  }

  static RawColumnData MergeColumnSubResults(const std::vector<RawColumnData>& column_sub_results) {
    size_t merged_byte_size = 0;
    for (const auto& column_sub_result : column_sub_results) {
      merged_byte_size += column_sub_result.size;
    }
    GOOGLE_CHECK_GE(merged_byte_size, 0) << "No results to merge.";
    char* buffer = new char[merged_byte_size];
    size_t buffer_pos = 0;
    for (const auto& column_sub_result : column_sub_results) {
      std::memcpy(buffer + buffer_pos, column_sub_result.data, column_sub_result.size);
      buffer_pos += column_sub_result.size;
      delete[] column_sub_result.data;
    }
    return {
        .data = buffer,
        .size = merged_byte_size,
    };
  }

  void LoadStringRefs() {
    proto::StringRefMap str_refs;
    if (!ReadProtoSafe(str_ref_path_, &str_refs)) {
      return;
    }
    GOOGLE_CHECK(string_ref_map_.empty()) << "Load string refs once at the init.";
    for (const auto& pair : str_refs.mapping()) {
      string_ref_map_[pair.first] = pair.second;
      inv_string_ref_map_[pair.second] = pair.first;
    }
  }

  void DumpStringRefs() {
    proto::StringRefMap str_refs;
    for (const auto& pair: string_ref_map_) {
      (*str_refs.mutable_mapping())[pair.first] = pair.second;
    }
    WriteProtoSafe(str_ref_path_, str_refs);
  }

  std::vector<StrRef> MintStringRefs(const std::vector<std::string>& strings) {
    std::vector<StrRef> result;
    bool did_mint = false;
    for (const auto& str : strings) {
      if (inv_string_ref_map_.contains(str)) {
        result.push_back(inv_string_ref_map_[str]);
      } else {
        if (!did_mint) {
          mu_mint_.WriterLock();
        }
        did_mint = true;
        if (inv_string_ref_map_.contains(str)) {
          continue;
        }
        StrRef mint = string_ref_map_.size() + 1;
        result.push_back(mint);
        inv_string_ref_map_[str] = mint;
        string_ref_map_[mint] = str;
      }
    }
    if (did_mint) {
      // VERY INEFFICIENT! Todo: append new references instead
      DumpStringRefs();
      mu_mint_.WriterUnlock();
    }
    return result;
  }

  std::vector<const std::string*> ResolveStringRefs(const std::vector<StrRef>& refs) const {
    absl::ReaderMutexLock lock(&mu_mint_);
    std::vector<const std::string*> result;
    for (const StrRef ref : refs) {
      if (string_ref_map_.contains(ref)) {
        result.push_back(&string_ref_map_.at(ref));
      } else {
        result.push_back(nullptr);
      }
    }
    return result;
  }

  proto::SubTableId MakeSubTableId(const absl::flat_hash_map<std::string, std::string>& str_tags) {
    proto::SubTableId sub_id;
    std::string id = "sub";
    for (const auto& tag_column : meta_.schema().tag_column()) {
      absl::StrAppend(&id, ",", tag_column, "=", str_tags.at(tag_column));
      (*sub_id.mutable_tag())[tag_column] = MintStringRefs({str_tags.at(tag_column)}).at(0);
      sub_id.add_str_tag(str_tags.at(tag_column));
    }
    sub_id.set_id(id);
    return sub_id;
  }

 private:
  friend class TableWrap;

  void MountSubTable(const proto::SubTableId& id) {
    sub_tables_.push_back(absl::make_unique<SubTable>(table_root_dir_, meta_, id));
    IndexSubTable(sub_tables_.back().get());
  }

  absl::flat_hash_map<std::string, StrRef> ConvertStrTags(const absl::flat_hash_map<std::string,
                                                                                    std::string>& str_tags) {
    absl::flat_hash_map<std::string, StrRef> result;
    // Cheap enough to do it stupidly rather than in batch.
    for (const auto&[tag, value] : str_tags) {
      result[tag] = MintStringRefs({tag}).at(0);
    }
    return result;
  }

  std::string GetUniqueIndexEntry(const proto::SubTableId& sub_table_id) {
    std::vector<StrRef> tag_str_refs;
    for (const auto& tag_column : meta_.schema().tag_column()) {
      tag_str_refs.push_back(sub_table_id.tag().at(tag_column));
    }
    return GetUniqueIndexEntry(tag_str_refs);
  }

  static std::string GetUniqueIndexEntry(const std::vector<StrRef>& tag_str_refs) {
    std::string entry;
    for (StrRef str_ref : tag_str_refs) {
      GOOGLE_CHECK_NE(str_ref, kInvalidStrRef);
      absl::StrAppend(&entry, ",", str_ref);
    }
    return entry;
  }

  std::vector<SubTable*> GetSelectedSubTables(const proto::SubTableSelector& selector) {
    // We cant not be selecting while subtables are being written.
    absl::ReaderMutexLock lock(&mu_subtable_);
    // This can be improved by taking the first tag selection as the initial selection.
    absl::flat_hash_set<SubTable*> selection;
    for (auto& sub_table : sub_tables_) {
      selection.insert(sub_table.get());
    }
    for (const auto& tag_selector : selector.tag_selector()) {
      absl::flat_hash_set<SubTable*> new_selection;
      for (const auto& tag_value : tag_selector.value()) {
        std::pair<std::string, std::string> key = {tag_selector.name(), tag_value};
        if (!sub_table_index_.contains(key)) {
          continue;
        }
        for (SubTable* sub_table : sub_table_index_[key]) {
          if (selection.contains(sub_table)) {
            new_selection.insert(sub_table);
          }
        }
      }
      selection = new_selection;
    }
    std::vector<SubTable*> result(selection.begin(), selection.end());

    if (!selector.tag_order().empty()) {
      std::vector<size_t> order;
      for (const auto& order_tag : selector.tag_order()) {
        order.push_back(tag_column_to_order_.at(order_tag));
      }
      auto cmp = [&order](const SubTable* a, const SubTable* b) {
        for (size_t tag_col : order) {
          const std::string a_s = a->GetId().str_tag(tag_col);
          const std::string b_s = b->GetId().str_tag(tag_col);
          if (a_s == b_s) {
            continue;
          }
          return a_s < b_s;
        }
        // Elements equal.
        return false;
      };
      std::sort(result.begin(), result.end(), cmp);
    }
    return result;
  }

  void IndexSubTable(SubTable* sub_table) {
//    spdlog::debug("Indexing table {}", sub_table->GetId().id());
    const std::string entry = GetUniqueIndexEntry(sub_table->GetId());
    GOOGLE_CHECK(!sub_table_unique_index_.contains(entry))
            << "Indexed table already exists: " << sub_table->GetId().id();
    sub_table_unique_index_[entry] = sub_table;

    for (int i = 0; i < meta_.schema().tag_column_size(); ++i) {
      sub_table_index_[{meta_.schema().tag_column(i), sub_table->GetId().str_tag(i)}]
          .push_back(sub_table);
    }
  }

  mutable absl::Mutex mu_subtable_;
  mutable absl::Mutex mu_mint_;

  const std::string table_root_dir_;
  const std::string meta_path_;
  const std::string str_ref_path_;
  proto::Table meta_;

  std::vector<std::unique_ptr<SubTable>> sub_tables_;
  // Pointer stability to string value required.
  absl::node_hash_map<StrRef, std::string> string_ref_map_;
  absl::flat_hash_map<std::string, StrRef> inv_string_ref_map_;
  std::vector<std::string> non_tag_columns_;
  absl::flat_hash_map<std::string, size_t> tag_column_to_order_;

  // Index by {tag_column_name, tag_value}  -> sub tables .
  absl::flat_hash_map<std::pair<std::string, std::string>, std::vector<SubTable*>> sub_table_index_;
  // Index by uint32 comma separated tags (in order of tag columns)  -> sub table
  // A bit stupid, but only relevant for writes.
  absl::flat_hash_map<std::string, SubTable*> sub_table_unique_index_;

//  std::vector<SubTable> sub_tables_;

};

} // namespace pytdb


