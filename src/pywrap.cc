#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include <string>

#include "proto/table.pb.h"
#include "table.h"

namespace pytdb {
namespace {

namespace py = pybind11;

using PyColumns = std::unordered_map<std::string, py::array>;

std::string show() {
  proto::Table table;
  table.set_name("chuj");
  return table.DebugString();
}

template<typename T>
py::array_t<T> MakeNpArray(std::vector<ssize_t> shape, T* data) {

  std::vector<ssize_t> strides(shape.size());
  size_t v = sizeof(T);
  size_t i = shape.size();
  while (i--) {
    strides[i] = v;
    v *= shape[i];
  }
  py::capsule free_when_done(data, [](void* f) {
    auto* foo = reinterpret_cast<T*>(f);
    delete[] foo;
  });
  return py::array_t<T>(shape, strides, data, free_when_done);
}

template<typename T>
py::array_t<T> MakeEmptyNpArray(std::vector<ssize_t> shape) {
  size_t size = 1;
  for (auto s : shape) {
    size *= s;
  }
  T* data = new T[size];
  return ConstructNumpyArr(data, shape);
}

py::array_t<float> BundleTable(const PyColumns& table_data,
                               const PyColumns& valid_indicators,
                               const std::vector<std::string>& indicators,
                               const std::vector<std::string> features) {
  if (table_data.empty()) {
    throw py::value_error("table_data cannot be empty");
  }
  std::vector<ssize_t> shape;
  std::vector<const int64_t*> indicator_arrs;
  std::vector<absl::flat_hash_map<int64_t, size_t>> valid_indicator_mappings;
  const size_t num_rows = table_data.begin()->second.size();
  for (const auto& indicator : indicators) {
    const auto& valid_col = valid_indicators.at(indicator);
    GOOGLE_CHECK_EQ(valid_col.ndim(), 1) << "Indicator must be int64";
    GOOGLE_CHECK_EQ(valid_col.dtype().kind(), 'i') << "Indicator must be int64";
    GOOGLE_CHECK_EQ(valid_col.dtype().itemsize(), 8) << "Indicator must be int64";
    shape.push_back(valid_indicators.at(indicator).size());
    const auto* valids = reinterpret_cast<const int64_t*>(valid_col.data());
    absl::flat_hash_map<int64_t, size_t> mapping;
    for (size_t i = 0; i < valid_col.size(); ++i) {
      mapping[valids[i]] = i;
    }
    valid_indicator_mappings.push_back(std::move(mapping));

    const auto& indicator_col = table_data.at(indicator);
    GOOGLE_CHECK_EQ(indicator_col.ndim(), 1) << "Indicator must be int64";
    GOOGLE_CHECK_EQ(indicator_col.size(), num_rows) << "Inconsistent number of rows";
    GOOGLE_CHECK_EQ(indicator_col.dtype().kind(), 'i') << "Indicator must be int64";
    GOOGLE_CHECK_EQ(indicator_col.dtype().itemsize(), 8) << "Indicator must be int64";
    indicator_arrs.push_back(reinterpret_cast<const int64_t*>(indicator_col.data()));
  }

  shape.push_back(features.size());
  std::vector<const float*> feature_arrs;
  for (const auto& feature : features) {
    const auto& feature_col = table_data.at(feature);
    GOOGLE_CHECK_EQ(feature_col.ndim(), 1) << "Feature must be float32";
    GOOGLE_CHECK_EQ(feature_col.size(), num_rows) << "Inconsistent number of rows";
    GOOGLE_CHECK_EQ(feature_col.dtype().kind(), 'f') << "Feature must be float32";
    GOOGLE_CHECK_EQ(feature_col.dtype().itemsize(), 4) << "Feature must be float32";
    feature_arrs.push_back(reinterpret_cast<const float*>(feature_col.data()));
  }

  std::vector<size_t> strides(shape.size());
  size_t num_items;
  {
    size_t v = 1;
    size_t i = strides.size();
    while (i--) {
      strides[i] = v;
      v *= shape[i];
    }
    num_items = v;
  }
  float* data = new float[num_items];
  // Zero the result.
  std::memset(data, 0, sizeof(float) * num_items);

  for (size_t i = 0; i < num_rows; ++i) {
    bool is_valid = true;
    size_t position = 0;
    for (size_t ind = 0; ind < indicator_arrs.size(); ++ind) {
      const auto it = valid_indicator_mappings[ind].find(indicator_arrs[ind][i]);
      if (it == valid_indicator_mappings[ind].end()) {
        is_valid = false;
        break;
      }
      position += it->second * strides[ind];
    }
    if (!is_valid) {
      continue;
    }
    for (size_t feature_id = 0; feature_id < feature_arrs.size(); ++feature_id) {
      data[position + feature_id] = feature_arrs[feature_id][i];
    }
  }

  return MakeNpArray(shape, data);
}

template<typename T, typename IDX_T = uint64_t>
py::tuple FastUnique(const py::array_t<T>& arr) {
  if (arr.ndim() != 1) {
    throw py::value_error("Array must have exactly 1 dimension.");
  }
  if (arr.shape(0) <= 0) {
    return py::make_tuple(py::array_t<T>(), py::array_t<T>());
  }
  if (std::numeric_limits<IDX_T>::max() <= arr.shape(0)) {
    throw py::value_error(absl::StrFormat(
        "Index type too small, can hold at most %d, but array is %d",
        std::numeric_limits<IDX_T>::max(),
        arr.shape(0)));
  }
  IDX_T size = arr.shape(0);
  IDX_T hint = std::min(size / 100, static_cast<IDX_T>(1000));

  absl::flat_hash_map<T, IDX_T> mapping;
  mapping.reserve(hint);
  IDX_T next = 0;

  const T* data = arr.data();
  auto* inv = new IDX_T[size];
  std::vector<T> vals;

  for (IDX_T i = 0; i < size; ++i) {
    auto it = mapping.find(data[i]);

    if (it == mapping.end()) {
      mapping[data[i]] = next;
      inv[i] = next;
      vals.push_back(data[i]);
      ++next;
    } else {
      inv[i] = it->second;
    }
  }

  auto* unique_elems_buf = new T[vals.size()];
  std::memcpy(unique_elems_buf, vals.data(), sizeof(T) * vals.size());

  std::vector<ssize_t> vals_shape = {static_cast<ssize_t>(vals.size())};
  std::vector<ssize_t> inv_shape = {static_cast<ssize_t>(size)};
  return py::make_tuple(MakeNpArray(vals_shape, unique_elems_buf), MakeNpArray(inv_shape, inv));
}

template<typename T>
std::optional<T> DeserializeProto(const std::optional<py::bytes>& wire) {
  if (!wire) {
    return {};
  }
  T result;
  if (!result.ParseFromString(*wire)) {
    throw py::value_error("Invalid proto wire.");
  }
  return result;
}

class TableWrap {
 public:
  // Shares the underlying data buffers, zero copy between cpp and py.

  struct ColumnMeta {
    std::string name;
    proto::ColumnSchema::Type type;
    size_t width;
    size_t type_size;
    size_t row_size;
    py::dtype dtype;
  };

  TableWrap(const std::string& root_dir, const std::optional<py::bytes>& table_proto_wire) {
    auto table_proto = DeserializeProto<proto::Table>(table_proto_wire);
    if (table_proto) {
      // Do this first because there might be some issues, construction also validates the meta.
      ConstructTableMeta(*table_proto);
      column_meta_.clear();
    }
    table_ = std::make_unique<Table>(root_dir, table_proto);
    ConstructTableMeta(table_->GetMeta());

  }

  void ConstructTableMeta(const proto::Table& meta) {
    for (const auto& value_column : meta.schema().value_column()) {
      column_meta_[value_column.name()] = ColumnMeta{
          .type=value_column.type(),
          .width=value_column.width(),
      };
    }

    for (const auto& tag_column : meta.schema().tag_column()) {
      column_meta_[tag_column] = ColumnMeta{
          .type=proto::ColumnSchema::STRING_REF,
          .width=1,
      };
    }
    column_meta_[meta.schema().time_column()] = ColumnMeta{
        .type=proto::ColumnSchema::INT64,
        .width=1,
    };

    for (auto&[name, column_meta] : column_meta_) {
      column_meta.name = name;
      column_meta.type_size = GetTypeSize(column_meta.type);
      column_meta.row_size = column_meta.type_size * column_meta.width;
      column_meta.dtype = GetDtype(column_meta);
      if (column_meta.dtype.itemsize() != column_meta.row_size) {
        throw std::runtime_error(absl::StrFormat("Unexpected dtype size for column %s", name));
      }
    }
  }

  void PrintMeta() const {
    py::print(table_->GetMeta().DebugString());
  }

  py::bytes GetMetaWire() const {
    return table_->GetMeta().SerializeAsString();
  }

  PyColumns Query(const std::optional<py::bytes>& wire) {
    auto sel = DeserializeProto<proto::Selector>(wire);
    if (!sel) {
      return {};
    }
    auto res = table_->Query(*sel);
    if (!res) {
      return {};
    }
    return FromRawColumns(*res);
  }

  void AppendData(const PyColumns& columns, AppendDataMode append_data_mode = kAppend) {
    if (columns.size() != column_meta_.size()) {
      throw py::value_error(absl::StrFormat(
          "All columns must be provided when appending data, expected %d columns, got %d",
          column_meta_.size(),
          columns.size()));
    }
    RawColumns raw_columns = ToRawColumns(columns);
    table_->AppendData(raw_columns, append_data_mode);
  }

  std::vector<std::optional<std::string>> ResolveStrRefs(const std::vector<StrRef>& str_refs,
                                                         bool throw_if_not_found) const {
    auto resolved = table_->ResolveStringRefs(str_refs);
    std::vector<std::optional<std::string>> result;
    result.reserve(resolved.size());
    size_t i = 0;
    for (const auto* res : resolved) {
      if (res) {
        result.push_back(*res);
      } else {
        if (throw_if_not_found) {
          throw py::value_error(absl::StrFormat(
              "Could not resolve str ref: %d (did you remember to mint it?)",
              str_refs.at(i)));
        }
        result.push_back({});
      }
      ++i;
    }
    return result;
  }

  std::vector<StrRef> MintStrRefs(const std::vector<std::string>& strings) {
    return table_->MintStringRefs(strings);
  }

 private:
  py::dtype GetDtype(const ColumnMeta& meta) {
    if (meta.width != 1) {
      throw py::value_error(absl::StrFormat("Column width must currently be 1, got %d",
                                            meta.width));
    }
    switch (meta.type) {
      case proto::ColumnSchema::FLOAT:return py::dtype("float32");
      case proto::ColumnSchema::DOUBLE:return py::dtype("double");
      case proto::ColumnSchema::INT32:return py::dtype("int32");
      case proto::ColumnSchema::INT64:return py::dtype("int64");
      case proto::ColumnSchema::BYTE:return py::dtype("byte");
      case proto::ColumnSchema::STRING_REF:return py::dtype("uint32");
      default:throw py::value_error("unknown column type");
    }
  }

  // PyColumns keep ownership.
  RawColumns ToRawColumns(const PyColumns& columns) const {
    RawColumns result;
    std::optional<ssize_t> num_rows;
    for (auto&[name, column] : columns) {
      if (column.ndim() != 1) {
        throw py::value_error("Multi dimensional are not supported (yet).");
      }
      if (!num_rows) {
        num_rows = column.shape(0);
      } else if (*num_rows != column.shape(0)) {
        throw py::value_error(absl::StrFormat(
            "Inconsistent number of rows between columns, seen both %d and %d",
            *num_rows,
            column.shape(0)));
      }
      if (!column_meta_.contains(name)) {
        throw py::value_error(absl::StrFormat("Column '%s' not in schema", name));
      }
      // Validate dtype.
      const auto& meta = column_meta_.at(name);
      if (column.dtype().kind() != meta.dtype.kind()) {
        throw py::value_error(absl::StrFormat(
            "Unexpected dtype.kind for column '%s', expected '%c', got '%c'",
            name,
            meta.dtype.kind(),
            column.dtype().kind()));
      }
      if (column.dtype().itemsize() != meta.dtype.itemsize()) {
        throw py::value_error(absl::StrFormat(
            "Unexpected dtype.itemsize for column '%s', expected %d, got %d",
            name,
            meta.dtype.itemsize(),
            column.dtype().itemsize()));
      }
      if (column.nbytes() != (*num_rows) * column.dtype().itemsize()) {
        throw std::runtime_error(absl::StrFormat("Unexpected data size for column %s", name));
      }
      result[name] = RawColumnData{
          // data is not touched anyway.
          .data = const_cast<char*>(reinterpret_cast<const char*>(column.data())),
          .size = static_cast<size_t>(column.nbytes()),
      };
    }
    return result;
  }
  // PyColumns take ownership.
  PyColumns FromRawColumns(RawColumns& columns) const {
    PyColumns result;
    for (auto&[name, column] : columns) {
      if (!column_meta_.contains(name)) {
        throw py::value_error(absl::StrFormat("Unexpected. Column '%s' not in schema", name));
      }
      result[name] = ConstructNumpyArr(column_meta_.at(name), &column);
    }
    return result;
  }

  py::array ConstructNumpyArr(const ColumnMeta& column_meta, RawColumnData* raw_column_data) const {
    if (raw_column_data->size % column_meta.row_size != 0) {
      throw std::runtime_error("Unexpected column result size.");
    }
    ssize_t num_rows = raw_column_data->size / column_meta.row_size;

    std::vector<ssize_t> strides = {column_meta.dtype.itemsize()};
    std::vector<ssize_t> shape = {num_rows};
//    spdlog::info("Constructing column");
    py::capsule free_when_done(raw_column_data->data, [](void* f) {
//      spdlog::info("Freeing column");
      char* foo = reinterpret_cast<char*>(f);
      delete[] foo;
    });
    return py::array(
        column_meta.dtype,
        shape,
        strides,
        raw_column_data->data,
        free_when_done);
  }

  std::unique_ptr<Table> table_;
  absl::flat_hash_map<std::string, ColumnMeta> column_meta_;

};

PYBIND11_MODULE(pytdb_cc, m) {
  m.def("show", &show);
  // Just like np.unique but 10x faster for a large number of dups, and comparable otherwise.
  m.def("fast_unique", &FastUnique<uint32_t, uint32_t>);
  m.def("bundle_table", &BundleTable, py::arg("table_data"), py::arg("valid_indicators"), py::arg("indicators"), py::arg("features"));

  py::enum_<AppendDataMode>(m, "AppendDataMode")
      .value("Append", kAppend)
      .value("TruncateExisting", kTruncateExisting)
      .value("TruncateExistingOverlap", kTruncateExistingOverlap)
      .value("SkipOverlap", kSkipOverlap)
      .export_values();

  py::class_<TableWrap>(m, "Table")
      .def(py::init<const std::string&, const std::optional<std::string>&>())
      .def("print_meta", &TableWrap::PrintMeta)
      .def("get_meta_wire", &TableWrap::GetMetaWire)
      .def("append_data",
           &TableWrap::AppendData,
           py::arg("columns"),
           py::arg("append_data_mode") = kAppend)
      .def("resolve_str_refs",
           &TableWrap::ResolveStrRefs,
           py::arg("str_refs"),
           py::arg("throw_if_not_found") = false)
      .def("mint_str_refs", &TableWrap::MintStrRefs)
      .def("query", &TableWrap::Query);

}

}  // namespace
}  // namespace pytdb