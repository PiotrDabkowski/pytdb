#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include <string>

#include "proto/table.pb.h"
#include "table.h"

namespace pydb {
namespace {

namespace py = pybind11;

int add(int i, int j) {
  return i + j;
}

std::string show() {
  pydb::proto::Table table;
  table.set_name("chuj");
  return table.DebugString();
}

template<typename T>
py::array_t<T> MakeNpArray(size_t size, T* data) {
  py::capsule free_when_done(data, [](void* f) {
    auto* foo = reinterpret_cast<T*>(f);
    delete[] foo;
  });
  std::vector<ssize_t> shape = {static_cast<ssize_t>(size)};
  std::vector<ssize_t> strides = {static_cast<ssize_t>(sizeof(T))};
  return py::array_t<T>(shape, strides, data, free_when_done);
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
    throw py::value_error(absl::StrFormat("Index type too small, can hold at most %d, but array is %d",
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
  return py::make_tuple(MakeNpArray(vals.size(), unique_elems_buf), MakeNpArray(size, inv));
}

int check(const std::optional<std::string>& x) {
  if (!x) {
    return -11;
  }
  return x->size();
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
  using PyColumns = std::unordered_map<std::string, py::array>;
  struct ColumnMeta {
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

  void AppendData(const PyColumns& columns) {
    if (columns.size() != column_meta_.size()) {
      throw py::value_error(absl::StrCat("All columns must be provided when appending data, expected %d columns, got %d",
                                         column_meta_.size(),
                                         columns.size()));
    }
    RawColumns raw_columns = ToRawColumns(columns);
    table_->AppendData(raw_columns);
  }

  std::vector<std::optional<std::string>> ResolveStrRefs(const std::vector<StrRef>& str_refs) const {
    auto resolved = table_->ResolveStringRefs(str_refs);
    std::vector<std::optional<std::string>> result;
    result.reserve(resolved.size());
    for (const auto* res : resolved) {
      if (res) {
        result.push_back(*res);
      } else {
        result.push_back({});
      }
    }
    return result;
  }

  std::vector<StrRef> MintStrRefs(const std::vector<std::string>& strings) {
    return table_->MintStringRefs(strings);
  }

 private:
  py::dtype GetDtype(const ColumnMeta& meta) {
    if (meta.width != 1) {
      throw py::value_error(absl::StrFormat("Column width must currently be 1, got %d", meta.width));
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
        throw py::value_error(absl::StrCat("Inconsistent number of rows between columns, seen both %d and %d",
                                           *num_rows,
                                           column.shape(0)));
      }
      if (!column_meta_.contains(name)) {
        throw py::value_error(absl::StrFormat("Column '%s' not in schema", name));
      }
      // Validate dtype.
      const auto& meta = column_meta_.at(name);
      if (column.dtype().kind() != meta.dtype.kind()) {
        throw py::value_error(absl::StrFormat("Unexpected dtype.kind for column '%s', expected '%c', got '%c'",
                                              name,
                                              meta.dtype.kind(),
                                              column.dtype().kind()));
      }
      if (column.dtype().itemsize() != meta.dtype.itemsize()) {
        throw py::value_error(absl::StrFormat("Unexpected dtype.itemsize for column '%s', expected %d, got %d",
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

    py::capsule free_when_done(raw_column_data->data, [](void* f) {
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

PYBIND11_MODULE(pywrap, m) {
  m.def("add", &add, R"pbdoc(
        Add two numbers
        Some other explanation about the add function.
    )pbdoc");

  m.def("show", &show);
  m.def("check", &check);
  m.def("fast_unique", &FastUnique<uint32_t, uint32_t>);

  py::class_<TableWrap>(m, "Table")
      .def(py::init<const std::string&, const std::optional<std::string>&>())
      .def("print_meta", &TableWrap::PrintMeta)
      .def("get_meta_wire", &TableWrap::GetMetaWire)
      .def("append_data", &TableWrap::AppendData)
      .def("resolve_str_refs", &TableWrap::ResolveStrRefs)
      .def("mint_str_refs", &TableWrap::MintStrRefs)
      .def("query", &TableWrap::Query);
}

}  // namespace
}  // namespace pydb