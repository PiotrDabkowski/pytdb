#include "table.h"

namespace pytdb {



bool MaybeCreateDir(const std::string& path) {
  const std::string abs_path = std::filesystem::absolute(path);
  if (!std::filesystem::exists(abs_path)) {
    return std::filesystem::create_directories(abs_path);
  }
  return true;
}

size_t GetTypeSize(proto::ColumnSchema::Type type) {
  switch (type) {
    case proto::ColumnSchema::FLOAT:
      return sizeof(float);
    case proto::ColumnSchema::DOUBLE:
      return sizeof(double);
    case proto::ColumnSchema::INT32:
      return sizeof(int32_t);
    case proto::ColumnSchema::INT64:
      return sizeof(int64_t);
    case proto::ColumnSchema::BYTE:
      return sizeof(char);
    case proto::ColumnSchema::STRING_REF:
      return sizeof(uint32_t);
    default:
      throw std::logic_error("unknown column type");
  }
}

// First greater element, if any, otherwise returns size.
size_t c_upper_bound(const int64_t* start, const int64_t* end, int64_t value) {
  const size_t diff = end - start;
  if (diff > 2) {
    const int64_t* mid = start + static_cast<size_t>(diff / 2);
    if (*mid > value) {
      // Mid still could be a result.
      return c_upper_bound(start, mid + 1, value);
    } else {
      // Mid is smaller or equal so it cannot be a result. The recursive fn will return the result from its point of reference so we need to offset.
      return (mid + 1 - start) + c_upper_bound(mid + 1, end, value);
    }
  }
  if (diff == 0) {
    return 0;
  }
  // Diff 1 or 2 at this point.
  if (*start > value) {
    return 0;
  }
  if (diff == 2 && *(start + 1) > value) {
    return 1;
  }
  return diff;
}

// First greater or equal element, if any, otherwise returns size.
size_t c_lower_bound(const int64_t* start, const int64_t* end, int64_t value) {
  const size_t diff = end - start;
  if (diff > 2) {
    const int64_t* mid = start + static_cast<size_t>(diff / 2);
    if (*mid >= value) {
      // Mid still could be a result.
      return c_lower_bound(start, mid + 1, value);
    } else {
      // Mid is smaller so it cannot be a result. The recursive fn will return the result from its point of reference so we need to offset.
      return (mid + 1 - start) + c_lower_bound(mid + 1, end, value);
    }
  }
  if (diff == 0) {
    return 0;
  }
  // Diff 1 or 2 at this point.
  if (*start >= value) {
    return 0;
  }
  if (diff == 2 && *(start + 1) >= value) {
    return 1;
  }
  return diff;
  return diff;
}

} // namespace pytdb