import sys
import os
from typing import Optional, Dict, List
import pandas as pd
import numpy as np

# The wrappers over cc library are a separate module, compiled separately.
import pytdb_cc
# The pb should be also compiled separately...
from . import table_pb2
import time

DT_STR_REF = np.dtype("uint32")

AppendDataMode = pytdb_cc.AppendDataMode

class Table:
    def __init__(self, db_root_dir, table_name: str, time_column: str = None,
                 tag_columns: [str] = None, float_value_columns: [str] = None):
        self.table_path = os.path.join(db_root_dir, table_name)
        if time_column is not None:
            self.config = table_pb2.Table(
                name=table_name,
                schema=table_pb2.Schema(
                    time_column=time_column,
                    tag_column=tag_columns,
                    value_column=[
                        table_pb2.ColumnSchema(name=float_value_column,
                                               type=table_pb2.ColumnSchema.FLOAT) for
                        float_value_column in float_value_columns
                    ],
                ))
            self.columns = self._get_columns_from_schema(self.config.schema)
        else:
            self.config = None

        self.table = pytdb_cc.Table(self.table_path,
                                  self.config.SerializeToString() if self.config is not None else None)
        self._update_config()
        self.columns = self._get_columns_from_schema(self.config.schema)
        self.columns_set = set(self._get_columns_from_schema(self.config.schema))
        self.tag_columns = set(self.config.schema.tag_column)
        self.time_column = self.config.schema.time_column
        self.datetime_dtype = pd.to_datetime("2011-11-11").to_datetime64().dtype # datetime64[ns] aka M8[ns]

    @staticmethod
    def _get_columns_from_schema(schema: table_pb2.Schema):
        columns = [schema.time_column] + list(schema.tag_column) + list(
            value_column.name for value_column in schema.value_column)
        expected_count = 1 + len(schema.tag_column) + len(schema.value_column)
        if len(set(columns)) != expected_count:
            raise ValueError("Column names must be unique in a single table.")
        return columns

    def _to_ns64(self, time_arg):
        if isinstance(time_arg, int):
            return time_arg
        return pd.to_datetime(time_arg).to_datetime64().astype(np.int64)

    def _ns64_to_time(self, ns_int64_arr: np.array):
        return ns_int64_arr.astype(self.datetime_dtype)

    def _update_config(self):
        if self.config is None:
            self.config = table_pb2.Table()
        self.config.ParseFromString(self.table.get_meta_wire())

    def _make_query_selector(self, time_start, time_end, include_start, include_end, last_n,
                             columns, tag_column_order, **tag_selectors):
        selector = table_pb2.Selector(column=columns if columns is not None else self.columns,
                                      time_selector=table_pb2.TimeSelector(start=self._to_ns64(time_start) if time_start else None,
                                                                           end=self._to_ns64(time_end) if time_end else None,
                                                                           include_start=include_start,
                                                                           include_end=include_end,
                                                                           last_n=last_n),
                                      )
        sub_selector = selector.sub_table_selector
        if tag_column_order is not None:
            sub_selector.tag_order.extend(
                [tag_column_order] if isinstance(tag_column_order, str) else tag_column_order)
            for order_by in sub_selector.tag_order:
                if order_by not in self.tag_columns:
                    raise ValueError("Column %s provided in tag_column_order is not a tag column.")
        else:
            # Order by tag columns by default, for result consistency.
            sub_selector.tag_order.extend(self.config.schema.tag_column)
        for tag, value in tag_selectors.items():
            if tag not in self.tag_columns:
                raise ValueError("Column %s provided in tag_selectors is not a tag column.")
            sub_selector.tag_selector.append(table_pb2.TagSelector(name=tag, value=[
                value] if isinstance(value, str) else value))
        return selector

    def query(self, time_start=None, time_end=None, include_start=True, include_end=False, last_n=None,
                 columns: Optional[List[str]] = None, resolve_strings=True,
                 tag_column_order: Optional[List[str]] = None,
                 **tag_selectors) -> Dict[str, np.ndarray]:
        selector = self._make_query_selector(time_start, time_end, include_start, include_end, last_n,
                             columns, tag_column_order, **tag_selectors)
        return self._query(selector, resolve_strings)

    def _query(self, selector: table_pb2.Selector, resolve_strings: bool):
        query_start = time.perf_counter()
        response = self.table.query(selector.SerializeToString())
        query_time = time.perf_counter() - query_start
        # print("Query time:", query_time)
        if resolve_strings:
            for tag_column in self.tag_columns:
                if tag_column not in response:
                    continue
                response[tag_column] = self._resolve_str_ref_column(response[tag_column])

        return response

    def _resolve_str_ref_column(self, arr: np.ndarray) -> np.ndarray:
        """Converts input array of str_ref dt(uint32) to array of resolved strings dt(object)."""
        if arr.dtype != DT_STR_REF:
            raise ValueError("Returned tag column does not have a str_ref type!")
        # Just like np.unique but 10x faster for a large number of dups, and comparable otherwise.
        unique_refs, inverse = pytdb_cc.fast_unique(arr)
        return np.array(self.table.resolve_str_refs(unique_refs, throw_if_not_found=True), dtype=np.dtype("object"))[inverse]

    def _encode_str_ref_column(self, arr: np.ndarray) -> np.ndarray:
        """Converts input array of str_ref dt(uint32) to array of resolved strings dt(object)."""
        if arr.dtype != np.dtype("object"):
            raise ValueError("Input string reference column must have object dtype!")
        unique_strs, inverse = np.unique(arr, return_inverse=True)
        return np.array(self.table.mint_str_refs([str(e) for e in unique_strs]), dtype=DT_STR_REF)[inverse]

    def query_df(self, time_start=None, time_end=None, include_start=True, include_end=False, last_n=None,
                 columns: Optional[List[str]] = None, resolve_strings=True,
                 tag_column_order: Optional[List[str]] = None,
                 **tag_selectors) -> pd.DataFrame:
        selector = self._make_query_selector(time_start, time_end, include_start, include_end, last_n,
                                             columns, tag_column_order, **tag_selectors)
        response = self._query(selector, resolve_strings)
        if not response:
            # Empty.
            return pd.DataFrame({}, columns=selector.column)
        if self.time_column in response:
            response[self.time_column] = self._ns64_to_time(response[self.time_column])
        return pd.DataFrame(response, columns=selector.column)

    def append_data_df(self, df: pd.DataFrame, sort=False, append_data_mode: AppendDataMode = AppendDataMode.Append):
        if set(df.columns) != self.columns_set:
            raise ValueError("Data frame to append needs to contain all the columns in the table and nothing else, expected %s, got %s" % (self.columns_set, set(df.columns)))
        if sort:
            df = df.sort_values(by=list(self.tag_columns) + [self.time_column])
        columns = {name: df[name].to_numpy() for name in self.columns}

        def maybe_convert_column(col, dtype):
            if col.dtype == dtype:
                return col
            return col.astype(dtype)
        converted_columns = {}
        for column_name, value in columns.items():
            if column_name in self.tag_columns:
                if value.dtype == np.dtype("object"):
                    converted_columns[column_name] = self._encode_str_ref_column(value)
                elif value.dtype.kind in ("u", "i"):
                    converted_columns[column_name] = maybe_convert_column(value, DT_STR_REF)
                else:
                    raise ValueError("Invalid dtype of tag column %s, expected either object or integer dtype" % column_name)
            elif column_name == self.time_column:
                converted_columns[column_name] = maybe_convert_column(value, np.dtype("int64"))
            else:
                # Value column.
                converted_columns[column_name] = maybe_convert_column(value, np.dtype("float32"))
        self.table.append_data(converted_columns, append_data_mode)
