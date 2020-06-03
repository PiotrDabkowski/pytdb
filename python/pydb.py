import sys
import os
import numpy as np
import time
from typing import Optional, Dict, List
import pandas as pd
import numpy as np
sys.path.insert(0,
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cmake-build-release"))
print(sys.path[0])

import pywrap
from . import table_pb2
import time


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
            self.columns = self.get_columns_from_schema(self.config.schema)
        else:
            self.config = None

        self.table = pywrap.Table(self.table_path,
                                  self.config.SerializeToString() if self.config is not None else None)
        self._update_config()
        self.columns = self.get_columns_from_schema(self.config.schema)
        self.tag_columns = set(self.config.schema.tag_column)
        self.datetime_dtype = pd.to_datetime("2011-11-11").to_datetime64().dtype # datetime64[ns] aka M8[ns]

    @staticmethod
    def get_columns_from_schema(schema: table_pb2.Schema):
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

    def query_df(self, time_start=None, time_end=None, include_start=True, include_end=False,
                 columns: Optional[List[str]] = None, resolve_strings=True,
                 tag_column_order: Optional[List[str]] = None,
                 **tag_selectors) -> pd.DataFrame:
        selector = table_pb2.Selector(column=columns if columns is not None else self.columns,
                                      time_selector=table_pb2.TimeSelector(start=time_start,
                                                                           end=time_end,
                                                                           include_start=include_start,
                                                                           include_end=include_end),
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
        query_start = time.perf_counter()
        response = self.table.query(selector.SerializeToString())
        query_time = time.perf_counter() - query_start
        calc_time = time.perf_counter() - query_start - query_time
        print("Query time:", query_time)
        if not response:
            # Empty.
            return pd.DataFrame({}, columns=selector.column)
        return pd.DataFrame(response, columns=selector.column)

    def append_data_df(self, df: pd.DataFrame, copy_and_sort=False):
        pass



# df = table.query_df(columns=["t"])
# s = time.perf_counter()
# r = pd.Timestamp(2014)
# print(r.dtype)
# print(time.perf_counter() - s)
# # table.mint_str_refs(list(map(str, range(1000))))
# # for e in range(1, 100):
# #     try:
# #         table.append_data({
# #             "t": np.arange(100000, 1000000).astype(np.int64),
# #             "s": np.ones(900000).astype(np.uint32) * e,
# #             "c": np.arange(100000, 1000000).astype(np.float32) / 10,
# #         })
# #         print(e)
# #     except:
# #         print("Data already in.")
# #         break
# #
# # while 1:
# #     sel = table_pb2.Selector(column=["t", "s", "c"])
# #     sel.sub_table_selector.tag_selector.append(table_pb2.TagSelector(name="s", value=["1", "2"]))
# #     table.query(sel.SerializeToString())
