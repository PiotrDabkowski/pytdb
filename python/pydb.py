import sys
import os
import numpy as np
import time

import pywrap
import table_pb2
a = table_pb2.Table()
print(a.SerializeToString())
print(pywrap.add(10, 1))
print(pywrap.show())
print(pywrap.check("aaa"))
print(pywrap.check(None))

a.name = "chuj"
# table_pb2.Table
table = pywrap.Table("/tmp/xx", a.SerializeToString())

a = table_pb2.Table()
a.ParseFromString(table.get_meta_wire())

table.append_data({"kurde": 5})
#
# arr = np.arange(1000000).astype(np.int64)
# print(arr.dtype, arr.shape)
t = time.time()
for e in range(1000000):
    x = table.query()
    print("ok", x["t"].shape)
    # k = {"t": arr}
    # table.append_data2(k)
    # print("done")
print(time.time() - t)

