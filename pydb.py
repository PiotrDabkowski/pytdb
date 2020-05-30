import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "cmake-build-debug"))

import pywrap
import table_pb2
a = table_pb2.Table()
print(a)
print(pywrap.add(10, 1))
print(pywrap.show())
