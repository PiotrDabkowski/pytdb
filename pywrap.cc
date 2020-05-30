#include <pybind11/pybind11.h>

#include <string>

#include "proto/table.pb.h"
#include "table.h"


namespace py = pybind11;


int add(int i, int j) {
  return i + j;
}

std::string show() {
  pydb::proto::Table table;
  table.set_name("chuj");
  return table.DebugString();
}


PYBIND11_MODULE(pywrap, m) {
m.def("add", &add, R"pbdoc(
        Add two numbers
        Some other explanation about the add function.
    )pbdoc");

m.def("show", &show);

}