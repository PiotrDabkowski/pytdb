#!/bin/bash

PLATFORM="manylinux2014_x86_64"

if [ "$1" != "mainrun" ]; then
  if [ ! -f "build_manylinux_wheels.sh" ]; then
    echo "FAILED: build_manylinux_wheels.sh does not exist, run script from the file directory.."
    exit 1
  fi
  echo "Executing the script in Docker container..."
  docker run -it -v $PWD:/parentfs quay.io/pypa/$PLATFORM /bin/bash /parentfs/build_manylinux_wheels.sh mainrun || ! echo "Failed, could not run Docker!" || exit 1
  echo "Success! Find wheels in wheelhouse folder."
  exit
fi
rm -f -R /parentfs/wheelhouse/*
mkdir -p /parentfs/wheelhouse
# --------------------------------------------------------------------------------------------------
# Main wheel build script, performed on $PLATFORM OS.
yum install autoconf automake libtool curl make cmake gcc-c++ unzip wget -y

# Install protobuf.
#yum install protobuf-compiler -y
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.14.0/protobuf-cpp-3.14.0.tar.gz
tar -zxvf protobuf-cpp-3.14.0.tar.gz
cd protobuf-3.14.0 || exit 1
./autogen.sh
./configure
make -j 4
make install
ldconfig
cd ..

# All deps must be installed at this point.

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLATFORM" -w wheelhouse_fixed/
    fi
}


# Compile wheels
for PYBIN in /opt/python/*/bin; do
  echo "Building wheel for: $PYBIN..."
  "${PYBIN}/pip" wheel /parentfs --no-deps -w wheelhouse/ || echo "Failed building wheel for $PYBIN."
done

# Bundle external shared libraries into the wheels
for whl in wheelhouse/*.whl; do
    repair_wheel "$whl"
done
# Copy the resulting wheels into the parent directory.
cp wheelhouse_fixed/* /parentfs/wheelhouse/
# Done.