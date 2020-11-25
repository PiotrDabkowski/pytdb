try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='pytdb',
    version='0.0.2',
    packages=['pytdb'],
    install_requires=[ "pytdb_cc>=0.0.5", "pandas", "numpy", "protobuf", "pytest"],
    author='Piotr Dabkowski',
    url='https://github.com/PiotrDabkowski/pytdb',
    description='Very fast and simple time series db, tightly integrated with Python and Pandas.'
)