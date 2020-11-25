try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='pytdb',
    version='0.0.1',
    packages=['pytdb'],
    install_requires=[ "pytdb_cc>=0.0.1"],
    author='Piotr Dabkowski',
    author_email='piodrus@gmail.com',
    description='Very fast and simple time series db, tightly integrated with Python and Pandas.')