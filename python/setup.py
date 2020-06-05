try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='pydb',
    version='0.0.1',
    packages=['pydb'],
    install_requires=[],
    author='Piotr Dabkowski',
    author_email='piodrus@gmail.com',
    description='Very fast and simple time series db, tightly integrated with Python and Pandas.')