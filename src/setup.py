import os
import sys
import platform
from setuptools import setup, find_packages
from distutils.core import Extension

setup(name = 'pyg-sql', version = '0.0.49', packages = find_packages(), python_requires = '>=3.6.')
