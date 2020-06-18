"""
- `File`: setup.py

- `Author`: Abdelhak Marouane

- `Email`: am0089@uah.edu

- `Github`: 0

- `Description`: A script for installing this package
"""

from distutils.core import setup
from configparser import ConfigParser
from setuptools import find_packages
import os

SETUP_CFG_PATH = 'setup.cfg'
# Instantiate a parser for reading setup.cfg metadata
parser = ConfigParser()
parser.read(SETUP_CFG_PATH)
metadata = dict(parser.items('metadata'))
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')
install_requires = [x.strip() for x in all_reqs]
setup(
    name=metadata['name'],
    version=metadata['version'],
    url=metadata['url'],
    description=metadata['short_description'],
    author=metadata['author'],
    include_package_data=True,
    author_email=metadata['author_email'],
    license=metadata['license'],
    install_requires=install_requires,
    classifiers=[
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6+'
    ],
    packages=[
        m for m in find_packages() if 'tests' not in m
    ],
)
