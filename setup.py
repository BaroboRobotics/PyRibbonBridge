#!/usr/bin/env python3

import codecs
import os
from setuptools import setup

import sys
if sys.version_info < (3, 4):
    print('Python 3.4 or higher is required to use PyRibbonBridge.')
    sys.exit(1)

here = os.path.abspath(os.path.dirname(__file__))
README = codecs.open(os.path.join(here, 'README.rst'), encoding='utf8').read()

setup (name = 'PyRibbonBridge',
       author = 'David Ko',
       author_email = 'david@barobo.com',
       version = '0.0.7',
       description = "This is a pure Python implementation of ribbon-bridge: An "
       "RPC Framework http://github.com/BaroboRobotics/ribbon-bridge",
       long_description = README,
       package_dir = {'':'src'},
       packages = ['ribbonbridge'],
       url = 'http://github.com/BaroboRobotics/PyRibbonBridge',
       install_requires=['protobuf>=3.0.0b2'],
       )
