#!/usr/bin/env python3

from setuptools import setup

import sys
if sys.version_info < (3, 5):
    print('Python 3.5 or higher is required to use PyRibbonBridge.')
    sys.exit(1)

setup (name = 'PyRibbonBridge',
       author = 'David Ko',
       author_email = 'david@barobo.com',
       version = '0.0.5',
       description = "This is a pure Python implementation of ribbon-bridge: An "
       "RPC Framework http://github.com/BaroboRobotics/ribbon-bridge",
       package_dir = {'':'src'},
       packages = ['ribbonbridge'],
       url = 'http://github.com/BaroboRobotics/PyRibbonBridge',
       install_requires=['protobuf>=3.0.0b2'],
       )
