#!/usr/bin/env python3

from distutils.core import setup, Extension

setup (name = 'PyRibbonBridge',
       author = 'David Ko',
       author_email = 'david@barobo.com',
       version = '0.1',
       description = "This is a pure Python implementation of ribbon-bridge: An"
       "RPC Framework http://github.com/BaroboRobotics/ribbon-bridge",
       package_dir = {'':'src'},
       packages = ['ribbonbridge'],
       url = 'http://github.com/BaroboRobotics/PyRibbonBridge',
       )
