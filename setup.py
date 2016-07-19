#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2016 ScyllaDB

# pylint: disable=E0611
from setuptools import setup

setup(name='sdcm',
      version='1.1.0',
      description='Scylla Distributed Cluster Manager',
      author='Lucas Meneghel Rodrigues',
      author_email='lmr@scylladb.com',
      url='https://github.com/scylladb/scylla-cluster-tests',
      packages=['sdcm'])
