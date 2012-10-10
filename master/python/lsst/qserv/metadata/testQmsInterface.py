#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

# testQmsInterface.py : A module with Python unittest code for testing
# functionality available through the qmsInterface module.


# Standard Python imports
import sys
import unittest

# Package imports
from lsst.qserv.metadata import qmsImpl
from lsst.qserv.master import config

class TestQmsInterface(unittest.TestCase):
    def setUp(self):
        config.load("/u1/qserv/qserv/master/examples/qmsConfig.cnf")
        pass

    def testPersistentInit(self):
        qmsImpl.persistentInit("qmsLogger")


if __name__ == '__main__':
    unittest.main()
