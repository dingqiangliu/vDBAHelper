#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases util for vDBAHelper 
# Author: DingQiang Liu

import unittest
from optparse import OptionParser
import os
import logging

import db.dbmanager as dbmanager
from testdb.dbtestcase import setConnection


logger = logging.getLogger("alltests")


if __name__ == "__main__":
  parser = OptionParser()  
  parser.add_option("-d", "--database", dest="vDBName", default="") 
  parser.add_option("-f", "--file", dest="vMetaFile", default="/opt/vertica/config/admintools.conf") 
  parser.add_option("-u", "--user", dest="vAdminOSUser", default="dbadmin") 
  (options, args) = parser.parse_args()
  
  sqliteDBFile = ""
  if len(args) > 0 :
    sqliteDBFile = args[0]
      
  connection = dbmanager.setup(options.vDBName, options.vMetaFile, options.vAdminOSUser, sqliteDBFile)
  setConnection(connection)

  loader = unittest.TestLoader()
  loader.sortTestMethodsUsing = cmp
  loader.testMethodPrefix = "test"
  loader.suiteClass = unittest.TestSuite
  if len(args) > 1 :
    alltests = unittest.TestSuite((loader.loadTestsFromNames(args[1:]),))
  else :
    alltests = unittest.TestSuite((loader.discover(os.path.dirname(os.path.realpath(__file__))),))
    
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run(alltests)
