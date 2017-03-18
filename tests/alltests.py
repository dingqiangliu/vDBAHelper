#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases util for vDBAHelper 
# Author: DingQiang Liu

import unittest
from optparse import OptionParser
import os

import apsw

import db.vcluster as vcluster
import db.vsource as vsource
from testdb.dbtestcase import setConnection

if __name__ == "__main__":
  parser = OptionParser()  
  parser.add_option("-d", "--database", dest="vDBName", default="") 
  parser.add_option("-f", "--file", dest="vMetaFile", default="/opt/vertica/config/admintools.conf") 
  parser.add_option("-u", "--user", dest="vAdminOSUser", default="dbadmin") 
  (options, args) = parser.parse_args()
  
  vc = None
  try :
    vc = vcluster.getVerticaCluster(vDbName = options.vDBName, vMetaFile = options.vMetaFile, vAdminOSUser = options.vAdminOSUser)
  except Exception, e:
    print """ERROR: connect to Vertica cluster failed because [%s: %s].
You can not access newest info of Vertica.
	  """ % (e.__class__.__name__, str(e))

  sqliteDBFile = ""
  if len(args) > 0 :
    sqliteDBFile = args[0]
      
  connection = apsw.Connection(sqliteDBFile)
  setConnection(connection)

  if not vc is None :
    vsource.setup(connection)

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
