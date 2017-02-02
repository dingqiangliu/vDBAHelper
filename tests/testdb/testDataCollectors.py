#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual tables for Vertica data collectors
# Author: DingQiang Liu

import unittest
import traceback, sys

import apsw

import db.vdatacollectors as vdatacollectors


class TestDataCollectors(unittest.TestCase):
  def setUp(self):
    try :
      global CONNECTION
      self.connection = CONNECTION
    except NameError:
      self.connection = None
    
    if self.connection is None :
      self.connection = apsw.Connection(":memory:")
      vdatacollectors.setup(self.connection)
    
  def tearDown(self):
    self.connection.close()
    self.connection = None
      
  def testStorage_Layer_Statistics_By_Day(self):
    """testing dc_storage_layer_statistics_by_day, including types: TIMESTAMP WITH TIME ZONE, VARCHAR, INTEGER, FLOAT """

    cursor = None 
    try :
      cursor = self.connection.cursor()
      if cursor.execute("select count(1) from dc_storage_layer_statistics_by_day limit 1").next()[0] > 0 :
        #time format: yyyy-mm-dd hh:mm:dd
        self.assertTrue( len(cursor.execute("select time from dc_storage_layer_statistics_by_day limit 1").next()[0].split('-')) == 3)
    except :
      self.fail(traceback.format_exc().decode(sys.stdout.encoding))
    finally :
      if not cursor is None :
        cursor.close();

  def testRequests_Completed(self):
    """testing dc_requests_completed, including types: TIMESTAMP WITH TIME ZONE, VARCHAR, INTEGER, BOOLEAN"""
    
    cursor = None 
    try :
      cursor = self.connection.cursor()
      cursor.execute("select * from dc_requests_completed limit 1")
      self.assertTrue(cursor.execute("select count(1) from dc_requests_completed").next()[0]>=0)
    except :
      self.fail(traceback.format_exc().decode(sys.stdout.encoding))
    finally :
      if not cursor is None :
        cursor.close();

  def testZ_OtherTables(self):
    """testing other tables except dc_storage_layer_statistics_by_day, dc_requests_completed """
    
    cursor = None 
    try :
      cursor = self.connection.cursor()

      print ""
      tables = [ t[0] for t in cursor.execute("select tbl_name from sqlite_master where lower(tbl_name) like 'dc__%' and not lower(tbl_name) in ('dc_storage_layer_statistics_by_day', 'dc_requests_completed', 'dc_lock_attempts')") ]
      ## TODO: performance tuning
      #tables = [ 'dc_network_info' ]
      for tablename in tables :
        #cursor.execute("select * from %s limit 1" % tablename)
        for r in cursor.execute("select * from %s" % tablename): pass
        print "    testing on table [%s] passed." % tablename
    except :
      self.fail("[%s] when query on table [%s]" % (traceback.format_exc(), tablename))
      # fix for: UnicodeDecodeError: 'ascii' codec can't decode byte 0xe6 in position 0: ordinal not in range(128)
      #self.fail("[%s] when query on table [%s]" % (traceback.format_exc().decode(sys.stdout.encoding), tablename)) 
    finally :
      if not cursor is None :
        cursor.close();
        
if __name__ == "__main__":
  unittest.main()

