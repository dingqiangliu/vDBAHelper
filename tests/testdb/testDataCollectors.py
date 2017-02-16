#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases for SQLite virtual tables of Vertica data collectors
# Author: DingQiang Liu

import unittest
import traceback, sys
import re

import apsw

from testdb.dbtestcase import DBTestCase


class TestDataCollectors(DBTestCase):
  def testStorage_Layer_Statistics_By_Day(self):
    """testing dc_storage_layer_statistics_by_day, including types: TIMESTAMP WITH TIME ZONE, VARCHAR, INTEGER, FLOAT """

    cursor = None 
    try :
      cursor = self.connection.cursor()
      sql = """
        select time, 
          node_name, node_name||'1', 
          reaper_queue_depth_sample_count, reaper_queue_depth_sample_count+1, 
          reaper_queue_depth_sample_sum, reaper_queue_depth_sample_sum+1 
          from dc_storage_layer_statistics_by_day
          limit 1;
        """
      for (ts, st1, st2, i1, i2, f1, f2) in cursor.execute(sql) :
        # check TIMESTAMP type, format: %Y-%m-%d %H:%M:%S.%f
        self.assertTrue(isinstance(ts, unicode) and not re.match("^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{6}$", ts) is None, "incorrect TIMESTAMP type output")
        # check VARCHAR type
        if not st1 is None :
          self.assertTrue(isinstance(st1, unicode) and st1+"1" == st2, "incorrect VARCHAR type output")
        # check INTEGER type
        if not i1 is None :
          self.assertTrue(isinstance(i1, int) and i1+1 == i2, "incorrect INTEGER type output")
        # check FLOAT type
        if not f1 is None :
          self.assertTrue(isinstance(f1, float) and f1+1 == f2, "incorrect FLOAT type output")
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
      for (b1, b2) in cursor.execute("select success, not success from dc_requests_completed limit 1") :
        # check BOOLEAN type
        if not b1 is None :
          self.assertTrue(b1 == (not b2), "incorrect BOOLEAN type output")
    except :
      self.fail(traceback.format_exc().decode(sys.stdout.encoding))
    finally :
      if not cursor is None :
        cursor.close();


  def testFilters(self):
    """testing filter on time, node_name columns """
    print ""

    cursor = None 
    try :
      cursor = self.connection.cursor()
      # first 2 nodes and about 10th data
      sql = """
        select /*testFilters*/ n1, n2, ti t1, datetime(strftime('%s',ti) + (strftime('%s',tx)-strftime('%s',ti))/10, 'unixepoch') t2
        from (
          select min(node_name) n1, max(node_name) n2, min(time) ti, max(time) tx
          from dc_storage_layer_statistics_by_day
          where node_name in (select distinct node_name from dc_storage_layer_statistics_by_day order by node_name limit 2)
        ) t;
        """
      for (n1, n2, t1, t2) in cursor.execute(sql) :
        # filter on "time = t1"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time = ? order by time", (t1,)): pass
        # filter on "time < t2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time < ? order by time", (t2,)): pass
        # filter on "time <= t2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time <= ? order by time", (t2,)): pass
        # filter on "time >= t1 and time <= t2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time >= ? and time <= ? order by time", (t1, t2,)): pass
        # filter on "time >= t1 and time < t2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time >= ? and time < ? order by time", (t1, t2,)): pass
        # filter on "time between t1 and t2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time between ? and ? order by time", (t1, t2,)): pass
            
        # filter on "node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? order by node_name", (n1,)): pass
        # filter on "node_name < n2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name < ? order by node_name", (n2,)): pass
        # filter on "node_name <= n2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name <= ? order by node_name", (n2,)): pass
        # filter on "node_name >= n1 and node_name <= n2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name >= ? and node_name <= ? order by node_name", (n1, n2,)): pass
        # filter on "node_name >= n1 and node_name < n2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name >= ? and node_name < ? order by node_name", (n1, n2,)): pass
        # filter on "node_name between n1 and n2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name between ? and ? order by node_name", (n1, n2,)): pass
        # filter on "node_name = n1 or node_name = n2"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? or node_name = ? order by node_name", (n1, n2,)): pass
        # filter on "node_name in (n1, n2)"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name in (?, ?) order by node_name", (n1, n2,)): pass

        # filter on "time = t1 and node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time = ? and node_name = ?", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 order by node_name, time""
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? and time = ?", (n1, t1,)): pass
        # filter on "time = t1 and node_name = n1 order by time, node_name"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time = ? and node_name = ? order by time, node_name", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 order by node_name, time""
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? and time = ? order by node_name, time", (n1, t1,)): pass
        # filter on "node_name = n1 and time = t1 order by time, node_name""
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? and time = ? order by time, node_name", (n1, t1,)): pass
        # filter on "time = t1 or node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where time = ? or node_name = ? order by time, node_name", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 and storage_id=1 order by time, node_name"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? and time = ? and storage_id = ? order by time, node_name", (n1, t1, 1,)): pass
        # filter on "node_name = n1 and time = t1 and storage_id=1"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? and time = ? and storage_id = ?", (n1, t1, 1,)): pass
        # filter on "node_name = n1 and time = t1 or storage_id=1"
        for r in cursor.execute("select /*testFilters*/ * from dc_storage_layer_statistics_by_day where node_name = ? and time = ? or storage_id = ?", (n1, t1, 1,)): pass
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

      sql = """
        select tbl_name from sqlite_master
        where lower(tbl_name) like 'dc__%' 
          and not lower(tbl_name) in ('dc_storage_layer_statistics_by_day', 'dc_requests_completed', 'dc_lock_attempts')
        """
      tables = [ t for (t) in cursor.execute(sql) ]
      for tablename in tables :
        for r in cursor.execute("select * from %s limit 1" % tablename): pass
        print "    testing on table [%s] passed." % tablename

      ## TODO: performance tuning
      #tables = [ 'dc_network_info' ]
      #for r in cursor.execute("select * from %s" % tablename): pass
      #  print "    testing on table [%s] passed." % tablename
    except :
      self.fail("[%s] when query on table [%s]" % (traceback.format_exc(), tablename))

      # fix for: UnicodeDecodeError: 'ascii' codec can't decode byte 0xe6 in position 0: ordinal not in range(128)
      #self.fail("[%s] when query on table [%s]" % (traceback.format_exc().decode(sys.stdout.encoding), tablename)) 
    finally :
      if not cursor is None :
        cursor.close();

if __name__ == "__main__":
  unittest.main()

