#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases for SQLite virtual tables dblog for Vertica dbLog files on each nodes
# Author: DingQiang Liu

import unittest
import traceback, sys
import re

import apsw

from testdb.dbtestcase import DBTestCase


class TestVDBLog(DBTestCase):
  def testVDBLogFilters(self):
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
          from dblog
          where node_name in (select distinct node_name from dblog order by node_name limit 2)
        ) t;
        """
      for (n1, n2, t1, t2) in cursor.execute(sql) :
        # filter on "time = t1"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time = ? order by time", (t1,)): pass
        # filter on "time < t2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time < ? order by time", (t2,)): pass
        # filter on "time <= t2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time <= ? order by time", (t2,)): pass
        # filter on "time >= t1 and time <= t2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time >= ? and time <= ? order by time", (t1, t2,)): pass
        # filter on "time >= t1 and time < t2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time >= ? and time < ? order by time", (t1, t2,)): pass
        # filter on "time between t1 and t2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time between ? and ? order by time", (t1, t2,)): pass
            
        # filter on "node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? order by node_name", (n1,)): pass
        # filter on "node_name < n2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name < ? order by node_name", (n2,)): pass
        # filter on "node_name <= n2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name <= ? order by node_name", (n2,)): pass
        # filter on "node_name >= n1 and node_name <= n2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name >= ? and node_name <= ? order by node_name", (n1, n2,)): pass
        # filter on "node_name >= n1 and node_name < n2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name >= ? and node_name < ? order by node_name", (n1, n2,)): pass
        # filter on "node_name between n1 and n2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name between ? and ? order by node_name", (n1, n2,)): pass
        # filter on "node_name = n1 or node_name = n2"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? or node_name = ? order by node_name", (n1, n2,)): pass
        # filter on "node_name in (n1, n2)"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name in (?, ?) order by node_name", (n1, n2,)): pass

        # filter on "time = t1 and node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time = ? and node_name = ?", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 order by node_name, time""
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? and time = ?", (n1, t1,)): pass
        # filter on "time = t1 and node_name = n1 order by time, node_name"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time = ? and node_name = ? order by time, node_name", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 order by node_name, time""
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? and time = ? order by node_name, time", (n1, t1,)): pass
        # filter on "node_name = n1 and time = t1 order by time, node_name""
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? and time = ? order by time, node_name", (n1, t1,)): pass
        # filter on "time = t1 or node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from dblog where time = ? or node_name = ? order by time, node_name", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 and component='SP_connect' order by time, node_name"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? and time = ? and component = ? order by time, node_name", (n1, t1, 'SP_connect',)): pass
        # filter on "node_name = n1 and time = t1 and component='SP_connect'"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? and time = ? and component = ?", (n1, t1, 'SP_connect',)): pass
        # filter on "node_name = n1 and time = t1 or component='SP_connect'"
        for r in cursor.execute("select /*testFilters*/ * from dblog where node_name = ? and time = ? or component = ?", (n1, t1, 'SP_connect',)): pass
    except :
      self.fail(traceback.format_exc().decode(sys.stdout.encoding))
    finally :
      if not cursor is None :
        cursor.close();


  def testVDbLog(self):
    """testing table dblog """
    
    cursor = None 
    try :
      cursor = self.connection.cursor()

      tablename = "dblog"
      for r in cursor.execute("select * from %s limit 1" % tablename): pass
    except :
      self.fail("[%s] when query on table [%s]" % (traceback.format_exc(), tablename))

      # fix for: UnicodeDecodeError: 'ascii' codec can't decode byte 0xe6 in position 0: ordinal not in range(128)
      #self.fail("[%s] when query on table [%s]" % (traceback.format_exc().decode(sys.stdout.encoding), tablename)) 
    finally :
      if not cursor is None :
        cursor.close();

if __name__ == "__main__":
  unittest.main()

