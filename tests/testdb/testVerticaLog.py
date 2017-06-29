#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases for SQLite virtual table vertica_log for vertica.log files on each nodes
# Author: DingQiang Liu

import unittest
import traceback, sys
import re

import apsw

from util.threadlocal import threadlocal_set, threadlocal_del
from testdb.dbtestcase import DBTestCase


class TestVerticaLog(DBTestCase):
  def testVerticaLogFilters(self):
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
          from vertica_log
          where node_name in (select distinct node_name from vertica_log order by node_name limit 2)
        ) t;
        """
      for (n1, n2, t1, t2) in cursor.execute(sql) :
        # filter on "time = t1"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time = ? order by time", (t1,)): pass
        # filter on "time < t2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time < ? order by time", (t2,)): pass
        # filter on "time <= t2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time <= ? order by time", (t2,)): pass
        # filter on "time >= t1 and time <= t2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time >= ? and time <= ? order by time", (t1, t2,)): pass
        # filter on "time >= t1 and time < t2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time >= ? and time < ? order by time", (t1, t2,)): pass
        # filter on "time between t1 and t2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time between ? and ? order by time", (t1, t2,)): pass
            
        # filter on "node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? order by node_name", (n1,)): pass
        # filter on "node_name < n2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name < ? order by node_name", (n2,)): pass
        # filter on "node_name <= n2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name <= ? order by node_name", (n2,)): pass
        # filter on "node_name >= n1 and node_name <= n2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name >= ? and node_name <= ? order by node_name", (n1, n2,)): pass
        # filter on "node_name >= n1 and node_name < n2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name >= ? and node_name < ? order by node_name", (n1, n2,)): pass
        # filter on "node_name between n1 and n2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name between ? and ? order by node_name", (n1, n2,)): pass
        # filter on "node_name = n1 or node_name = n2"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? or node_name = ? order by node_name", (n1, n2,)): pass
        # filter on "node_name in (n1, n2)"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name in (?, ?) order by node_name", (n1, n2,)): pass

        # filter on "time = t1 and node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time = ? and node_name = ?", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 order by node_name, time""
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? and time = ?", (n1, t1,)): pass
        # filter on "time = t1 and node_name = n1 order by time, node_name"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time = ? and node_name = ? order by time, node_name", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 order by node_name, time""
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? and time = ? order by node_name, time", (n1, t1,)): pass
        # filter on "node_name = n1 and time = t1 order by time, node_name""
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? and time = ? order by time, node_name", (n1, t1,)): pass
        # filter on "time = t1 or node_name = n1"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where time = ? or node_name = ? order by time, node_name", (t1, n1,)): pass
        # filter on "node_name = n1 and time = t1 and level='ERROR' order by time, node_name"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? and time = ? and level = ? order by time, node_name", (n1, t1, 'ERROR',)): pass
        # filter on "node_name = n1 and time = t1 and level='ERROR'"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? and time = ? and level = ?", (n1, t1, 'ERROR',)): pass
        # filter on "node_name = n1 and time = t1 or level='ERROR'"
        for r in cursor.execute("select /*testFilters*/ * from vertica_log where node_name = ? and time = ? or level = ?", (n1, t1, 'ERROR',)): pass
    except :
      self.fail(traceback.format_exc().decode(sys.stdout.encoding))
    finally :
      if not cursor is None :
        cursor.close()


  def testVerticaLog(self):
    """testing table vertica_log """
    
    cursor = None 
    try :
      cursor = self.connection.cursor()

      tablename = "vertica_log"
      for r in cursor.execute("select * from %s limit 1" % tablename): pass
    except :
      self.fail("[%s] when query on table [%s]" % (traceback.format_exc(), tablename))
    finally :
      if not cursor is None :
        cursor.close()


  def testVerticaLogFilterByKeywords(self):
    """testing table vertica_log """
    
    cursor = None 
    try :
      cursor = self.connection.cursor()

      sql = """
SELECT TIME,
       LEVEL,
       transaction_id,
       message,
       'Unkown' cat_name,
                'vertica_log' TABLE_NAME
FROM
  ( SELECT TIME, (CASE
                      WHEN thread_name='SafetyShutdown'
                           AND message='Shutting down this node' THEN 'FATAL'
                      WHEN thread_name='Spread Client'
                           AND message LIKE 'Cluster partitioned%' THEN 'FATAL'
                      WHEN thread_name='LowDiskSpaceCheck'
                           AND message LIKE '%Low disk space detected%' THEN 'WARNING'
                      ELSE LEVEL
                  END) LEVEL,
                       transaction_id transaction_id,
                       message
   FROM vertica_log log) t0
WHERE LEVEL IN ('FATAL',
                'ERROR')
LIMIT 1;
      """
      threadlocal_set("CURRENTSQL", sql)

      for r in cursor.execute(sql): pass
    except :
      self.fail("[%s] when query on table [%s]" % (traceback.format_exc(), sql))
    finally :
      threadlocal_del("CURRENTSQL")
      if not cursor is None :
        cursor.close()


if __name__ == "__main__":
  unittest.main()

