#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: Errors Navigation web controller
# Author: DingQiang Liu

import logging
import traceback
from datetime import datetime
from datetime import timedelta 
from random import randint

from bottle import route, post, error, static_file, template, request, response, HTTPError

from util.threadlocal import threadlocal_set, threadlocal_del

logger = logging.getLogger(__name__)


@route('/errnav/errorlist')
@post('/errnav/errorlist')
def errorlist(db):
    try: 
        dtbegin = request.forms.get('dtbegin')
        dtend = request.forms.get('dtend')
        msgtype = request.forms.getall('msgtype')
        if dtbegin is None or len(dtbegin) == 0:
            dtbegin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        if dtend is None or len(dtend) == 0:
            dtend = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        if msgtype is None or len(msgtype) == 0:
            msgtype = ['FATAL', 'ERROR']

        # target table
        tmptable = "temp.tmp_errnavlist_%s" % randint(0, 10000)
        sql = """drop table if exists %s;""" % tmptable
        logger.debug("sql=%s" %sql)
        db.execute(sql)

        sql = """CREATE VIRTUAL TABLE %s USING fts4(
              time timestamp,
              level varchar(20),
              transaction_id integer,
              message varchar(2322),
              cat_name varchar(30),
              table_name varchar(30),
              notindexed=time, 
              notindexed=level, 
              notindexed=transaction_id, 
              notindexed=cat_name,
              notindexed=table_name
            );""" % tmptable
        logger.debug("sql=%s" %sql)
        db.execute(sql)

        # from each message tables
        sql = """select distinct table_name from log_message_level order by table_name;"""
        logger.debug("sql=%s" % sql)
        for (messagetable, ) in db.execute(sql).fetchall():
            cloumn_transactionid = "null"
            cloumn_level = "null"
            if messagetable == "vertica_log":
                cloumn_transactionid = "transaction_id"
                cloumn_level = "level"

            # caculate level according rules in log_message_level
            levelexpression = ""
            sql = """select expression, name from log_message_level where table_name = ?;"""
            logger.debug("parameters=(%s), sql=%s" %(messagetable, sql))
            for (expression, name, ) in db.execute(sql, (messagetable,)):
                levelexpression += "when " + expression + " then '" + name + "'"
            if len(levelexpression) > 0 :
                levelexpression = "( case " + levelexpression + " else %s end) level" % cloumn_level
            else :
                levelexpression = "%s level" % cloumn_level

            # caculate level filter according selecting
            levelvalues = ("'" + "','".join(msgtype) + "'") if (len(msgtype) > 0 and not 'ALL' in msgtype) else None
            levelfilter = "where level in (%s)" % levelvalues if not levelvalues is None else ""

            # filter messages by time and loglevel
            sql = """insert into %s
                select time, level, transaction_id, message, 'Unkown' cat_name, '%s' table_name
                from (
                    select time, %s, %s transaction_id, message 
                    from %s log
                    where time >= ? and time <= ? ) t0
                %s ;""" % (tmptable, messagetable, levelexpression, cloumn_transactionid, messagetable, levelfilter)
            logger.debug("parameters=(%s, %s), sql=%s" %(dtbegin, dtend, sql))
            threadlocal_set("CURRENTSQL", sql)
            db.execute(sql, (dtbegin, dtend,))
            threadlocal_del("CURRENTSQL")

            # caculate category by pattern, set category by delete+insert 
            sql = """drop table if exists %s_1;""" % tmptable
            logger.debug("sql=%s" %sql)
            db.execute(sql)

            sql = """create table %s_1 as
                select time, level, transaction_id, message, cat.name cat_name, cat.table_name
                from %s log, issue_category cat
                where log.message match cat.pattern 
                    and log.table_name = cat.table_name
                    and cat.table_name = ?;""" % (tmptable, tmptable)
            logger.debug("parameters=(%s), sql=%s" %(messagetable, sql))
            db.execute(sql, (messagetable,))
                
            sql = """delete from %s
                where rowid in (
                  select log.rowid
                  from %s log, issue_category cat
                  where log.message match cat.pattern
                    and log.table_name = cat.table_name
                      and cat.table_name = ?
                );""" %(tmptable, tmptable)
            logger.debug("parameters=(%s), sql=%s" %(messagetable, sql))
            db.execute(sql, (messagetable,))
                
            sql = """insert into %s select * from %s_1;""" % (tmptable, tmptable)
            logger.debug("sql=%s" %sql)
            db.execute(sql)
            
        sql = """select time, level, transaction_id, cat_name
              from %s 
              order by time desc;""" % tmptable
        errors = db.execute(sql)
        return template("errnav/errorlist", dtbegin=dtbegin, dtend=dtend, msgtype=msgtype, errors=errors)
    except Exception, e:
        msg = "Failed when get error list!"
        return HTTPError(body=msg, exception=e, traceback=traceback.format_exc())
    finally:
        sql = """drop table if exists %s_1;""" % tmptable
        logger.debug("sql=%s" %sql)
        db.execute(sql)

        sql = """drop table if exists %s;""" % tmptable
        logger.debug("sql=%s" %sql)
        db.execute(sql)


@route('/errnav/errordetail')
def errordetail(db):
    try: 
        time = request.params['time']
        transaction_id = request.params['tid']
        dtbegin = request.params['dtbegin']
        dtend = request.params['dtend']
        issue = request.params['issue']

        TIME_SPAN_SECONDS = 5
        if '.' in time :
            dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")
        elif ':' in time :
            dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        else :
            dt = datetime.strptime(time, "%Y-%m-%d")
        # [:-3]: "time" of vertica_log is millisecond, not microsecond
        dtspanbegin = (dt + timedelta(seconds=-TIME_SPAN_SECONDS)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        dtspanend = (dt + timedelta(seconds=+TIME_SPAN_SECONDS)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
        messagespredicates = ""
        # get messages by time span
        messagespredicates = "time from %s to %s" % (dtspanbegin, dtspanend)
        sql = """select *
              from messages 
              where time >= ? and time <= ?
              order by time desc;
              """
        logger.debug("parameters=(%s, %s), sql=%s" %(dtspanbegin, dtspanend, sql))
        db.execute(sql, (dtspanbegin, dtspanend))
        try :
            messagescolumns = [c for (c, _, ) in db.getdescription()]
            messages = db.fetchall()
        except :
            messagescolumns = None
            messages = None

        verticalogpredicates = ""
        if transaction_id is None or len(transaction_id) == 0:
            # get verticalogs by time span
            verticalogpredicates = "time from %s to %s" % (dtspanbegin, dtspanend)
            sql = """select *
                  from vertica_log 
                  where time >= ? and time <= ?
                  order by time desc;
                  """
            logger.debug("parameters=(%s, %s), sql=%s" %(dtspanbegin, dtspanend, sql))
            db.execute(sql, (dtspanbegin, dtspanend))
        else:
            # get verticalogs by transaction_id, and time span for better performance
            verticalogpredicates = "transaction_id = %s" % transaction_id
            sql = """select *
                  from vertica_log 
                  where time >= ? and time <= ?
                      and transaction_id = ?
                  order by time desc;"""
            logger.debug("parameters=(%s, %s, %s), sql=%s" %(dtbegin, dtend, transaction_id, sql))
            db.execute(sql, (dtbegin, dtend, transaction_id,))

        try :
            verticalogcolumns = [c for (c, _, ) in db.getdescription()]
            verticalogs = db.fetchall()
        except :
            verticalogcolumns = None
            verticalogs = None

        # get issue reasons acording issue_reason
        reasons = []
        if not issue is None and len(issue) > 0:
            sql = """select distinct table_name
                  from issue_reason
                  where issue_cat_name = ? ;"""
            logger.debug("parameters=(%s), sql=%s" %(issue, sql))
            for (tablename,) in db.execute(sql, (issue,)).fetchall():
                sql = """select reason_name, filter_columns, reason_pattern, action 
                      from issue_reason
                      where issue_cat_name = ? and table_name = ? 
                      order by privilege;"""
                logger.debug("parameters=(%s, %s), sql=%s" %(issue, tablename, sql))
                rules = db.execute(sql, (issue, tablename,)).fetchall()
                if len(rules) > 0:
                    # fill data into temp FTS table
                    tmptable = "temp.tmp_issue_analysis_%s" % randint(0, 10000)
                    sql = """drop table if exists %s;""" % tmptable
                    logger.debug("sql=%s" %sql)
                    db.execute(sql)

                    columns = ','.join([columnname for _, columnname, _, _, _, _ in db.execute("pragma table_info('%s');" % tablename)])
                    sql = """CREATE VIRTUAL TABLE %s USING fts4(%s);""" % (tmptable, columns)
                    logger.debug("sql=%s" %sql)
                    db.execute(sql)

                    sql = """insert into %s
                          select * 
                          from %s
                          where time >= ? and time <= ? ;""" % (tmptable, tablename)
                    logger.debug("parameters=(%s, %s), sql=%s" %(dtbegin, dtend, sql))
                    db.execute(sql, (dtbegin, dtend))
                    # filter by rules
                    for rule in rules:
                        reason_name, filter_columns, reason_pattern, action = rule[0], rule[1], rule[2], rule[3]
                        if not transaction_id is None and len(transaction_id) > 0 and 'transaction_id' in filter_columns.split(","):
                            filterexpression = "and transaction_id = %s" % transaction_id
                        else:
                            filterexpression = ""
                        sql = """select *
                              from %s
                              where %s match ? %s
                              order by time desc; """ % (tmptable, tmptable.split('.')[1], filterexpression)
                        logger.debug("parameters=(%s), sql=%s" %(reason_pattern, sql))
                        db.execute(sql, (reason_pattern, ))
                        try :
                            reasoncolumns = [c for (c, _, ) in db.getdescription()]
                            reason = db.fetchall()
                        except :
                            reasoncolumns = None
                            reason = None

                        if not reasoncolumns is None :
                            reasons.append([ \
                                reason_name, \
                                action, \
                                reasoncolumns, \
                                reason
                                ])
                    # clear temp table
                    sql = """drop table if exists %s;""" % tmptable
                    logger.debug("sql=%s" %sql)
                    db.execute(sql)
    

        return template("errnav/errordetail", reasons=reasons, \
            messagespredicates=messagespredicates, messagescolumns=messagescolumns, messages=messages, \
            verticalogpredicates=verticalogpredicates, verticalogcolumns=verticalogcolumns, verticalogs=verticalogs)
    except Exception, e:
        msg = "Failed when get error detail!"
        return HTTPError(body=msg, exception=e, traceback=traceback.format_exc())
