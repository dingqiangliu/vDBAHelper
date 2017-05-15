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

        # caculate level according rules in log_message_level
        levelexpression = ""
        sql = """select expression, name from log_message_level;"""
        logger.debug("sql=%s" %sql)
        for (expression, name, ) in db.execute(sql):
            levelexpression += "when " + expression + " then '" + name + "'"
        if len(levelexpression) > 0 :
            levelexpression = "( case " + levelexpression + " else level end) level"
        else :
            levelexpression = "level"

        # caculate level filter according selecting
        levelvalues = ("'" + "','".join(msgtype) + "'") if (len(msgtype) > 0 and not 'ALL' in msgtype) else None
        levelfilter = "where level in (%s)" % levelvalues if not levelvalues is None else ""

        tmptable = "temp.tmp_vertica_log_%s" % randint(0, 10000)
        sql = """drop table if exists %s;""" % tmptable
        logger.debug("sql=%s" %sql)
        db.execute(sql)

        sql = """CREATE VIRTUAL TABLE %s USING fts4(
              time timestamp,
              level varchar(20),
              transaction_id integer,
              message varchar(2322),
              cat_name varchar(30),
              notindexed=time, 
              notindexed=level, 
              notindexed=transaction_id, 
              notindexed=cat_name
            );""" % tmptable
        logger.debug("sql=%s" %sql)
        db.execute(sql)
           
        sql = """insert into %s
            select time, level, transaction_id, message, 'Unkown' cat_name 
            from (
                select time, %s, transaction_id, message 
                from vertica_log log
                where time >= ? and time <= ? ) t0
            %s ;""" % (tmptable, levelexpression, levelfilter)
        logger.debug("parameters=(%s, %s), sql=%s" %(dtbegin, dtend, sql))
        db.execute(sql, (dtbegin, dtend))

        sql = """create table %s_1 as
            select time, level, transaction_id, message, cat.name cat_name
            from %s log, issue_category cat
            where log.message match cat.pattern;""" % (tmptable, tmptable)
        logger.debug("sql=%s" %sql)
        db.execute(sql)
            
        sql = """delete from %s
            where rowid in (
              select log.rowid
              from %s log, issue_category cat
              where log.message match cat.pattern
            );""" %(tmptable, tmptable)
        logger.debug("sql=%s" %sql)
        db.execute(sql)
            
        sql = """insert into %s select * from %s_1;""" % (tmptable, tmptable)
        logger.debug("sql=%s" %sql)
        db.execute(sql)
            
        sql = """select * from %s;""" % tmptable
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
        predicates = ""
        if transaction_id is None or len(transaction_id) == 0:
            # get verticalogs by time span
            TIME_SPAN_SECONDS = 5
            dt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")
            # [:-3]: "time" of vertica_log is millisecond, not microsecond
            dtbegin = (dt + timedelta(seconds=-TIME_SPAN_SECONDS)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            dtend = (dt + timedelta(seconds=+TIME_SPAN_SECONDS)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            sql = """select *
                  from vertica_log 
                  where time >= ? and time <= ?
                  order by time desc;
                  """
            logger.debug("parameters=(%s, %s), sql=%s" %(dtbegin, dtend, sql))
            verticalogs = db.execute(sql, (dtbegin, dtend)).fetchall()
            predicates = "time from %s to %s" % (dtbegin, dtend)
        else:
            # get verticalogs by transaction_id, and time span for better performance
            sql = """select *
                  from vertica_log 
                  where time >= ? and time <= ?
                      and transaction_id = ?
                  order by time desc;"""
            logger.debug("parameters=(%s, %s, %s), sql=%s" %(dtbegin, dtend, transaction_id, sql))
            verticalogs = db.execute(sql, (dtbegin, dtend, transaction_id,)).fetchall()
            predicates = "transaction_id = %s" % transaction_id

        # get issue reasons acording issue_reason
        reasons = {}
        if not issue is None and len(issue) > 0:
            sql = """select distinct table_name
                  from issue_reason
                  where issue_cat_name = ? ;"""
            logger.debug("parameters=(%s), sql=%s" %(issue, sql))
            for (tablename,) in db.execute(sql, (issue,)).fetchall():
                sql = """select reason_name, filter_columns, reason_pattern 
                      from issue_reason
                      where issue_cat_name = ? and table_name = ? 
                      order by privilege;"""
                logger.debug("parameters=(%s, %s), sql=%s" %(issue, tablename, sql))
                rules = db.execute(sql, (issue, tablename,)).fetchall()
                # fill data into temp FTS table
                if len(rules) > 0:
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
                    reason_name, filter_columns, reason_pattern = rule[0], rule[1], rule[2]
                    if not transaction_id is None and len(transaction_id) > 0 and 'transaction_id' in filter_columns.split(","):
                        filterexpression = "and transaction_id = %s" % transaction_id
                    else:
                        filterexpression = ""
                    sql = """select *
                          from %s
                          where %s match ? %s
                          order by time desc; """ % (tmptable, tmptable.split('.')[1], filterexpression)
                    logger.debug("parameters=(%s), sql=%s" %(reason_pattern, sql))
                    reasons.update({reason_name: db.execute(sql, (reason_pattern, )).fetchall()})

        return template("errnav/errordetail", verticalogs=verticalogs, reasons=reasons, predicates=predicates)
    except Exception, e:
        msg = "Failed when get error detail!"
        return HTTPError(body=msg, exception=e, traceback=traceback.format_exc())
