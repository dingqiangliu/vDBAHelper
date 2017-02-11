#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual tables for Vertica data collectors
# Author: DingQiang Liu

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
import os
import glob


def prevRow(lines, recBegin, nFrom=None, nTo=None):
    recEnd="." 
    row = None
    pos = (len(lines)-1) if nFrom is None else nFrom
    for line in lines[pos:nTo:-1] :
        pos -= 1
        line = line[:-1] # remove tailing '\n'
        if line == recEnd :
            row = []
        elif not row is None and len(row) > 0 and line == recBegin :
            yield pos, row 
            row = None
        elif not row is None :
            lparts = line.split(":")
            columnName = lparts[0]
            columnValue = ":".join(lparts[1:]) if len(lparts) > 0 else ""
            row.insert(0, columnValue)

def nextRow(lines, recBegin, nFrom=None, nTo=None):
    recEnd="." 
    row = None
    pos = 0 if nFrom is None else nFrom
    for line in lines[pos:nTo:1] :
        pos += 1
        line = line[:-1] # remove tailing '\n'
        if line == recBegin :
            row = []
        elif not row is None and len(row) > 0 and line == recEnd :
            yield pos, row 
            row = None
        elif not row is None :
            lparts = line.split(":")
            columnName = lparts[0]
            columnValue = ":".join(lparts[1:]) if len(lparts) > 0 else ""
            row.append(columnValue)


def parseFile(f, args):
    predicates = args["predicates"]
    columns = args["columns"]
    nodenum = args["nodenum"]
    rowWidth = len(columns) - 1 + 2

    data = []

    recBegin=":DC" + args["tabletag"]

    minPredOp, minPredValue, maxPredOp, maxPredValue = None, None, None, None
    if 0 in predicates :
        for op, val in predicates[0] :
            #operators = {2: "==", 4: ">", 8: "<=", 16: "<", 32: ">="}
            if op in (2, 4, 32, ) :
                if not minPredValue is None and ((val > minPredValue) and (minPredOp == 2) or (val < minPredValue) and (op == 2)) :
                    return None
                if minPredValue is None or (val >= minPredValue) and (op <= minPredOp) :
                    minPredOp, minPredValue = op, val
            if op in (2, 16, 8, ) :
                if not maxPredValue is None and ((val < maxPredValue) and (maxPredOp == 2) or (val > maxPredValue) and (op == 2)) :
                    return None
                if maxPredValue is None or (val <= maxPredValue) and (op,maxPredOp, ) in ((2,16,), (2,8,), (16,8,),) :
                    maxPredOp, maxPredValue = op, val
    if not minPredValue is None and not maxPredValue is None and minPredValue > maxPredValue :
        return None

    try :
        with open(f) as fin :
            lines = fin.readlines()
            if 0 in predicates :
                # binary search on time. operators = {2: "==", 4: ">", 8: "<=", 16: "<", 32: ">="}
                minPos, maxPos = None, None
                # check first line from min side
                pos, row = None, None
                for pos, row in nextRow(lines, recBegin) : break
                if not maxPredOp is None :
                    time = long(row[0])
                    if ((maxPredOp==2) and(time > maxPredValue) or (maxPredOp==16) and (time >= maxPredValue) or (maxPredOp==8) and (time > maxPredValue)) :
                        return None
                # move first line from min side
                if not minPredOp is None :
                    minPos = minPos if not minPos is None else 0 
                    tmpMaxPos = maxPos if not maxPos is None else len(lines)
                    while not row is None :
                        time = long(row[0])
                        if (pos - minPos == rowWidth) and ((minPredOp==2) and (time == minPredValue) or (minPredOp==4) and (time > minPredValue) or (minPredOp==32) and (time >= minPredValue)) :
                            # found the first!
                            minPos = pos - rowWidth
                            break
                        elif ((minPredOp==2) and (time < minPredValue) or (minPredOp==4) and (time <= minPredValue) or (minPredOp==32) and (time < minPredValue)) :
                            # move a half up
                            minPos = pos
                        else :
                            # move a half down
                            tmpMaxPos = pos - rowWidth
                        pos, row = None, None
                        for pos, row in nextRow(lines, recBegin, minPos + (tmpMaxPos - minPos)/2/rowWidth*rowWidth, tmpMaxPos) : break
                # check last line from max side
                pos, row = None, None
                for pos, row in prevRow(lines, recBegin) : break
                if not minPredOp is None :
                    time = long(row[0])
                    if ((minPredOp==2) and (time < minPredValue) or (minPredOp==4) and (time <= minPredValue) or (minPredOp==32) and (time < minPredValue)) :
                        return None
                # move last line from max side
                if not maxPredOp is None :
                    tmpMinPos = minPos - 1 if not minPos is None else None # -1 for comming prevRow generator
                    maxPos = maxPos - 1 if not maxPos is None else len(lines) - 1
                    while not row is None :
                        time = long(row[0])
                        if (maxPos - pos == rowWidth) and ((maxPredOp==2) and(time == maxPredValue) or (maxPredOp==16) and (time < maxPredValue) or (maxPredOp==8) and (time <= maxPredValue)) :
                            maxPos = pos + rowWidth + 1 # more +1 for comming nextRow generator
                            break
                        elif ((maxPredOp==2) and(time > maxPredValue) or (maxPredOp==16) and (time >= maxPredValue) or (maxPredOp==8) and (time > maxPredValue)) :
                            # move a half down
                            maxPos = pos
                        else :
                            # move a half up
                            tmpMinPos = pos + rowWidth
                        pos, row = None, None
                        for pos, row in prevRow(lines, recBegin, maxPos - (maxPos - (tmpMinPos if not tmpMinPos is None else -1))/2/rowWidth*rowWidth, tmpMinPos) : break
                    if row is None :
                        maxPos = maxPos + 1 # more +1 for comming nextRow generator

                # get result after predicates
                for _, row in nextRow(lines, recBegin, minPos, maxPos) :
                    time = long(row[0])
                    # rowid = time * 10000 + nodenum
                    row.insert(0, str(time*10000 + nodenum))
                    data.append('\1'.join(row))
            else :
                # rowid = time * 10000 + nodenum
                data = [ "%s\1%s" % (str(long(row[0])*10000 + nodenum), '\1'.join(row)) for _, row in nextRow(lines, recBegin) ]

    except IOError, e :
        # ignore "IOError: [Errno 2] No such file or directory...", when datacollectors file rotating
        if 'No such file or directory' in str(e) :
            pass
    if len(data) > 0 :
        return '\2'.join(data)
    else :
        return None


if __name__ == '__channelexec__' :
    nodeName = channel.gateway.id.split('-')[0] # remove the tailing '-slave'
    args = channel.receive()
    args["nodenum"] = int(nodeName[-4:])
    tablename = args["tablename"]
    catalogpath = args["catalogpath"]
  
    
    # predicate by node. predicates={columnIndx: [[predicate1:value1, predicate2:value2]]}
    operators = {2: "==", 4: ">", 8: "<=", 16: "<", 32: ">="}
    predicates = args["predicates"]
    if not 1 in predicates or all([eval("nodeName %s val" % operators[op]) for op, val in predicates[1]]) :
        # log filename rule from tablename: remove leading 'dc_', remove '_' and capitalize first character of each word
        tabletag = "".join([w.capitalize() for w in tablename.split('_')[1:] ])
        args["tabletag"] = tabletag
        path = '%s/%s_catalog/DataCollector' % (catalogpath, nodeName)
    
        # TODO: why multiple threads parsing not benifit for performance? The bottleneck is on I/O performance of my laptop?
        data = [ parseFile(f, args) for f in glob.glob(path + "/" + tabletag + "_*.log") ]
        data = [x for x in data if x is not None] # ignore empty file
  
        #pool = ThreadPool()
        #data = pool.map( partial(parseFile, args=args) , glob.glob(path + "/" + tabletag + "_*.log") )
        #pool.close()
        #pool.join()
        #data = [x for x in data if x is not None] # ignore empty file
  
        if not channel.isclosed() and len(data) > 0 :
            channel.send('\2'.join(data))
            data = []

