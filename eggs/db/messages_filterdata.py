#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual table messages for /var/log/messages files on each nodes
# Author: DingQiang Liu

import re
from datetime import datetime
import os, sys
import glob


COLUMNS = ["time", "host_name", "component", "message"]
idxMessage = COLUMNS.index("message")

ROWPATTERN = re.compile("^(?P<time>[\d\w][A-Za-z0-9 ]+ \d{2}:\d{2}:\d{2}) ((?P<host_name>[A-Za-z0-9_\.]+) )?((?P<component>[A-Za-z0-9()\[\]_ ]+): )?(?P<message>.*)")

class LogFile:
    """ log file class supporting bi-direction reading. """

    def __init__(self, fo):
        self.fo = fo

        # caculate size of file
        self.fo.seek(0, 2) #os.SEEK_END
        self.filesize = fo.tell()


    def __readblocks(self, forward, pfrom=None, pto=None, anchor=None, blocksize=4096):
        """
        Generate blocks of file's contents.
          
        args : 
        * forward: moving direction, True mean from BEGIN to END
        * pfrom: low bound of character position in file
        * pto: upper bound of character position in file
        * anchor: anchor position for line. It will be move to begin of current/next row if it's not, 
    
        return : 
        * pos: block begin of character position in file
        * block: string 
        """
    
        pfrom = pfrom if not pfrom is None else 0 
        pto = pto if not pto is None else self.filesize
        anchor = anchor if not anchor is None else (pfrom if forward else pto)

        pos = self.fo.tell()
        if anchor != pos :
            self.fo.seek(anchor)
            pos = anchor

        if forward :
            if anchor != pfrom :
                # adjust pos align with line according anchor, move to begin of row
                searching = True
                line = ""
                while searching and (pos > pfrom):
                    length = min(blocksize, pos - pfrom)
                    self.fo.seek(pos-length)
                    block = self.fo.read(length) + line # attach first "line"" of next block to avoid broking real line
                    pos += len(line)
                    lines = block.split("\n")
                    pos += 1 # move forward 1 position early for comming loop, as there is no "\n" character in last item of lines,  
                    for i in range(len(lines) - 1, -1, -1):
                        line = lines[i]
                        pos -= len(line) + 1
                        # match format of a row
                        if not ROWPATTERN.search(line) is None :
                            self.fo.seek(pos)
                            searching = False
                            break
            moredata = True
            while moredata :
                length = min(blocksize, pto - pos)
                pos += length
                moredata = (pos < pto) 
                if length > 0 :
                    # saving a movement, as sequential reading forward
                    yield pos-length, self.fo.read(length)
        else :
            if anchor != pto :
                # adjust pos align with line according anchor, move to end of row
                searching = True
                line = ""
                while searching and (pos < pto):
                    length = min(blocksize, pto - pos)
                    block = line + self.fo.read(length) # attach first "line"" of next block to avoid broking real line
                    pos -= len(line)
                    lines = block.split("\n")
                    linesCount = len(lines)
                    for i in range(linesCount) :
                        line = lines[i]
                        pos += len(line) + 1
                        if i == linesCount - 1 :
                            pos -= 1 # move backward 1 position, as there is no "\n" character in last item of lines 
                        # match format of a row
                        if not ROWPATTERN.search(line) is None :
                            pos -= 1 # move backward 1 position, skip "\n" to avoid prevRow always try the last empty line 
                            searching = False
                            break
            moredata = True
            while moredata :
                length = min(blocksize, pos - pfrom)
                pos -= length
                moredata = (pos > pfrom)
                if length > 0 :
                    self.fo.seek(pos)
                    yield pos, self.fo.read(length)


    def nextRow(self, pfrom=None, pto=None, anchor=None):
        """
        get next row from front end.
    
        args : 
        * pfrom: low bound of character position in file
        * pto: upper bound of character position in file
        * anchor: anchor position for line. It will be move to begin of current/next row if it's not, 
    
        return : 
        * pos: position begin of return row
        * nRowLength: row length, including line seperator
        * row: list of column values.
        """
        
        match = None
        posRowBegin = nRowLength = 0
        part = ""
        lines = None
        lRowBegin = -1
        for pos, block in self.__readblocks(True, pfrom, pto, anchor):
            pos -= len(part)
            block = part + block
            part = ""

            lines = block.split("\n")
            linesCount = len(lines)
            for i in range(linesCount):
                line = lines[i]
                # match format of a row
                matchNext = ROWPATTERN.search(line)
                if not matchNext is None :
                    if not match is None and (posRowBegin != pos) : # avoid "posRowBegin == pos", as former row maybe not a completed 
                        # row end, output row
                        row = [match.group(col) or '' for col in COLUMNS]
                        if i > lRowBegin + 1 :
                            # pend message with multiple lines
                            row[idxMessage] = row[idxMessage] + "\n" + "\n".join( lines[lRowBegin+1:i] )
                        nRowLength = sum( [ len(ln) + 1 for ln in lines[lRowBegin:i] ] ) 
                        row[0] = str(long(datetime.strptime(row[0], "%b %d %H:%M:%S").replace(year=datetime.today().year).strftime('%s%f')) - 946684800*1000000)
                        yield posRowBegin, nRowLength, row
                    
                    # next row begain
                    posRowBegin = pos
                    match = matchNext
                    lRowBegin = i

                # keep unfinished part, maybe it's including part of next block.
                if i == linesCount - 1 :
                    if match :
                        part = "\n".join( lines[lRowBegin:linesCount] )
                    else :
                        part = block
                pos += len(line) + 1
                if i == linesCount - 1 :
                    pos -= 1 # move backward 1 position, as there is no "\n" character in last item of lines 

        # out put last matched row.
        if not match is None :
            row = [match.group(col) or '' for col in COLUMNS]
            if lRowBegin < linesCount - 1 :
                # pend message with multiple lines
                row[idxMessage] = row[idxMessage] + "\n" + "\n".join( lines[lRowBegin+1:linesCount] )
            nRowLength = sum( [ len(ln) + 1 for ln in lines[lRowBegin:linesCount] ] ) - 1 # -1 as there is no "\n" character in last item of lines
            row[0] = str(long(datetime.strptime(row[0], "%b %d %H:%M:%S").replace(year=datetime.today().year).strftime('%s%f')) - 946684800*1000000)

            yield posRowBegin, nRowLength, row
                

    def prevRow(self, pfrom=None, pto=None, anchor=None):
        """
        get previous row from front end.
    
        args : 
        * pfrom: low bound of character position in file
        * pto: upper bound of character position in file
        * anchor: anchor position for line. It will be move to begin of current/next row if it's not, 
    
        return : 
        * pos: position begin of return row
        * nRowLength: row length, including line seperator
        * row: list of column values.
        """
        
        pto = pto if not pto is None else self.filesize

        part = ""
        for pos, block in self.__readblocks(False, pfrom, pto, anchor):
            block = block + part
            part = ""
            pos += len(block)

            lines = block.split("\n")
            pos += 1 # move forward 1 position early for comming loop, as there is no "\n" character in last item of lines,  
            linesCount = len(lines)
            lRowEnd = -1
            for i in range(linesCount-1, -1, -1):
                line = lines[i]
                pos -= len(line) + 1
                if lRowEnd < 0:
                    lRowEnd = i

                # match format of a row
                match = ROWPATTERN.search(line)
                if not match is None :
                    # row begain, output row
                    row = [match.group(col) or '' for col in COLUMNS]
                    if i < lRowEnd :
                        # pend message with multiple lines
                        row[idxMessage] = row[idxMessage] + "\n" + "\n".join( lines[i+1:lRowEnd+1] )
                    nRowLength = sum( [ len(ln) + 1 for ln in lines[i:lRowEnd+1] ] ) 
                    if pos + nRowLength > pto :
                        nRowLength -= 1 # -1 as there is no "\n" character in last item of lines
                    row[0] = str(long(datetime.strptime(row[0], "%b %d %H:%M:%S").replace(year=datetime.today().year).strftime('%s%f')) - 946684800*1000000)

                    yield pos, nRowLength, row
                    match = None
                    lRowEnd = -1

                # keep unfinished part, maybe it's including part of next block.
                if (i == 0) and (lRowEnd >= 0) :
                    part = "\n".join( lines[0:lRowEnd+1] )


def parseFile(f, args):
    predicates = args["predicates"]
    nodeName = args["nodeName"]
    nodenum = int(nodeName[-4:])

    data = []
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
        with open(f) as fo :
            fin = LogFile(fo)
            # minPos/maxPos is bound of character position in file, pos is position of current row 
            minPos = 0 
            maxPos = fin.filesize 
            if 0 in predicates :
                # binary search on time. operators = {2: "==", 4: ">", 8: "<=", 16: "<", 32: ">="}
                # check first line from min side
                pos, row = None, None
                for pos, rowWidth, row in fin.nextRow(minPos, maxPos) : break
                if row is None :
                    return None
                minPos = pos
                if not maxPredOp is None :
                    time = long(row[0])
                    if ((maxPredOp==2) and(time > maxPredValue) or (maxPredOp==16) and (time >= maxPredValue) or (maxPredOp==8) and (time > maxPredValue)) :
                        return None
                # move first line from min side
                if not minPredOp is None :
                    tmpMaxPos = maxPos if not maxPos is None else fin.filesize
                    while not row is None :
                        time = long(row[0])
                        if (pos == minPos) and ((minPredOp==2) and (time == minPredValue) or (minPredOp==4) and (time > minPredValue) or (minPredOp==32) and (time >= minPredValue)) :
                            # found the first!
                            break
                        elif ((minPredOp==2) and (time < minPredValue) or (minPredOp==4) and (time <= minPredValue) or (minPredOp==32) and (time < minPredValue)) :
                            # move a half forward
                            minPos = pos + rowWidth
                        else :
                            # move a half backward
                            tmpMaxPos = pos 
                        pos, row = None, None
                        for pos, rowWidth, row in fin.nextRow(minPos, tmpMaxPos, minPos + (tmpMaxPos - minPos)/2) : break
                # check last line from max side
                pos, row = None, None
                for pos, rowWidth, row in fin.prevRow(minPos, maxPos) : break
                if row is None:
                    return None
                maxPos = pos + rowWidth
                if not minPredOp is None :
                    time = long(row[0])
                    if ((minPredOp==2) and (time < minPredValue) or (minPredOp==4) and (time <= minPredValue) or (minPredOp==32) and (time < minPredValue)) :
                        return None
                # move last line from max side
                if not maxPredOp is None :
                    tmpMinPos = minPos if not minPos is None else None 
                    while not row is None :
                        time = long(row[0])
                        if (maxPos - pos == rowWidth) and ((maxPredOp==2) and(time == maxPredValue) or (maxPredOp==16) and (time < maxPredValue) or (maxPredOp==8) and (time <= maxPredValue)) :
                            # found the last!
                            break
                        elif ((maxPredOp==2) and(time > maxPredValue) or (maxPredOp==16) and (time >= maxPredValue) or (maxPredOp==8) and (time > maxPredValue)) :
                            # move a half backward
                            maxPos = pos
                        else :
                            # move a half forward
                            tmpMinPos = pos + rowWidth 
                        pos, row = None, None
                        for pos, rowWidth, row in fin.prevRow(tmpMinPos, maxPos, maxPos - (maxPos - tmpMinPos)/2) : break
    
            # get result after predicates
            for _, _, row in fin.nextRow(minPos, maxPos) :
                ltime = long(row[0])
                # convert time format "%Y-%m-%d %H:%M:%S" to avoid input too much "0" on microsecond part when query in SQLite
                if ltime == -0x8000000000000000 :
                    # -9223372036854775808(-0x8000000000000000) means null in Vertica
                    row[0] = None
                else:
                    # 946684800 is secondes between '1970-01-01 00:00:00'(Python) and '2000-01-01 00:00:00'(Vertica)
                    row[0] = datetime.fromtimestamp(float(ltime)/1000000+946684800).strftime("%Y-%m-%d %H:%M:%S")
                # remove '\000' to avoid misleading vsourceparser written by C language.
                message = row[idxMessage].replace("\000", "")
                row[idxMessage] = message
                # rowid = vertica_time % 9999999999999 * 1000000 + nodenum * 1000 + abs(hash(message)) % (10 ** 3)
                row.insert(0, str(ltime % 9999999999999 * 1000000 + nodenum * 1000 + abs(hash(message)) % (10 ** 3)) )
                # node_name
                row.insert(2, nodeName)
                data.append('\1'.join(row))

    except IOError as e :
        # ignore "IOError: [Errno 2] No such file or directory...", when log file rotating
        if 'No such file or directory' in str(e) :
            pass
        else :
            raise

    if len(data) > 0 :
        return '\2'.join(data)
    else :
        return None


if __name__ == '__channelexec__' or __name__ == '__main__' :
    # ignore stderr message when 'non-unicode character' == u'...' : UnicodeWarning: Unicode equal comparison failed to convert both arguments to Unicode - interpreting them as being unequal
    sys.stderr = open(os.devnull, 'w')
    
    nodeName = channel.gateway.id.split('-')[0] # remove the tailing '-slave'
    args = channel.receive()
    args["nodeName"] = nodeName

    catalogpath = args["catalogpath"]
  
    
    # predicate by node. predicates={columnIndx: [[predicate1:value1, predicate2:value2]]}
    operators = {2: "==", 4: ">", 8: "<=", 16: "<", 32: ">="}
    predicates = args["predicates"]

    if not 1 in predicates or all([eval("nodeName %s val" % operators[op]) for op, val in predicates[1]]) :
        path = '/var/log/messages'
        # parse all rotated log files
        data = [ parseFile(f, args) for f in glob.glob(path + "*") ]
        data = [x for x in data if x is not None] # ignore empty file
  
        if not channel.isclosed() and len(data) > 0 :
            channel.send('\2'.join(data))
            data = []

