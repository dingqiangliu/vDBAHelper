#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual table vertica_log for vertica.log files on each nodes
# Author: DingQiang Liu

import re
from datetime import datetime
import os, sys
from multiprocessing import cpu_count
from multiprocessing import Pool
from functools import partial


COLUMNS = ["time", "thread_name", "thread_id", "transaction_id", "component", "level", "elevel", "enode", "message"]
idxTime = COLUMNS.index("time")
idxTransactionID = COLUMNS.index("transaction_id")
idxMessage = COLUMNS.index("message")

## pattern for vertica.log file, original version from Michael Flower
#ROWPATTERN = re.compile("^(?P<time>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) (?P<thread_name>[A-Za-z ]+):(?P<thread_id>0x[0-9a-f]+)-?(?P<transaction_id>[0-9a-f]+)? (?:\[(?P<component>\w+)\] \<(?P<level>\w+)\> )?(?:<(?P<elevel>\w+)> @\[?(?P<enode>\w+)\]?: )?(?P<message>.*)")

# upgrade:
#   * mark <thread_name>:<thread_id> optional to support format "2016-11-27 17:36:14.990 INFO New log"
#   * <thread_id> of vertica 8 do not begin with "0x"
#   * <thread_name> maybe contains "()" and numbers, eg. "TM Mergeout(01)"
ROWPATTERN = re.compile("^(?P<time>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+)( (?P<thread_name>[A-Za-z0-9() ]+):(?P<thread_id>(0x)?[0-9a-f]+)-?(?P<transaction_id>[0-9a-f]+)?)? (?:\[(?P<component>\w+)\] \<(?P<level>\w+)\> )?(?:<(?P<elevel>\w+)> @\[?(?P<enode>\w+)\]?: )?(?P<message>.*)")

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
                    yield pos, nRowLength, row
                    match = None
                    lRowEnd = -1

                # keep unfinished part, maybe it's including part of next block.
                if (i == 0) and (lRowEnd >= 0) :
                    part = "\n".join( lines[0:lRowEnd+1] )

    def nextLineWithFilter(self, keywords, pfrom=None, pto=None, anchor=None):
        """
        get next line from front end with keywords filter.
    
        args : 
        * keywords: keywords list to filter lines
        * pfrom: low bound of character position in file
        * pto: upper bound of character position in file
        * anchor: anchor position for line. It will be move to begin of current/next row if it's not, 
    
        return : 
        * line: line match with sum of filters.
        """
        
        match = None
        part = ""
        for _, block in self.__readblocks(True, pfrom, pto, anchor, 32768):
            block = part + block
            part = ""
            lines = block.split("\n")

            for line in lines[:-1] :
                if any(wd in line for wd in keywords) :
                    if not ROWPATTERN.search(line) is None :
                        yield line
                
            # keep unfinished part, maybe it's including part of next block.
            part = lines[-1]

        # out put last matched row.
        if part != "" :
            if any(wd in part for wd in keywords) :
                if not ROWPATTERN.search(part) is None :
                    yield part


def parseFile(f, args):
    predicates = args["predicates"]
    nodeName = args["nodeName"]
    nodenum = int(nodeName[-4:])

    data = []
    minPredOp, minPredValue, maxPredOp, maxPredValue = None, None, None, None
    if 0 in predicates :
        for op, val in predicates[0] :
            # time: vertica long to string format
            try :
                if val == -0x8000000000000000 : 
                    # -9223372036854775808(-0x8000000000000000) means null in Vertica
                    val = None
                else :
                  # 946684800 is secondes between '1970-01-01 00:00:00'(Python) and '2000-01-01 00:00:00'(Vertica)
                  val = datetime.fromtimestamp(float(val)/1000000+946684800).strftime("%Y-%m-%d %H:%M:%S.%f")
                  # vertica.log time format: "2008-12-19 15:28:46.123"
                  val =  val[:-3]
            except ValueError:
                pass

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
                    time = row[idxTime]
                    if ((maxPredOp==2) and(time > maxPredValue) or (maxPredOp==16) and (time >= maxPredValue) or (maxPredOp==8) and (time > maxPredValue)) :
                        return None
                # move first line from min side
                if not minPredOp is None :
                    tmpMaxPos = maxPos if not maxPos is None else fin.filesize
                    while not row is None :
                        time = row[idxTime]
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
                    time = row[idxTime]
                    if ((minPredOp==2) and (time < minPredValue) or (minPredOp==4) and (time <= minPredValue) or (minPredOp==32) and (time < minPredValue)) :
                        return None
                # move last line from max side
                if not maxPredOp is None :
                    tmpMinPos = minPos if not minPos is None else None 
                    while not row is None :
                        time = row[idxTime]
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
                # column transaction_id, from hex to integer
                transactionID = row[idxTransactionID]
                if not transactionID is None and len(transactionID) > 0:
                    row[idxTransactionID] = str(long(transactionID, 16))

                time = row[idxTime]
                # remove '\000' to avoid misleading vsourceparser written by C language.
                message = row[idxMessage].replace("\000", "")
                row[idxMessage] = message
                # rowid = abs(hash(time)) % 9999999999999 * 1000000 + nodenum * 1000 + abs(hash(message)) % (10 ** 3)
                row.insert(0, str(abs(hash(time))%9999999999999*1000000 + nodenum * 1000 + abs(hash(message)) % (10 ** 3)) )
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


def filterFilePortion(positions, filename, keywords):
    with open(filename) as fo :
        fin = LogFile(fo)
        posFrom = positions[0] if not positions is None and len(positions)>=1  else None
        posTo = positions[1] if not positions is None and len(positions)>=2  else None
        return [line for line in fin.nextLineWithFilter(keywords, posFrom, posTo)]


def parseFileWithFilter(filename, args):
    keywords = args.get("keywords", None)
    if keywords :
        # Note: keywords can not be unicode when "in" match with utf8 string, otherwise "in" will meet issue "UnicodeDecodeError: 'ascii' codec can't decode byte 0x... : ordinal not in range(128)" 
        keywords = [wd.encode("utf-8") for wd in keywords]

        # pre filter log file with key words
        tmpfilename = filename + ".tmp"
        try :
            parallelism = cpu_count()
            filesize = os.path.getsize(filename)
            chunksize = filesize / parallelism

            pool = Pool(parallelism)
            linesList = pool.imap(partial(filterFilePortion, filename=filename, keywords=keywords) \
                , ([n * chunksize, (filesize if (n == parallelism - 1) else (n+1) * chunksize)] for n in range(parallelism)) \
                )
            pool.close()
            pool.join()

            with open(tmpfilename, "w") as tmpfile :
                tmpfile.writelines(l+"\n" for ll in linesList for l in ll)
            
            return parseFile(tmpfilename, args)
        finally :
            os.remove(tmpfilename)
    else :
        return parseFile(filename, args)


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
        path = '%s/%s_catalog/' % (catalogpath, nodeName)
        data = [ parseFileWithFilter(path + "vertica.log", args) ]
        if not channel.isclosed() and len(data) > 0 :
            channel.send('\2'.join(r for r in data if r is not None))
            data = []

