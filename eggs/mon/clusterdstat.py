#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: cluster dstat, override dstat module
# Author: DingQiang Liu


import mon.dstat as dstatmodule


class dstat_nodename(dstatmodule.dstat):
    """
    nodename output plugin. 
    """

    def __init__(self):
        self.name = 'node'
        self.nick = ('name',)
        self.vars = ('text',)
        self.type = 's'
        self.width = 12
        self.scale = 0

    def extract(self):
        self.val['text'] = ''
        if "nodeName" in globals() :
            global nodeName
            self.val['text'] = nodeName
            self.width = len(nodeName)


dstatmodule.dstat_nodename = dstat_nodename


if __name__ == '__main__' :
    pass
else :
    # add dstat.Options.__repr__ function to generate constant object 
    def Options_repr (self):
        return "Options('%s')" % self.args

    dstatmodule.Options.__repr__ = Options_repr
