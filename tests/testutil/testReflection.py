#!/usr/bin/python
#encoding: utf-8

import unittest
import traceback, sys
from cStringIO import StringIO

import util.reflection as reflection

import basemodule 
import submodule


class TestReflection(unittest.TestCase):

    def testOverrideModule(self):
        """testing module override  """

        self.assertEqual("Hello", basemodule.hello())

        old_stdout = sys.stdout
        try :
            src = reflection.overridemodule(basemodule, submodule)
            #print src

            self.assertEqual("Hello", basemodule.hello())
            self.assertEqual("您好, 您好, 世界", submodule.hello())
            self.assertEqual("您好", basemodule.hello())

            sys.stdout = mystdout = StringIO() 
            exec src
            self.assertEqual("您好, 您好, 世界\n", mystdout.getvalue())
        except :
            self.fail(traceback.format_exc())
        finally :
            sys.stdout = old_stdout


if __name__ == "__main__":
    unittest.main()