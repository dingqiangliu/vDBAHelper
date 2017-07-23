#!/usr/bin/python
#encoding: utf-8

# sub
import basemodule


def hello() :
    basemodule.caption = "您好"
    basemodule.per = basemodule.Person("世界")
    return "%s, %s" % (basemodule.caption, basemodule.per.hello())


if __name__ == '__main__' :
    print hello()
else :
    # add basemodule.Person.__repr__ function to generate constant object 
    def Person_repr (self):
        return "Person('%s')" % self.value

    basemodule.Person.__repr__ = Person_repr

