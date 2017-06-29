def __bootstrap__():
   global __bootstrap__, __loader__, __file__
   import sys, pkg_resources, imp

   import platform
   __file__ = pkg_resources.resource_filename(__name__,'vsourceparser.so'+'.'+platform.system())

   __loader__ = None; del __bootstrap__, __loader__
   imp.load_dynamic(__name__,__file__)
__bootstrap__()
