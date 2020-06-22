# 
# Copyright (c) 2019, John Grundback
# All rights reserved.
# 

# 
from __future__ import print_function
from future.utils import iteritems

# 
import os
import sys
import logging
import errno
import stat
import uuid
import re
import traceback
import string

import contextlib

# 
from time import time

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO



class GremlinFSObj():

    def __init__(self, **kwargs):

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        # super().__init__()

        self.setall(kwargs);

    def fromobj(self, obj):
        for key, val in obj.items():
            if key and "T.label" == str(key):
                self.set('label', val)
            elif key and "T.id" == str(key):
                self.set('id', val['@value'])
            elif key and type(val) in (tuple, list):
                self.set(key, val[0])
            else:
                self.set(key, val)

    def setall(self, _dict_ = {}, prefix = None):
        for key in dict(_dict_):
            value = _dict_[key] # .get(key, None)
            self.set(key, value, prefix)

    def getall(self, prefix = None):
        props = {}
        for prop, value in iteritems(vars(self)):
            if prefix:
                if prop and len(prop) > 0 and prop.startswith("_" + prefix + "."):
                    props[prop.replace("_" + prefix + ".", "", 1)] = value
            else:
                if prop and len(prop) > 1 and prop[0] == "_":
                    props[prop[1:]] = value
        return props

    def all(self, prefix = None):
        return self.getall(prefix)

    def keys(self, prefix = None):
        return self.getall(prefix).keys()

    def has(self, key, prefix = None):
        if prefix:
            key = prefix + "." + key
        if hasattr(self, "_" + key):
            return True
        return False

    def set(self, key, value, prefix = None):
        if key != "__class__":
            if prefix:
                key = prefix + "." + key
            setattr(self, "_" + key, value)

    def get(self, key, _default_ = None, prefix = None):
        if not self.has(key, prefix):
            return _default_
        if prefix:
            key = prefix + "." + key
        value = getattr(self, "_" + key, _default_)
        # if isinstance(value, GremlinFSList):
        #     return value.tolist()

        return value



class GremlinFSList():

    def __init__(self, **kwargs):
        self._list = []

    def __str__(self):
        return str(self._list)

    def all(self):
        return self._list

    def fromlist(self, list):
        self._list = list

    def tolist(self):
        ret = []
        getall = self._list
        for val in getall:
            if isinstance(val, GremlinFSMap):
                ret.append(val.tomap())
            elif isinstance(val, GremlinFSList):
                ret.append(val.tolist())
            else:
                ret.append(val)
        return ret

    def append(self, item):
        self._list.append(item)
        return self._list

    def extend(self, list):
        self._list.extend(list)
        return self._list



class GremlinFSMap(GremlinFSObj):

    def __init__(self, **kwargs):

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

        self.setall(kwargs);

    def __str__(self):
        return str(self)

    def frommap(self, map):
        self.setall(map)

    def tomap(self):
        ret = {}
        getall = self.getall()
        for key in getall:
            val = getall[key]
            if isinstance(val, GremlinFSMap):
                ret[key] = val.tomap()
            elif isinstance(val, GremlinFSList):
                ret[key] = val.tolist()
            else:
                ret[key] = val
        return ret

    def update(self, map):
        self.setall(map)



def gfslist(list = []):
    gfslist = GremlinFSList()
    gfslist.fromlist(list)
    return gfslist



def gfsmap(map = {}):
    gfsmap = GremlinFSMap()
    gfsmap.frommap(map)
    return gfsmap



__all__ = [
    'GremlinFSObj',
    'GremlinFSList',
    'gfslist',
    'gfsmap'
]

__default__ = 'GremlinFSObj'
