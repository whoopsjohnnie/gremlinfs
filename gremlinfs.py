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

# 
from fuse import FUSE
from fuse import Operations
from fuse import FuseOSError


# 3.3.0
# http://tinkerpop.apache.org/docs/3.3.0-SNAPSHOT/reference/#gremlin-python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection



# 
# 
import config



# 
logging.basicConfig(level=config.gremlinfs['log_level'])



# 
# https://webkul.com/blog/string-and-bytes-conversion-in-python3-x/
# String===>(encode)==>Byte===>(decode)==>String
# 
# String Encode:
# 
#     encode(...) method of builtins.str instance
#     S.encode(encoding='utf-8', errors='strict') -> bytes
# 

def tobytes(data):

    if data:

        # ensure that we have a string
        try:

            data = str(data)

        except:
            pass

        try:

            # convert to byte
            data = data.encode(
                encoding='utf-8', 
                errors='strict'
            )

            return data

        except:
            return data

    return data

# 
# Bytes Decode:
# 
#     decode(...) method of builtins.bytes instance
# 
#     B.decode(encoding='utf-8', errors='strict') -> str
# 

def tostring(data):

    if data:

        try:

            # convert to string
            data = data.decode(
                encoding='utf-8', 
                errors='strict'
            )

            return data

        except:
            return data

    return data



class GremlinFSBase(object):

    def all(self, prefix = None):
        props = {}
        for prop, value in iteritems(vars(self)):
            if prefix:
                if prop and len(prop) > 0 and prop.startswith("_%s." % (prefix)):
                    props[prop.replace("_%s." % (prefix), "", 1)] = value
            else:
                if prop and len(prop) > 1 and prop[0] == "_":
                    props[prop[1:]] = value
        return props

    def keys(self, prefix = None):
        return self.all(prefix).keys()

    def has(self, key, prefix = None):
        if prefix:
            key = "%s.%s" % (prefix, key)
        if hasattr(self, "_%s" % (key)):
            return True
        return False

    def set(self, key, value, prefix = None):
        if key != "__class__":
            if prefix:
                key = "%s.%s" % (prefix, key)
            setattr(self, "_%s" % (key), value)

    def get(self, key, default = None, prefix = None):
        if not self.has(key, prefix = prefix):
            return default
        if prefix:
            key = "%s.%s" % (prefix, key)
        value = getattr(self, "_%s" % (key), default)
        return value

    def property(self, name, default = None, prefix = None):
        return self.get(name, default, prefix = prefix)



class GremlinFSPath(GremlinFSBase):

    @classmethod
    def paths(clazz):
        return {
            "root": {
                "type": "folder",
                "debug": False
            },
            "atpath": {
                "type": None,
                "debug": False
            },
            "vertex_labels": {
                "type": "folder",
                "debug": False
            },
            "vertex_label": {
                "type": "file",
                "debug": False
            },
            "vertexes": {
                "type": "folder",
                "debug": False
            },
            "vertex": {
                "type": "folder",
                "debug": False
            },
            "vertex_properties": {
                "type": "folder",
                "debug": False,
            },
            "vertex_folder_property": {
                "type": "folder",
                "debug": False,
            },
            "vertex_property": {
                "type": "file",
                "debug": False
            },
            "vertex_edges": {
                "type": "folder",
                "debug": False
            },
            "vertex_in_edges": {
                "type": "folder",
                "debug": False
            },
            "vertex_out_edges": {
                "type": "folder",
                "debug": False
            },
            "vertex_edge": {
                "type": "link",
                "debug": False
            },
            "vertex_in_edge": {
                "type": "link",
                "debug": False
            },
            "vertex_out_edge": {
                "type": "link",
                "debug": False
            },
            "create_vertex": {
                "type": "file",
                "debug": False
            },
        }

    @classmethod
    def expand(clazz, path):

        groupIDs = (os.path.normpath(os.path.splitdrive(path)[1]))
        if groupIDs == "/":
            return None
        else:
            if not "/" in groupIDs:
                return [groupIDs]
            else:
                groupIDs = groupIDs.split('/')

                if groupIDs[0] == "" and len(groupIDs) > 1:
                    return groupIDs[1:]
                else:
                    logging.error(' GremlinFS: Error parsing path [{}] '.format(path))
                    raise ValueError(' GremlinFS: Error parsing path [{}] '.format(path))

    @classmethod
    def match(clazz, path):

        match = {

            "path": None,
            "full": None,

            "vertexlabel": None,
            "vertexid": None,
            "vertexuuid": None,
            "vertexname": None,
            "vertexproperty": None,
            "vertexedge": None

        }

        match["full"] = clazz.expand(path)
        expanded = match["full"]

        if not expanded:
            match.update({
                "path": "root"
            })

        elif expanded and len(expanded) == 0:
            match.update({
                "path": "root"
            })

        elif expanded and GremlinFSUtils.conf("vertex_folder", ".V") in expanded:

            vindex = 0
            for i, item in enumerate(expanded):
                if item == GremlinFSUtils.conf("vertex_folder", ".V"):
                    vindex = i

            if len(expanded) == vindex + 1:
                match.update({
                    "path": "vertex_labels"
                })

            elif len(expanded) == vindex + 2:
                match.update({
                    "path": "vertexes",
                    "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] )
                })

            elif len(expanded) == vindex + 3:
                if expanded[vindex + 2] == "mkV":
                    match.update({
                        "path": "create_vertex",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                    })
                else:
                    match.update({
                        "path": "vertex",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 2] ),
                    })

            elif len(expanded) == vindex + 4:
                if expanded[vindex + 3] == 'EI':
                    match.update({
                        "path": "vertex_in_edges",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 2] )
                    })

                elif expanded[vindex + 3] == 'EO':
                    match.update({
                        "path": "vertex_out_edges",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 2] )
                    })

                # elif ... == ...:
                #     match.update({
                #         "path": "vertex_folder_property",
                #         "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                #         "vertexid": GremlinFSUtils.found( expanded[vindex + 2] ),
                #         "vertexproperty": GremlinFSUtils.found( expanded[vindex + 3] )
                #     })

                # else:
                #     # Note; Unless I throw this here, I am unable to
                #     # touch files as attributes. I think the default
                #     # here should be to throw FuseOSError(errno.ENOENT)
                #     # unless file/node is actually found
                #     raise FuseOSError(errno.ENOENT)
                else:
                    match.update({
                        "path": "vertex_property",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 2] ),
                        "vertexproperty": GremlinFSUtils.found( expanded[vindex + 3] )
                    })

            elif len(expanded) == vindex + 5:
                if expanded[vindex + 3] == "EI":
                    match.update({
                        "path": "vertex_in_edge",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 2] ),
                        "vertexedge": GremlinFSUtils.found( expanded[vindex + 4] )
                    })

                elif expanded[vindex + 3] == "EO":
                    match.update({
                        "path": "vertex_out_edge",
                        "vertexlabel": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 2] ),
                        "vertexedge": GremlinFSUtils.found( expanded[vindex + 4] )
                    })

        elif expanded and len(expanded) == 1:
            match.update({
                "path": "atpath",
                "name": expanded[0],
                "parent": None
            })

        elif expanded and len(expanded) == 2:
            match.update({
                "path": "atpath",
                "name": expanded[1],
                "parent": [expanded[0]] # Should be a list
            })

        elif expanded and len(expanded) > 2:
            match.update({
                "path": "atpath",
                "name": expanded[-1],
                "parent": expanded[0:-1]
            })

        if match and match["path"] in clazz.paths():
            match.update(
                clazz.paths()[match["path"]]
            )

        if match and "debug" in match and match["debug"]:
            logging.debug(' GremlinFSPath: MATCH: %s ' % (match.get("path")))
            logging.debug( match )

        return GremlinFSPath(**match)

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def g(self):
        return GremlinFS.operations().g()

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    def utils(self):
        return GremlinFSUtils.utils()

    # 

    def enter(self, functioname, *args, **kwargs):
        # logging.debug(' GremlinFSPath: ENTER: %s ' % (functioname))
        # logging.debug(args)
        # logging.debug(kwargs)
        pass

    # 

    def root(self):

        root = None
        if self.config().get("fs_root"):
            root = GremlinFSVertex.load(
                self.config().get("fs_root")
            )

        return root

    def path(self, path, node = None):

        if not node:
            node = self.root()

        if not path:
            return node

        elif path and len(path) == 1 and path[0] == "":
            return node

        elem = path[0]

        nodes = None
        if node:
            nodes = node.readFolderEntries()

        else:
            nodes = GremlinFSVertex.fromVs(
                self.g().V().where(
                    __.out(
                        self.config("in_label")
                    ).count().is_(0)
                )
            )

        if nodes:
            for cnode in nodes:
                if cnode.toid(True) == elem:
                    return self.path(path[1:], cnode)

        return None

    def node(self):

        # if not self._vertexid:
        #     return None

        if self._vertexid:
            # label = self._vertexlabel
            node = GremlinFSVertex.load( self._vertexid )
            if node:
                return node

        else:
            node = self.path( self._full )
            if node:
                return node

        return None

    def parent(self, path = []):

        parent = self.root()

        if not path:
            return parent

        vindex = 0
        for i, item in enumerate(path):
            if item == self.config("vertex_folder"):
                vindex = i

        if vindex:
            if vindex > 0:
                parent = self.path( path[0:vindex] )
                if parent:
                    return parent

        else:
            parent = self.path( path )
            if parent:
                return parent

        return parent

    # 

    def isFolder(self):

        default = False
        if self._type and self._type == "folder":
            default = True

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node()
            if node and node.isFolder():
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def isFile(self):

        default = False
        if self._type and self._type == "file":
            default = True

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node()
            if node and node.isFile():
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def isLink(self):

        default = False
        if self._type and self._type == "link":
            default = True

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node()
            if node and node.isLink():
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def isFound(self):

        default = False
        if self._type:
            default = True

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node()
            if node:
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        elif self._path == "vertex":
            node = self.node()
            if not node:
                return False
            return True

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            node = self.node()
            if not node:
                return False
            if not node.has(self._vertexproperty):
                return False
            else:
                return True

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        elif self._path == "vertex_in_edge":

            node = GremlinFSUtils.found( self.node() )
            edge = node.edge( self._vertexedge, True )

            if not edge:
                return False
            else:
                return True

        elif self._path == "vertex_out_edge":

            node = GremlinFSUtils.found( self.node() )
            edge = node.edge( self._vertexedge, False )

            if not edge:
                return False
            else:
                return True

        # elif self._path == "create_vertex":
        #     return default

        return default


    # 
    # Folder CRUD
    # 
    # - createFolder
    # - readFolder
    # - renameFolder
    # - deleteFolder
    # 


    def createFolder(self, mode = None):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            newname = GremlinFSVertex.infer("name", self._name)
            newlabel = GremlinFSVertex.infer("label", self._name, GremlinFS.operations().defaultFolderLabel())
            newlabel = GremlinFSVertex.label(newname, newlabel, "folder", GremlinFS.operations().defaultFolderLabel())
            newuuid = GremlinFSVertex.infer("uuid", self._name)
            parent = self.parent(self.get("parent", []))

            if not newname:
                raise FuseOSError(errno.ENOENT)

            parent = self.parent(self.get("parent", []))
            newfolder = GremlinFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).createFolder(
                parent = parent,
                mode = mode
            )

            return True

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        elif self._path == "vertex":

            newname = GremlinFSVertex.infer("name", self._name)
            newlabel = GremlinFSVertex.infer("label", self._name, "vertex")
            newlabel = GremlinFSVertex.label(newname, newlabel, "file", "vertex")
            newuuid = GremlinFSVertex.infer("uuid", self._name)
            parent = self.parent(self.get("parent", []))

            # Do not create an A vertex in /V/B, unless A is vertex
            if label != "vertex":
                if label != newlabel:
                    raise FuseOSError(errno.ENOENT)

            if not newname:
                raise FuseOSError(errno.ENOENT)

            if GremlinFS.operations().isFolderLabel(newlabel):
                newfolder = GremlinFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).createFolder(
                    parent = None,
                    mode = mode
                )

            else:
                newfile = GremlinFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).create(
                    parent = None,
                    mode = mode
                )

            return True

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def readFolder(self):

        entries = []

        if self._path == "root":
            entries.extend([
                self.config("vertex_folder")
            ])

            root = self.root()

            nodes = None
            if root:
                nodes = root.readFolderEntries()

            else:
                nodes = GremlinFSVertex.fromVs(
                    self.g().V().where(
                        __.out(
                            self.config("in_label")
                        ).count().is_(0)
                    )
                )

            if nodes:
                for node in nodes:
                    nodeid = node.toid(True)
                    if nodeid:
                        entries.append(nodeid)

            return entries

        elif self._path == "atpath":
            entries.extend([
                self.config("vertex_folder")
            ])
            parent = self.parent(self.get("full", []))
            nodes = parent.readFolderEntries()
            if nodes:
                for node in nodes:
                    nodeid = node.toid(True)
                    if nodeid:
                        entries.append(nodeid)

            return entries

        elif self._path == "vertex_labels":
            labels = self.g().V().label().dedup()
            if labels:
                for label in labels:
                    if label:
                        entries.append(label.strip().replace("\t", "").replace("\t", ""))

            return entries

        # elif self._path == "vertex_label":
        #     return entries

        elif self._path == "vertexes":
            label = self._vertexlabel
            nodes = None

            parent = self.parent(self.get("full", []))

            if parent:
                if label == "vertex":
                    nodes = GremlinFSVertex.fromVs(
                        self.g().V(
                            parent.get("id")
                        ).inE(
                            self.config("in_label")
                        ).has(
                            'name', self.config("in_name")
                        ).outV()
                    )
                else:
                    nodes = GremlinFSVertex.fromVs(
                        self.g().V(
                            parent.get("id")
                        ).inE(
                            self.config("in_label")
                        ).has(
                            'name', self.config("in_name")
                        ).outV().hasLabel(
                            label
                        )
                    )

            else:
                if label == "vertex":
                    nodes = GremlinFSVertex.fromVs(
                        self.g().V()
                    )
                else:
                    nodes = GremlinFSVertex.fromVs(
                        self.g().V().hasLabel(
                            label
                        )
                    )

            nodes = GremlinFSUtils.found( nodes )
            for node in nodes:
                nodeid = node.toid()
                if nodeid:
                    entries.append( nodeid )

            return entries

        elif self._path == "vertex":
            label = self._vertexlabel
            node = GremlinFSUtils.found( self.node() )
            entries.extend(node.keys())
            entries.extend(['EI', 'EO'])

            return entries

        # elif self._path == "vertex_properties":
        #     return entries

        # elif self._path == "vertex_folder_property":
        #     return entries

        # elif self._path == "vertex_property":
        #     return entries

        # elif self._path == "vertex_edges":
        #     return entries

        elif self._path == "vertex_in_edges":
            label = self._vertexlabel
            node = GremlinFSUtils.found( self.node() )
            nodes = GremlinFSVertex.fromVs(
                self.g().V(
                    node.get("id")
                ).inE()
            )
            if nodes:
                for cnode in nodes:
                    if cnode.get("label") and cnode.get("name"):
                        entries.append( "%s@%s" % (cnode.get("name"), cnode.get("label")) )
                    elif cnode.get("label"):
                        entries.append( "%s" % (cnode.get("label")) )

            return entries

        elif self._path == "vertex_out_edges":
            label = self._vertexlabel
            node = GremlinFSUtils.found( self.node() )
            nodes = GremlinFSVertex.fromVs(
                self.g().V(
                    node.get("id")
                ).outE()
            )
            if nodes:
                for cnode in nodes:
                    if cnode.get("label") and cnode.get("name"):
                        entries.append( "%s@%s" % (cnode.get("name"), cnode.get("label")) )
                    elif cnode.get("label"):
                        entries.append( "%s" % (cnode.get("label")) )

            return entries

        # elif self._path == "vertex_edge":
        #     return entries

        # elif self._path == "vertex_in_edge":
        #     return entries

        # elif self._path == "vertex_out_edge":
        #     return entries

        # elif self._path == "create_vertex":
        #     return entries

        return entries

    def renameFolder(self, newmatch):
        return self.moveNode(newmatch)

    def deleteFolder(self):
        return self.deleteNode()


    # 
    # File CRUD
    # 
    # - createFile
    # - readFile
    # - writeFile
    # - renameFile
    # - deleteFile
    # 


    def createFile(self, mode = None, data = None):

        if not data:
            data = ""

        default = data
        if self._type:
            default = data

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            newname = GremlinFSVertex.infer("name", self._name)
            newlabel = GremlinFSVertex.infer("label", self._name, "vertex")
            newlabel = GremlinFSVertex.label(newname, newlabel, "file", "vertex")
            newuuid = GremlinFSVertex.infer("uuid", self._name)
            parent = self.parent(self.get("parent", []))

            if not newname:
                raise FuseOSError(errno.ENOENT)

            newfile = GremlinFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).createFile(
                parent = parent,
                mode = mode
            )

            return True

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )
            node.setProperty(
                self._vertexproperty,
                data
            )

            return True

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def readFile(self, size = 0, offset = 0):
        data = self.readNode(size, offset)
        if data:
            return data
        return None

    def readFileLength(self):
        data = self.readNode()
        if data:
            return len(data)
        return 0

    def writeFile(self, data, offset = 0):
        return self.writeNode(data, offset)

    def clearFile(self):
        return self.clearNode()

    def renameFile(self, newmatch):
        return self.moveNode(newmatch)

    def deleteFile(self):
        return self.deleteNode()


    # 
    # Link CRUD
    # 
    # - createLink
    # - readLink
    # - renameLink
    # - deleteLink
    # 


    def createLink(self, sourcematch, mode = None):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            return default

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        elif self._path == "vertex_in_edge":

            node = self.node()

            # We are the target
            # To create an inbound link, we shall pass source=source and target=target
            # To create an outbound link, we shall pass source=target and target=source
            source = sourcematch.node()
            target = node

            label = GremlinFSEdge.infer("label", self._vertexedge, None)
            name = GremlinFSEdge.infer("name", self._vertexedge, None)

            if not label and name:
                label = name
                name = None

            # Create link from source to target
            # Inbound means source=target and target=source
            source.createLink(
                target = target,
                label = label,
                name = name,
                mode = mode
            )

            return True

        elif self._path == "vertex_out_edge":

            node = self.node()

            # We are the target
            # To create an inbound link, we shall pass source=sourcematch, target=node
            # To create an outbound link, we shall pass source=source and target=target
            source = node
            target = sourcematch.node()

            label = GremlinFSEdge.infer("label", self._vertexedge, None)
            name = GremlinFSEdge.infer("name", self._vertexedge, None)

            if not label and name:
                label = name
                name = None

            # Create link from source to target
            # Outbound means source=source and target=target
            source.createLink(
                target = target,
                label = label,
                name = name,
                mode = mode
            )

            return True

        # elif self._path == "create_vertex":
        #     return default

        return default        

    def readLink(self):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            return default

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        elif self._path == "vertex_in_edge":
            node = GremlinFSUtils.found( self.node() )
            edgenode = node.edgenode( self._vertexedge, True, False )

            newpath = self.utils().nodelink(
                edgenode
            )

            return newpath

        elif self._path == "vertex_out_edge":
            node = GremlinFSUtils.found( self.node() )
            edgenode = node.edgenode( self._vertexedge, False, True )

            newpath = self.utils().nodelink(
                edgenode
            )

            return newpath

        # elif self._path == "create_vertex":
        #     return default

        return default

    def deleteLink(self):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            return default

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        # elif self._path == "vertex_property":
        #     return default

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        elif self._path == "vertex_in_edge":

            node = self.node()

            label = GremlinFSEdge.infer("label", self._vertexedge, None)
            name = GremlinFSEdge.infer("name", self._vertexedge, None)

            if not label and name:
                label = name
                name = None

            if label and name:
                # we are the target, in edge means ...
                node.deleteLink(
                    label = label,
                    name = name,
                    ine = True
                )

            elif label:
                # we are the target, in edge means ...
                node.deleteLink(
                    label = label,
                    name = None,
                    ine = True
                )

            return True

        elif self._path == "vertex_out_edge":

            node = self.node()

            label = GremlinFSEdge.infer("label", self._vertexedge, None)
            name = GremlinFSEdge.infer("name", self._vertexedge, None)

            if not label and name:
                label = name
                name = None

            if label and name:
                # we are the target, out edge means ...
                node.deleteLink(
                    label = label,
                    name = name,
                    ine = False
                )

            elif label:
                # we are the target, out edge means ...
                node.deleteLink(
                    label = label,
                    name = None,
                    ine = False
                )

            return True

        # elif self._path == "create_vertex":
        #     return default

        return default

    # 

    def readNode(self, size = 0, offset = 0):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node().file()

            data = node.readProperty(
                self.config("data_property"),
                "",
                encoding = "base64"
            )

            data = tobytes(data)

            if data and size > 0 and offset > 0:
                return data[offset:offset + size]
            elif data and offset > 0:
                return data[offset:]
            elif data and size > 0:
                return data[0:size]
            else:
                return data

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )
            data = node.readProperty(
                self._vertexproperty,
                ""
            )

            data = tobytes(data)

            if size > 0 and offset > 0:
                return data[offset:offset + size]
            elif offset > 0:
                return data[offset:]
            elif size > 0:
                return data[0:size]
            else:
                return data

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def writeNode(self, data, offset = 0):

        default = data
        if self._type:
            default = data

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node().file()

            old = node.readProperty(
                self.config("data_property"),
                None,
                encoding = "base64"
            )

            old = tobytes(old)

            new = GremlinFSUtils.irepl(old, data, offset)

            new = tostring(new)

            node.writeProperty(
                self.config("data_property"),
                new,
                encoding = "base64"
            )

            return data

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )

            old = node.readProperty(
                self._vertexproperty,
                None
            )

            old = tobytes(old)

            new = GremlinFSUtils.irepl(old, data, offset)

            new = tostring(new)

            node.writeProperty(
                self._vertexproperty,
                new
            )

            return data

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def clearNode(self):

        default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node().file()

            node.writeProperty(
                self.config("data_property"),
                ""
            )

            return None

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )

            node.writeProperty(
                self._vertexproperty,
                ""
            )

            return None

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def moveNode(self, newmatch):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            node = GremlinFSUtils.found(self.node())
            parent = newmatch.parent(newmatch.get("parent", []))

            node.rename(newmatch._name)
            node.move(parent)

            return True

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":

            oldnode = GremlinFSUtils.found( self.node() )
            oldname = self._vertexproperty

            newnode = newmatch.node()
            newname = newmatch._vertexproperty

            data = ""

            data = oldnode.readProperty(
                oldname,
                ""
            )

            newnode.writeProperty(
                newname,
                data
            )

            newdata = newnode.readProperty(
                newname,
                ""
            )

            if newdata == data:

                oldnode.unsetProperty(
                    oldname
                )

            return True

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def deleteNode(self):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            node = GremlinFSUtils.found(self.node())
            node.delete()

            return True

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        # elif self._path == "vertex":
        #     return default

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            # label = self._vertexlabel
            node = GremlinFSUtils.found( self.node() )
            node.unsetProperty(
                self._vertexproperty
            )

            return True

        # elif self._path == "vertex_edges":
        #     return default

        # elif self._path == "vertex_in_edges":
        #     return default

        # elif self._path == "vertex_out_edges":
        #     return default

        # elif self._path == "vertex_edge":
        #     return default

        # elif self._path == "vertex_in_edge":
        #     return default

        # elif self._path == "vertex_out_edge":
        #     return default

        # elif self._path == "create_vertex":
        #     return default

        return default

    def setProperty(self, key, value):

        if self._path == "atpath":
            node = self.node()
            if node:
                node.setProperty(
                    key,
                    value
                )

        return True

    def getProperty(self, key, default = None):

        if self._path == "atpath":
            node = self.node()
            if node:
                return node.getProperty(
                    key,
                    default
            )

        return default



class GremlinFSNode(GremlinFSBase):

    @classmethod
    def parse(clazz, id):
        return None

    @classmethod
    def infer(clazz, field, obj, default = None):
        parts = clazz.parse( obj )
        if not field in parts:
            return default
        return parts.get(field, default)

    @classmethod
    def label(clazz, name, label, fstype = "file", default = "vertex"):
        return label

    @classmethod
    def vals(clazz, invals):
        if not invals:
            return {}
        vals = {}
        for key, val in invals.items():
            if key and "T.label" == str(key):
                vals['label'] = val
            elif key and "T.id" == str(key):
                vals['id'] = val['@value']
            # elif key and "uuid" in str(key):
            #     vals['uuid'] = val[0]
            # elif key and "name" in str(key):
            #     vals['name'] = val[0]
            elif key and type(val) in (tuple, list):
                vals[key] = val[0]
            else:
                vals[key] = val
        return vals

    @classmethod
    def fromMap(clazz, map):
        vals = clazz.vals(map)
        return clazz(**vals)

    @classmethod
    def fromMaps(clazz, maps):
        nodes = []
        for map in maps:
            vals = clazz.vals(map)
            nodes.append(clazz(**vals))
        return nodes

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def g(self):
        return GremlinFS.operations().g()

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    def utils(self):
        return GremlinFSUtils.utils()

    # 

    def toid(self, short = False):
        node = self
        mapid = node.get('id', None)
        mapuuid = node.get('uuid', None)
        maplabel = node.get('label', None)
        mapname = node.get('name', None)

        if mapname:
            mapname = mapname.strip().replace("\t", "").replace("\t", "")

        if mapname and mapuuid and maplabel and not short:
            return "%s@%s@%s" % (mapname, maplabel, mapuuid)

        elif mapname and maplabel and short:
            return mapname

        elif mapname and maplabel:
            return mapname

        elif mapname:
            return mapname

        elif mapuuid:
            return mapuuid

    def matches(self, inmap):
        node = self
        mapid = inmap.get('id', None)
        mapuuid = inmap.get('uuid', None)
        maplabel = inmap.get('label', None)
        mapname = inmap.get('name', None)

        if mapname and mapname == node.get('name', None) and \
            maplabel and maplabel == node.get('label', None):
            return True

        return False

    def hasProperty(self, name, prefix = None):

        node = self

        if not node:
            return False

        data = node.has(name, prefix = prefix)
        if not data:
            return False

        return True

    def getProperty(self, name, default = None, encoding = None, prefix = None):

        node = self

        if not node:
            return default

        data = node.get(name, None, prefix = prefix)
        if not data:
            return default

        if encoding:
            import base64
            data = tobytes(data)
            data = base64.b64decode(data)
            data = tostring(data)

        return data

    def setProperty(self, name, data = None, encoding = None, prefix = None):

        node = self

        if not node:
            return

        if not data:
            data = ""

        nodeid = node.get("id")

        if encoding:
            import base64
            data = tobytes(data)
            data = base64.b64encode(data)
            data = tostring(data)

        node.set(name, data, prefix = prefix)

        if prefix:
            name = "%s.%s" % (prefix, name)

        # GremlinFSVertex.fromV(
        self.g().V(
            nodeid
        ).property(
            name, data
        ).next()
        # )

        return data

    def unsetProperty(self, name, prefix = None):

        node = self

        if not node:
            return

        nodeid = node.get("id")

        node.set(name, None, prefix = prefix)

        if prefix:
            name = "%s.%s" % (prefix, name)

        # Having issues with exception throwing, even though deletion works
        try:
            # GremlinFSVertex.fromV(
            self.g().V(
                nodeid
            ).properties(
                name
            ).drop().next()
            # )
        except:
            pass

    def readProperty(self, name, default = None, encoding = None, prefix = None):
        return self.getProperty(name, default, encoding = encoding, prefix = prefix)

    def writeProperty(self, name, data, encoding = None, prefix = None):
        return self.setProperty(name, data, encoding = encoding, prefix = prefix)



class GremlinFSVertex(GremlinFSNode):

    @classmethod
    def parse(clazz, id):

        if not id:
            return {}

        # name.type@label@uuid
        # ^(.+)\.(.+)\@(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$
        matcher = re.match(
            r"^(.+)\.(.+)\@(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$",
            id
        )
        if matcher:
            nodenme = matcher.group(1)
            nodetpe = matcher.group(2)
            nodelbl = matcher.group(3)
            nodeuid = matcher.group(4)
            return {
                "name": "%s.%s" % (nodenme, nodetpe),
                "type": nodetpe,
                "label": nodelbl,
                "uuid": nodeuid
            }

        # name@label@uuid
        # ^(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$
        matcher = re.match(
            r"^(.+)\@(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$",
            id
        )
        if matcher:
            nodenme = matcher.group(1)
            nodelbl = matcher.group(2)
            nodeuid = matcher.group(3)
            return {
                "name": nodenme,
                "label": nodelbl,
                "uuid": nodeuid
            }

        # name.type@uuid
        # ^(.+)\.(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$
        matcher = re.match(
            r"^(.+)\.(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$",
            id
        )
        if matcher:
            nodenme = matcher.group(1)
            nodetpe = matcher.group(2)
            nodeuid = matcher.group(3)
            return {
                "name": "%s.%s" % (nodenme, nodetpe),
                "type": nodetpe,
                "uuid": nodeuid
            }

        # name@uuid
        # ^(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$
        matcher = re.match(
            r"^(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$",
            id
        )
        if matcher:
            nodenme = matcher.group(1)
            nodeuid = matcher.group(2)
            return {
                "name": nodenme,
                "uuid": nodeuid
            }

        # name.type
        # ^(.+)\.(.+)$
        matcher = re.match(
            r"^(.+)\.(.+)$",
            id
        )
        if matcher:
            nodenme = matcher.group(1)
            nodetpe = matcher.group(2)
            return {
                "name": "%s.%s" % (nodenme, nodetpe),
                "type": nodetpe
            }

        # uuid
        # ([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})
        matcher = re.match(
            r"^([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$",
            id
        )
        if matcher:
            nodeuid = matcher.group(1)
            return {
                "uuid": nodeuid
            }

        # Default to name
        return {
            "name": id
        }

    @classmethod
    def make(clazz, name, label, uuid = None):
        return clazz(name = name, label = label, uuid = uuid)

    @classmethod
    def load(clazz, id):

        parts = clazz.parse(id)
        if parts and \
            "uuid" in parts and \
            "name" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GremlinFSVertex.fromV(
                        GremlinFS.operations().g().V().has(
                            "uuid", parts["uuid"]
                        )
                    )
                else:
                    return GremlinFSVertex.fromV(
                        GremlinFS.operations().g().V().hasLabel(
                            parts["label"]
                        ).has(
                            "uuid", parts["uuid"]
                        )
                    )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        elif parts and \
            "uuid" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GremlinFSVertex.fromV(
                        GremlinFS.operations().g().V().has(
                            "uuid", parts["uuid"]
                        )
                    )
                else:
                    return GremlinFSVertex.fromV(
                        GremlinFS.operations().g().V().hasLabel(
                            parts["label"]
                        ).has(
                            "uuid", parts["uuid"]
                        )
                    )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        elif parts and \
            "uuid" in parts:
            try:
                return GremlinFSVertex.fromV(
                    GremlinFS.operations().g().V().has(
                        "uuid", parts["uuid"]
                    )
                )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        # Fallback try as straigt up DB id
        # OrientDB doesn't like invalid ID queries?
        elif id and ":" in id:
            try:
                return GremlinFSVertex.fromV(
                    GremlinFS.operations().g().V(
                        id
                    )
                )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        return None

    @classmethod
    def fromV(clazz, v):
        node = GremlinFSVertex.fromMap(
            v.valueMap(True).next()
        )
        return node

    @classmethod
    def fromVs(clazz, vs):
        return GremlinFSVertex.fromMaps(
            vs.valueMap(True).toList()
        )

    def edge(self, edgeid, ine = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            try:

                if ine:
                    return GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).has(
                            "name", name
                        )
                    )
                else:
                    return GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).has(
                            "name", name
                        )
                    )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

        elif node and label:

            try:

                if ine:
                    return GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        )
                    )
                else:
                    return GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        )
                    )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

        return None

    def edgenode(self, edgeid, ine = True, inv = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            try:

                if ine:
                    if inv:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).has(
                                "name", name
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).has(
                                "name", name
                            ).outV()
                        )

                else:
                    if inv:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).has(
                                "name", name
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).has(
                                "name", name
                            ).outV()
                        )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

        elif node and label:

            try:

                if ine:
                    if inv:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).outV()
                        )

                else:
                    if inv:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromV(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).outV()
                        )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

    def follow(self, edgeid ):
        node = self.edgenode( edgeid, False )
        if not node:
            return []
        return node

    # 

    def isFolder(self):

        node = self

        if not node:
            return False

        if not GremlinFS.operations().isFolderLabel(node.get("label")):
            return False

        return True

    def folder(self):

        node = self

        if not node:
            raise FuseOSError(errno.ENOENT)
        if not self.isFolder():
            raise FuseOSError(errno.ENOENT)
        return node

    def isFile(self):

        node = self

        if not node:
            return False

        if not GremlinFS.operations().isFileLabel(node.get("label")):
            return False

        return True

    def file(self):

        node = self

        if not node:
            raise FuseOSError(errno.ENOENT)
        if self.isFolder():
            raise FuseOSError(errno.EISDIR)
        elif not self.isFile():
            raise FuseOSError(errno.ENOENT)
        return node

    def create(self, parent = None, mode = None, owner = None, group = None):

        node = self

        UUID = node.get('uuid', None)
        label = node.get('label', None)
        name = node.get('name', None)

        if not name:
            return None

        if not mode:
            mode = GremlinFSUtils.conf("default_mode", 0o644)

        if not owner:
            owner = GremlinFSUtils.conf("default_uid", 0)

        if not group:
            group = GremlinFSUtils.conf("default_gid", 0)

        newnode = None

        try:

            pathuuid = None
            if UUID:
                pathuuid = uuid.UUID(UUID)
            else:
                pathuuid = uuid.uuid1()

            pathtime = time()

            # txn = self.graph().tx()

            newnode = None
            if label:
                newnode = self.g().addV(
                    label
                )
            else:
                newnode = self.g().addV()

            newnode.property(
                'name', name
            ).property(
                'uuid', str(pathuuid)
            ).property(
                'filesystem', self.config("fs_id")
            ).property(
                'created', int(pathtime)
            ).property(
                'modified', int(pathtime)
            ).property(
                'mode', mode
            ).property(
                'owner', owner
            ).property(
                'group', group
            )

            if parent:
                newnode.addE(
                    self.config("in_label")
                ).property(
                    'name', self.config("in_name")
                ).property(
                    'uuid', str(uuid.uuid1())
                ).to(__.V(
                    parent.get("id")
                )).next()
            else:
                newnode.next()

            newnode = GremlinFSVertex.fromV(
                self.g().V().has(
                    'uuid', str(pathuuid)
                )
            )

            # self.graph().tx().commit()

        except:
            logging.error(' GremlinFS: create exception ')
            traceback.print_exc()
            return None

        return newnode

    def rename(self, name):

        node = self

        if not node:
            return None

        # if not name:
        #     return None

        newnode = None

        if name:

            try:

                newnode = GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).has(
                        'filesystem', self.config("fs_id")
                    ).property(
                        'name', name
                    )
                )

            except:
                logging.error(' GremlinFS: rename exception ')
                traceback.print_exc()
                return None

        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                ).has(
                    'filesystem', self.config("fs_id")
                )
            )

        except:
            logging.error(' GremlinFS: rename exception ')
            traceback.print_exc()
            return None

        return newnode

    def move(self, parent = None):

        node = self

        if not node:
            return None

        # if not name:
        #     return None

        newnode = None

        # drop() on edges often/always? throw exceptions?
        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                ).has(
                    'filesystem', self.config("fs_id")
                ).outE(
                    self.config("in_label")
                ).has(
                    'name', self.config("in_name")
                ).drop()
            )

        except:
            pass

        if parent:

            try:

                newnode = GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).has(
                        'filesystem', self.config("fs_id")
                    ).addE(
                        self.config("in_label")
                    ).property(
                        'name', self.config("in_name")
                    ).property(
                        'uuid', str(uuid.uuid1())
                    ).to(__.V(
                        parent.get("id")
                    ))
                )

            except:
                logging.error(' GremlinFS: move exception ')
                traceback.print_exc()
                return None

        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                ).has(
                    'filesystem', self.config("fs_id")
                )
            )

        except:
            logging.error(' GremlinFS: move exception ')
            traceback.print_exc()
            return None

        return newnode

    def delete(self):

        node = self

        if not node:
            return None

        try:

            self.g().V(
                node.get("id")
            ).has(
                'filesystem', self.config("fs_id")
            ).drop().next()

        except:
            # logging.error(' GremlinFS: delete exception ')
            # traceback.print_exc()
            return False

        return True

    def createFolder(self, parent = None, mode = None, owner = None, group = None):

        node = self

        UUID = node.get('uuid', None)
        label = node.get('label', None)
        name = node.get('name', None)

        if not name:
            return None

        if not label:
            label = GremlinFS.operations().defaultFolderLabel()

        if not mode:
            mode = GremlinFSUtils.conf("default_mode", 0o644)

        if not owner:
            owner = GremlinFSUtils.conf("default_uid", 0)

        if not group:
            group = GremlinFSUtils.conf("default_gid", 0)

        newfolder = self.create(parent = parent, mode = mode, owner = owner, group = group)

        try:

            # txn = self.graph().tx()

            GremlinFSVertex.fromV(
                self.g().V(
                    newfolder.get("id")
                ).property(
                    'type', self.config("folder_label")
                ).property(
                    'in_label', self.config("in_label")
                ).property(
                    'in_name', self.config("in_name")
                ).property(
                    'query', "g.V('%s').has('uuid', '%s').has('type', '%s').inE('%s').outV()" % (
                        str(newfolder.get("id")),
                        str(newfolder.get("uuid")),
                        'group',
                        self.config("in_label")
                    )
                )
            )

            GremlinFSVertex.fromV(
                self.g().V(
                    newfolder.get("id")
                ).addE(
                    self.config("self_label")
                ).property(
                    'name', self.config("self_name")
                ).property(
                    'uuid', str(uuid.uuid1())
                ).to(__.V(
                    newfolder.get("id")
                ))
            )

            # self.graph().tx().commit()

        except:
            logging.error(' GremlinFS: createFolder exception ')
            traceback.print_exc()
            return None

        return newfolder

    def createFile(self, parent = None, mode = None, owner = None, group = None):
        return self.create(parent = parent, mode = mode, owner = owner, group = group)

    def createLink(self, target, label, name = None, mode = None, owner = None, group = None):

        source = self

        if not source:
            return None

        if not target:
            return None

        if not label:
            return None

        if not mode:
            mode = GremlinFSUtils.conf("default_mode", 0o644)

        if not owner:
            owner = GremlinFSUtils.conf("default_uid", 0)

        if not group:
            group = GremlinFSUtils.conf("default_gid", 0)

        newnode = None

        try:

            if name:

                newnode = GremlinFSVertex.fromV(
                    self.g().V(
                        source.get("id")
                    ).addE(
                        label
                    ).property(
                        'name', name
                    ).property(
                        'uuid', str(uuid.uuid1())
                    ).to(__.V(
                        target.get("id")
                    ))
                )

            else:

                newnode = GremlinFSVertex.fromV(
                    self.g().V(
                        source.get("id")
                    ).addE(
                        label
                    ).property(
                        'uuid', str(uuid.uuid1())
                    ).to(__.V(
                        target.get("id")
                    ))
                )

        except:
            logging.error(' GremlinFS: createLink exception ')
            traceback.print_exc()
            return None

        return newnode

    def deleteLink(self, label, name = None, ine = True):

        node = self

        if not node:
            return None

        if not label:
            return None

        newnode = None

        # drop() on edges often/always? throw exceptions?
        try:

            if name:

                if ine:
                    newnode = GremlinFSVertex.fromV(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).has(
                            'name', name
                        ).drop()
                    )

                else:
                    newnode = GremlinFSVertex.fromV(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).has(
                            'name', name
                        ).drop()
                    )

            else:

                if ine:
                    newnode = GremlinFSVertex.fromV(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).drop()
                    )

                else:
                    newnode = GremlinFSVertex.fromV(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).drop()
                    )

        except:
            pass

        return True

    def readFolderEntries(self):

        node = self

        if not node:
            return GremlinFSVertex.fromMaps(
                self.g().V().where(
                    __.out(
                        self.config("in_label")
                    ).count().is_(0)
                ).valueMap(True).toList()
            )

        else:
            return GremlinFSVertex.fromMaps(
                self.g().V(
                    node.get("id")
                ).inE(
                    self.config("in_label")
                ).outV().valueMap(True).toList()
            )

        return []



class GremlinFSEdge(GremlinFSNode):

    @classmethod
    def parse(clazz, id):

        if not id:
            return {}

        # name@label
        # ^(.+)\@(.+)$
        matcher = re.match(
            r"^(.+)\@(.+)$",
            id
        )
        if matcher:
            nodenme = matcher.group(1)
            nodelbl = matcher.group(2)
            return {
                "name": nodenme,
                "label": nodelbl
            }

        # default to label
        return {
            "label": id
        }

    @classmethod
    def make(clazz, name, label, uuid = None):
        return clazz(name = name, label = label, uuid = uuid)

    @classmethod
    def load(clazz, id):
        return None

    @classmethod
    def fromE(clazz, e):
        return GremlinFSEdge.fromMap(
            e.valueMap(True).next()
        )

    @classmethod
    def fromEs(clazz, es):
        return GremlinFSEdge.fromMaps(
            es.valueMap(True).toList()
        )


class GremlinFSUtils(GremlinFSBase):

    @classmethod
    def conf(clazz, key, default = None):
        if key in config.gremlinfs:
            return config.gremlinfs[key]
        return default

    @classmethod
    def missing(clazz, value):
        if value:
            raise FuseOSError(errno.EEXIST)

    @classmethod
    def found(clazz, value):
        if not value:
            raise FuseOSError(errno.ENOENT)
        return value

    @classmethod
    def irepl(clazz, old, data, index = 0):

        offset = index

        if not old:
            if data and index == 0:
                return data
            return None

        if not data:
            return old

        if index < 0:
            return old

        if offset > len(old):
            return old

        new = ""

        prefix = ""
        lprefix = 0

        infix = data
        linfix = len(data)

        suffix = None
        lsuffix = 0

        # if not old and index

        if offset > 0 and offset <= len(old):

            prefix = old[:offset]
            lprefix = len(prefix)

        if len(old) > lprefix + linfix:

            suffix = old[lprefix + linfix:]
            lsuffix = len(old)

        if lprefix > 0 and linfix > 0 and lsuffix > 0:
            new = prefix + infix + suffix
        elif lprefix > 0 and linfix > 0:
            new = prefix + infix
        elif linfix > 0 and lsuffix > 0:
            new = infix + suffix
        else:
            new = infix

        return new

    @classmethod
    def link(clazz, path):
        pass

    @classmethod
    def utils(clazz):
        return GremlinFSUtils()

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def g(self):
        return GremlinFS.operations().g()

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    # def utils(self):
    #     return GremlinFSUtils.utils()

    # 

    def nodelink(self, node, path = None):

        newpath = None

        if node and path:
            newpath = self.linkpath("%s/.V/%s/%s" % (
                path,
                node.get("label"),
                node.toid()
            ))
        elif node:
            newpath = self.linkpath("/.V/%s/%s" % (
                node.get("label", None),
                node.toid()
            ))

        return newpath

    def linkpath(self, path):

        if not path:
            return None

        return "%s%s" % (self.config("mount_point"), path)



class GremlinFSOperations(Operations):

    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    def __init__(
        self,
        **kwargs):

        self._g = None

        self._config = None

    # def __init__(
    def configure(
        self,

        mount_point,

        gremlin_host,
        gremlin_port,
        gremlin_username,
        gremlin_password,

        rabbitmq_host = None,
        rabbitmq_port = None,
        rabbitmq_username = None,
        rabbitmq_password = None,

        **kwargs):

        self.mount_point = mount_point

        logging.debug(' GremlinFS mount point: %s' % (str(self.mount_point)))

        self.gremlin_host = gremlin_host
        self.gremlin_port = gremlin_port
        self.gremlin_username = gremlin_username
        self.gremlin_password = gremlin_password

        self.gremlin_url = "ws://" + self.gremlin_host + ":" + self.gremlin_port + "/gremlin"

        logging.debug(' GremlinFS gremlin host: %s' % (str(self.gremlin_host)))
        logging.debug(' GremlinFS gremlin port: %s' % (str(self.gremlin_port)))
        logging.debug(' GremlinFS gremlin username: %s' % (str(self.gremlin_username)))
        # logging.debug(' GremlinFS gremlin password: %s' % (str(self.gremlin_password)))
        logging.debug(' GremlinFS gremlin URL: %s' % (str(self.gremlin_url)))

        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_username = rabbitmq_username
        self.rabbitmq_password = rabbitmq_password

        logging.debug(' GremlinFS rabbitmq host: %s' % (str(self.rabbitmq_host)))
        logging.debug(' GremlinFS rabbitmq port: %s' % (str(self.rabbitmq_port)))
        logging.debug(' GremlinFS rabbitmq username: %s' % (str(self.rabbitmq_username)))
        # logging.debug(' GremlinFS rabbitmq password: %s' % (str(self.rabbitmq_password)))

        self._g = None

        self._config = None

        return self

    def connection(self):

        graph = Graph()

        g = graph.traversal().withRemote(DriverRemoteConnection(
            self.gremlin_url,
            'g',
            username = self.gremlin_username,
            password = self.gremlin_password
        ))
        return g

    def g(self):

        if self._g:
            return self._g

        g = self.connection()
        self._g = g

        return self._g

    def config(self, key = None, default = None):

        if self._config:

            if key:
                return self._config.get(key, default)

            return self._config

        config = {

            "mount_point": self.mount_point,

            "gremlin_host": self.gremlin_host,
            "gremlin_port": self.gremlin_port,
            "gremlin_username": self.gremlin_username,
            "gremlin_password": self.gremlin_password,
            "gremlin_url": self.gremlin_url,

            "rabbitmq_host": self.rabbitmq_host,
            "rabbitmq_port": self.rabbitmq_port,
            "rabbitmq_username": self.rabbitmq_username,
            "rabbitmq_password": self.rabbitmq_password,

            "log_level": GremlinFSUtils.conf('log_level', logging.DEBUG),

            "fs_id": GremlinFSUtils.conf('fs_id', "gfs1"),
            "fs_root": self.getfs(
                GremlinFSUtils.conf('fs_root', None),
                GremlinFSUtils.conf('fs_root_init', False)
            ),
            "fs_root_init": GremlinFSUtils.conf('fs_root_init', False),

            "folder_label": GremlinFSUtils.conf('folder_label', 'group'),
            "ref_label": GremlinFSUtils.conf('ref_label', 'ref'),
            "in_label": GremlinFSUtils.conf('in_label', 'ref'),
            "self_label": GremlinFSUtils.conf('self_label', 'ref'),

            "in_name": GremlinFSUtils.conf('in_name', 'in0'),
            "self_name": GremlinFSUtils.conf('self_name', 'self0'),

            "vertex_folder": GremlinFSUtils.conf('vertex_folder', '.V'),
            "edge_folder": GremlinFSUtils.conf('edge_folder', '.E'),

            "uuid_property": GremlinFSUtils.conf('uuid_property', 'uuid'),
            "name_property": GremlinFSUtils.conf('name_property', 'name'),
            "data_property": GremlinFSUtils.conf('data_property', 'data'),

            "default_uid": GremlinFSUtils.conf('default_uid', 1001),
            "default_gid": GremlinFSUtils.conf('default_gid', 1001),
            "default_mode": GremlinFSUtils.conf('default_mode', 0o777),

            "labels": GremlinFSUtils.conf('labels', [])

        }

        # Build label regexes
        if "labels" in config:
            for pattern in config["labels"]:
                if "pattern" in pattern:
                    try:
                        pattern["compiled"] = re.compile(pattern["pattern"])
                    except:
                        logging.error(' GremlinFS: failed to compile pattern %s ' % (
                            pattern["pattern"]
                        ))
                    pass

        self._config = config

        if key:
            return self._config.get(key, default)

        return self._config

    def utils(self):
        return GremlinFSUtils.utils()

    # def initfs(self):
    # 
    #     newfs = GremlinFSVertex.make(
    #         name = self.config("fs_root"),
    #         label = ...,
    #         uuid = None
    #     ).createFolder(
    #         parent = None,
    #         mode = None
    #     )

    #     return newfs.get("id")

    def getfs(self, fsroot, fsinit = False):

        fsid = fsroot

        # if not ... and fsinit:
        #     fs = None
        #     fsid = self.initfs()

        return fsid

    def defaultLabel(self):
        return "vertex"

    def defaultFolderLabel(self):
        return self.config("folder_label")

    def isFileLabel(self, label):
        if self.isFolderLabel(label):
            return False
        return True

    def isFolderLabel(self, label):
        if label == self.defaultFolderLabel():
            return True
        return False

    # 

    def enter(self, functioname, *args, **kwargs):
        pass

    # 

    def __call__(self, op, *args):
        if not hasattr(self, op):
            raise FuseOSError(errno.EFAULT)
        return getattr(self, op)(*args)

    def access(self, path, amode):
        return 0

    # 

    def chmod(self, path, mode):
        self.notReadOnly()
        try:

            match = GremlinFSPath.match(path)
            match.enter("chmod", path, mode)
            if match:
                if match.isFound():
                    match.setProperty(
                        "mode", 
                        mode
                    )

                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: chmod exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        return 0

    def chown(self, path, uid, gid):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("chown", path, uid, gid)
            if match:
                if match.isFound():
                    match.setProperty(
                        "owner", 
                        uid
                    )
                    match.setProperty(
                        "group", 
                        gid
                    )

                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: chown exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        return 0

    def create(self, path, mode, fi=None):
        self.notReadOnly()

        created = False

        try:

            match = GremlinFSPath.match(path)
            match.enter("create", path, mode)
            if match:
                if not match.isFound():
                    created = match.createFile(mode)

                else:
                    # TODO: Wrong exception
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: create exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    # def destroy(self, path):
    #     pass

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    # def fsyncdir(self, path, datasync, fh):
    #     return 0

    def getattr(self, path, fh=None):

        now = time()
        owner = GremlinFSUtils.conf("default_uid", 0) # 1001 # 0
        group = GremlinFSUtils.conf("default_gid", 0) # 1001 # 0
        mode = GremlinFSUtils.conf("default_mode", 0o777) # 0o777
        # data = None

        attrs = {
            "st_ino": 0,
            "st_mode": 0,
            "st_nlink": 0,
            "st_uid": owner,
            "st_gid": group,
            "st_size": 0,
            "st_atime": now,
            "st_mtime": now,
            "st_ctime": now,
        }

        try:

            match = GremlinFSPath.match(path)
            match.enter("getattr", path)
            if match:
                if match.isFolder() and match.isFound():
                    attrs.update({
                        "st_mode": (stat.S_IFDIR | int( match.getProperty("mode", 0o777) ) ),
                        "st_nlink": int( match.getProperty("links", 1) ),
                        "st_uid": int( match.getProperty("owner", owner) ),
                        "st_gid": int( match.getProperty("group", group) ),
                        "st_size": 1024,
                        # "st_atime": match.getProperty("", now),
                        # "st_mtime": match.getProperty("modified", now),
                        # "st_ctime": match.getProperty("created", now)
                    })

                elif match.isFile() and match.isFound():
                    attrs.update({
                        "st_mode": (stat.S_IFREG | int( match.getProperty("mode", 0o777) ) ),
                        "st_nlink": int( match.getProperty("links", 1) ),
                        "st_uid": int( match.getProperty("owner", owner) ),
                        "st_gid": int( match.getProperty("group", group) ),
                        "st_size": match.readFileLength(),
                        # "st_atime": match.getProperty("", now),
                        # "st_mtime": match.getProperty("modified", now),
                        # "st_ctime": match.getProperty("created", now)
                    })

                elif match.isLink() and match.isFound():
                    attrs.update({
                        "st_mode": (stat.S_IFLNK | int( match.getProperty("mode", 0o777) ) ),
                        "st_nlink": int( match.getProperty("links", 1) ),
                        "st_uid": int( match.getProperty("owner", owner) ),
                        "st_gid": int( match.getProperty("group", group) ),
                        "st_size": 0,
                        # "st_atime": match.getProperty("", now),
                        # "st_mtime": match.getProperty("modified", now),
                        # "st_ctime": match.getProperty("created", now)
                    })

                else:
                    # Note; Unless I throw this here, I am unable to
                    # touch files as attributes. I think the default
                    # here should be to throw FuseOSError(errno.ENOENT)
                    # unless file/node is actually found
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: getattr exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        return attrs

    # def getxattr(self, path, name, position=0):
    #     raise FuseOSError(ENOTSUP)

    # def init(self, path):
    #     pass

    # def ioctl(self, path, cmd, arg, fip, flags, data):
    #     raise FuseOSError(errno.ENOTTY)

    def link(self, target, source):
        self.notReadOnly()
        raise FuseOSError(errno.ENOTTY)

    # def listxattr(self, path):
    #     return []

    # 

    def mkdir(self, path, mode):
        self.notReadOnly()

        created = False

        try:

            match = GremlinFSPath.match(path)
            match.enter("mkdir", path, mode)

            if match:
                if not match.isFound():
                    created = match.createFolder(mode)

                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: mkdir exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    def mknod(self, path, mode, dev):
        self.notReadOnly()
        raise FuseOSError(errno.ENOENT)

    def open(self, path, flags):

        found = False

        try:

            match = GremlinFSPath.match(path)
            match.enter("open", path, flags)
            if match:
                if match.isFile() and match.isFound():
                    found = True
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: open exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if found:
            return 0

        return 0

    # def opendir(self, path):
    #     return 0

    def read(self, path, size, offset, fh):

        data = None

        try:

            match = GremlinFSPath.match(path)
            match.enter("read", path, size, offset)
            if match:
                if match.isFile() and match.isFound():
                    data = match.readFile(size, offset)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: read exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if data:
            return data

        return None

    def readdir(self, path, fh):

        entries = [
            '.',
            '..'
        ]

        try:

            match = GremlinFSPath.match(path)
            match.enter("readdir", path)
            if match:
                if match.isFolder() and match.isFound():
                    entries.extend(
                        match.readFolder()
                    )
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            # logging.error(' GremlinFS: readdir exception ')
            # logging.error(sys.exc_info()[0])
            # traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        return entries

    def readlink(self, path):

        newpath = None

        try:

            match = GremlinFSPath.match(path)
            match.enter("readlink", path)
            if match:
                if match.isLink() and match.isFound():
                    newpath = match.readLink()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: readlink exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if newpath:
            return newpath

        return None

    def release(self, path, fh):
        return 0

    # def releasedir(self, path, fh):
    #     return 0

    # def removexattr(self, path, name):
    #     raise FuseOSError(ENOTSUP)

    def rename(self, old, new):
        self.notReadOnly()

        renamed = False

        try:

            oldmatch = GremlinFSPath.match(old)
            newmatch = GremlinFSPath.match(new)
            oldmatch.enter("rename", old, new)
            if oldmatch and newmatch:

                # if oldmatch.isFile() and \
                #    oldmatch.isFound() and \
                #    not newmatch.isFound():
                #    renamed = oldmatch.renameFile(newmatch)
                # elif oldmatch.isFolder() and \
                #    oldmatch.isFound() and \
                #    not newmatch.isFound():
                #    renamed = oldmatch.renameFolder(newmatch)

                if oldmatch.isFile() and \
                   oldmatch.isFound():
                    if newmatch.isFound():
                        newmatch.deleteFile()
                    renamed = oldmatch.renameFile(newmatch)
                elif oldmatch.isFolder() and \
                   oldmatch.isFound():
                    if newmatch.isFound():
                        newmatch.deleteFolder()
                    renamed = oldmatch.renameFolder(newmatch)

                else:
                    raise FuseOSError(errno.ENOENT)

        # except FuseOSError:
        #     # Don't log here
        #     raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: rename exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if renamed:
            return 0

        return 0

    def rmdir(self, path):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("rmdir", path)
            if match:
                if match.isFolder() and match.isFound():
                    match.deleteFolder()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: rmdir exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        return 0

    # def setxattr(self, path, name, value, options, position=0):
    #     raise FuseOSError(ENOTSUP)

    def statfs(self, path):
        return {}

    def symlink(self, target, source):
        self.notReadOnly()

        created = False

        try:

            targetmatch = GremlinFSPath.match(target)
            sourcematch = GremlinFSPath.match(source)

            targetmatch.enter("symlink", target, source)
            if targetmatch and sourcematch:
                if not targetmatch.isFound():
                    created = targetmatch.createLink(sourcematch)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: readlink exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    def truncate(self, path, length, fh=None):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("truncate", path)
            if match:
                if match.isFile() and match.isFound():
                    match.clearFile()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: truncate exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        # raise FuseOSError(errno.ENOENT)
        return 0

    def unlink(self, path):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("unlink", path)
            if match:
                if match.isFile() and match.isFound():
                    match.deleteFile()
                elif match.isLink() and match.isFound():
                    match.deleteLink()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: unlink exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        return 0

    def utimens(self, path, times=None):
        self.enter("utimens", path)
        return 0

    def write(self, path, data, offset, fh):
        self.notReadOnly()

        if not data:
            data = ""

        try:

            match = GremlinFSPath.match(path)
            match.enter("write", path, data, offset)
            if match:
                if match.isFile() and match.isFound():
                    data = match.writeFile(data, offset)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: write exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if data:
            return len(data)

        # raise FuseOSError(errno.ENOENT)
        return 0

    # 

    def isReadOnly(self):
        return False

    def notReadOnly(self):
        if self.isReadOnly():
            raise FuseOSError(errno.EROFS)
        return True



class GremlinFS(object):

    __operations = None

    @staticmethod
    def operations():

        if GremlinFS.__operations == None:
            GremlinFS.__operations = GremlinFSOperations()

        return GremlinFS.__operations 



def main(

    mount_point,

    gremlin_host,
    gremlin_port,
    gremlin_username,
    gremlin_password,

    rabbitmq_host = None,
    rabbitmq_port = None,
    rabbitmq_username = None,
    rabbitmq_password = None,

    **kwargs):

    try:

        operations = GremlinFS.operations().configure(

            mount_point = mount_point,

            gremlin_host = gremlin_host,
            gremlin_port = gremlin_port,
            gremlin_username = gremlin_username,
            gremlin_password = gremlin_password,

            rabbitmq_host = rabbitmq_host,
            rabbitmq_port = rabbitmq_port,
            rabbitmq_username = rabbitmq_username,
            rabbitmq_password = rabbitmq_password

        )

        FUSE(
            operations,
            mount_point,
            nothreads = True,
            foreground = True
        )

    except:
        logging.error(' GremlinFS: main/init exception ')
        traceback.print_exc()


def sysarg(
    args,
    index,
    default = None):
    if args and len(args) > 0 and index >= 0 and index < len(args):
        return args[index]
    return default


if __name__ == '__main__':

    mount_point = sysarg(sys.argv, 1)

    gremlin_host = sysarg(sys.argv, 2)
    gremlin_port = sysarg(sys.argv, 3)
    gremlin_username = sysarg(sys.argv, 4)
    gremlin_password = sysarg(sys.argv, 5)

    rabbitmq_host = sysarg(sys.argv, 6)
    rabbitmq_port = sysarg(sys.argv, 7)
    rabbitmq_username = sysarg(sys.argv, 8)
    rabbitmq_password = sysarg(sys.argv, 9)

    main(

        mount_point = mount_point,

        gremlin_host = gremlin_host,
        gremlin_port = gremlin_port,
        gremlin_username = gremlin_username,
        gremlin_password = gremlin_password,

        rabbitmq_host = rabbitmq_host,
        rabbitmq_port = rabbitmq_port,
        rabbitmq_username = rabbitmq_username,
        rabbitmq_password = rabbitmq_password

    )
