# 
# Copyright (c) 2019, 2020, John Grundback
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
import string

import contextlib

# 
from time import time

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# 3.3.0
# http://tinkerpop.apache.org/docs/3.3.0-SNAPSHOT/reference/#gremlin-python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph, Vertex, Edge
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 
import pika

# 
from .gremlinfslog import GremlinFSLogger
from .gremlinfsobj import GremlinFSObj
from .gremlinfsobj import gfslist
from .gremlinfsobj import gfsmap

# 
# 
import config



class GremlinFSError(Exception):

    def __init__(self, path = None):
        self.path = path



class GremlinFSExistsError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSNotExistsError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSIsFileError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSIsFolderError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSBase(GremlinFSObj):

    logger = GremlinFSLogger.getLogger("GremlinFSBase")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def property(self, name, default = None, prefix = None):
        return self.get(name, default, prefix = prefix)



class GremlinFSPath(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSPath")

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
    def path(clazz, path):
        paths = GremlinFSPath.paths()
        if paths and path in paths:
            return paths[path]
        return None

    @classmethod
    def expand(clazz, path):
        return GremlinFS.operations().utils().splitpath(path)

    @classmethod
    def atpath(clazz, path, node = None):

        if not node:
            root = None
            if GremlinFS.operations().config("fs_root", None):
                root = GremlinFSVertex.load(
                    GremlinFS.operations().config("fs_root", None)
                )
            node = root

        if not path:
            return node

        elif path and len(path) == 0:
            return node

        elif path and len(path) == 1 and path[0] == "":
            return node

        elem = path[0]

        nodes = None
        if node:
            nodes = node.readFolderEntries()

        else:
            nodes = GremlinFSVertex.fromVs(
                GremlinFS.operations().g().V().where(
                    GremlinFS.operations().a().out(
                        GremlinFS.operations().config("in_label", "in")
                    ).count().is_(0)
                )
            )

        if nodes:
            for cnode in nodes:
                if cnode.toid(True) == elem:
                    return GremlinFSPath.atpath(path[1:], cnode)

        return None

    @classmethod
    def pathnode(clazz, nodeid, parent, path):

        node = None

        if parent and nodeid:
            nodes = parent.readFolderEntries()
            if nodes:
                for cnode in nodes:
                    if cnode and cnode.get("name") == nodeid:
                        node = cnode
                        break

        elif nodeid:
            node = GremlinFSVertex.load( nodeid )

        elif path:
            node = GremlinFSPath.atpath( path )

        return node

    @classmethod
    def pathparent(clazz, path = []):

        root = None
        if GremlinFS.operations().config("fs_root", None):
            root = GremlinFSVertex.load(
                GremlinFS.operations().config("fs_root", None)
            )

        parent = root

        if not path:
            return parent

        vindex = 0
        for i, item in enumerate(path):
            if item == GremlinFS.operations().config("vertex_folder", ".V"):
                vindex = i

        if vindex:
            if vindex > 0:
                parent = GremlinFSPath.atpath( path[0:vindex] )

        else:
            parent = GremlinFSPath.atpath( path )

        return parent

    @classmethod
    def match(clazz, path):

        match = {

            "path": None,
            "full": None,
            "parent": None,
            "node": None,
            "name": None,

            "vertexlabel": "vertex",
            "vertexid": None,
            "vertexuuid": None,
            "vertexname": None,
            "vertexproperty": None,
            "vertexedge": None

        }

        match.update({
            "full": GremlinFS.operations().utils().splitpath(path)
        })
        expanded = match.get("full", [])

        if not expanded:
            match.update({
                "path": "root"
            })

        elif expanded and len(expanded) == 0:
            match.update({
                "path": "root"
            })

        elif expanded and GremlinFS.operations().config("vertex_folder", ".V") in expanded:

            vindex = 0
            for i, item in enumerate(expanded):
                if item == GremlinFS.operations().config("vertex_folder", ".V"):
                    vindex = i

            if len(expanded) == vindex + 1:

                parent = GremlinFSPath.pathparent(
                     expanded
                )

                if parent:
                    match.update({
                        "parent": parent
                    })

                match.update({
                    "path": "vertexes",
                })

            elif len(expanded) == vindex + 2:

                parent = GremlinFSPath.pathparent(
                     expanded
                )

                if parent:
                    match.update({
                        "parent": parent
                    })

                node = GremlinFSPath.pathnode(
                    expanded[vindex + 1],
                    match.get("parent", None),
                    match.get("full", None),
                )

                if node:
                    match.update({
                        "node": node
                    })

                match.update({
                    "path": "vertex",
                    "vertexid": GremlinFSUtils.found( expanded[vindex + 1] ),
                })

            elif len(expanded) == vindex + 3:

                parent = GremlinFSPath.pathparent(
                     expanded
                )

                if parent:
                    match.update({
                        "parent": parent
                    })

                node = GremlinFSPath.pathnode(
                    expanded[vindex + 1],
                    match.get("parent", None),
                    match.get("full", None),
                )

                if node:
                    match.update({
                        "node": node
                    })

                if expanded[vindex + 2] == GremlinFS.operations().config("in_edge_folder", "EI"):
                    match.update({
                        "path": "vertex_in_edges",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] )
                    })

                elif expanded[vindex + 2] == GremlinFS.operations().config("out_edge_folder", "EO"):
                    match.update({
                        "path": "vertex_out_edges",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] )
                    })

                # else:
                #     # Note; Unless I throw this here, I am unable to
                #     # touch files as attributes. I think the default
                #     # here should be to throw GremlinFSNotExistsError
                #     # unless file/node is actually found
                #     raise GremlinFSNotExistsError
                else:
                    match.update({
                        "path": "vertex_property",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexproperty": GremlinFSUtils.found( expanded[vindex + 2] )
                    })

            elif len(expanded) == vindex + 4:

                parent = GremlinFSPath.pathparent(
                     expanded
                )

                if parent:
                    match.update({
                        "parent": parent
                    })

                node = GremlinFSPath.pathnode(
                    expanded[vindex + 1],
                    match.get("parent", None),
                    match.get("full", None),
                )

                if node:
                    match.update({
                        "node": node
                    })

                if expanded[vindex + 2] == GremlinFS.operations().config("in_edge_folder", "EI"):
                    match.update({
                        "path": "vertex_in_edge",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexedge": GremlinFSUtils.found( expanded[vindex + 3] )
                    })

                elif expanded[vindex + 2] == GremlinFS.operations().config("out_edge_folder", "EO"):
                    match.update({
                        "path": "vertex_out_edge",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexedge": GremlinFSUtils.found( expanded[vindex + 3] )
                    })

        elif expanded and len(expanded) == 1:
            match.update({
                "path": "atpath",
                "name": expanded[0],
                "parent": None,
                "node": GremlinFSPath.pathnode(
                    match.get("vertexid", None),
                    None,
                    match.get("full", None),
                )
            })

        elif expanded and len(expanded) == 2:
            match.update({
                "path": "atpath",
                "name": expanded[1],
                "parent": GremlinFSPath.pathparent([expanded[0]])
            })
            match.update({
                "node": GremlinFSPath.pathnode(
                    match.get("vertexid", None),
                    match.get("parent", None),
                    match.get("full", None),
                )
            })

        elif expanded and len(expanded) > 2:
            match.update({
                "path": "atpath",
                "name": expanded[-1],
                "parent": GremlinFSPath.pathparent(expanded[0:-1])
            })
            match.update({
                "node": GremlinFSPath.pathnode(
                    match.get("vertexid", None),
                    match.get("parent", None),
                    match.get("full", None),
                )
            })

        # if match and match.get("path") in GremlinFSPath.paths():
        #     match.update(
        #         GremlinFSPath.paths()[clazz.paths()[match.get("path")]]
        #     )

        match.update(
            GremlinFSPath.path(match.get("path"))
        )

        debug = False
        if match and match.get("debug", False):
            debug = True

        # if debug:
        #     clazz.logger.debug(' GremlinFSPath: MATCH: ' + match.get("path"))
        #     clazz.logger.debug( match )

        return GremlinFSPath(

            type = match.get("type"),
            debug = debug, # match.get("debug"),

            path = match.get("path"),
            full = match.get("full"),
            parent = match.get("parent"),
            node = match.get("node"),
            name = match.get("name"),

            vertexlabel = match.get("vertexlabel"),
            vertexid = match.get("vertexid"),
            vertexuuid = match.get("vertexuuid"),
            vertexname = match.get("vertexname"),
            vertexproperty = match.get("vertexproperty"),
            vertexedge = match.get("vertexedge")

        )

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def g(self):
        return GremlinFS.operations().g()

    def ro(self):
        return GremlinFS.operations().ro()

    def a(self):
        return GremlinFS.operations().a()

    def mq(self):
        return GremlinFS.operations().mq()

    def mqevent(self, event):
        return GremlinFS.operations().mqevent(event)

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    def utils(self):
        return GremlinFS.operations().utils()

    # 

    def enter(self, functioname, *args, **kwargs):
        # self.logger.debug(' GremlinFSPath: ENTER: %s ' % (functioname))
        # self.logger.debug(args)
        # self.logger.debug(kwargs)
        pass

    # 

    def root(self):

        root = None
        if self.config("fs_root"):
            root = GremlinFSVertex.load(
                self.config("fs_root")
            )

        return root

    def node(self):
        return self._node

    def parent(self):
        return self._parent

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
            if node:
                return True
            return False

        # elif self._path == "vertex_properties":
        #     return default

        # elif self._path == "vertex_folder_property":
        #     return default

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )
            if node.has( self._vertexproperty ):
                return True
            elif node.edge( self._vertexproperty, False ):
                return True
            return False

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
            if node.edge( self._vertexedge, True ):
                return True
            return False

        elif self._path == "vertex_out_edge":
            node = GremlinFSUtils.found( self.node() )
            if node.edge( self._vertexedge, False ):
                return True
            return False

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


    def createFolder(self):

        if self.isFound():
            raise GremlinFSExistsError(self)

        if self.isFile():
            raise GremlinFSIsFileError(self)

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
            parent = self.parent()

            if not newname:
                raise GremlinFSNotExistsError(self)

            parent = self.parent()
            # newfolder = 
            GremlinFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).createFolder(
                parent = parent
            )

#             self.mqevent(GremlinFSEvent(
#                 event = "create_node",
#                 node = newfolder
#             ))

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
            parent = self.parent()

            # Do not create an A vertex in /V/B, unless A is vertex
            if newlabel != "vertex":
                if newlabel != newlabel:
                    raise GremlinFSNotExistsError(self)

            if not newname:
                raise GremlinFSNotExistsError(self)

            if GremlinFS.operations().isFolderLabel(newlabel):
                # newfolder = 
                GremlinFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).createFolder(
                    parent = None
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "create_node",
#                     node = newfolder
#                 ))

            else:
                # newfile = 
                GremlinFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).create(
                    parent = None
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "create_node",
#                     node = newfile
#                 ))

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

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
                        GremlinFS.operations().a().out(
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
            parent = self.node()
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
            if not label:
                label = "vertex"

            short = False

            parent = self.parent()
            nodes = None

            if parent:
                short = True
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
                nodeid = node.toid( short )
                if nodeid:
                    entries.append( nodeid )

            return entries

        elif self._path == "vertex":
            label = self._vertexlabel
            if not label:
                label = "vertex"

            node = GremlinFSUtils.found( self.node() )
            entries.extend(node.keys())
            entries.extend([
                GremlinFS.operations().config("in_edge_folder", "EI"), 
                GremlinFS.operations().config("out_edge_folder", "EO"),
            ])

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
            if not label:
                label = "vertex"

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
            if not label:
                label = "vertex"

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        return self.moveNode(newmatch)

    def deleteFolder(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

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


    def createFile(self, data = None):

        if self.isFound():
            raise GremlinFSExistsError(self)

        if self.isFolder():
            raise GremlinFSIsFolderError(self)

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
            parent = self.parent()

            if not newname:
                raise GremlinFSNotExistsError(self)

            # newfile = 
            GremlinFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).create(
                parent = parent
            )

#             self.mqevent(GremlinFSEvent(
#                 event = "create_node",
#                 node = newfile
#             ))

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

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        data = self.readNode(size, offset)
        if data:
            return data
        return None

    def readFileLength(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        data = self.readNode()
        if data:
            try:
                return len(data)
            except Exception as e:
                pass

        return 0

    def writeFile(self, data, offset = 0):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        return self.writeNode(data, offset)

    def clearFile(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        return self.clearNode()

    def renameFile(self, newmatch):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        return self.moveNode(newmatch)

    def deleteFile(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        return self.deleteNode()


    # 
    # Link CRUD
    # 
    # - createLink
    # - readLink
    # - renameLink
    # - deleteLink
    # 


    def createLink(self, sourcematch):

        if self.isFound():
            raise GremlinFSExistsError(self)

        if not sourcematch.isFound():
            raise GremlinFSNotExistsError(self)

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
            # newlink = 
            source.createLink(
                target = target,
                label = label,
                name = name
            )

#             self.mqevent(GremlinFSEvent(
#                 event = "create_link",
#                 link = newlink,
#                 source = source,
#                 target = target
#             ))

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
            # newlink = 
            source.createLink(
                target = target,
                label = label,
                name = name
            )

#             self.mqevent(GremlinFSEvent(
#                 event = "create_link",
#                 link = newlink,
#                 source = source,
#                 target = target
#             ))

            return True

        # elif self._path == "create_vertex":
        #     return default

        return default        

    def readLink(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

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
#                 link = node.getLink(
#                     label = label,
#                     name = name,
#                     ine = True
#                 )

                node.deleteLink(
                    label = label,
                    name = name,
                    ine = True
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "delete_link",
#                     link = link
#                 ))

            elif label:
                # we are the target, in edge means ...
#                 link = node.getLink(
#                     label = label,
#                     name = None,
#                     ine = True
#                 )

                node.deleteLink(
                    label = label,
                    name = None,
                    ine = True
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "delete_link",
#                     link = link
#                 ))

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
#                 link = node.getLink(
#                     label = label,
#                     name = name,
#                     ine = False
#                 )

                node.deleteLink(
                    label = label,
                    name = name,
                    ine = False
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "delete_link",
#                     link = link
#                 ))

            elif label:
                # we are the target, out edge means ...
#                 link = node.getLink(
#                     label = label,
#                     name = None,
#                     ine = False
#                 )

                node.deleteLink(
                    label = label,
                    name = None,
                    ine = False
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "delete_link",
#                     link = link
#                 ))

            return True

        # elif self._path == "create_vertex":
        #     return default

        return default

    # 

    def readNode(self, size = 0, offset = 0):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node().file()

            data = node.render()

            if data:
                data = self.utils().tobytes(data)

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

            data = self.utils().tobytes(data)

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        default = data
        if self._type:
            default = data

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":
            node = self.node().file()

            label_config = node.labelConfig()

            writefn = None

            old = node.readProperty(
                self.config("data_property"),
                None,
                encoding = "base64"
            )

            old = self.utils().tobytes(old)

            new = GremlinFSUtils.irepl(old, data, offset)

            new = self.utils().tostring(new)

            node.writeProperty(
                self.config("data_property"),
                new,
                encoding = "base64"
            )

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

            try:

                if label_config and "writefn" in label_config:
                    writefn = label_config["writefn"]

            except Exception as e:
                pass

            try:

                if writefn:

                    writefn(
                        node = node,
                        data = data
                    )

            except Exception as e:
                pass

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

            old = self.utils().tobytes(old)

            new = GremlinFSUtils.irepl(old, data, offset)

            new = self.utils().tostring(new)

            node.writeProperty(
                self._vertexproperty,
                new
            )

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

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

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

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

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        # if newmatch.isFound():
        #     raise GremlinFSExistsError(self)

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            node = GremlinFSUtils.found(self.node())
            parent = newmatch.parent()

            node.rename(newmatch._name)
            node.move(parent)

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

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

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = newnode
#             ))

            newdata = newnode.readProperty(
                newname,
                ""
            )

            if newdata == data:

                oldnode.unsetProperty(
                    oldname
                )

#                 self.mqevent(GremlinFSEvent(
#                     event = "update_node",
#                     node = oldnode
#                 ))

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            node = GremlinFSUtils.found(self.node())
            node.delete()

#             self.mqevent(GremlinFSEvent(
#                 event = "delete_node",
#                 node = node
#             ))

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

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

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

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        if self._path == "atpath":
            node = self.node()
            if node:
                node.setProperty(
                    key,
                    value
                )

#             self.mqevent(GremlinFSEvent(
#                 event = "update_node",
#                 node = node
#             ))

        return True

    def getProperty(self, key, default = None):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        if self._path == "atpath":
            node = self.node()
            if node:
                return node.getProperty(
                    key,
                    default
            )

        return default



class GremlinFSNode(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSNode")

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
        if not name:
            return default
        if not label:
            return default
        for label_config in GremlinFS.operations().config("labels", []):
            if "type" in label_config and label_config["type"] == fstype:
                compiled = None
                if "compiled" in label_config:
                    compiled = label_config["compiled"]
                else:
                    compiled = GremlinFS.operations().utils().recompile(label_config["pattern"])

                if compiled:
                    if compiled.search(name):
                        label = label_config.get("label", default)
                        break

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

    @classmethod
    def fromVal(clazz, val = [], names = []):
        vals = clazz.vals(map)
        return clazz(**vals)

    @classmethod
    def fromVals(clazz, vals = [], names = []):
        nodes = []
        for val in vals:
            vals = {}
            for i, name in enumerate(names):
                vals[name] = val[i]
            nodes.append(clazz(**vals))
        return nodes

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def g(self):
        return GremlinFS.operations().g()

    def ro(self):
        return GremlinFS.operations().ro()

    def a(self):
        return GremlinFS.operations().a()

    def mq(self):
        return GremlinFS.operations().mq()

    def mqevent(self, event):
        return GremlinFS.operations().mqevent(event)

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    def utils(self):
        return GremlinFS.operations().utils()

    def labelConfig(self):
        node = self
        config = None

        for label_config in GremlinFS.operations().config("labels", []):
            if "label" in label_config and label_config["label"] == node.get('label', None):
                config = label_config

        return config

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
            if maplabel == "vertex":
                return "%s@%s" % (mapname, mapuuid)
            else:
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
            data = self.utils().tobytes(data)
            data = self.utils().decode(data, encoding)
            data = self.utils().tostring(data)

        return data

    def setProperty(self, name, data = None, encoding = None, prefix = None):

        node = self

        if not node:
            return

        if not data:
            data = ""

        nodeid = node.get("id")

        if encoding:
            data = self.utils().tobytes(data)
            data = self.utils().encode(data, encoding)
            data = self.utils().tostring(data)

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

        self.mqevent(GremlinFSEvent(
            event = "update_node",
            node = node
        ))

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
        # next() throws errors on delete
        try:
            # GremlinFSVertex.fromV(
            self.g().V(
                nodeid
            ).properties(
                name
            ).drop().toList() # .next()
            # )
        except Exception as e:
            self.logger.exception(' GremlinFS: unsetProperty exception ', e)

        self.mqevent(GremlinFSEvent(
            event = "update_node",
            node = node
        ))

    def setProperties(self, properties, prefix = None):

        node = self

        existing = {}

        existing.update(node.all(prefix))

        if existing:
            for key, value in existing.items():
                if not key in properties:
                    node.unsetProperty(
                        key,
                        prefix = prefix
                    )

        if properties:
            for key, value in properties.items():
                try:
                    node.setProperty(
                        key,
                        value,
                        prefix = prefix
                    )
                except Exception as e:
                    self.logger.exception(' GremlinFS: setProperties exception ', e)

    def getProperties(self, prefix = None):

        node = self

        properties = {}
        properties.update(node.all(prefix))

        return properties

    def readProperty(self, name, default = None, encoding = None, prefix = None):
        return self.getProperty(name, default, encoding = encoding, prefix = prefix)

    def writeProperty(self, name, data, encoding = None, prefix = None):
        return self.setProperty(name, data, encoding = encoding, prefix = prefix)

    def invoke(self, handler, event, chain = [], data = {}):

        import subprocess

        node = self

        try:

            path = data.get("path", None)
            property = data.get("property", None)
            value = data.get("value", None)

            if node and handler:

                handlercwd = "%s/%s/vertex/%s" % (
                    self.config("mount_point"),
                    self.config("vertex_folder"),
                    node.get("uuid")
                )

                handlerpath = "%s/%s/vertex/%s/%s" % (
                    self.config("mount_point"),
                    self.config("vertex_folder"),
                    node.get("uuid"),
                    handler
                )

                env = {}

                cwd = handlercwd
                args = [handlerpath]

                if node:

                    env["UUID"] = node.get("uuid", None)
                    args.append("-i")
                    args.append(node.get("uuid", None))

                    env["NAME"] = node.get("name", None)
                    args.append("-n")
                    args.append(node.get("name", None))

                if event:

                    env["EVENT"] = event
                    args.append("-e")
                    args.append(event)

                if property:

                    env["PROPERTY"] = property
                    args.append("-k")
                    args.append(property)

                if value:

                    env["VALUE"] = value
                    args.append("-v")
                    args.append(value)

                nodes = {}
                chainnodes = []
                if chain and len(chain) > 0:
                    for elem in chain:

                        chainelem = {}
                        # chaininnode = None
                        # chaininnode = None
                        chainoutnodew = None
                        chainoutnodew = None
                        chainlink = None
                        chainlinkw = None

                        if "innode" in elem:
                            # chaininnode = elem.get('innode', None)
                            # chaininnodew = GremlinFSNodeWrapper(
                            #     node = chaininnode
                            # )
                            # chainelem["node"] = chaininnodew
                            pass

                        if "outnode" in elem:
                            chainoutnode = elem.get('outnode', None)
                            chainoutnodew = GremlinFSNodeWrapper(
                                node = chainoutnode
                            )
                            chainelem["node"] = chainoutnodew

                        if "link" in elem:
                            chainlink = elem.get('link', None)
                            chainlinkw = GremlinFSNodeWrapper(
                                node = chainlink
                            )
                            chainelem["link"] = chainlinkw

                        if chainoutnodew and chainlink and chainlink.has("name") and chainlink.has("label"):
                            nodes["%s@%s" % (chainlink.get("name"), chainlink.get("label"))] = chainoutnodew

                        elif chainoutnodew and chainlink and chainlink.has("name"):
                            nodes[chainlink.get("name")] = chainoutnodew

                        elif chainoutnodew and chainlink and chainlink.has("label"):
                            nodes[chainlink.get("label")] = chainoutnodew

                        chainnodes.append(chainelem)

                data = node.getProperty(handler, default = None, encoding = None, prefix = None)

                script = data

                try:

                    template = data
                    if template:

                        import pystache
                        renderer = pystache.Renderer()

                        templatectx = {
                            "event": event,
                            "property": property,
                            "value": value,
                            "self": GremlinFSNodeWrapper(
                                node = node
                            ),
                            # "node": ...,
                            "nodes": nodes,
                            "chain": chainnodes
                        }

                        for idx, chainnode in enumerate(chainnodes):
                            templatectx["chain%d" % (idx)] = chainnode

                        script = pystache.render(
                            template, templatectx
                        )

                except Exception as e:
                    self.logger.exception(' GremlinFS: invoke handler render exception ', e)
                    script = data

                executable = "sh"
                subprocess.call(
                    [executable, '-c', script],
                    cwd = cwd,
                    env = env
                )

        except Exception as e:
            self.logger.exception(' GremlinFS: node invoke exception ')

    def event(self, event, chain = [], data = {}, propagate = True):

        node = self

        try:

            property = data.get("property", None)
            value = data.get("value", None)

            if node and property and node.has("on.%s.%s" % (event, property)):
                node.invoke(
                    handler = "on.%s.%s" % (event, property),
                    event = event,
                    chain = chain,
                    data = data
                )

            if node and node.has("on.%s" % (event)):
                node.invoke(
                    handler = "on.%s" % (event),
                    event = event,
                    chain = chain,
                    data = data
                )

            # Check if this event should be propagated
            if node and propagate:
                inedges = node.edges(None, True)
                for inedge in inedges:
                    if inedge:

                        innode = inedge.node(False)
                        outnode = inedge.node(True)

                        if inedge.has("name") and inedge.has("label"):
                            newchain = chain.copy()
                            newchain.insert(0, {
                                "innode": innode,
                                "outnode": outnode,
                                "link": inedge
                            })
                            innode.event(
                                event = "%s@%s.%s" % (inedge.get("name"), inedge.get("label"), event),
                                chain = newchain,
                                data = data,
                                propagate = propagate
                            )

                        if inedge.has("name"):
                            newchain = chain.copy()
                            newchain.insert(0, {
                                "innode": innode,
                                "outnode": outnode,
                                "link": inedge
                            })
                            innode.event(
                                event = "%s.%s" % (inedge.get("name"), event),
                                chain = newchain,
                                data = data,
                                propagate = propagate
                            )

                        if inedge.has("label"):
                            newchain = chain.copy()
                            newchain.insert(0, {
                                "innode": innode,
                                "outnode": outnode,
                                "link": inedge
                            })
                            innode.event(
                                event = "%s.%s" % (inedge.get("label"), event),
                                chain = newchain,
                                data = data,
                                propagate = propagate
                            )

                        newchain = chain.copy()
                        newchain.insert(0, {
                            "innode": innode,
                            "outnode": outnode,
                            "link": inedge
                        })
                        innode.event(
                            event = "%s.%s" % ("x", event),
                            chain = newchain,
                            data = data,
                            propagate = propagate
                        )

        except Exception as e:
            self.logger.exception(' GremlinFS: node event exception ', e)



class GremlinFSVertex(GremlinFSNode):

    logger = GremlinFSLogger.getLogger("GremlinFSVertex")

    @classmethod
    def parse(clazz, id):

        if not id:
            return {}

        # name.type@label@uuid
        # ^(.+)\.(.+)\@(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$
        matcher = GremlinFS.operations().utils().rematch(
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
        matcher = GremlinFS.operations().utils().rematch(
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
        matcher = GremlinFS.operations().utils().rematch(
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
        matcher = GremlinFS.operations().utils().rematch(
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
        matcher = GremlinFS.operations().utils().rematch(
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
        matcher = GremlinFS.operations().utils().rematch(
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
            except Exception as e:
                # self.logger.exception(' GremlinFS: node from path ID exception ', e)
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
            except Exception as e:
                # self.logger.exception(' GremlinFS: node from path ID exception ', e)
                return None

        elif parts and \
            "uuid" in parts:
            try:
                return GremlinFSVertex.fromV(
                    GremlinFS.operations().g().V().has(
                        "uuid", parts["uuid"]
                    )
                )
            except Exception as e:
                # self.logger.exception(' GremlinFS: node from path ID exception ', e)
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
            except Exception as e:
                # self.logger.exception(' GremlinFS: node from path ID exception ', e)
                return None

        return None

    @classmethod
    def fromV(clazz, v, names = []):
        if names:
            # return GremlinFSVertex.fromVal(
            return GremlinFSVertex.fromMap(
                v.valueMap(*names).next()
            )

        else:
            return GremlinFSVertex.fromMap(
                v.valueMap(True).next()
            )

    @classmethod
    def fromVs(clazz, vs, names = []):
        if names:
            # return GremlinFSVertex.fromVals(
            return GremlinFSVertex.fromMaps(
                vs.valueMap(*names).toList()
            )

        else:
            return GremlinFSVertex.fromMaps(
                vs.valueMap(True).toList()
            )

    def edges(self, edgeid = None, ine = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            try:

                if ine:
                    return GremlinFSEdge.fromEs(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).has(
                            "name", name
                        )
                    )
                else:
                    return GremlinFSEdge.fromEs(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).has(
                            "name", name
                        )
                    )

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif node and label:

            try:

                if ine:
                    return GremlinFSEdge.fromEs(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        )
                    )
                else:
                    return GremlinFSEdge.fromEs(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        )
                    )

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif node:

            try:

                if ine:
                    return GremlinFSEdge.fromEs(
                        self.g().V(
                            node.get("id")
                        ).inE()
                    )
                else:
                    return GremlinFSEdge.fromEs(
                        self.g().V(
                            node.get("id")
                        ).outE()
                    )

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

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

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
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

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        return None

    def edgenodes(self, edgeid = None, ine = True, inv = True):

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
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).has(
                                "name", name
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromVs(
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
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).has(
                                "name", name
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).has(
                                "name", name
                            ).outV()
                        )

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif node and label:

            try:

                if ine:
                    if inv:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                label
                            ).outV()
                        )

                else:
                    if inv:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).inV()
                        )

                    else:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                label
                            ).outV()
                        )

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif node:

            try:

                if ine:
                    if inv:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).inE().inV()
                        )

                    else:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).inE().outV()
                        )

                else:
                    if inv:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).outE().inV()
                        )

                    else:
                        return GremlinFSVertex.fromVs(
                            self.g().V(
                                node.get("id")
                            ).outE().outV()
                        )

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
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

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
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

            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

    def inbound(self, edgeid = None):
        nodes = self.edgenodes(edgeid, True, False)
        if not nodes:
            return []
        return nodes        

    def outbound(self, edgeid = None):
        nodes = self.edgenodes(edgeid, False, True)
        if not nodes:
            return []
        return nodes 

    def follow(self, edgeid):
        return self.outbound(edgeid)

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
            raise GremlinFSNotExistsError(self)
        if not self.isFolder():
            raise GremlinFSNotExistsError(self)
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
            raise GremlinFSNotExistsError(self)
        if self.isFolder():
            raise GremlinFSIsFolderError(self)
        elif not self.isFile():
            raise GremlinFSNotExistsError(self)
        return node

    def create(self, parent = None, namespace = None):

        node = self

        UUID = node.get('uuid', None)
        label = node.get('label', None)
        name = node.get('name', None)

        # if not name:
        #     return None

        if not namespace:
            namespace = GremlinFS.operations().config("fs_ns")

        newnode = None

        try:

            pathuuid = self.utils().genuuid(UUID)
            pathtime = self.utils().gentime()

            checknodes = self.g().V().has(
                'uuid', str(pathuuid)
            ).toList()

            if checknodes and len(checknodes) > 0:
                return None

            if not name:
                name = str(pathuuid)[0:8]

            # txn = self.graph().tx()

            newnode = None
            if label:
                newnode = self.g().addV(
                    label
                )
            else:
                newnode = self.g().addV()

            for key in node.all():
                if key != "id" and \
                   key != "uuid" and \
                   key != "label" and \
                   key != "name" and \
                   key != "namespace": 
                    val = node.get(key)
                    newnode.property(key, val)

            newnode.property(
                'name', name
            ).property(
                'uuid', str(pathuuid)
            ).property(
                'namespace', namespace
            ).property(
                'created', int(pathtime)
            ).property(
                'modified', int(pathtime)
            )

            if parent:
                newnode.addE(
                    self.config("in_label")
                ).property(
                    'name', self.config("in_name")
                ).property(
                    'uuid', str(self.utils().genuuid())
                ).to(
                    GremlinFS.operations().a().V(
                        parent.get("id")
                    )
                ).next()
            else:
                newnode.next()

            newnode = GremlinFSVertex.fromV(
                self.g().V().has(
                    'uuid', str(pathuuid)
                )
            )

            # self.graph().tx().commit()

            self.mqevent(GremlinFSEvent(
                event = "create_node",
                node = newnode
            ))

        except Exception as e:
            self.logger.exception(' GremlinFS: create exception ', e)
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
                    ).property(
                        'name', name
                    )
                )

            except Exception as e:
                self.logger.exception(' GremlinFS: rename exception ', e)
                return None

        self.mqevent(GremlinFSEvent(
            event = "update_node",
            node = node
        ))

        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                )
            )

        except Exception as e:
            self.logger.exception(' GremlinFS: rename exception ', e)
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
        # next() throws errors on delete
        try:

            # newnode = GremlinFSVertex.fromV(
            self.g().V(
                node.get("id")
            ).outE(
                self.config("in_label")
            ).has(
                'name', self.config("in_name")
            ).drop().toList()
            # )

        except Exception as e:
            pass

        if parent:

            try:

                newnode = GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).addE(
                        self.config("in_label")
                    ).property(
                        'name', self.config("in_name")
                    ).property(
                        'uuid', str(self.utils().genuuid())
                    ).to(
                        GremlinFS.operations().a().V(
                            parent.get("id")
                        )
                    )
                )

            except Exception as e:
                self.logger.exception(' GremlinFS: move exception ', e)
                return None

        self.mqevent(GremlinFSEvent(
            event = "update_node",
            node = node
        ))

        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                )
            )

        except Exception as e:
            self.logger.exception(' GremlinFS: move exception ', e)
            return None

        return newnode

    def delete(self):

        node = self

        if not node:
            return None

        try:

            # next() throws errors on delete
            self.g().V(
                node.get("id")
            ).drop().toList() # .next()

        except Exception as e:
            self.logger.exception(' GremlinFS: delete exception ', e)
            return False

        self.mqevent(GremlinFSEvent(
            event = "delete_node",
            node = node
        ))

        return True

    def render(self):

        node = self

        data = ""

        label_config = node.labelConfig()

        template = None
        readfn = None

        data = node.readProperty(
            self.config("data_property"),
            "",
            encoding = "base64"
        )

        try:

            templatenodes = node.follow(self.config("template_label"))
            if templatenodes and len(templatenodes) >= 1:
                template = templatenodes[0].readProperty(
                    self.config("data_property"),
                    "",
                    encoding = "base64"
                )

            elif node.hasProperty(self.config("template_property")):
                template = node.getProperty(
                    self.config("template_property"),
                    ""
                )

            elif label_config and "template" in label_config:
                template = label_config["template"]

            elif label_config and "readfn" in label_config:
                readfn = label_config["readfn"]

        except Exception as e:
            self.logger.exception(' GremlinFS: readNode template exception ', e)

        try:

            ps = GremlinFS.operations().g().V( node.get('id') ).emit().repeat(
                GremlinFS.operations().a().inE().outV()
            ).until(
                GremlinFS.operations().a().inE().count().is_(0).or_().loops().is_(P.gt(10))
            ).path().toList()

            vs = GremlinFSVertex.fromVs(GremlinFS.operations().g().V( node.get('id') ).emit().repeat(
                GremlinFS.operations().a().inE().outV()
            ).until(
                GremlinFS.operations().a().inE().count().is_(0).or_().loops().is_(P.gt(10))
            ))

            vs2 = {}
            for v in vs:
                vs2[v.get('id')] = v

            templatectx = vs2[ node.get('id') ].all()
            templatectxi = templatectx

            for v in ps:
                templatectxi = templatectx
                haslabel = False
                for v2 in v.objects:
                    v2id = (v2.id)['@value']
                    if isinstance(v2, Vertex):
                        if haslabel:
                            found = None
                            for ctemplatectxi in templatectxi:
                                if ctemplatectxi.get('id') == v2id:
                                    found = ctemplatectxi

                            if found:
                                templatectxi = found

                            else:
                                templatectxi.append(vs2[v2id].all())
                                templatectxi = templatectxi[-1]

                    elif isinstance(v2, Edge):
                        haslabel = True
                        if v2.label in templatectxi:
                            pass
                        else:
                            templatectxi[v2.label] = []

                        templatectxi = templatectxi[v2.label]

            if template:

                data = self.utils().render(
                    template,
                    templatectx
                )

            elif readfn:

                data = readfn(
                    node = node,
                    wrapper = templatectx,
                    data = data
                )

        except Exception as e:
            self.logger.exception(' GremlinFS: readNode render exception ', e)

        return data

    def createFolder(self, parent = None, namespace = None):

        node = self

        UUID = node.get('uuid', None)
        label = node.get('label', None)
        name = node.get('name', None)

        if not name:
            return None

        if not label:
            label = GremlinFS.operations().defaultFolderLabel()

        if not namespace:
            namespace = GremlinFS.operations().config("fs_ns")

        newfolder = self.create(parent = parent, namespace = namespace)

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
                    'uuid', str(self.utils().genuuid())
                ).to(
                    GremlinFS.operations().a().V(
                        newfolder.get("id")
                    )
                )
            )

            # self.graph().tx().commit()

        except Exception as e:
            self.logger.exception(' GremlinFS: createFolder exception ', e)
            return None

        return newfolder

    def createLink(self, target, label, name = None):

        source = self

        if not source:
            return None

        if not target:
            return None

        if not label:
            return None

        newedge = None

        try:

            if name:

                newedge = GremlinFSEdge.fromE(
                    self.g().V(
                        source.get("id")
                    ).addE(
                        label
                    ).property(
                        'name', name
                    ).property(
                        'uuid', str(self.utils().genuuid())
                    ).to(
                        GremlinFS.operations().a().V(
                            target.get("id")
                        )
                    )
                )

            else:

                newedge = GremlinFSEdge.fromE(
                    self.g().V(
                        source.get("id")
                    ).addE(
                        label
                    ).property(
                        'uuid', str(self.utils().genuuid())
                    ).to(
                        GremlinFS.operations().a().V(
                            target.get("id")
                        )
                    )
                )

            self.mqevent(GremlinFSEvent(
                event = "create_link",
                link = newedge,
                source = source,
                target = target
            ))

        except Exception as e:
            self.logger.exception(' GremlinFS: createLink exception ', e)
            return None

        return newedge

    def getLink(self, label, name = None, ine = True):

        node = self

        if not node:
            return None

        if not label:
            return None

        try:

            if name:

                if ine:
                    return GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).has(
                            'name', name
                        )
                    )

                else:
                    return GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).has(
                            'name', name
                        )
                    )

            else:

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
            pass

        return None

    def deleteLink(self, label, name = None, ine = True):

        node = self

        if not node:
            return None

        if not label:
            return None

        link = node.getLink(
            label = label,
            name = name,
            ine = ine
        )

        # drop() on edges often/always? throw exceptions?
        # next() throws errors on delete
        try:

            if name:

                if ine:
                    # GremlinFSEdge.fromE(
                    self.g().V(
                        node.get("id")
                    ).inE(
                        label
                    ).has(
                        'name', name
                    ).drop().toList()
                    # )

                else:
                    # GremlinFSEdge.fromE(
                    self.g().V(
                        node.get("id")
                    ).outE(
                        label
                    ).has(
                        'name', name
                    ).drop().toList()
                    # )

            else:

                if ine:
                    # GremlinFSEdge.fromE(
                    self.g().V(
                        node.get("id")
                    ).inE(
                        label
                    ).drop().toList()
                    # )

                else:
                    # GremlinFSEdge.fromE(
                    self.g().V(
                        node.get("id")
                    ).outE(
                        label
                    ).drop().toList()
                    # )

            self.mqevent(GremlinFSEvent(
                event = "delete_link",
                link = link
            ))

        except Exception as e:
            self.logger.exception(' GremlinFS: deleteLink exception ', e)
            return False

        return True

    def parent(self):

        node = self

        try:

            return GremlinFSVertex.fromMap(
                self.g().V(
                    node.get("id")
                ).outE(
                    self.config("in_label")
                ).inV().valueMap(True).next()
            )

        except Exception as e:
            # self.logger.exception(' GremlinFS: parent exception ', e)
            return None

    def parents(self, list = []):

        node = self

        if not list:
            list = []

        parent = node.parent()
        if parent and parent.get("id") and parent.get("id") != node.get("id"):
            list.append(parent)
            return parent.parents(list)

        return list

    def path(self):
        return [ele for ele in reversed(self.parents([self]))]

    def children(self):

        node = self

        if not node:
            return GremlinFSVertex.fromMaps(
                self.g().V().where(
                    GremlinFS.operations().a().out(
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

    def readFolderEntries(self):
        return self.children();



class GremlinFSEdge(GremlinFSNode):

    logger = GremlinFSLogger.getLogger("GremlinFSEdge")

    @classmethod
    def parse(clazz, id):

        if not id:
            return {}

        # name@label
        # ^(.+)\@(.+)$
        matcher = GremlinFS.operations().utils().rematch(
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

        parts = GremlinFSEdge.parse(id)
        if parts and \
            "uuid" in parts and \
            "name" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GremlinFSEdge.fromE(
                        GremlinFS.operations().g().E().has(
                            "uuid", parts["uuid"]
                        )
                    )
                else:
                    return GremlinFSEdge.fromE(
                        GremlinFS.operations().g().E().hasLabel(
                            parts["label"]
                        ).has(
                            "uuid", parts["uuid"]
                        )
                    )
            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif parts and \
            "uuid" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GremlinFSEdge.fromE(
                        GremlinFS.operations().g().E().has(
                            "uuid", parts["uuid"]
                        )
                    )
                else:
                    return GremlinFSEdge.fromE(
                        GremlinFS.operations().g().E().hasLabel(
                            parts["label"]
                        ).has(
                            "uuid", parts["uuid"]
                        )
                    )
            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif parts and \
            "uuid" in parts:
            try:
                return GremlinFSEdge.fromE(
                    GremlinFS.operations().g().E().has(
                        "uuid", parts["uuid"]
                    )
                )
            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        # Fallback try as straigt up DB id
        # OrientDB doesn't like invalid ID queries?
        elif id and ":" in id:
            try:
                return GremlinFSEdge.fromE(
                    GremlinFS.operations().g().E(
                        id
                    )
                )
            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        return None

#     @classmethod
#     def fromMap(clazz, map):
#         node = GremlinFSEdge()
#         node.fromobj(map)
#         return node
# 
#     @classmethod
#     def fromMaps(clazz, maps):
#         nodes = gfslist([])
#         for map in maps:
#             node = GremlinFSEdge()
#             node.fromobj(map)
#             nodes.append(node)
#         return nodes.tolist()

#     @classmethod
#     def fromE(clazz, e):
#         return GremlinFSEdge.fromMap(
#             e.valueMap(True).next()
#         )
# 
#     @classmethod
#     def fromEs(clazz, es):
#         return GremlinFSEdge.fromMaps(
#             es.valueMap(True).toList()
#         )

    @classmethod
    def fromE(clazz, e):
        # var clazz;
        # clazz = this;
        obj = e.next();
        # if obj and obj['value']:
        #     obj = obj['value']

        edge = GremlinFSEdge();
        edge.set('id', obj.id['@value']);
        edge.set('label', obj.label);
        edge.set('outV', obj.outV.id['@value']);
        # edge.set('outV', obj.outV.id);
        # edge.set('outVLabel', obj.outV.label);
        edge.set('inV', obj.inV.id['@value']);
        # edge.set('inV', obj.inV.id);
        # edge.set('inVLabel', obj.inV.label);
        return edge;

    @classmethod
    def fromEs(clazz, es):
        # var clazz;
        # clazz = this;
        edges = gfslist([]);
        objs2 = es.toList();
        # for( var i=0; i<objs2.length; i++ ) {
        #     var obj = objs2[i];
        for obj in objs2:
            edge = GremlinFSEdge();
            edge.set('id', obj.id['@value']);
            edge.set('label', obj.label);
            edge.set('outV', obj.outV.id['@value']);
            # edge.set('outV', obj.outV.id);
            # edge.set('outVLabel', obj.outV.label);
            edge.set('inV', obj.inV.id['@value']);
            # edge.set('inV', obj.inV.id);
            # edge.set('inVLabel', obj.inV.label);
            edges.append(edge);
        return edges.tolist();

    def node(self, inv = True):

        edge = self

        if edge:

            try:

                if inv:
                    return GremlinFSVertex.fromV(
                        self.g().E(
                            edge.get("id")
                        ).inV()
                    )

                else:
                    return GremlinFSVertex.fromV(
                        self.g().E(
                            edge.get("id")
                        ).outV()
                    )

            except Exception as e:
                # self.logger.exception(' GremlinFS: node exception ', e)
                return None

    def delete(self):

        node = self

        if not node:
            return None

        # next() throws errors on delete
        try:

            self.g().E(
                node.get("id")
            ).drop().toList() # .next()

        except Exception as e:
            self.logger.exception(' GremlinFS: delete exception ', e)
            return False

        return True



# Decorator/Adapter pattern
class GremlinFSNodeWrapper(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSNodeWrapper")

    def __init__(self, node):
        self.node = node

    def __getattr__(self, attr):

        node = self.node

        try:

            data = None

            # file contents shortcut
            if attr == "content" or attr == "contents":
                data = node.render()
                # if data:
                #     data = self.utils().tobytes(data)

            if data:
                return data

            edgenodes = None

            if attr == "inbound":
                edgenodes = node.inbound()

            elif attr == "outbound":
                edgenodes = node.outbound()

            elif attr and attr.startswith("inbound__"):
                edgenodes = node.inbound( attr.replace("inbound__", "") )

            elif attr and attr.startswith("outbound__"):
                edgenodes = node.outbound( attr.replace("outbound__", "") )

            else:
                edgenodes = node.outbound(attr)

            if edgenodes:
                if len(edgenodes) > 1:
                    ret = []
                    for edgenode in edgenodes:
                        ret.append(GremlinFSNodeWrapper(edgenode))
                    return ret
                elif len(edgenodes) == 1:
                    return GremlinFSNodeWrapper(edgenodes[0])

        except Exception as e:
            pass

        return self.get(attr)

    def all(self, prefix = None):

        node = self.node

        dsprefix = "ds"
        if prefix:
            dsprefix = "ds.%s" % (prefix)

        existing = {}
        existing.update(node.all(prefix))

        datasources = {}
        datasources.update(node.all(dsprefix))

        props = {}

        for key in existing:
            if key and not key.startswith("ds."):
                try:
                    if key in datasources:
                        ret, log, err = GremlinFS.operations().eval(
                            datasources.get(key),
                            self
                        )
                        if ret:
                            # Mustache does not allow properties with '.' in the name
                            # as '.' denotes field/object boundary. Therefore all mustache
                            # given properties has to use '__' to indicated '.'
                            # props[key.replace(".", "__")] = str(ret).strip()
                            props[key] = str(ret).strip()
                    # else:
                    elif key in existing:
                        value = existing.get(key)
                        if value:
                            # Mustache does not allow properties with '.' in the name
                            # as '.' denotes field/object boundary. Therefore all mustache
                            # given properties has to use '__' to indicated '.'
                            # props[key.replace(".", "__")] = str(value).strip()
                            props[key] = str(value).strip()
                except Exception as e:
                    self.logger.exception(' GremlinFS: all exception ', e)

        return props

    def keys(self, prefix = None):
        return self.all(prefix).keys()

    def has(self, key, prefix = None):
        pass

    def set(self, key, value, prefix = None):
        pass

    def get(self, key, default = None, prefix = None):

        node = self.node

        # Mustache does not allow properties with '.' in the name
        # as '.' denotes field/object boundary. Therefore all mustache
        # given properties has to use '__' to indicated '.'
        key = key.replace("__", ".")

        dsprefix = "ds"
        if prefix:
            dsprefix = "ds.%s" % (prefix)

        existing = None
        if node.has(key = key, prefix = prefix):
            existing = node.get(key = key, default = default, prefix = prefix)

        datasource = None
        if node.has(key = key, prefix = dsprefix):
            datasource = node.get(key = key, default = default, prefix = dsprefix)

        prop = None

        if datasource:
            try:
                ret, log, err = GremlinFS.operations().eval(
                    datasource,
                    self
                )
                if ret:
                    prop = str(ret).strip()

            except Exception as e:
                self.logger.exception(' GremlinFS: get exception ', e)

        else:
            prop = existing

        return prop

    def property(self, name, default = None, prefix = None):
        pass



class GremlinFSUtils(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSUtils")

    @classmethod
    def missing(clazz, value):
        if value:
            raise GremlinFSExistsError()

    @classmethod
    def found(clazz, value):
        if not value:
            raise GremlinFSNotExistsError()
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

    def ro(self):
        return GremlinFS.operations().ro()

    def a(self):
        return GremlinFS.operations().a()

    def mq(self):
        return GremlinFS.operations().mq()

    def mqevent(self, event):
        return GremlinFS.operations().mqevent(event)

    def query(self, query, node = None, default = None):
        pass

    def eval(self, command, node = None, default = None):
        pass

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    # def utils(self):
    #     return GremlinFS.operations().utils()

    # 

    def nodelink(self, node, path = None):

#         newpath = None
# 
#         if node and path:
#             newpath = self.linkpath("%s/.V/%s" % (
#                 path,
#                 node.toid()
#             ))
#         elif node:
#             newpath = self.linkpath("/.V/%s" % (
#                 node.toid()
#             ))

        nodepath = ""

        if node:
            path = node.path()
            if path:
                for node in path:
                    nodename = node.get("name", None)
                    nodepath += "/" + nodename

        return self.linkpath("%s" % (nodepath))

    def linkpath(self, path):

        if not path:
            return None

        return "%s%s" % (self.config("mount_point"), path)

    # 

    def tobytes(self, data):

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

    def tostring(self, data):

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

    def decode(self, data, encoding = "base64"):
        import base64
        data = base64.b64decode(data)
        return data

    def encode(self, data, encoding = "base64"):
        import base64
        data = base64.b64encode(data)
        return data

    def render(self, template, templatectx):
        pass

    def splitpath(self, path):
        if path == "/":
            return None
        elif not "/" in path:
            return [path]
        elems = path.split("/")
        if elems[0] == "" and len(elems) > 1:
            return elems[1:]
        return elems

    def rematch(self, pattern, data):
        import re
        return re.match(pattern, data)

    def recompile(self, pattern):
        import re
        return re.compile(pattern)

    def genuuid(self, UUID = None):
        import uuid
        if UUID:
            return uuid.UUID(UUID)
        else:
            return uuid.uuid1()

    def gentime(self):
        from time import time
        return time()



class GremlinFSEvent(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSEvent")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def toJSON(self):

        data = {
           "event": self.get("event")
        }

        if self.has("node") and self.get("node"):
            data["node"] = self.get("node").all()

        # This one fails to serialize
        # if self.has("link") and self.get("link"):
        #     data["link"] = self.get("link").all()

        if self.has("source") and self.get("source"):
            data["source"] = self.get("source").all()

        if self.has("target") and self.get("target"):
            data["target"] = self.get("target").all()

        return data



class GremlinFSConfig(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSConfig")

    @classmethod
    def defaults(clazz):
        return {
            "mount_point": None,

            "gremlin_host": None,
            "gremlin_port": None,
            "gremlin_username": None,
            # "gremlin_password": None,
            "gremlin_url": None,

            "rabbitmq_host": None,
            "rabbitmq_port": None,
            # "rabbitmq_username": None,
            "rabbitmq_password": None,

            "mq_exchange": 'gfs-exchange',
            "mq_exchange_type": 'topic',
            "mq_routing_key": "gfs1.info",
            "mq_routing_keys": ["gfs1.*"],

            "log_level": GremlinFSLogger.getLogLevel(),

            "client_id": "0010",
            "fs_ns": "gfs1",
            "fs_root": None,
            "fs_root_init": False,

            "folder_label": 'group',
            "ref_label": 'ref',
            "in_label": 'in',
            "self_label": 'self',
            "template_label": 'template',

            "in_name": 'in0',
            "self_name": 'self0',

            "vertex_folder": '.V',
            "edge_folder": '.E',
            "in_edge_folder": 'IN', # 'EI',
            "out_edge_folder": 'OUT', # 'EO',

            "uuid_property": 'uuid',
            "name_property": 'name',
            "data_property": 'data',
            "template_property": 'template',

            "default_uid": 1001,
            "default_gid": 1001,
            "default_mode": 0o777,

            "labels": []
        }

    def __init__(self, **kwargs):

        # Defaults
        self.setall(GremlinFSConfig.defaults())

        # Overrides
        self.setall(kwargs)

        # Build label regexes
        if self.has("labels"):
            for label_config in self.get("labels"):
                if "pattern" in label_config:
                    try:
                        label_config["compiled"] = re.compile(label_config["pattern"])
                    except Exception as e:
                        self.logger.exception(' GremlinFS: failed to compile pattern ' + label_config["pattern"], e)
                    pass



class GremlinFS():

    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a GremlinFSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    logger = GremlinFSLogger.getLogger("GremlinFS")

    __instance = None

    @classmethod
    def instance(clazz, instance = None):
        if instance:
            GremlinFS.__instance = instance
        return GremlinFS.__instance

    @classmethod
    def operations(clazz):
        return GremlinFS.__instance

    def __init__(
        self,
        **kwargs):

        self._g = None
        self._ro = None
        self._mq = None

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

        self.logger.info(' GremlinFS mount point: ' + self.mount_point)

        self.gremlin_host = gremlin_host
        self.gremlin_port = gremlin_port
        self.gremlin_username = gremlin_username
        self.gremlin_password = gremlin_password

        self.gremlin_url = "ws://" + self.gremlin_host + ":" + self.gremlin_port + "/gremlin"

        self.logger.info(' GremlinFS gremlin host: ' + self.gremlin_host)
        self.logger.info(' GremlinFS gremlin port: ' + self.gremlin_port)
        self.logger.info(' GremlinFS gremlin username: ' + self.gremlin_username)
        # self.logger.debug(' GremlinFS gremlin password: ' + self.gremlin_password)
        self.logger.info(' GremlinFS gremlin URL: ' + self.gremlin_url)

        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_username = rabbitmq_username
        self.rabbitmq_password = rabbitmq_password

        self.logger.info(' GremlinFS rabbitmq host: ' + self.rabbitmq_host)
        self.logger.info(' GremlinFS rabbitmq port: ' + self.rabbitmq_port)
        self.logger.info(' GremlinFS rabbitmq username: ' + self.rabbitmq_username)
        # self.logger.debug(' GremlinFS rabbitmq password: ' + self.rabbitmq_password)

        self._g = None
        self._ro = None
        self._mq = None

        self._config = GremlinFSConfig(

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

        self._utils = GremlinFSUtils()

        # register
        self.register()

        return self

    def connection(self, ro = False):

        graph = Graph()

        if ro:
            strategy = ReadOnlyStrategy() # .build().create()
            ro = graph.traversal().withStrategies(strategy).withRemote(DriverRemoteConnection(
                self.gremlin_url,
                'g',
                username = self.gremlin_username,
                password = self.gremlin_password
            ))
            return ro

        g = graph.traversal().withRemote(DriverRemoteConnection(
            self.gremlin_url,
            'g',
            username = self.gremlin_username,
            password = self.gremlin_password
        ))
        return g

    def mqconnection(self):

        # url = 'amqp://rabbitmq:rabbitmq@rabbitmq:5672/%2f'
        #        amqp://rabbitmq:rabbitmq@rabbitmq:5672/%2f
        url = "amqp://%s:%s@%s:%s/%s" % (
            self.rabbitmq_username,
            self.rabbitmq_password,
            self.rabbitmq_host,
            str(self.rabbitmq_port),
            '%2f'
        )

        params = pika.URLParameters(url)
        params.socket_timeout = 5

        connection = pika.BlockingConnection(params) # Connect to CloudAMQP

        return connection

    def mqchannel(self):

        mqexchangename = self.config("mq_exchange")
        mqexchangetype = self.config("mq_exchange_type")

        mqqueuename = self.config("client_id") + '-' + self.config("fs_ns")

        mqroutingkeys = self.config("mq_routing_keys")
        mqroutingkey = self.config("mq_routing_key")

        self.logger.info( ' GremlinFS: AMQP DETAILS: ' )
        self.logger.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
        self.logger.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
        self.logger.info( ' GremlinFS: MQ ROUTING KEYS: ' )
        self.logger.info(mqroutingkeys)
        if mqroutingkey:
            self.logger.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )

        mqconnection = self.mqconnection()
        mqchannel = mqconnection.channel()
        mqchannel.exchange_declare(
            exchange = mqexchangename,
            exchange_type = mqexchangetype,
        )

        return mqchannel

    def mqlistener(self, mqdetails = {}, callback = None, reconnect = True):

        while(reconnect):

            mqexchangename = mqdetails["mq_exchange"]
            mqexchangetype = mqdetails["mq_exchange_type"]

            mqqueuename = mqdetails["mq_queue"]

            mqroutingkeys = mqdetails["mq_routing_keys"]
            mqroutingkey = mqdetails["mq_routing_key"]

            self.logger.info( ' GremlinFS: AMQP DETAILS: ' )
            self.logger.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
            self.logger.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
            self.logger.info( ' GremlinFS: MQ ROUTING KEYS: ' )
            self.logger.info(mqroutingkeys)
            if mqroutingkey:
                self.logger.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )

            try:
                self.logger.info(
                    ' GremlinFS: Connecting to AMQP: queue: ' + 
                    mqqueuename + 
                    ' with exchange: ' + 
                    mqexchangetype + '/' + mqexchangename)

                mqconnection = self.mqconnection()
                mqchannel = mqconnection.channel()
                mqchannel.exchange_declare(
                    exchange = mqexchangename,
                    exchange_type = mqexchangetype
                )
                mqchannel.queue_declare(
                    queue = mqqueuename
                )
                routing_keys = mqroutingkeys
                for routing_key in routing_keys:
                    self.logger.info(
                        ' GremlinFS: Binding AMQP queue: ' + 
                        mqqueuename + 
                        ' in exchange: ' + 
                        mqexchangetype + '/' + mqexchangename + 
                        ' with routing key: ' + routing_key)
                    mqchannel.queue_bind(
                        exchange = mqexchangename,
                        queue = mqqueuename,
                        routing_key = routing_key
                    )
                self.logger.info(
                    ' GremlinFS: Setting up AMQP queue consumer: ' + 
                    mqqueuename)
                mqchannel.basic_consume(
                    queue = mqqueuename,
                    auto_ack = True,
                    on_message_callback = callback
                )
                self.logger.info(
                    ' GremlinFS: Consuming AMQP queue: ' + 
                    mqqueuename)
                mqchannel.start_consuming()
                self.logger.info(
                    ' GremlinFS: Exit, closing AMQP queue: ' + 
                    mqqueuename)
                mqconnection.close()

            except pika.exceptions.ConnectionClosedByBroker:
                # Uncomment this to make the example not attempt recovery
                # from server-initiated connection closure, including
                # when the node is stopped cleanly
                #
                # break
                time.sleep(10)
                continue

            # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError as err:
                self.logger.error(' GremlinFS: Caught an AMQP connection error: {} '.format(err))
                break

            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                self.logger.info(' GremlinFS: AMQP connection was closed, reconnecting ')
                time.sleep(10)
                continue

    def g(self):

        if self._g:
            return self._g

        g = self.connection()
        self._g = g

        return self._g

    def ro(self):

        if self._ro:
            return self._ro

        ro = self.connection(True)
        self._ro = ro

        return self._ro

    def a(self):
        return __

    def mq(self):

        if self._mq:
            return self._mq

        mqchannel = self.mqchannel()
        self._mq = mqchannel

        return self._mq

    def mqevent(self, event):

        import simplejson as json

        data = event.toJSON()

        mqexchangename = self.config("mq_exchange")
        mqexchangetype = self.config("mq_exchange_type")

        mqqueuename = self.config("client_id") + '-' + self.config("fs_ns")

        mqroutingkeys = self.config("mq_routing_keys")
        mqroutingkey = self.config("mq_routing_key")

        self.logger.info( ' GremlinFS: AMQP DETAILS: ' )
        self.logger.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
        self.logger.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
        self.logger.info( ' GremlinFS: MQ ROUTING KEYS: ' )
        self.logger.info(mqroutingkeys)
        if mqroutingkey:
            self.logger.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )
            self.logger.info(' GremlinFS: OUTBOUND AMQP/RABBIT EVENT: routing: ' + mqroutingkey + ' @ exchange: ' + mqexchangename)

        self.logger.info(' event: ')
        self.logger.info(data)

        try:

            self.mq().basic_publish(
                exchange = mqexchangename,
                routing_key = mqroutingkey,
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

        except pika.exceptions.ConnectionClosedByBroker:

            self.logger.info(' GremlinFS: Outbound AMQP/RABBIT event, connection was closed, retry ')

            self._mq = None

            self.mq().basic_publish(
                exchange = mqexchangename,
                routing_key = mqroutingkey,
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            self.logger.error(' GremlinFS: Outbound AMQP/RABBIT event error: {} '.format(err))
            return

        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:

            self.logger.info(' GremlinFS: Outbound AMQP/RABBIT event, connection was closed, retry ')

            self._mq = None

            self.mq().basic_publish(
                exchange = mqexchangename,
                routing_key = mqroutingkey,
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

        except Exception as e:
            self.logger.exception(' GremlinFS: MQ/AMQP send exception ', e)

    def mqonevent(self, node, event, chain = [], data = {}, propagate = True):

        self.logger.info(' GremlinFS: INBOUND AMQP/RABBIT ON EVENT ')

        try:

            if node:
                node.event(
                    event = event, 
                    chain = chain, 
                    data = data, 
                    propagate = True
                )

        except Exception as e:
            self.logger.exception(' GremlinFS: INBOUND AMQP/RABBIT ON EVENT EXCEPTION ', e)

    def mqonmessage(self, ch, method, properties, body):

        self.logger.info(' GremlinFS: INBOUND AMQP/RABBIT ON MESSAGE ')

        try:

            # import json
            import simplejson as json

            json = json.loads(body)

            if "node" in json and "event" in json:
                node = json.get("node", None)
                event = json.get("event", None)
                node = GremlinFSVertex.load(node.get("uuid", None))
                self.mqonevent(
                    node = node,
                    event = event,
                    chain = [],
                    data = json,
                    propagate = True
                )

        except Exception as e:
            self.logger.exception(' GremlinFS: INBOUND AMQP/RABBIT ON MESSAGE EXCEPTION ', e)

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

    def config(self, key = None, default = None):
        return self._config.get(key, default)

    def utils(self):
        return GremlinFSUtils.utils()

    # def initfs(self):
    # 
    #     newfs = GremlinFSVertex.make(
    #         name = self.config("fs_root"),
    #         label = ...,
    #         uuid = None
    #     ).createFolder(
    #         parent = None
    #     )

    #     return newfs.get("id")

    def getfs(self, fsroot, fsinit = False):

        fsid = fsroot

        # if not ... and fsinit:
        #     fs = None
        #     fsid = self.initfs()

        return fsid

    def register(self):

        import socket
        import platform

        client_id = self.config("client_id")
        namespace = self.config("fs_ns")
        type_name = "register"

        hostname = socket.gethostname()
        ipaddr = socket.gethostbyname(hostname)

        # hwaddr = hex(uuid.getnode())
        hwaddr = ':'.join(['{:02x}'.format((uuid.getnode() >> ele) & 0xff) for ele in range(0,8*6,8)][::-1]).upper()

        exists = False
        node = None

        try:

            match = GremlinFSVertex.fromVs(
                self.g().V().hasLabel(
                    type_name
                ).has(
                    'namespace', namespace
                ).has(
                    'name', client_id + "@" + hostname
                ).has(
                    'hw_address', hwaddr
                )
            )

            if match:
                exists = True
                node = match[0]

            else:
                exists = False
                node = None

        except Exception as e:
            self.logger.exception(' GremlinFS: Failed to register ', e)
            exists = False

        try:

            if not exists:

                pathuuid = uuid.uuid1()
                pathtime = time()

                self.g().addV(
                    type_name
                ).property(
                    'name', client_id + "@" + hostname
                ).property(
                    'uuid', str(pathuuid)
                ).property(
                    'namespace', namespace
                ).property(
                    'created', int(pathtime)
                ).property(
                    'modified', int(pathtime)
                ).property(
                    'client_id', client_id
                ).property(
                    'hostname', hostname
                ).property(
                    'ip_address', ipaddr
                ).property(
                    'hw_address', hwaddr
                ).property(
                    'machine_architecture', platform.processor()
                ).property(
                    'machine_hardware', platform.machine()
                ).property(
                    'system_name', platform.system()
                ).property(
                    'system_release', platform.release()
                ).property(
                    'system_version', platform.version()
                ).next()

        except Exception as e:
            self.logger.exception(' GremlinFS: Failed to register ', e)

        return node

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
