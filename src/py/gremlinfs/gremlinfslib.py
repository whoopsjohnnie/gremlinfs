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

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

        self.setall(kwargs)

    def property(self, name, _default_ = None, prefix = None):
        return self.get(name, _default_, prefix)



class GremlinFSPath(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSPath")

    @classmethod
    def paths(self):
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
    def path(self, path):
        paths = GremlinFSPath.paths()
        if paths and path in paths:
            return paths[path]
        return None

    @classmethod
    def expand(self, path):
        return GremlinFS.operations().utils().splitpath(path)

    @classmethod
    def atpath(self, path, node = None):

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
    def pathnode(self, nodeid, parent, path):

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
    def pathparent(self, path = []):

        root = None
        if GremlinFS.operations().config("fs_root", None):
            root = GremlinFSVertex.load(
                GremlinFS.operations().config("fs_root", None)
            )

        parent = root

        if not path:
            return parent

        vindex = 0
        for item in path:
            if item == GremlinFS.operations().config("vertex_folder", ".V"):
                break
            vindex += 1

        if vindex:
            if vindex > 0:
                parent = GremlinFSPath.atpath( path[0:vindex] )

        else:
            parent = GremlinFSPath.atpath( path )

        return parent

    @classmethod
    def match(self, path):
        clazz = self

        match = gfsmap({

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

        })

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
            for item in expanded:
                if item == GremlinFS.operations().config("vertex_folder", ".V"):
                    break
                vindex += 1

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
                #     # touch files as attributes. I think the _default_
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

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

        self.setall(kwargs)

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

    def query(self, query, node = None, _default_ = None):
        return self.utils().query(query, node, _default_)

    def eval(self, command, node = None, _default_ = None):
        return self.utils().eval(command, node, _default_)

    def config(self, key = None, _default_ = None):
        return GremlinFS.operations().config(key, _default_)

    def utils(self):
        return GremlinFS.operations().utils()

    # 

    # JS jump:
    # UnsupportedSyntaxError: Having both param accumulator and keyword args is unsupported
    def enter(self, functioname, *args):
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

        _default_ = False
        if self._type and self._type == "folder":
            _default_ = True

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            node = self.node()
            if node and node.isFolder():
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def isFile(self):

        _default_ = False
        if self._type and self._type == "file":
            _default_ = True

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            node = self.node()
            if node and node.isFile():
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def isLink(self):

        _default_ = False
        if self._type and self._type == "link":
            _default_ = True

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            node = self.node()
            if node and node.isLink():
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def isFound(self):

        _default_ = False
        if self._type:
            _default_ = True

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            node = self.node()
            if node:
                return True
            return False

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        elif self._path == "vertex":
            node = self.node()
            if node:
                return True
            return False

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )
            if node.has( self._vertexproperty ):
                return True
            elif node.edge( self._vertexproperty, False ):
                return True
            return False

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

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
        #     return _default_

        return _default_


    # 
    # Folder CRUD
    # 
    # - createFolder
    # - readFolder
    # - renameFolder
    # - deleteFolder
    # 


    def createFolder(self, mode = None):

        if self.isFound():
            raise GremlinFSExistsError(self)

        if self.isFile():
            raise GremlinFSIsFileError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

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
            newfolder = GremlinFSVertex.make(
                newname,
                newlabel,
                newuuid
            ).createFolder(
                parent,
                mode
            )

            self.mqevent(GremlinFSEvent(
                event = "create_node",
                node = newfolder
            ))

            return True

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        elif self._path == "vertex":

            newname = GremlinFSVertex.infer("name", self._name)
            newlabel = GremlinFSVertex.infer("label", self._name, "vertex")
            newlabel = GremlinFSVertex.label(newname, newlabel, "file", "vertex")
            newuuid = GremlinFSVertex.infer("uuid", self._name)
            parent = self.parent()

            # Do not create an A vertex in /V/B, unless A is vertex
            if label != "vertex":
                if label != newlabel:
                    raise GremlinFSNotExistsError(self)

            if not newname:
                raise GremlinFSNotExistsError(self)

            if GremlinFS.operations().isFolderLabel(newlabel):
                newfolder = GremlinFSVertex.make(
                    newname,
                    newlabel,
                    newuuid
                ).createFolder(
                    None,
                    mode
                )

                self.mqevent(GremlinFSEvent(
                    event = "create_node",
                    node = newfolder
                ))

            else:
                newfile = GremlinFSVertex.make(
                    newname,
                    newlabel,
                    newuuid
                ).create(
                    None,
                    mode
                )

                self.mqevent(GremlinFSEvent(
                    event = "create_node",
                    node = newfile
                ))

            return True

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def readFolder(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        entries = gfslist([])

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

            return entries.tolist()

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

            return entries.tolist()

        elif self._path == "vertex_labels":
            labels = self.g().V().label().dedup()
            if labels:
                for label in labels:
                    if label:
                        # TODO: Fix
                        # list(entries).append(label.strip().replace("\t", "").replace("\t", ""))
                        entries.append(label)

            return entries.tolist()

        # elif self._path == "vertex_label":
        #     return entries

        elif self._path == "vertexes":
            label = self._vertexlabel
            if not label:
                label = "vertex"

            _short_ = False

            parent = self.parent()
            nodes = None

            if parent:
                _short_ = True
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
                nodeid = node.toid( _short_ )
                if nodeid:
                    entries.append( nodeid )

            return entries.tolist()

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

            return entries.tolist()

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
                        entries.append( cnode.get("name") + "@" + cnode.get("label") )
                    elif cnode.get("label"):
                        entries.append( cnode.get("label") )

            return entries.tolist()

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
                        entries.append( cnode.get("name") + "@" + cnode.get("label") )
                    elif cnode.get("label"):
                        entries.append( cnode.get("label") )

            return entries.tolist()

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


    def createFile(self, mode = None, data = None):

        if self.isFound():
            raise GremlinFSExistsError(self)

        if self.isFolder():
            raise GremlinFSIsFolderError(self)

        if not data:
            data = ""

        _default_ = data
        if self._type:
            _default_ = data

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":

            newname = GremlinFSVertex.infer("name", self._name)
            newlabel = GremlinFSVertex.infer("label", self._name, "vertex")
            newlabel = GremlinFSVertex.label(newname, newlabel, "file", "vertex")
            newuuid = GremlinFSVertex.infer("uuid", self._name)
            parent = self.parent()

            if not newname:
                raise GremlinFSNotExistsError(self)

            newfile = GremlinFSVertex.make(
                newname,
                newlabel,
                newuuid
            ).create(
                parent,
                mode
            )

            self.mqevent(GremlinFSEvent(
                event = "create_node",
                node = newfile
            ))

            return True

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )
            node.setProperty(
                self._vertexproperty,
                data
            )

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

            return True

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

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


    def createLink(self, sourcematch, mode = None):

        if self.isFound():
            raise GremlinFSExistsError(self)

        if not sourcematch.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            return _default_

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

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
            newlink = source.createLink(
                target = target,
                label = label,
                name = name,
                mode = mode
            )

            self.mqevent(GremlinFSEvent(
                event = "create_link",
                link = newlink,
                source = source,
                target = target
            ))

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
            newlink = source.createLink(
                target = target,
                label = label,
                name = name,
                mode = mode
            )

            self.mqevent(GremlinFSEvent(
                event = "create_link",
                link = newlink,
                source = source,
                target = target
            ))

            return True

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_        

    def readLink(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            return _default_

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

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
        #     return _default_

        return _default_

    def deleteLink(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            return _default_

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        # elif self._path == "vertex_property":
        #     return _default_

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        elif self._path == "vertex_in_edge":

            node = self.node()

            label = GremlinFSEdge.infer("label", self._vertexedge, None)
            name = GremlinFSEdge.infer("name", self._vertexedge, None)

            if not label and name:
                label = name
                name = None

            if label and name:
                # we are the target, in edge means ...
                link = node.getLink(
                    label = label,
                    name = name,
                    ine = True
                )

                node.deleteLink(
                    label = label,
                    name = name,
                    ine = True
                )

                self.mqevent(GremlinFSEvent(
                    event = "delete_link",
                    link = link
                ))

            elif label:
                # we are the target, in edge means ...
                link = node.getLink(
                    label = label,
                    name = None,
                    ine = True
                )

                node.deleteLink(
                    label = label,
                    name = None,
                    ine = True
                )

                self.mqevent(GremlinFSEvent(
                    event = "delete_link",
                    link = link
                ))

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
                link = node.getLink(
                    label = label,
                    name = name,
                    ine = False
                )

                node.deleteLink(
                    label = label,
                    name = name,
                    ine = False
                )

                self.mqevent(GremlinFSEvent(
                    event = "delete_link",
                    link = link
                ))

            elif label:
                # we are the target, out edge means ...
                link = node.getLink(
                    label = label,
                    name = None,
                    ine = False
                )

                node.deleteLink(
                    label = label,
                    name = None,
                    ine = False
                )

                self.mqevent(GremlinFSEvent(
                    event = "delete_link",
                    link = link
                ))

            return True

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    # 

    def readNode(self, size = 0, offset = 0):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

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
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

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
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def writeNode(self, data, offset = 0):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = data
        if self._type:
            _default_ = data

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            node = self.node().file()

            label_config = node.labelConfig()

            writefn = None

            old = node.readProperty(
                self.config("data_property"),
                None,
                "base64"
            )

            old = self.utils().tobytes(old)

            _new_ = GremlinFSUtils.irepl(old, data, offset)

            _new_ = self.utils().tostring(_new_)

            node.writeProperty(
                self.config("data_property"),
                _new_,
                "base64"
            )

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

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
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )

            old = node.readProperty(
                self._vertexproperty,
                None
            )

            old = self.utils().tobytes(old)

            _new_ = GremlinFSUtils.irepl(old, data, offset)

            _new_ = self.utils().tostring(_new_)

            node.writeProperty(
                self._vertexproperty,
                _new_
            )

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

            return data

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def clearNode(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = None

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":
            node = self.node().file()

            node.writeProperty(
                self.config("data_property"),
                ""
            )

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

            return None

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        elif self._path == "vertex_property":
            node = GremlinFSUtils.found( self.node() )

            node.writeProperty(
                self._vertexproperty,
                ""
            )

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

            return None

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def moveNode(self, newmatch):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        # if newmatch.isFound():
        #     raise GremlinFSExistsError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":

            node = GremlinFSUtils.found(self.node())
            parent = newmatch.parent()

            node.rename(newmatch._name)
            node.move(parent)

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

            return True

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

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

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = newnode
            ))

            newdata = newnode.readProperty(
                newname,
                ""
            )

            if newdata == data:

                oldnode.unsetProperty(
                    oldname
                )

                self.mqevent(GremlinFSEvent(
                    event = "update_node",
                    node = oldnode
                ))

            return True

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

    def deleteNode(self):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        _default_ = None
        if self._type:
            _default_ = None

        # if self._path == "root":
        #     return _default_

        # el
        if self._path == "atpath":

            node = GremlinFSUtils.found(self.node())
            node.delete()

            self.mqevent(GremlinFSEvent(
                event = "delete_node",
                node = node
            ))

            return True

        # elif self._path == "vertex_labels":
        #     return _default_

        # elif self._path == "vertex_label":
        #     return _default_

        # elif self._path == "vertexes":
        #     return _default_

        # elif self._path == "vertex":
        #     return _default_

        # elif self._path == "vertex_properties":
        #     return _default_

        # elif self._path == "vertex_folder_property":
        #     return _default_

        elif self._path == "vertex_property":
            # label = self._vertexlabel
            node = GremlinFSUtils.found( self.node() )
            node.unsetProperty(
                self._vertexproperty
            )

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

            return True

        # elif self._path == "vertex_edges":
        #     return _default_

        # elif self._path == "vertex_in_edges":
        #     return _default_

        # elif self._path == "vertex_out_edges":
        #     return _default_

        # elif self._path == "vertex_edge":
        #     return _default_

        # elif self._path == "vertex_in_edge":
        #     return _default_

        # elif self._path == "vertex_out_edge":
        #     return _default_

        # elif self._path == "create_vertex":
        #     return _default_

        return _default_

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

            self.mqevent(GremlinFSEvent(
                event = "update_node",
                node = node
            ))

        return True

    def getProperty(self, key, _default_ = None):

        if not self.isFound():
            raise GremlinFSNotExistsError(self)

        if self._path == "atpath":
            node = self.node()
            if node:
                return node.getProperty(
                    key,
                    _default_
            )

        return _default_



class GremlinFSNode(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSNode")

    @classmethod
    def parse(self, id):
        return None

    @classmethod
    def infer(self, field, obj, _default_ = None):
        clazz = self
        parts = clazz.parse( obj )
        if not field in parts:
            return _default_
        return parts.get(field, _default_)

    @classmethod
    def label(self, name, label, fstype = "file", _default_ = "vertex"):
        if not name:
            return _default_
        if not label:
            return _default_
        for label_config in GremlinFS.operations().config("labels", []):
            if "type" in label_config and label_config["type"] == fstype:
                compiled = None
                if "compiled" in label_config:
                    compiled = label_config["compiled"]
                else:
                    compiled = GremlinFS.operations().utils().recompile(label_config["pattern"])

                if compiled:
                    if compiled.search(name):
                        label = label_config.get("label", _default_)
                        break

        return label

#     @classmethod
#     def fromV(self, v):
#         clazz = self
#         return clazz.fromMap(
#             v.valueMap(True).next()
#         )
#  
#     @classmethod
#     def fromVs(self, vs):
#         clazz = self
#         return clazz.fromMaps(
#             vs.valueMap(True).toList()
#         )

    def __init__(self, **kwargs):

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

        self.setall(kwargs)

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

    def query(self, query, node = None, _default_ = None):
        return self.utils().query(query, node, _default_)

    def eval(self, command, node = None, _default_ = None):
        return self.utils().eval(command, node, _default_)

    def config(self, key = None, _default_ = None):
        return GremlinFS.operations().config(key, _default_)

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

    def toid(self, _short_ = False):
        node = self
        mapid = node.get('id', None)
        mapuuid = node.get('uuid', None)
        maplabel = node.get('label', None)
        mapname = node.get('name', None)

        # TODO: Fix
        # if mapname:
        #     mapname = mapname.strip().replace("\t", "").replace("\t", "")

        if mapname and mapuuid and maplabel and not _short_:
            if maplabel == "vertex":
                return mapname + "@" + mapuuid
            else:
                return mapname + "@" + maplabel + "@" + mapuuid

        elif mapname and maplabel and _short_:
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

        data = node.has(name, prefix)
        if not data:
            return False

        return True

    def getProperty(self, name, _default_ = None, encoding = None, prefix = None):

        node = self

        if not node:
            return _default_

        data = node.get(name, None, prefix)
        if not data:
            return _default_

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

        node.set(name, data, prefix)

        if prefix:
            name = prefix + "." + name

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

        node.set(name, None, prefix)

        if prefix:
            name = prefix + "." + name

        # Having issues with exception throwing, even though deletion works
        try:
            # GremlinFSVertex.fromV(
            self.g().V(
                nodeid
            ).properties(
                name
            ).drop().next()
            # )
        except Exception as e:
            pass

    def setProperties(self, properties, prefix = None):

        node = self

        existing = gfsmap({})

        existing.update(node.all(prefix))

        if existing:
            for key in dict(existing):
                value = existing[key]
                if not key in properties:
                    node.unsetProperty(
                        key,
                        prefix
                    )

        if properties:
            for key in dict(properties):
                value = properties[key]
                try:
                    node.setProperty(
                        key,
                        value,
                        None,
                        prefix
                    )
                except Exception as e:
                    self.logger.exception(' GremlinFS: setProperties exception ', e)

    def getProperties(self, prefix = None):

        node = self

        properties = gfsmap({})
        properties.update(node.all(prefix))

        return properties.tomap()

    def readProperty(self, name, _default_ = None, encoding = None, prefix = None):
        return self.getProperty(name, _default_, encoding, prefix)

    def writeProperty(self, name, data, encoding = None, prefix = None):
        return self.setProperty(name, data, encoding, prefix)

    def invoke(self, handler, event, chain = [], data = {}):
        pass

    def event(self, event, chain = [], data = {}, propagate = True):
        pass



class GremlinFSVertex(GremlinFSNode):

    logger = GremlinFSLogger.getLogger("GremlinFSVertex")

    @classmethod
    def parse(self, id):

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
                "name": nodenme + "." + nodetpe,
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
                "name": nodenme + "." + nodetpe,
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
                "name": nodenme + "." + nodetpe,
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
    def make(self, name, label, uuid = None):
        return GremlinFSVertex(name = name, label = label, uuid = uuid)

    @classmethod
    def load(self, id):
        clazz = self

        parts = GremlinFSVertex.parse(id)
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
    def fromMap(self, map):

        # JS jump:
        # UnsupportedSyntaxError: '**kwargs' syntax isn't supported
        # return clazz(**vals)
        node = GremlinFSVertex()
        node.fromobj(map)

        return node

    @classmethod
    def fromMaps(self, maps):
        # clazz = self
        nodes = gfslist([])

        for map in maps:

            # JS jump:
            # UnsupportedSyntaxError: '**kwargs' syntax isn't supported
            # list(nodes).append(clazz(**vals))
            node = GremlinFSVertex()
            node.fromobj(map)

            # list(nodes).append(node)
            nodes.append(node)

        return nodes.tolist()

    @classmethod
    def fromV(self, v):
        clazz = self
        return GremlinFSVertex.fromMap(
            v.valueMap(True).next()
        )

    @classmethod
    def fromVs(self, vs):
        clazz = self
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

    def create(self, parent = None, mode = None, owner = None, group = None, namespace = None):

        node = self

        UUID = node.get('uuid', None)
        label = node.get('label', None)
        name = node.get('name', None)

        if not name:
            return None

        if not mode:
            mode = GremlinFS.operations().config("default_mode", 0o644)

        if not owner:
            owner = GremlinFS.operations().config("default_uid", 0)

        if not group:
            group = GremlinFS.operations().config("default_gid", 0)

        if not namespace:
            namespace = GremlinFS.operations().config("fs_ns")

        newnode = None

        try:

            pathuuid = self.utils().genuuid(UUID)
            pathtime = self.utils().gentime()

            self.logger.error(' !! CREATE !! ')
            self.logger.error(self.utils())
            self.logger.error(pathuuid)
            self.logger.error(pathtime)

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
                'namespace', namespace
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
        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                ).outE(
                    self.config("in_label")
                ).has(
                    'name', self.config("in_name")
                ).drop()
            )

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

            self.g().V(
                node.get("id")
            ).drop().next()

        except Exception as e:
            # self.logger.exception(' GremlinFS: delete exception ', e)
            return False

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
            "base64"
        )

        try:

            templatenodes = node.follow(self.config("template_label"))
            if templatenodes and len(templatenodes) >= 1:
                template = templatenodes[0].readProperty(
                    self.config("data_property"),
                    "",
                    "base64"
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

            templatectx = vs2[ node.get('id') ].getall()
            templatectxi = templatectx

            for v in ps:
                templatectxi = templatectx
                haslabel = False;
                for v2 in v.objects:
                    if isinstance(v2, gremlin.structure.Vertex):
                        if haslabel:
                            found = undefined;
                            for ctemplatectxi in templatectxi:
                                if ctemplatectxi.id == v2.id:
                                    found = ctemplatectxi; # templatectxi[iii]

                            if found:
                                templatectxi = found

                            else:
                                templatectxi.append(vs2[v2id].all())
                                templatectxi = templatectxi[-1]

                    elif isinstance(v2, gremlin.structure.Edge):
                        haslabel = true
                        if v2.label in templatectxi:
                            pass
                        else:
                            templatectxi[v2.label] = []

                        templatectxi = templatectxi[v2.label];

            if template:

                data = self.utils().render(
                    template,
                    templatectx
                )

            elif readfn:

                data = readfn(
                    node,
                    templatectx,
                    data
                )

        except Exception as e:
            self.logger.exception(' GremlinFS: readNode render exception ', e)

        return data

    def createFolder(self, parent = None, mode = None, owner = None, group = None, namespace = None):

        node = self

        UUID = node.get('uuid', None)
        label = node.get('label', None)
        name = node.get('name', None)

        if not name:
            return None

        if not label:
            label = GremlinFS.operations().defaultFolderLabel()

        if not mode:
            mode = GremlinFS.operations().config("default_mode", 0o644)

        if not owner:
            owner = GremlinFS.operations().config("default_uid", 0)

        if not group:
            group = GremlinFS.operations().config("default_gid", 0)

        if not namespace:
            namespace = GremlinFS.operations().config("fs_ns")

        newfolder = self.create(parent, mode, owner, group, namespace)

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
                    'query', "g.V('" + str(newfolder.get("id")) + "').has('uuid', '" + str(newfolder.get("uuid")) + "').has('type', '" + 'group' + "').inE('" + self.config("in_label") + "').outV()"
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

    def createLink(self, target, label, name = None, mode = None, owner = None, group = None):

        source = self

        if not source:
            return None

        if not target:
            return None

        if not label:
            return None

        if not mode:
            mode = GremlinFS.operations().config("default_mode", 0o644)

        if not owner:
            owner = GremlinFS.operations().config("default_uid", 0)

        if not group:
            group = GremlinFS.operations().config("default_gid", 0)

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

        newedge = None

        # drop() on edges often/always? throw exceptions?
        try:

            if name:

                if ine:
                    newedge = GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).has(
                            'name', name
                        ).drop()
                    )

                else:
                    newedge = GremlinFSEdge.fromE(
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
                    newedge = GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).drop()
                    )

                else:
                    newedge = GremlinFSEdge.fromE(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).drop()
                    )

        except Exception as e:
            pass

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

    def parents(self, _list_ = []):

        node = self

        if not _list_:
            _list_ = gfslist([])
        else:
            _list_ = gfslist(_list_)

        parent = node.parent()
        if parent and parent.get("id") and parent.get("id") != node.get("id"):
            _list_.append(parent)
            return parent.parents(_list_.tolist())

        return _list_.tolist()

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
        return self.children()



class GremlinFSEdge(GremlinFSNode):

    logger = GremlinFSLogger.getLogger("GremlinFSEdge")

    @classmethod
    def parse(self, id):

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

        # _default_ to label
        return {
            "label": id
        }

    @classmethod
    def make(self, name, label, uuid = None):
        return GremlinFSEdge(name = name, label = label, uuid = uuid)

    @classmethod
    def load(self, id):
        clazz = self

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

    @classmethod
    def fromMap(self, map):

        # JS jump:
        # UnsupportedSyntaxError: '**kwargs' syntax isn't supported
        # return clazz(**vals)
        node = GremlinFSEdge()
        node.fromobj(map)

        return node

    @classmethod
    def fromMaps(self, maps):
        # clazz = self
        nodes = gfslist([])

        for map in maps:

            # JS jump:
            # UnsupportedSyntaxError: '**kwargs' syntax isn't supported
            # list(nodes).append(clazz(**vals))
            node = GremlinFSEdge()
            node.fromobj(map)
            nodes.append(node)

        return nodes.tolist()

    @classmethod
    def fromE(self, e):
        clazz = self
        return GremlinFSEdge.fromMap(
            e.valueMap(True).next()
        )

    @classmethod
    def fromEs(self, es):
        clazz = self
        return GremlinFSEdge.fromMaps(
            es.valueMap(True).toList()
        )

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

        try:

            self.g().E(
                node.get("id")
            ).drop().next()

        except Exception as e:
            # self.logger.exception(' GremlinFS: delete exception ', e)
            return False

        return True



# Decorator/Adapter pattern
class GremlinFSNodeWrapper(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSNodeWrapper")

    def __init__(self, node):

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

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
                    ret = gfslist([])
                    for edgenode in edgenodes:
                        ret.append(GremlinFSNodeWrapper(edgenode))
                    return ret.tolist()
                elif len(edgenodes) == 1:
                    return GremlinFSNodeWrapper(edgenodes[0])

        except Exception as e:
            pass

        return self.get(attr)

    def all(self, prefix = None):

        node = self.node

        dsprefix = "ds"
        if prefix:
            dsprefix = "ds." + prefix

        existing = gfsmap({})
        existing.update(node.all(prefix))

        datasources = gfsmap({})
        datasources.update(node.all(dsprefix))

        props = gfsmap({})

        for key in dict(existing):
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
                            # TODO: Fix
                            props[key] = str(ret) # .strip()
                    # else:
                    elif key in existing:
                        value = existing.get(key)
                        if value:
                            # Mustache does not allow properties with '.' in the name
                            # as '.' denotes field/object boundary. Therefore all mustache
                            # given properties has to use '__' to indicated '.'
                            # props[key.replace(".", "__")] = str(value).strip()
                            # TODO: Fix
                            props[key] = str(value) # .strip()
                except Exception as e:
                    self.logger.exception(' GremlinFS: all exception ', e)

        return props.tomap()

    def keys(self, prefix = None):
        return self.all(prefix).keys()

    def has(self, key, prefix = None):
        pass

    def set(self, key, value, prefix = None):
        pass

    def get(self, key, _default_ = None, prefix = None):

        node = self.node

        # Mustache does not allow properties with '.' in the name
        # as '.' denotes field/object boundary. Therefore all mustache
        # given properties has to use '__' to indicated '.'
        key = key.replace("__", ".")

        dsprefix = "ds"
        if prefix:
            dsprefix = "ds." + prefix

        existing = None
        if node.has(key, prefix):
            existing = node.get(key, _default_, prefix)

        datasource = None
        if node.has(key, dsprefix):
            datasource = node.get(key, _default_, dsprefix)

        prop = None

        if datasource:
            try:
                ret, log, err = GremlinFS.operations().eval(
                    datasource,
                    self
                )
                if ret:
                    # TODO: Fix
                    prop = str(ret) # .strip()

            except Exception as e:
                self.logger.exception(' GremlinFS: get exception ', e)

        else:
            prop = existing

        return prop

    def property(self, name, _default_ = None, prefix = None):
        pass



class GremlinFSUtils(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSUtils")

    @classmethod
    def missing(self, value):
        if value:
            raise GremlinFSExistsError()

    @classmethod
    def found(self, value):
        if not value:
            raise GremlinFSNotExistsError()
        return value

    @classmethod
    def irepl(self, old, data, index = 0):

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

        _new_ = ""

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
            _new_ = prefix + infix + suffix
        elif lprefix > 0 and linfix > 0:
            _new_ = prefix + infix
        elif linfix > 0 and lsuffix > 0:
            _new_ = infix + suffix
        else:
            _new_ = infix

        return _new_

    @classmethod
    def link(self, path):
        pass

    @classmethod
    def utils(self):
        return GremlinFSUtils()

    def __init__(self, **kwargs):

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

        self.setall(kwargs)

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

    def query(self, query, node = None, _default_ = None):
        pass

    def eval(self, command, node = None, _default_ = None):
        pass

    def config(self, key = None, _default_ = None):
        return GremlinFS.operations().config(key, _default_)

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

        return self.linkpath(nodepath)

    def linkpath(self, path):

        if not path:
            return None

        return self.config("mount_point") + path

    # 

    def tobytes(self, data):
        return data

    def tostring(self, data):
        return data

    def decode(self, data, encoding = "base64"):
        return data

    def encode(self, data, encoding = "base64"):
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
        pass

    def recompile(self, pattern):
        pass

    def genuuid(self, UUID = None):
        pass

    def gentime(self):
        pass



class GremlinFSEvent(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSEvent")

    def __init__(self, **kwargs):

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

        self.setall(kwargs)

    def toJSON(self):

        data = {
           "event": self.get("event")
        }

        if self.has("node") and self.get("node"):
            data["node"] = self.get("node").all()

        if self.has("link") and self.get("link"):
            data["link"] = self.get("link").all()

        if self.has("source") and self.get("source"):
            data["source"] = self.get("source").all()

        if self.has("target") and self.get("target"):
            data["target"] = self.get("target").all()

        return data
        


class GremlinFSConfig(GremlinFSBase):

    logger = GremlinFSLogger.getLogger("GremlinFSConfig")

    @classmethod
    def defaults(self):
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
            "mq_queue": 'gfs-queue',

            "log_level": GremlinFSLogger.getLogLevel(),

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

        # JS jump:
        # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
        super().__init__()

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
    def instance(self, instance = None):
        if instance:
            GremlinFS.__instance = instance
        return GremlinFS.__instance

    @classmethod
    def operations(self):
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

        return self

    def connection(self, ro = False):
        pass

    def mqconnection(self):
        pass

    def mqchannel(self):
        pass

    def g(self):
        pass

    def ro(self):
        pass

    def a(self):
        pass

    def mq(self):
        pass

    def mqevent(self, event):
        pass

    def mqonevent(self, node, event, chain = [], data = {}, propagate = True):
        pass

    def mqonmessage(self, ch, method, properties, body):
        pass

    def query(self, query, node = None, _default_ = None):
        pass

    def eval(self, command, node = None, _default_ = None):
        pass

    def config(self, key = None, _default_ = None):
        return self._config.get(key, _default_)

    def utils(self):
        return self._utils

    def getfs(self, fsroot, fsinit = False):
        return fsroot

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



__all__ = [
    'GremlinFSBase',
    # 'GremlinFSMap',
    'GremlinFSPath',
    'GremlinFSNode',
    'GremlinFSVertex',
    'GremlinFSEdge',
    'GremlinFSNodeWrapper',
    'GremlinFSUtils',
    'GremlinFSEvent',
    'GremlinFSConfig',
    'GremlinFS' 
]

__default__ = 'GremlinFS'
