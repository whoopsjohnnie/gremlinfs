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
import addict
from addict import Dict

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
# from gremlin_python import statics
# from gremlin_python.structure.graph import Graph, Vertex, Edge
# from gremlin_python.process.graph_traversal import __
# from gremlin_python.process.strategies import *
# from gremlin_python.process.traversal import T, P, Operator
# from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 
# import pika

# 
from gfs.common.log import GFSLogger
from gfs.common.obj import GFSObj

# from gfs.api.common.api import GFSAPI
# from gfs.api.common.api import GFSCachingAPI

from gfs.gfs import GremlinFS
from gfs.gfs import GFSBase
from gfs.gfs import GremlinFSUtils
from gfs.gfs import GremlinFSConfig

from gfs.model.vertex import GFSVertex
from gfs.model.edge import GFSEdge

# 
# 
# import config



# 
logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)



class GremlinFSPath(GFSBase):

    logger = GFSLogger.getLogger("GremlinFSPath")

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
                root = GFSVertex.load(
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
            nodes = GFSVertex.fromVs(
                GremlinFS.operations().api().verticesWithEdge(
                    "in", 
                    node.get("id")
                )
            )

        else:
            nodes = GFSVertex.fromVs(
                GremlinFS.operations().api().verticesWithoutEdge(
                    "in"
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
            nodes = GFSVertex.fromVs(
                GremlinFS.operations().api().verticesWithEdge(
                    "in", 
                    parent.get("id")
                )
            )
            if nodes:
                for cnode in nodes:
                    if cnode and cnode.get("name") == nodeid:
                        node = cnode
                        break

        elif nodeid:
            node = GFSVertex.load( nodeid )

        elif path:
            node = GremlinFSPath.atpath( path )

        return node

    @classmethod
    def pathparent(clazz, path = []):

        root = None
        if GremlinFS.operations().config("fs_root", None):
            root = GFSVertex.load(
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

                elif node and node.edge( expanded[vindex + 2], False ):
                    match.update({
                        "path": "vertex_out_edge",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexedge": GremlinFSUtils.found( expanded[vindex + 2] )
                    })

                # else:
                #     # Note; Unless I throw this here, I am unable to
                #     # touch files as attributes. I think the default
                #     # here should be to throw FuseOSError(errno.ENOENT)
                #     # unless file/node is actually found
                #     raise FuseOSError(errno.ENOENT)
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

    def api(self):
        return GremlinFS.operations().api()

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
        self.logger.debug(' GremlinFSPath: ENTER: %s ' % (functioname))
        self.logger.debug(args)
        self.logger.debug(kwargs)

    # 

    def root(self):

        root = None
        if self.config("fs_root"):
            root = GFSVertex.load(
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
            raise FuseOSError(errno.ENOENT)

        if self.isFile():
            raise FuseOSError(errno.ENOENT)

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            newname = GFSVertex.infer("name", self._name)
            newlabel = GFSVertex.infer("label", self._name, GremlinFS.operations().defaultFolderLabel())
            newlabel = GFSVertex.label(newname, newlabel, "folder", GremlinFS.operations().defaultFolderLabel())
            newuuid = GFSVertex.infer("uuid", self._name)
            parent = self.parent()

            if not newname:
                raise FuseOSError(errno.ENOENT)

            parent = self.parent()
            newfolder = GFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).createFolder()

            # TODO: assign/move to parent
            if parent:
                newfolder.move(parent)

            return True

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        elif self._path == "vertex":

            newname = GFSVertex.infer("name", self._name)
            newlabel = GFSVertex.infer("label", self._name, "vertex")
            newlabel = GFSVertex.label(newname, newlabel, "file", "vertex")
            newuuid = GFSVertex.infer("uuid", self._name)
            parent = self.parent()

            # Do not create an A vertex in /V/B, unless A is vertex
            if newlabel != "vertex":
                if newlabel != newlabel:
                    raise FuseOSError(errno.ENOENT)

            if not newname:
                raise FuseOSError(errno.ENOENT)

            if GremlinFS.operations().isFolderLabel(newlabel):
                # newfolder = 
                GFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).createFolder()

                # TODO: assign/move to parent
                if parent:
                    node.move(parent)

            else:
                # newfile = 
                GFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).create()

                # TODO: assign/move to parent
                if parent:
                    node.move(parent)

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
            raise FuseOSError(errno.ENOENT)

        entries = []

        if self._path == "root":
            entries.extend([
                self.config("vertex_folder")
            ])

            root = self.root()

            nodes = None
            if root:
                nodes = GFSVertex.fromVs(
                    GremlinFS.operations().api().verticesWithEdge(
                        "in", 
                        root.get("id")
                    )
                )

            else:
                nodes = GFSVertex.fromVs(
                    GremlinFS.operations().api().verticesWithoutEdge(
                        "in"
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
            nodes = GFSVertex.fromVs(
                GremlinFS.operations().api().verticesWithEdge(
                    "in", 
                    parent.get("id")
                )
            )
            if nodes:
                for node in nodes:
                    nodeid = node.toid(True)
                    if nodeid:
                        entries.append(nodeid)

            return entries

        elif self._path == "vertex_labels":
            # ...
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
                    nodes = GFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            parent.get("id"), 
                            self.config("in_label"), 
                            {
                                "name": self.config("in_name")
                            }
                        )
                    )

                else:
                    nodes = GFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            parent.get("id"), 
                            self.config("in_label"), 
                            {
                                "name": self.config("in_name")
                            }, 
                            label
                        )
                    )

            else:
                if label == "vertex":
                    nodes = GFSVertex.fromVs(
                        self.api().vertices()
                    )

                else:
                    nodes = GFSVertex.fromVs(
                        self.api().vertices(
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

            edges = GFSEdge.fromEs(
                self.api().outEdges(
                    node.get("id")
                )
            )
            if edges:
                for cedge in edges:
                    if cedge.get("label") and cedge.get("name"):
                        entries.append( "%s@%s" % (cedge.get("name"), cedge.get("label")) )
                    elif cedge.get("label"):
                        entries.append( "%s" % (cedge.get("label")) )

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
            edges = GFSEdge.fromEs(
                self.api().inEdges(
                    node.get("id")
                )
            )
            if edges:
                for cedge in edges:
                    if cedge.get("label") and cedge.get("name"):
                        entries.append( "%s@%s" % (cedge.get("name"), cedge.get("label")) )
                    elif cedge.get("label"):
                        entries.append( "%s" % (cedge.get("label")) )

            return entries

        elif self._path == "vertex_out_edges":
            label = self._vertexlabel
            if not label:
                label = "vertex"

            node = GremlinFSUtils.found( self.node() )
            edges = GFSEdge.fromEs(
                self.api().outEdges(
                    node.get("id")
                )
            )
            if edges:
                for cedge in edges:
                    if cedge.get("label") and cedge.get("name"):
                        entries.append( "%s@%s" % (cedge.get("name"), cedge.get("label")) )
                    elif cnode.get("label"):
                        entries.append( "%s" % (cedge.get("label")) )

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
            raise FuseOSError(errno.ENOENT)

        return self.moveNode(newmatch)

    def deleteFolder(self):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

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
            raise FuseOSError(errno.ENOENT)

        if self.isFolder():
            raise FuseOSError(errno.ENOENT)

        if not data:
            data = ""

        default = data
        if self._type:
            default = data

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            newname = GFSVertex.infer("name", self._name)
            newlabel = GFSVertex.infer("label", self._name, "vertex")
            newlabel = GFSVertex.label(newname, newlabel, "file", "vertex")
            newuuid = GFSVertex.infer("uuid", self._name)
            parent = self.parent()

            if not newname:
                raise FuseOSError(errno.ENOENT)

            newfile = GFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).create()

            # TODO: assign/move to parent
            if parent:
                newfile.move(parent)

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

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        data = self.readNode(size, offset)
        if data:
            return data
        return None

    def readFileLength(self):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        data = self.readNode()
        if data:
            try:
                return len(data)
            except Exception as e:
                pass

        return 0

    def writeFile(self, data, offset = 0):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        return self.writeNode(data, offset)

    def clearFile(self):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        return self.clearNode()

    def renameFile(self, newmatch):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        return self.moveNode(newmatch)

    def deleteFile(self):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

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
            raise FuseOSError(errno.ENOENT)

        if not sourcematch.isFound():
            raise FuseOSError(errno.ENOENT)

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

        elif self._path == "vertex_property":

            node = self.node()

            # We are the target
            # To create an inbound link, we shall pass source=sourcematch, target=node
            # To create an outbound link, we shall pass source=source and target=target
            source = node
            target = sourcematch.node()

            label = GFSEdge.infer("label", self._vertexproperty, None)
            name = GFSEdge.infer("name", self._vertexproperty, None)

            if not label and name:
                label = name
                name = None

            # Create link from source to target
            # Inbound means source=target and target=source
            # newlink = 
            source.createLink(
                target = target,
                label = label,
                name = name,
                mode = mode
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

        elif self._path == "vertex_in_edge":

            node = self.node()

            # We are the target
            # To create an inbound link, we shall pass source=source and target=target
            # To create an outbound link, we shall pass source=target and target=source
            source = sourcematch.node()
            target = node

            label = GFSEdge.infer("label", self._vertexedge, None)
            name = GFSEdge.infer("name", self._vertexedge, None)

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

            return True

        elif self._path == "vertex_out_edge":

            node = self.node()

            # We are the target
            # To create an inbound link, we shall pass source=sourcematch, target=node
            # To create an outbound link, we shall pass source=source and target=target
            source = node
            target = sourcematch.node()

            label = GFSEdge.infer("label", self._vertexedge, None)
            name = GFSEdge.infer("name", self._vertexedge, None)

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

            return True

        # elif self._path == "create_vertex":
        #     return default

        return default        

    def readLink(self):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

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
            raise FuseOSError(errno.ENOENT)

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

        elif self._path == "vertex_property":

            node = self.node()

            label = GFSEdge.infer("label", self._vertexedge, None)
            name = GFSEdge.infer("name", self._vertexedge, None)

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

            node = self.node()

            label = GFSEdge.infer("label", self._vertexedge, None)
            name = GFSEdge.infer("name", self._vertexedge, None)

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

            return True

        elif self._path == "vertex_out_edge":

            node = self.node()

            label = GFSEdge.infer("label", self._vertexedge, None)
            name = GFSEdge.infer("name", self._vertexedge, None)

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

            return True

        # elif self._path == "create_vertex":
        #     return default

        return default

    # 

    def readNode(self, size = 0, offset = 0):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

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
            raise FuseOSError(errno.ENOENT)

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
            raise FuseOSError(errno.ENOENT)

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

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        # if newmatch.isFound():
        #     raise FuseOSError(errno.ENOENT)

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

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

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

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        if self._path == "atpath":
            node = self.node()
            if node:
                node.setProperty(
                    key,
                    value
                )

        return True

    def getProperty(self, key, default = None):

        if not self.isFound():
            raise FuseOSError(errno.ENOENT)

        if self._path == "atpath":
            node = self.node()
            if node:
                return node.getProperty(
                    key,
                    default
            )

        return default
