# 
# Copyright (c) 2019, John Grundback
# All rights reserved.
# 

# 
import os
import sys
import logging
import errno
import stat
import uuid
import re
import traceback

# 
from time import time

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



class GremlinFSBase(object):

    def all(self):
        props = {}
        for prop, value in vars(self).iteritems():
            if prop and len(prop) > 1 and prop[0] == "_":
                props[prop[1:]] = value
        return props

    def keys(self):
        keys = []
        for prop, value in vars(self).iteritems():
            if prop and len(prop) > 1 and prop[0] == "_":
                keys.append(prop[1:])
        return keys

    def has(self, key):
        if not hasattr(self, "_%s" % (key)):
            return False
        return True

    def set(self, key, value):
        if key != "__class__":
            setattr(self, "_%s" % (key), value)

    def get(self, key, default = None):
        if not self.has(key):
            return default
        value = getattr(self, "_%s" % (key), default)
        return value



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
    def match(clazz, path, config, graph, g):

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

        return GremlinFSPath(config=config, graph=graph, g=g, **match)

    def __init__(self, config, graph, g, **kwargs):

        self._config = config
        self._graph = graph
        self._g = g

        self._utils = GremlinFSUtils.utils(self.config(), self.graph(), self.g())
        self._debug = False

        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def config(self, key = None, default = None):
        if key:
            return self._config.get(key, default)
        return self._config

    def graph(self):
        return self._graph

    def g(self):
        return self._g

    def utils(self):
        return self._utils

    # 

    def enter(self, functioname, *args, **kwargs):
        if self._debug:
            logging.debug(' GremlinFSPath: ENTER: %s ' % (functioname))
            logging.debug(args)
            logging.debug(kwargs)

    # 

    def root(self):

        root = None
        if self.config().get("folder_root"):
            root = self.atid(
                self.config().get("folder_root")
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

        nodes = self.utils().listFolderEntries(node)
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
            node = self.atid( self._vertexid )
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

    def edge(self, node, edgeid, ine = True):

        parts = self.fromid(edgeid)

        if not parts:
            return None

        if node and "name" in parts and "label" in parts:

            try:

                if ine:
                    return GremlinFSNode.fromMap(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            parts.get("label")
                        ).has(
                            "name", parts.get("name")
                        ).valueMap(True).next()
                    )
                else:
                    return GremlinFSNode.fromMap(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            parts.get("label")
                        ).has(
                            "name", parts.get("name")
                        ).valueMap(True).next()
                    )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

        elif node and "name" in parts:

            try:

                if ine:
                    return GremlinFSNode.fromMap(
                        self.g().V(
                            node.get("id")
                        ).inE().has(
                            "name", parts.get("name")
                        ).valueMap(True).next()
                    )
                else:
                    return GremlinFSNode.fromMap(
                        self.g().V(
                            node.get("id")
                        ).outE().has(
                            "name", parts.get("name")
                        ).valueMap(True).next()
                    )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

        return None

    def edgenode(self, node, edgeid, ine = True, inv = True):

        parts = self.fromid(edgeid)
        if not parts:
            return None

        if node and "name" in parts and "label" in parts:

            try:

                if ine:
                    if inv:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                parts.get("label")
                            ).has(
                                "name", parts.get("name")
                            ).inV().valueMap(True).next() )

                    else:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).inE(
                                parts.get("label")
                            ).has(
                                "name", parts.get("name")
                            ).outV().valueMap(True).next() )

                else:
                    if inv:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                parts.get("label")
                            ).has(
                                "name", parts.get("name")
                            ).inV().valueMap(True).next() )

                    else:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).outE(
                                parts.get("label")
                            ).has(
                                "name", parts.get("name")
                            ).outV().valueMap(True).next() )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

        elif node and "name" in parts:

            try:

                if ine:
                    if inv:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).inE().has(
                                "name", parts.get("name")
                            ).inV().valueMap(True).next() )

                    else:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).inE().has(
                                "name", parts.get("name")
                            ).outV().valueMap(True).next() )

                else:
                    if inv:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).outE().has(
                                "name", parts.get("name")
                            ).inV().valueMap(True).next() )

                    else:
                        return GremlinFSNode.fromMap(
                            self.g().V(
                                node.get("id")
                            ).outE().has(
                                "name", parts.get("name")
                            ).outV().valueMap(True).next() )

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
                return None

    # 

    def fromid(self, id):

        if not id:
            return None

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
        # ^(.+)\@(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$
        matcher = re.match(
            r"^(.+)\.(.+)\@(.+)\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$",
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
        # ^(.+)\.(.+)\@.+$
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

    def atid(self, id):

        parts = self.fromid(id)
        if parts and \
            "uuid" in parts and \
            "name" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GremlinFSNode.fromMap(
                        self.g().V().has(
                            "uuid", parts["uuid"]
                        ).valueMap(True).next()
                    )
                else:
                    return GremlinFSNode.fromMap(
                        self.g().V().hasLabel(
                            parts["label"]
                        ).has(
                            "uuid", parts["uuid"]
                        ).valueMap(True).next()
                    )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        elif parts and \
            "uuid" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GremlinFSNode.fromMap(
                        self.g().V().has(
                            "uuid", parts["uuid"]
                        ).valueMap(True).next()
                    )
                else:
                    return GremlinFSNode.fromMap(
                        self.g().V().hasLabel(
                            parts["label"]
                        ).has(
                            "uuid", parts["uuid"]
                        ).valueMap(True).next()
                    )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        elif parts and \
            "uuid" in parts:
            try:
                return GremlinFSNode.fromMap(
                    self.g().V().has(
                        "uuid", parts["uuid"]
                    ).valueMap(True).next()
                )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        # Fallback try as straigt up DB id
        else:
            try:
                return GremlinFSNode.fromMap(
                    self.g().V(
                        id
                    ).valueMap(True).next()
                )
            except:
                # logging.error(' GremlinFS: node from path ID exception ')
                return None

        return None

    def infer(self, field, obj, default = None):
        parts = self.fromid( obj ) # self._name )
        if not field in parts:
            return default;
        return parts.get(field, default);

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
            if node and self.utils().isFolder(node):
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
            if node and self.utils().isFile(node):
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
            if node and self.utils().isLink(node):
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
            edge = self.edge( node, self._vertexedge, True )

            return False

        elif self._path == "vertex_out_edge":

            node = GremlinFSUtils.found( self.node() )
            edge = self.edge( node, self._vertexedge, False )

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


    def createFolder(self, mode = None):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            newname = self.infer("name", self._name)
            newlabel = self.infer("label", self._name, self.config("folder_label"))
            newuuid = self.infer("uuid", self._name)
            parent = self.parent(self.get("parent", []))

            if newlabel != self.config("folder_label"):
                raise FuseOSError(errno.ENOENT)

            if not newname:
                raise FuseOSError(errno.ENOENT)

            parent = self.parent(self.get("parent", []))
            newfolder = self.utils().createFolder(
                newname,
                self.config("folder_label"),
                parent,
                newuuid,
                mode
            )

            return True

        # elif self._path == "vertex_labels":
        #     return default

        # elif self._path == "vertex_label":
        #     return default

        # elif self._path == "vertexes":
        #     return default

        elif self._path == "vertex":

            newname = self.infer("name", self._name)
            newlabel = self.infer("label", self._name, "vertex")
            newuuid = self.infer("uuid", self._name)
            parent = self.parent(self.get("parent", []))

            # Do not create an A vertex in /V/B, unless A is vertex
            if label != "vertex":
                if label != newlabel:
                    raise FuseOSError(errno.ENOENT)

            if not newname:
                raise FuseOSError(errno.ENOENT)

            if newlabel == self.config("folder_label"):
                newfolder = self.utils().createFolder(
                    newname,
                    self.config("folder_label"),
                    None,
                    newuuid,
                    mode
                )

            else:
                newfile = self.utils().createNode(
                    newname,
                    newlabel,
                    None,
                    newuuid,
                    mode
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

            nodes = self.utils().listFolderEntries(root)
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

            nodes = self.utils().listFolderEntries(parent)
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
                    nodes = GremlinFSNode.fromMaps(
                        self.g().V(
                            parent.get("id")
                        ).inE(
                            self.config("in_label")
                        ).has(
                            'name', self.config("in_name")
                        ).outV().valueMap(True).toList()
                    )
                else:
                    nodes = GremlinFSNode.fromMaps(
                        self.g().V(
                            parent.get("id")
                        ).inE(
                            self.config("in_label")
                        ).has(
                            'name', self.config("in_name")
                        ).outV().hasLabel(
                            label
                        ).valueMap(True).toList()
                    )

            else:
                if label == "vertex":
                    nodes = GremlinFSNode.fromMaps(
                        self.g().V().valueMap(True).toList()
                    )
                else:
                    nodes = GremlinFSNode.fromMaps(
                        self.g().V().hasLabel(
                            label
                        ).valueMap(True).toList()
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
            nodes = GremlinFSNode.fromMaps(
                self.g().V(
                    node.get("id")
                ).inE().valueMap(True).toList()
            )
            if nodes:
                for cnode in nodes:
                    nodeid = cnode.toid()
                    if nodeid:
                        entries.append( nodeid )

            return entries

        elif self._path == "vertex_out_edges":
            label = self._vertexlabel
            node = GremlinFSUtils.found( self.node() )
            nodes = GremlinFSNode.fromMaps(
                self.g().V(
                    node.get("id")
                ).outE().valueMap(True).toList()
            )
            if nodes:
                for cnode in nodes:
                    nodeid = cnode.toid()
                    if nodeid:
                        entries.append( nodeid )

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

            newname = self.infer("name", self._name)
            newlabel = self.infer("label", self._name, "vertex")
            newuuid = self.infer("uuid", self._name)
            parent = self.parent(self.get("parent", []))

            if not newname:
                raise FuseOSError(errno.ENOENT)

            newfile = self.utils().createFile(
                newname,
                newlabel,
                parent,
                newuuid,
                mode
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
            self.utils().setNodeProperty(
                node,
                self._vertexproperty,
                data
            )
            return self.utils().getNodeProperty(
                node,
                self._vertexproperty,
                ""
            )

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
            return str(data) # data
        return None

    def readFileLength(self):
        data = self.readNode()
        if data:
            return len(str(data))
        return 0

    def writeFile(self, data, offset = 0):
        return self.writeNode(data, offset)

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
            source = sourcematch.node()

            parts = self.fromid( self._vertexedge )
            if parts and "name" in parts and "label" in parts:
                # we are the target, in edge means ...
                self.utils().createLink(
                    source,
                    node,
                    parts["label"],
                    parts["name"],
                    mode
                )
            elif parts and "name" in parts:
                # we are the target, in edge means ...
                self.utils().createLink(
                    source,
                    node,
                    None,
                    parts["name"],
                    mode
                )

            return True

        elif self._path == "vertex_out_edge":

            node = self.node()
            source = sourcematch.node()

            parts = self.fromid( self._vertexedge )
            if parts and "name" in parts and "label" in parts:
                # we are the target, out edge means ...
                self.utils().createLink(
                    node,
                    source,
                    parts["label"],
                    parts["name"],
                    mode
                )
            elif parts and "name" in parts:
                # we are the target, out edge means ...
                self.utils().createLink(
                    node,
                    source,
                    None,
                    parts["name"],
                    mode
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
            edgenode = self.edgenode( node, self._vertexedge, True, False )
            newpath = self.utils().nodelink(
                edgenode
            )

            return newpath

        elif self._path == "vertex_out_edge":
            node = GremlinFSUtils.found( self.node() )
            edgenode = self.edgenode( node, self._vertexedge, False, True )
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

            parts = self.fromid( self._vertexedge )
            if parts and "name" in parts and "label" in parts:
                # we are the target, in edge means ...
                self.utils().deleteLink(
                    node,
                    parts["label"],
                    True,
                    parts["name"]
                )
            elif parts and "name" in parts:
                # we are the target, in edge means ...
                self.utils().deleteLink(
                    node,
                    None,
                    True,
                    parts["name"]
                )

            return True

        elif self._path == "vertex_out_edge":

            node = self.node()

            parts = self.fromid( self._vertexedge )
            if parts and "name" in parts and "label" in parts:
                # we are the target, out edge means ...
                self.utils().deleteLink(
                    node,
                    parts["label"],
                    False,
                    parts["name"]
                )
            elif parts and "name" in parts:
                # we are the target, out edge means ...
                self.utils().deleteLink(
                    node,
                    None,
                    False,
                    parts["name"]
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
            node = self.utils().file(self.node())
            data = self.utils().readNodeProperty(
                node,
                self.config("data_property"),
                ""
            )

            if size > 0 and offset > 0:
                return data[offset:offset + size]
            elif offset > 0:
                return data[offset:]
            elif size > 0:
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
            data = self.utils().getNodeProperty(
                node,
                self._vertexproperty,
                ""
            )

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
            node = self.utils().file(self.node())

            # self.utils().setNodeProperty(
            #     node,
            #     self.config("data_property"),
            #     data
            # )

            self.utils().writeNodeProperty(
                node,
                self.config("data_property"),
                data,
                offset
            )

            # newdata = 
            self.utils().readNodeProperty(
                self.node(),
                self.config("data_property"),
                ""
            )

            # if data != newdata:
            #     logging.error(' GremlinFS: written file data differs: input: "%s", output: "%s" ' % (data, newdata))
            #     raise ValueError(' GremlinFS: written file data differs: input: "%s", output: "%s" ' % (data, newdata))
            # # elif len(data) != len(newdata):
            # #     ...

            return data # newdata

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

            # self.utils().setNodeProperty(
            #     node,
            #     self._vertexproperty,
            #     data
            # )

            self.utils().setNodeProperty(
                node,
                self._vertexproperty,
                data,
                offset
            )

            newnode = GremlinFSUtils.found( self.node() )
            # newdata = 
            self.utils().getNodeProperty(
                newnode,
                self._vertexproperty,
                ""
            )

            # if data != newdata:
            #     logging.error(' GremlinFS: written file data differs: input: "%s", output: "%s" ' % (data, newdata))
            #     raise ValueError(' GremlinFS: written file data differs: input: "%s", output: "%s" ' % (data, newdata))
            # # elif len(data) != len(newdata):
            # #     ...

            return data # newdata

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

        elif self._path == "create_vertex":
            return default

        return default

    def moveNode(self, newmatch):

        default = None
        if self._type:
            default = None

        # if self._path == "root":
        #     return default

        # el
        if self._path == "atpath":

            parts = newmatch.fromid( newmatch._name )
            newuuid = parts.get("uuid", None)
            newname = parts.get("name", None)
            newtype = parts.get("type", None)
            # Cannot change node label at this time
            # newlabel = parts.get("label", "vertex")

            node = GremlinFSUtils.found(self.node())
            parent = newmatch.parent(newmatch.get("parent", []))

            self.utils().renameNode(node, newmatch._name)
            self.utils().moveNode(node, parent)

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

            data = self.utils().getNodeProperty(
                oldnode,
                oldname,
                ""
            )

            newmatch.utils().setNodeProperty(
                newnode,
                newname,
                data
            )

            newdata = newmatch.utils().getNodeProperty(
                newnode,
                newname,
                ""
            )
            if newdata == data:
                self.utils().unsetNodeProperty(
                    oldnode,
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
            self.utils().deleteNode(node)

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
            self.utils().unsetNodeProperty(
                node,
                self._vertexproperty
            )

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
                self.utils().setNodeProperty(
                    node,
                    key,
                    value
                )

    def getProperty(self, key, default = None):

        if self._path == "atpath":
            node = self.node()
            if node:
                return self.utils().getNodeProperty(
                    node,
                    key,
                    default
            )

        return default



class GremlinFSNode(GremlinFSBase):

    @classmethod
    def vals(self, invals):
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
    def fromV(clazz, v):
        pass

    # @classmethod
    # def fromVs(clazz, vs):
    #     pass

    @classmethod
    def fromMap(clazz, map):
        vals = GremlinFSNode.vals(map)
        return GremlinFSNode(**vals)

    @classmethod
    def fromMaps(clazz, maps):
        nodes = []
        for map in maps:
            vals = GremlinFSNode.vals(map)
            nodes.append(GremlinFSNode(**vals))
        return nodes

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    def toid(self, short = False):
        node = self
        mapid = node.get('id', None)
        mapuuid = node.get('uuid', None)
        maplabel = node.get('label', None)
        mapname = node.get('name', None)

        if mapname:
            mapname = mapname.strip().replace("\t", "").replace("\t", "")

        # if maplabel == "vertex":
        #     maplabel = None

        # elif maplabel == GremlinFSUtils.conf('folder_label', 'group'):
        #     maplabel = None

        if mapname and mapuuid and maplabel and not short:
            return "%s@%s@%s" % (mapname, maplabel, mapuuid)

        elif mapname and maplabel and short:

            if maplabel == "vertex":
                return mapname

            elif maplabel == GremlinFSUtils.conf('folder_label', 'group'):
                return mapname

            else:
                return mapname # "%s.%s" % (mapname, maplabel)

        elif mapname and maplabel:
            return mapname # "%s.%s" % (mapname, maplabel)

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
    def utils(clazz, config, graph, g):
        return GremlinFSUtils(config=config, graph=graph, g=g)

    def __init__(self, config, graph, g, **kwargs):

        self._config = config

        self._graph = graph
        self._g = g
        self._debug = False

        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def config(self, key = None, default = None):
        if key:
            return self._config.get(key, default)
        return self._config

    def graph(self):
        return self._graph

    def g(self):
        return self._g

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

    # 

    def isFolder(self, node):

        if not node:
            return False

        if node.get("label") != "group":
            return False

        return True

    def folder(self, node):
        if not node:
            raise FuseOSError(errno.ENOENT)
        if not self.isFolder(node):
            raise FuseOSError(errno.ENOENT)
        return node

    def isFile(self, node):

        if not node:
            return False

        if node.get("label") == "group":
            return False

        return True

    def file(self, node):
        if not node:
            raise FuseOSError(errno.ENOENT)
        if self.isFolder(node):
            raise FuseOSError(errno.EISDIR)
        elif not self.isFile(node):
            raise FuseOSError(errno.ENOENT)
        return node

    def createNode(self, name, label = None, parent = None, UUID = None, mode = None, owner = None, group = None):

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

            if label:
                if parent:
                    newnode = GremlinFSNode.fromV(
                        self.g().addV(
                            label
                        ).property(
                            'name', name
                        ).property(
                            'uuid', str(pathuuid)
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
                        ).addE(
                            self.config("in_label")
                        ).property(
                            'name', self.config("in_name")
                        ).to(__.V(
                            parent.get("id")
                        )).next()
                    )
                else:
                    newnode = GremlinFSNode.fromV(
                        self.g().addV(
                            label
                        ).property(
                            'name', name
                        ).property(
                            'uuid', str(pathuuid)
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
                        ).next()
                    )
            else:
                if parent:
                    newnode = GremlinFSNode.fromV(
                        self.g().addV().property(
                            'name', name
                        ).property(
                            'uuid', str(pathuuid)
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
                        ).addE(
                            self.config("in_label")
                        ).property(
                            'name', self.config("in_name")
                        ).to(__.V(
                            parent.get("id")
                        )).next()
                    )
                else:
                    newnode = GremlinFSNode.fromV(
                        self.g().addV().property(
                            'name', name
                        ).property(
                            'uuid', str(pathuuid)
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
                        ).next()
                    )

            newnode = GremlinFSNode.fromMap(
                self.g().V().has(
                    'uuid', str(pathuuid)
                ).valueMap(True).next()
            )

            # self.graph().tx().commit()

        except:
            logging.error(' GremlinFS: createNode exception ')
            traceback.print_exc()
            return None

        return newnode

    def renameNode(self, node, name):

        if not node:
            return None

        # if not name:
        #     return None

        newnode = None

        if name:

            try:

                newnode = GremlinFSNode.fromV(
                    self.g().V(
                        node.get("id")
                    ).property(
                        'name', name
                    ).next()
                )

            except:
                logging.error(' GremlinFS: renameNode exception ')
                traceback.print_exc()
                return None

        try:

            newnode = GremlinFSNode.fromMap(
                self.g().V(
                    node.get("id")
                ).valueMap(True).next()
            )

        except:
            logging.error(' GremlinFS: renameNode exception ')
            traceback.print_exc()
            return None

        return newnode

    def moveNode(self, node, parent = None):

        if not node:
            return None

        # if not name:
        #     return None

        newnode = None

        # drop() on edges often/always? throw exceptions?
        try:

            newnode = GremlinFSNode.fromV(
                self.g().V(
                    node.get("id")
                ).outE(
                    self.config("in_label")
                ).has(
                    'name', self.config("in_name")
                ).drop().next()
            )

        except:
            pass

        if parent:

            try:

                newnode = GremlinFSNode.fromV(
                    self.g().V(
                        node.get("id")
                    ).addE(
                        self.config("in_label")
                    ).property(
                        'name', self.config("in_name")
                    ).to(__.V(
                        parent.get("id")
                    )).next()
                )

            except:
                logging.error(' GremlinFS: moveNode exception ')
                traceback.print_exc()
                return None

        try:

            newnode = GremlinFSNode.fromMap(
                self.g().V(
                    node.get("id")
                ).valueMap(True).next()
            )

        except:
            logging.error(' GremlinFS: moveNode exception ')
            traceback.print_exc()
            return None

        return newnode

    def deleteNode(self, node):

        if not node:
            return None

        try:

            self.g().V(
                node.get("id")
            ).drop().next()

        except:
            # logging.error(' GremlinFS: deleteNode exception ')
            # traceback.print_exc()
            return False

        return True

    def createFolder(self, name, label = None, parent = None, UUID = None, mode = None, owner = None, group = None):

        if not name:
            return None

        if not label:
            label = self.config("folder_label")

        if not mode:
            mode = GremlinFSUtils.conf("default_mode", 0o644)

        if not owner:
            owner = GremlinFSUtils.conf("default_uid", 0)

        if not group:
            group = GremlinFSUtils.conf("default_gid", 0)

        newfolder = None

        try:

            pathuuid = None
            if UUID:
                pathuuid = uuid.UUID(UUID)
            else:
                pathuuid = uuid.uuid1()

            pathtime = time()

            # txn = self.graph().tx()

            if parent:
                newfolder = GremlinFSNode.fromV(
                    self.g().addV(
                        label
                    ).property(
                        'name', name
                    ).property(
                        'uuid', str(pathuuid)
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
                    ).addE(
                        self.config("in_label")
                    ).property(
                        'name', self.config("in_name")
                    ).to(__.V(
                        parent.get("id")
                    )).next()
                )
            else:
                newfolder = GremlinFSNode.fromV(
                    self.g().addV(
                        label
                    ).property(
                        'name', name
                    ).property(
                        'uuid', str(pathuuid)
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
                    ).next()
                )

            newfolder = GremlinFSNode.fromMap(
                self.g().V().hasLabel(
                    label
                ).has(
                    'uuid', str(pathuuid)
                ).valueMap(True).next()
            )

            GremlinFSNode.fromV(
                self.g().V(
                    newfolder.get("id")
                ).addE(
                    self.config("self_label")
                ).property(
                    'name', self.config("self_name")
                ).to(__.V(
                    newfolder.get("id")
                )).next()
            )

            # self.graph().tx().commit()

        except:
            logging.error(' GremlinFS: createFolder exception ')
            traceback.print_exc()
            return None

        return newfolder

    def createFile(self, name, label = None, parent = None, UUID = None, mode = None, owner = None, group = None):
        return self.createNode(name, label, parent, UUID, mode, owner, group)

    def createLink(self, source, target, label, name = None, mode = None, owner = None, group = None):

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

                newnode = GremlinFSNode.fromV(
                    self.g().V(
                        source.get("id")
                    ).addE(
                        label
                    ).property(
                        'name', name
                    ).to(__.V(
                        target.get("id")
                    )).next()
                )

            else:

                newnode = GremlinFSNode.fromV(
                    self.g().V(
                        source.get("id")
                    ).addE(
                        label
                    ).to(__.V(
                        target.get("id")
                    )).next()
                )

        except:
            logging.error(' GremlinFS: createLink exception ')
            traceback.print_exc()
            return None

        return newnode

    def deleteLink(self, node, label, ine = True, name = None):

        if not node:
            return None

        if not label:
            return None

        newnode = None

        # drop() on edges often/always? throw exceptions?
        try:

            if name:

                if ine:
                    newnode = GremlinFSNode.fromV(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).property(
                            'name', name
                        ).drop().next()
                    )

                else:
                    newnode = GremlinFSNode.fromV(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).property(
                            'name', name
                        ).drop().next()
                    )

            else:

                if ine:
                    newnode = GremlinFSNode.fromV(
                        self.g().V(
                            node.get("id")
                        ).inE(
                            label
                        ).drop().next()
                    )

                else:
                    newnode = GremlinFSNode.fromV(
                        self.g().V(
                            node.get("id")
                        ).outE(
                            label
                        ).drop().next()
                    )

        except:
            pass

        return True

    # def readNodeProperty(self, node, name, default=None):
    def getNodeProperty(self, node, name, default=None, encoding = None):

        if not node:
            return default

        data = node.get(name, None)
        if not data:
            return default

        if encoding:
            import base64
            # data = base64.b64decode(data).decode('utf-8')
            data = base64.b64decode(data) # .decode('utf-8')

        return data

    def setNodeProperty(self, node, name = None, data = None, encoding = None):

        if not node:
            return

        if not data:
            data = ""

        nodeid = node.get("id")

        if encoding:
            import base64
            # data = base64.b64encode(data.encode('utf-8'))
            data = base64.b64encode(data) # .encode('utf-8'))

        # GremlinFSNode.fromV(
        self.g().V(
            nodeid
        ).property(
            name, data
        ).next()
        # )

        return data

    def unsetNodeProperty(self, node, name = None):

        if not node:
            return

        nodeid = node.get("id")

        # Having issues with exception throwing, even though deletion works
        try:
            GremlinFSNode.fromV(
                self.g().V(
                    nodeid
                ).properties(
                    name
                ).drop().next()
            )
        except:
            pass

    def readNodeProperty(self, node, name, default=None):
        return self.getNodeProperty(node, name, default, "base64")

    def writeNodeProperty(self, node, name, data, offset = 0):

        # def irepl(clazz, old, data, index = 0):
        old = self.getNodeProperty(node, name, None, "base64")
        new = GremlinFSUtils.irepl(old, data, offset)

        self.setNodeProperty(node, name, new, "base64")

        return new

    def listFolderEntryNames(self, node = None):

        if not node:
            return self.g().V().where(
                __.out(
                    self.config("in_label")
                ).count().is_(0)
            ).values(
                "name"
            ).dedup().toList()

        else:
            return self.g().V(
                node.get("id")
            ).inE(
                self.config("in_label")
            ).outV().values(
                'name'
            ).dedup().toList()

        return []

    def listFolderEntries(self, node = None):

        if not node:
            return GremlinFSNode.fromMaps(
                self.g().V().where(
                    __.out(
                        self.config("in_label")
                    ).count().is_(0)
                ).valueMap(True).toList()
            )

        else:
            return GremlinFSNode.fromMaps(
                self.g().V(
                    node.get("id")
                ).inE(
                    self.config("in_label")
                ).outV().valueMap(True).toList()
            )

        return []



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

        self.gremlin_url = "ws://" + self.gremlin_host + ":" + self.gremlin_port + "/gremlin";

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

        self._graph = None
        self._g = None

    def connection(self):

        if self._g:
            return self._g

        graph = Graph()
        g = graph.traversal().withRemote(DriverRemoteConnection(
            self.gremlin_url,
            'g',
            username = self.gremlin_username,
            password = self.gremlin_password
        ))

        self._graph = graph
        self._g = g

        return self._g

    def graph(self):

        if not self._graph:
            g = self.connection()            

        return self._graph

    def g(self):

        if not self._g:
            g = self.connection()            

        return self._g

    # def initfs(self):
    # 
    #     newfs = self.utils().createFolder(
    #         self.config("folder_root"),
    #         self.config("folder_label"),
    #         None,
    #         None
    #     )
    # 
    #     return newfs.get("id")

    def getfs(self, fsroot, fsinit = False):

        fsid = fsroot

        # if not ... and fsinit:
        #     fs = None
        #     fsid = self.initfs();

        return fsid

    def config(self, key = None, default = None):
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

            "folder_root": self.getfs(
                GremlinFSUtils.conf('folder_root', None),
                GremlinFSUtils.conf('folder_root_init', False)
            ),
            "folder_root_init": GremlinFSUtils.conf('folder_root_init', False)

        }

        if key:
            return config.get(key, default)

        return config

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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
            match.enter("chmod", path, mode)
            if match:
                if match.isFound():
                    match.setProperty("mode", mode)

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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
            match.enter("chown", path, uid, gid)
            if match:
                if match.isFound():
                    match.setProperty("owner", uid)
                    match.setProperty("group", gid)

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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
            match.enter("create", path, mode)
            if match:
                if not match.isFound():
                    created = match.createFile(mode)
                    # if created:
                    #     match.setProperty("mode", mode)

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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
            match.enter("mkdir", path, mode)

            if match:
                if not match.isFound():
                    created = match.createFolder(mode)
                    # if created:
                    #     match.setProperty("mode", mode)

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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

            oldmatch = GremlinFSPath.match(old, self.config(), self.graph(), self.g())
            newmatch = GremlinFSPath.match(new, self.config(), self.graph(), self.g())
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

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

            targetmatch = GremlinFSPath.match(target, self.config(), self.graph(), self.g())
            sourcematch = GremlinFSPath.match(source, self.config(), self.graph(), self.g())

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
        return 0

    def unlink(self, path):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
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

        newdata = None

        try:

            match = GremlinFSPath.match(path, self.config(), self.graph(), self.g())
            match.enter("write", path, data, offset)
            if match:
                if match.isFile() and match.isFound():
                    data = match.writeFile(data, offset)
                else:
                    raise FuseOSError(errno.ENOENT)

            # if data != newdata:
            #     logging.error(' GremlinFS: written file data differs: input: "%s", output: "%s" ' % (data, newdata))
            #     raise ValueError(' GremlinFS: written file data differs: input: "%s", output: "%s" ' % (data, newdata))
            # # elif len(data) != len(newdata):
            # #     ...

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: write exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if data:
            return len(str(data))

        # raise FuseOSError(errno.ENOENT)
        return 0

    # 

    def isReadOnly(self):
        return False

    def notReadOnly(self):
        if self.isReadOnly():
            raise FuseOSError(errno.EROFS)
        return True



class GremlinFS(GremlinFSOperations):
    pass



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
        FUSE(
            GremlinFS(

                mount_point = mount_point,

                gremlin_host = gremlin_host,
                gremlin_port = gremlin_port,
                gremlin_username = gremlin_username,
                gremlin_password = gremlin_password,

                rabbitmq_host = rabbitmq_host,
                rabbitmq_port = rabbitmq_port,
                rabbitmq_username = rabbitmq_username,
                rabbitmq_password = rabbitmq_password

            ),
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
        return args[index];
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
