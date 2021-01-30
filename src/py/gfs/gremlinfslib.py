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

from gfs.error.error import GremlinFSError
from gfs.error.error import GremlinFSExistsError
from gfs.error.error import GremlinFSNotExistsError
from gfs.error.error import GremlinFSIsFileError
from gfs.error.error import GremlinFSIsFolderError

# from gfs.api.common.api import GFSAPI
from gfs.api.common.api import GFSCachingAPI

# 
# 
# import config



class GremlinFSBase(GFSObj):

    logger = GFSLogger.getLogger("GremlinFSBase")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def property(self, name, default = None, prefix = None):
        return self.get(name, default, prefix = prefix)



class GremlinFSNode(GremlinFSBase):

    logger = GFSLogger.getLogger("GremlinFSNode")

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
        # if not name:
        #     return default
        # if not label:
        #     return default
        # for label_config in GremlinFS.operations().config("labels", []):
        #     if "type" in label_config and label_config["type"] == fstype:
        #         compiled = None
        #         if "compiled" in label_config:
        #             compiled = label_config["compiled"]
        #         else:
        #             compiled = GremlinFS.operations().utils().recompile(label_config["pattern"])
        # 
        #         if compiled:
        #             if compiled.search(name):
        #                 label = label_config.get("label", default)
        #                 break
        # 
        return label

    @classmethod
    def vals(clazz, invals):
        if not invals:
            return {}

        vals = {}
        vals["id"] = GremlinFSUtils.value( invals.get("id") )
        vals["label"] = GremlinFSUtils.value( invals.get("label") )
        for key, val in invals.get("properties").items():
            vals[key] = GremlinFSUtils.value( val )

        return vals

    @classmethod
    def fromMap(clazz, map):
        vals = clazz.vals(map.get("@value"))
        return clazz(**vals)

    @classmethod
    def fromMaps(clazz, maps):
        nodes = []
        for map in maps:
            vals = clazz.vals(map.get("@value"))
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
            # Mimic traditional filesystem extensions
            if maplabel == "vertex":
                return mapname
            elif maplabel == self.config("folder_label"):
                return mapname
            else:
                return mapname + "." + maplabel

        elif mapname and maplabel:
            # Mimic traditional filesystem extensions
            if maplabel == "vertex":
                return mapname
            elif maplabel == self.config("folder_label"):
                return mapname
            else:
                return mapname + "." + maplabel

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

        # if encoding:
        #     data = self.utils().decode(data, encoding)
        # 
        # elif data.startswith("base64:"):
        #     data = self.utils().decode(data, "base64")
        # 
        # return data

        # Rely on encoding prefix
        return self.utils().decode(data)

    def setProperty(self, name, data = None, encoding = None, prefix = None):

        node = self

        if not node:
            return

        if not data:
            data = ""

        nodeid = node.get("id")

        if encoding:
            data = self.utils().encode(data, encoding)

        node.set(name, data, prefix = prefix)
        self.api().setVertexProperty(
            nodeid,
            name,
            data
        )

        return data

    def unsetProperty(self, name, prefix = None):

        node = self

        if not node:
            return

        nodeid = node.get("id")

        node.set(name, None, prefix = prefix)
        self.api().unsetVertexProperty(
            nodeid,
            name,
            prefix = prefix
        )

    def setProperties(self, properties, prefix = None):

        node = self

        if not node:
            return

        nodeid = node.get("id")

        self.api().setVertexProperties(
            nodeid,
            name,
            prefix = prefix
        )

    def getProperties(self, prefix = None):

        node = self

        properties = {}
        properties.update(node.all(prefix))

        return properties

    def readProperty(self, name, default = None, encoding = None, prefix = None):
        return self.getProperty(name, default, encoding = encoding, prefix = prefix)

    def writeProperty(self, name, data, encoding = None, prefix = None):
        return self.setProperty(name, data, encoding = encoding, prefix = prefix)

    def invoke(self, script, handler, context = {}):
        pass

    def handlers(self):
        pass

    def handler(self, event):
        pass

    def event(self, event, chain = [], data = {}, propagate = True):
        pass



class GremlinFSVertex(GremlinFSNode):

    logger = GFSLogger.getLogger("GremlinFSVertex")

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
                        GremlinFS.operations().api().vertices(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GremlinFSVertex.fromV(
                        GremlinFS.operations().api().vertices(
                            parts["label"], 
                            {
                                "uuid": parts["uuid"]
                            }
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
                        GremlinFS.operations().api().vertices(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GremlinFSVertex.fromV(
                        GremlinFS.operations().api().vertices(
                            parts["label"], 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
            except Exception as e:
                # self.logger.exception(' GremlinFS: node from path ID exception ', e)
                return None

        elif parts and \
            "uuid" in parts:
            try:
                return GremlinFSVertex.fromV(
                    GremlinFS.operations().api().vertices(
                        None, 
                        {
                            "uuid": parts["uuid"]
                        }
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
                    GremlinFS.operations().api().vertex(
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
                v # .valueMap(*names).next()
            )

        else:
            return GremlinFSVertex.fromMap(
                v # .valueMap(True).next()
            )

    @classmethod
    def fromVs(clazz, vs, names = []):
        if names:
            # return GremlinFSVertex.fromVals(
            return GremlinFSVertex.fromMaps(
                vs # .valueMap(*names).toList()
            )

        else:
            return GremlinFSVertex.fromMaps(
                vs # .valueMap(True).toList()
            )

    def edges(self, edgeid = None, ine = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                return GremlinFSEdge.fromEs(
                    self.api().inEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            else:
                return GremlinFSEdge.fromEs(
                    self.api().outEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        elif node and label:

            # try:

            if ine:
                return GremlinFSEdge.fromEs(
                    self.api().inEdges(
                        node.get("id"), 
                        label
                    )
                )

            else:
                return GremlinFSEdge.fromEs(
                    self.api().outEdges(
                        node.get("id"), 
                        label
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        elif node:

            # try:

            if ine:
                return GremlinFSEdge.fromEs(
                    self.api().inEdges(
                        node.get("id")
                    )
                )

            else:
                return GremlinFSEdge.fromEs(
                    self.api().outEdges(
                        node.get("id")
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

    def edge(self, edgeid, ine = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                return GremlinFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            else:
                return GremlinFSEdge.fromE(
                    self.api().outEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        elif node and label:

            # try:

            if ine:
                return GremlinFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label
                    )
                )

            else:
                return GremlinFSEdge.fromE(
                    self.api().outEdges(
                        node.get("id"), 
                        label
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        return None

    def edgenodes(self, edgeid = None, ine = True, inv = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                if inv:
                    return GremlinFSVertex.fromVs(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GremlinFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

            else:
                if inv:
                    return GremlinFSVertex.fromVs(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GremlinFSVertex.fromVs(
                        self.api().outEdgesOutVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        elif node and label:

            # try:

            if ine:
                if inv:
                    return GremlinFSVertex.fromVs(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GremlinFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label
                        )
                    )

            else:
                if inv:
                    return GremlinFSVertex.fromVs(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GremlinFSVertex.fromVs(
                        self.api().outEdgesOutVertices(
                            node.get("id"), 
                            label
                        )
                    )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        elif node:

            # try:

            if ine:
                if inv:
                    return GremlinFSVertex.fromVs(
                        self.api().inEdgesInVertices(
                            node.get("id")
                        )
                    )

                else:
                    return GremlinFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            node.get("id")
                        )
                    )

            else:
                if inv:
                    return GremlinFSVertex.fromVs(
                        self.api().outEdgesInVertices(
                            node.get("id")
                        )
                    )

                else:
                    return GremlinFSVertex.fromVs(
                        self.api().outEdgesOutVertices(
                            node.get("id")
                        )
                    )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

    def edgenode(self, edgeid, ine = True, inv = True):

        node = self

        label = GremlinFSEdge.infer("label", edgeid, None)
        name = GremlinFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                if inv:
                    return GremlinFSVertex.fromV(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GremlinFSVertex.fromV(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

            else:
                if inv:
                    return GremlinFSVertex.fromV(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GremlinFSVertex.fromV(
                        self.api().outEdgesOutVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

        elif node and label:

            # try:

            if ine:
                if inv:
                    return GremlinFSVertex.fromV(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GremlinFSVertex.fromV(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label
                        )
                    )

            else:
                if inv:
                    return GremlinFSVertex.fromV(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GremlinFSVertex.fromV(
                        self.api().outEdgesOutVertices(
                            node.get("id"), 
                            label
                        )
                    )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

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

    def create(self):

        node = self

        label = node.get('label', None)
        name = node.get('name', None)

        newnode = GremlinFSVertex.fromV(
            self.api().createVertex(
                label, {
                    "name": name
                }
            )
        )

        return newnode

    def rename(self, name):

        node = self

        node.setProperty(
            "name",
            name
        )

    def move(self, parent = None):

        node = self

        self.api().deleteOutEdges(
            node.get("id"),
            self.config("in_label")
        )

        self.api().createEdge(
            node.get("id"),
            parent.get("id"),
            self.config("in_label"), {
                'name': self.config("in_label")
            }
        )

    def delete(self):

        node = self

        if not node:
            return None

        # try:

        self.api().deleteVertex(
            node.get("id")
        )

        # except Exception as e:
        #     self.logger.exception(' GremlinFS: delete exception ', e)
        #     return False

        return True

    def context(self):

        node = self

        return self.api().context(
            node.get("id")
        )

    def render(self):

        node = self

        return self.api().render(
            node.get("id")
        )

    def createFolder(self):

        node = self

        label = node.get('label', None)
        name = node.get('name', None)

        if not label:
            label = GremlinFS.operations().defaultFolderLabel()

        newfolder = GremlinFSVertex.fromV(
            self.api().createVertex(
                label, {
                    "name": name
                }
            )
        )

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

        # try:

        if name:
            newedge = GremlinFSEdge.fromE(
                self.api().createEdge(
                    source.get("id"),
                    target.get("id"),
                    label, {
                        'name', name,
                        'uuid', str(self.utils().genuuid())
                    }
                )
            )

        else:
            newedge = GremlinFSEdge.fromE(
                self.api().createEdge(
                    source.get("id"),
                    target.get("id"),
                    label, {
                        'name', name,
                        'uuid', str(self.utils().genuuid())
                    }
                )
            )

        # except Exception as e:
        #     self.logger.exception(' GremlinFS: createLink exception ', e)
        #     return None

        return newedge

    def getLink(self, label, name = None, ine = True):

        node = self

        if not node:
            return None

        if not label:
            return None

        # try:

        if name:
            if ine:
                return GremlinFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            else:
                return GremlinFSEdge.fromE(
                    self.api().outEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

        else:
            if ine:
                return GremlinFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label
                    )
                )

            else:
                return GremlinFSEdge.fromE(
                    self.api().outEdges(
                        node.get("id"), 
                        label
                    )
                )

        # except:
        #     pass

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

        # try:

        if name:
            if ine:
                # GremlinFSEdge.fromE(
                # nodeid, edgelabel, edgeprops
                self.api().deleteVertexInEdge(
                    node.get("id"),
                    label, {
                        'name', name
                    }
                )
                # )

            else:
                # GremlinFSEdge.fromE(
                # nodeid, edgelabel, edgeprops
                self.api().deleteVertexOutEdge(
                    node.get("id"),
                    label, {
                        'name', name
                    }
                )
                # )

        else:
            if ine:
                # GremlinFSEdge.fromE(
                self.api().deleteVertexInEdge(
                    node.get("id"),
                    label
                )
                # )

            else:
                # GremlinFSEdge.fromE(
                self.api().deleteVertexOutEdge(
                    node.get("id"),
                    label
                )
                # )

        # except Exception as e:
        #     self.logger.exception(' GremlinFS: deleteLink exception ', e)
        #     return False

        return True

    def parent(self):

        node = self

        # try:

        return GremlinFSVertex.fromMap(
            self.api().inEdgeInVertices(
                node.get("id"),
                self.config("in_label")
            )
        )

        # except Exception as e:
        #     # self.logger.exception(' GremlinFS: parent exception ', e)
        #     return None

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

    # def children(self):
    #     pass

    # def readFolderEntries(self):
    #     return self.children()



class GremlinFSEdge(GremlinFSNode):

    logger = GFSLogger.getLogger("GremlinFSEdge")

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
                        GremlinFS.operations().api().edges(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GremlinFSEdge.fromE(
                        GremlinFS.operations().api().edges(
                            parts["label"], 
                            {
                                "uuid": parts["uuid"]
                            }
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
                        GremlinFS.operations().api().edges(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GremlinFSEdge.fromE(
                        GremlinFS.operations().api().edges(
                            parts["label"], 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
            except Exception as e:
                # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
                return None

        elif parts and \
            "uuid" in parts:
            try:
                return GremlinFSEdge.fromE(
                    GremlinFS.operations().api().edges(
                        None, 
                        {
                            "uuid": parts["uuid"]
                        }
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
                    GremlinFS.operations().api().edge(
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
        edge.set('id', GremlinFSUtils.value( obj.id ) );
        edge.set('label', obj.label);
        edge.set('outV', GremlinFSUtils.value( obj.outV.id ) );
        # edge.set('outVLabel', obj.outV.label);
        edge.set('inV', GremlinFSUtils.value( obj.inV.id ) );
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
            edge.set('id', GremlinFSUtils.value( obj.id ) );
            edge.set('label', obj.label);
            edge.set('outV', GremlinFSUtils.value( obj.outV.id ) );
            # edge.set('outVLabel', obj.outV.label);
            edge.set('inV', GremlinFSUtils.value( obj.inV.id ) );
            # edge.set('inVLabel', obj.inV.label);
            edges.append(edge);
        return edges.tolist();

    def node(self, inv = True):

        edge = self

        if edge:

            # try:

            if inv:
                return GremlinFSVertex.fromV(
                    self.api().inVertices(
                        edge.get("id")
                    )
                )

            else:
                return GremlinFSVertex.fromV(
                    self.api().outVertices(
                        edge.get("id")
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: node exception ', e)
            #     return None

    def delete(self):

        node = self

        if not node:
            return None

        # try:

        self.api().deleteEdge(
            node.get("id")
        )

        # except Exception as e:
        #     self.logger.exception(' GremlinFS: delete exception ', e)
        #     return False

        return True



class GremlinFSUtils(GremlinFSBase):

    logger = GFSLogger.getLogger("GremlinFSUtils")

    @classmethod
    def value(clazz, value, default = None):

        # if value and "@value" in value:
        if value and type(value) == dict and "@value" in value:
            return value["@value"]

        elif type(value) in (tuple, list):
            return value[0]

        # Check truthy, int value 0 is False, but defined
        # hence can't check simple if
        # elif value:
        elif value is not None:
            return value

        else:
            return default

    # TODO, remove?
    # clean up config and let be dict that we pass in
    @classmethod
    def conf(clazz, key, default = None):
        # 
        return default

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

    def api(self):
        return GremlinFS.operations().api()

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

        # if not path:
        #     return None

        # return "%s%s" % (self.config("mount_point"), path)
        pass

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

    # Rely on encoding prefix
    # def decode(self, data, encoding = "base64"):
    def decode(self, data):
        try:
            if data and data.startswith("base64:"):
                import base64
                data = self.utils().tostring(
                    base64.b64decode(
                        self.utils().tobytes(data[7:])
                    )
                )
        except:
            return data
        # else:
        #     data = self.utils().tostring(
        #         base64.b64decode(
        #             self.utils().tobytes(data)
        #         )
        #     )
        return data

    def encode(self, data, encoding = "base64"):
        import base64
        data = "%s:%s" % ( encoding, self.utils().tostring(
            base64.b64encode(
                self.utils().tobytes(data)
            )
        ))
        return data

    # def render(self, template, templatectx):
    #     pass

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

    logger = GFSLogger.getLogger("GremlinFSEvent")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def toJSON(self):
        pass



class GremlinFSConfig(GremlinFSBase):

    logger = GFSLogger.getLogger("GremlinFSConfig")

    @classmethod
    def defaults(clazz):
        return {
            # "mount_point": None,

            "gfs_host": None,
            "gfs_port": None,
            "gfs_username": None,
            # "gfs_password": None,
            "gfs_url": None,

            "log_level": GFSLogger.getLogLevel(),

            "client_id": "0010",
            "fs_ns": "gfs1",
            "fs_root": None,
            "fs_root_init": False,

            "folder_label": 'group',
            "ref_label": 'ref',
            "in_label": 'in',
            "self_label": 'self',
            "template_label": 'template',
            "view_label": 'view',

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

    logger = GFSLogger.getLogger("GremlinFS")

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



        self._config = None

    # def __init__(
    def configure(
        self,

        # mount_point,

        gfs_host,
        gfs_port,
        gfs_username,
        gfs_password,

        **kwargs):

        # self.mount_point = mount_point
        # 
        # self.logger.debug(' GremlinFS mount point: ' + self.mount_point)

        self.gfs_host = gfs_host
        self.gfs_port = gfs_port
        self.gfs_username = gfs_username
        self.gfs_password = gfs_password

        self.gfs_url = "http://" + self.gfs_host + ":" + self.gfs_port

        self.logger.debug(' GremlinFS gfs host: ' + self.gfs_host)
        self.logger.debug(' GremlinFS gfs port: ' + self.gfs_port)
        # self.logger.debug(' GremlinFS gfs username: ' + self.gfs_username)
        # self.logger.debug(' GremlinFS gfs password: ' + self.gfs_password)
        self.logger.debug(' GremlinFS gfs URL: ' + self.gfs_url)

        self._config = GremlinFSConfig(

            # mount_point = mount_point,

            gfs_host = gfs_host,
            gfs_port = gfs_port,
            gfs_username = gfs_username,
            gfs_password = gfs_password,

        )

        # self._api = GFSAPI(
        self._api = GFSCachingAPI(
            gfs_host = gfs_host,
            gfs_port = gfs_port,
            gfs_username = gfs_username,
            gfs_password = gfs_password,
        )

        self._utils = GremlinFSUtils()

        # register
        self.register()

        return self

    # 

    def api(self):
        return self._api

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

    def config(self, key = None, default = None):
        return self._config.get(key, default)

    def utils(self):
        return GremlinFSUtils.utils()

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
                self.api().vertices(
                    type_name, {
                        'namespace': namespace,
                        'name': client_id + "@" + hostname,
                        'hw_address': hwaddr
                    }
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

                self.api().createVertex(
                    type_name, {
                        'name': client_id + "@" + hostname,
                        'uuid': str(pathuuid),
                        'namespace': namespace,
                        'created': int(pathtime),
                        'modified': int(pathtime),
                        'client_id': client_id,
                        'hostname': hostname,
                        'ip_address': ipaddr,
                        'hw_address': hwaddr,
                        'machine_architecture': platform.processor(),
                        'machine_hardware': platform.machine(),
                        'system_name': platform.system(),
                        'system_release': platform.release(),
                        'system_version': platform.version()
                    }
                )

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
