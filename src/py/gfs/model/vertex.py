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
from gfs.common.base import GFSBase

from gfs.error.error import GFSError
from gfs.error.error import GFSExistsError
from gfs.error.error import GFSNotExistsError
from gfs.error.error import GFSIsFileError
from gfs.error.error import GFSIsFolderError

# from gfs.api.common.api import GFSAPI
from gfs.api.common.api import GFSCachingAPI

# 
# 
# import config



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
            raise GFSNotExistsError(self)
        if not self.isFolder():
            raise GFSNotExistsError(self)
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
            raise GFSNotExistsError(self)
        if self.isFolder():
            raise GFSIsFolderError(self)
        elif not self.isFile():
            raise GFSNotExistsError(self)
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
