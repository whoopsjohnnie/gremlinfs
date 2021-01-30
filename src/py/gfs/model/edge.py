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



class GFSEdge(GFSNode):

    logger = GFSLogger.getLogger("GFSEdge")

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

        parts = GFSEdge.parse(id)
        if parts and \
            "uuid" in parts and \
            "name" in parts and \
            "label" in parts:
            try:
                if parts["label"] == "vertex":
                    return GFSEdge.fromE(
                        GremlinFS.operations().api().edges(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GFSEdge.fromE(
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
                    return GFSEdge.fromE(
                        GremlinFS.operations().api().edges(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GFSEdge.fromE(
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
                return GFSEdge.fromE(
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
                return GFSEdge.fromE(
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
#         node = GFSEdge()
#         node.fromobj(map)
#         return node
# 
#     @classmethod
#     def fromMaps(clazz, maps):
#         nodes = gfslist([])
#         for map in maps:
#             node = GFSEdge()
#             node.fromobj(map)
#             nodes.append(node)
#         return nodes.tolist()

#     @classmethod
#     def fromE(clazz, e):
#         return GFSEdge.fromMap(
#             e.valueMap(True).next()
#         )
# 
#     @classmethod
#     def fromEs(clazz, es):
#         return GFSEdge.fromMaps(
#             es.valueMap(True).toList()
#         )

    @classmethod
    def fromE(clazz, e):
        # var clazz;
        # clazz = this;
        obj = e.next();
        # if obj and obj['value']:
        #     obj = obj['value']

        edge = GFSEdge();
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
            edge = GFSEdge();
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
                return GFSVertex.fromV(
                    self.api().inVertices(
                        edge.get("id")
                    )
                )

            else:
                return GFSVertex.fromV(
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
