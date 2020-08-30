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
from gremlin_python import statics
from gremlin_python.structure.graph import Graph, Vertex, Edge
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 
import pika

# 
# 
import config



# 
logging.basicConfig(level=config.gremlinfs['log_level'])
# logging.basicConfig(level=logging.DEBUG)



# 
# https://pika.readthedocs.io/en/stable/examples/blocking_consume_recover_multiple_hosts.html
# 
def amqp_rcve(operations = None):

    import time

    reconnect = True

    while(reconnect):

        mqexchangename = operations.config("mq_exchange")
        mqexchangetype = operations.config("mq_exchange_type")

        mqqueuename = operations.config("client_id") + '-' + operations.config("fs_ns")

        mqroutingkeys = operations.config("mq_routing_keys")
        mqroutingkey = operations.config("mq_routing_key")

        logging.info( ' GremlinFS: AMQP DETAILS: ' )
        logging.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
        logging.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
        logging.info( ' GremlinFS: MQ ROUTING KEYS: ' )
        logging.info(mqroutingkeys)
        logging.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )

        try:
            logging.info(
                ' GremlinFS: Connecting to AMQP: queue: ' + 
                mqqueuename + 
                ' with exchange: ' + 
                mqexchangetype + '/' + mqexchangename)

            mqconnection = operations.mqconnection()
            mqchannel = mqconnection.channel()
            mqchannel.exchange_declare(
                exchange = mqexchangename,
                exchange_type = mqexchangetype,
            )
            mqchannel.queue_declare(
                queue = mqqueuename
            )
            routing_keys = mqroutingkeys
            for routing_key in routing_keys:
                logging.info(
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
            logging.info(
                ' GremlinFS: Setting up AMQP queue consumer: ' + 
                mqqueuename)
            mqchannel.basic_consume(
                queue = mqqueuename,
                auto_ack = True,
                on_message_callback = operations.mqonmessage
            )
            logging.info(
                ' GremlinFS: Consuming AMQP queue: ' + 
                mqqueuename)
            mqchannel.start_consuming()
            logging.info(
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
            logging.error(' GremlinFS: Caught an AMQP connection error: {} '.format(err))
            break

        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            logging.info(' GremlinFS: AMQP connection was closed, reconnecting ')
            time.sleep(10)
            continue



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
    def path(clazz, path, node = None):

        if not node:
            root = None
            if GremlinFSUtils.conf("fs_root", None):
                root = GremlinFSVertex.load(
                    GremlinFSUtils.conf("fs_root", None)
                )
            node = root

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
                GremlinFS.operations().g().V().where(
                    __.out(
                        GremlinFSUtils.conf("in_label", "in")
                    ).count().is_(0)
                )
            )

        if nodes:
            for cnode in nodes:
                if cnode.toid(True) == elem:
                    return GremlinFSPath.path(path[1:], cnode)

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
            node = GremlinFSPath.path( path )

        return node

    @classmethod
    def pathparent(clazz, path = []):

        root = None
        if GremlinFSUtils.conf("fs_root", None):
            root = GremlinFSVertex.load(
                GremlinFSUtils.conf("fs_root", None)
            )

        parent = root

        if not path:
            return parent

        vindex = 0
        for i, item in enumerate(path):
            if item == GremlinFSUtils.conf("vertex_folder", ".V"):
                vindex = i

        if vindex:
            if vindex > 0:
                parent = GremlinFSPath.path( path[0:vindex] )

        else:
            parent = GremlinFSPath.path( path )

        return parent

    @classmethod
    def match(clazz, path):

        match = {

            "path": None,
            "full": None,
            "parent": None,
            "node": None,

            "vertexlabel": "vertex",
            "vertexid": None,
            "vertexuuid": None,
            "vertexname": None,
            "vertexproperty": None,
            "vertexedge": None

        }

        # Remove mount point from path if present. Symlinks often give the full path
        # including mount point
        if( (path) and (path.startswith( GremlinFS.operations().mount_point )) ):
            path = path[len(GremlinFS.operations().mount_point):]

        match["full"] = clazz.expand(path)
        expanded = match.get("full", [])

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

                if expanded[vindex + 2] == GremlinFSUtils.conf("in_edge_folder", "EI"):
                    match.update({
                        "path": "vertex_in_edges",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] )
                    })

                elif expanded[vindex + 2] == GremlinFSUtils.conf("out_edge_folder", "EO"):
                    match.update({
                        "path": "vertex_out_edges",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] )
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

                if expanded[vindex + 2] == GremlinFSUtils.conf("in_edge_folder", "EI"):
                    match.update({
                        "path": "vertex_in_edge",
                        "vertexid": GremlinFSUtils.found( expanded[vindex + 1] ),
                        "vertexedge": GremlinFSUtils.found( expanded[vindex + 3] )
                    })

                elif expanded[vindex + 2] == GremlinFSUtils.conf("out_edge_folder", "EO"):
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

    def ro(self):
        return GremlinFS.operations().ro()

    def mq(self):
        return GremlinFS.operations().mq()

    def mqevent(self, event, **kwargs):
        return GremlinFS.operations().mqevent(event, **kwargs)

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

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
            parent = self.parent()

            if not newname:
                raise FuseOSError(errno.ENOENT)

            parent = self.parent()
            # newfolder = 
            GremlinFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).createFolder(
                parent = parent,
                mode = mode
            )

#             self.mqevent(
#                 event = "create_node",
#                 node = newfolder
#             )

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
                    raise FuseOSError(errno.ENOENT)

            if not newname:
                raise FuseOSError(errno.ENOENT)

            if GremlinFS.operations().isFolderLabel(newlabel):
                # newfolder = 
                GremlinFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).createFolder(
                    parent = None,
                    mode = mode
                )

#                 self.mqevent(
#                     event = "create_node",
#                     node = newfolder
#                 )

            else:
                # newfile = 
                GremlinFSVertex.make(
                    name = newname,
                    label = newlabel,
                    uuid = newuuid
                ).create(
                    parent = None,
                    mode = mode
                )

#                 self.mqevent(
#                     event = "create_node",
#                     node = newfile
#                 )

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
                GremlinFSUtils.conf("in_edge_folder", "EI"), 
                GremlinFSUtils.conf("out_edge_folder", "EO"),
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
            parent = self.parent()

            if not newname:
                raise FuseOSError(errno.ENOENT)

            # newfile = 
            GremlinFSVertex.make(
                name = newname,
                label = newlabel,
                uuid = newuuid
            ).create(
                parent = parent,
                mode = mode
            )

#             self.mqevent(
#                 event = "create_node",
#                 node = newfile
#             )

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

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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
            # newlink = 
            source.createLink(
                target = target,
                label = label,
                name = name,
                mode = mode
            )

#             self.mqevent(
#                 event = "create_link",
#                 link = newlink,
#                 source = source,
#                 target = target
#             )

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
                name = name,
                mode = mode
            )

#             self.mqevent(
#                 event = "create_link",
#                 link = newlink,
#                 source = source,
#                 target = target
#             )

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

#                 self.mqevent(
#                     event = "delete_link",
#                     link = link
#                 )

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

#                 self.mqevent(
#                     event = "delete_link",
#                     link = link
#                 )

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

#                 self.mqevent(
#                     event = "delete_link",
#                     link = link
#                 )

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

#                 self.mqevent(
#                     event = "delete_link",
#                     link = link
#                 )

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

            data = node.render()

            if data:
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

            label_config = node.labelConfig()

            writefn = None

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

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

            try:

                if label_config and "writefn" in label_config:
                    writefn = label_config["writefn"]

            except:
                pass

            try:

                if writefn:

                    writefn(
                        node = node,
                        data = data
                    )

            except:
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

            old = tobytes(old)

            new = GremlinFSUtils.irepl(old, data, offset)

            new = tostring(new)

            node.writeProperty(
                self._vertexproperty,
                new
            )

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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
            parent = newmatch.parent()

            node.rename(newmatch._name)
            node.move(parent)

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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

#             self.mqevent(
#                 event = "update_node",
#                 node = newnode
#             )

            newdata = newnode.readProperty(
                newname,
                ""
            )

            if newdata == data:

                oldnode.unsetProperty(
                    oldname
                )

#                 self.mqevent(
#                     event = "update_node",
#                     node = oldnode
#                 )

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

#             self.mqevent(
#                 event = "delete_node",
#                 node = node
#             )

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

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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

#             self.mqevent(
#                 event = "update_node",
#                 node = node
#             )

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
                    compiled = re.compile(label_config["pattern"])

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

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def g(self):
        return GremlinFS.operations().g()

    def ro(self):
        return GremlinFS.operations().ro()

    def mq(self):
        return GremlinFS.operations().mq()

    def mqevent(self, event, **kwargs):
        return GremlinFS.operations().mqevent(event, **kwargs)

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    def utils(self):
        return GremlinFSUtils.utils()

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

        self.mqevent(
            event = "update_node",
            node = node
        )

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
        except:
            logging.error(' GremlinFS: unsetProperty exception ')

        self.mqevent(
            event = "update_node",
            node = node
        )

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
                except:
                    logging.error(' GremlinFS: setProperties exception ')
                    traceback.print_exc()

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
                # env = self.config()
                # for prop, value in iteritems(self.config()):
                #     env[str(prop)] = str(value)

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

                except:
                    logging.error(' GremlinFS: invoke handler render exception ')
                    traceback.print_exc()
                    script = data

                executable = "sh"
                subprocess.call(
                    [executable, '-c', script],
                    cwd = cwd,
                    env = env
                )

        except:
            logging.error(' GremlinFS: node invoke exception ')
            traceback.print_exc()

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

        except:
            logging.error(' GremlinFS: node event exception ')
            traceback.print_exc()



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
        return GremlinFSVertex.fromMap(
            v.valueMap(True).next()
        )

    @classmethod
    def fromVs(clazz, vs):
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

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
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

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
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

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
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

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
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

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
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

            except:
                # logging.error(' GremlinFS: edge from path ID exception ')
                # traceback.print_exc()
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
                'namespace', self.config("fs_ns")
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

            self.mqevent(
                event = "create_node",
                node = newnode
            )

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
                        'namespace', self.config("fs_ns")
                    ).property(
                        'name', name
                    )
                )

            except:
                logging.error(' GremlinFS: rename exception ')
                traceback.print_exc()
                return None

        self.mqevent(
            event = "update_node",
            node = node
        )

        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                ).has(
                    'namespace', self.config("fs_ns")
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
        # next() throws errors on delete
        try:

            # newnode = GremlinFSVertex.fromV(
            self.g().V(
                node.get("id")
            ).has(
                'namespace', self.config("fs_ns")
            ).outE(
                self.config("in_label")
            ).has(
                'name', self.config("in_name")
            ).drop().toList()
            # )

        except:
            pass

        if parent:

            try:

                newnode = GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).has(
                        'namespace', self.config("fs_ns")
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

        self.mqevent(
            event = "update_node",
            node = node
        )

        try:

            newnode = GremlinFSVertex.fromV(
                self.g().V(
                    node.get("id")
                ).has(
                    'namespace', self.config("fs_ns")
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

            # next() throws errors on delete
            self.g().V(
                node.get("id")
            ).has(
                'namespace', self.config("fs_ns")
            ).drop().toList() # .next()

        except:
            logging.error(' GremlinFS: delete exception ')
            traceback.print_exc()
            return False

        self.mqevent(
            event = "delete_node",
            node = node
        )

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

        except:
            logging.error(' GremlinFS: readNode template exception ')
            traceback.print_exc()


        try:

            ps = GremlinFS.operations().g().V( node.get('id') ).emit().repeat(
                __.inE().outV()
            ).until(
                __.inE().count().is_(0).or_().loops().is_(P.gt(10))
            ).path().toList()

            vs = GremlinFSVertex.fromVs(GremlinFS.operations().g().V( node.get('id') ).emit().repeat(
                __.inE().outV()
            ).until(
                __.inE().count().is_(0).or_().loops().is_(P.gt(10))
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

                import pystache
                renderer = pystache.Renderer()

                data = pystache.render(
                    template,
                    templatectx
                )

            elif readfn:

                data = readfn(
                    node = node,
                    wrapper = templatectx,
                    data = data
                )

        except:
            logging.error(' GremlinFS: readNode render exception ')
            traceback.print_exc()

        return data

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

                newedge = GremlinFSEdge.fromE(
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

                newedge = GremlinFSEdge.fromE(
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

            self.mqevent(
                event = "create_link",
                link = newedge,
                source = source,
                target = target
            )

        except:
            logging.error(' GremlinFS: createLink exception ')
            traceback.print_exc()
            return None

        return newnode

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
                    # GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).inE(
                        label
                    ).has(
                        'name', name
                    ).drop().toList()
                    # )

                else:
                    # GremlinFSVertex.fromV(
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
                    # GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).inE(
                        label
                    ).drop().toList()
                    # )

                else:
                    # GremlinFSVertex.fromV(
                    self.g().V(
                        node.get("id")
                    ).outE(
                        label
                    ).drop().toList()
                    # )

            self.mqevent(
                event = "delete_link",
                link = link
            )

        except:
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

        except:
            # logging.error(' GremlinFS: parent exception ')
            # traceback.print_exc()
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

    def readFolderEntries(self):
        return self.children();



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

            except:
                # logging.error(' GremlinFS: node exception ')
                # traceback.print_exc()
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

        except:
            logging.error(' GremlinFS: delete exception ')
            traceback.print_exc()
            return False

        return True



# Decorator/Adapter pattern
class GremlinFSNodeWrapper(GremlinFSBase):

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
                #     data = tobytes(data)

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

        except:
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
                except:
                    logging.error(' GremlinFS: all exception ')
                    traceback.print_exc()

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

            except:
                logging.error(' GremlinFS: get exception ')
                traceback.print_exc()

        else:
            prop = existing

        return prop

    def property(self, name, default = None, prefix = None):
        pass



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

    def ro(self):
        return GremlinFS.operations().ro()

    def mq(self):
        return GremlinFS.operations().mq()

    def mqevent(self, event, **kwargs):
        return GremlinFS.operations().mqevent(event, **kwargs)

    def query(self, query, node = None, default = None):

        if node and query:

            # try:

            pyexec = PyExec.instance(
                environment={
                    "g": self.ro(),
                    "self": node,
                    # "node": node,
                    "config": self.config()
                },
                blacklist=[],
                allowed=[
                    # g.V()
                    '^g\.V\(\)([a-zA-Z0-9\(\)\.\'\,])*$',
                    # g.V().inE(config.get('in_label')).outV()
                    '^g\.V\(\)([a-zA-Z0-9\(\)\#\:\.\,\-\_\'\"])*$',
                    # g.V('#17:68').inE('in').outV()
                    # g.V(node.get('id')).inE(config.get('in_label')).outV()
                    '^g\.V\(([a-zA-Z0-9\(\) \#\:\.\,\-\_\'\"]*)\)([a-zA-Z0-9\(\)\#\:\.\,\-\_\'\"])*$',
                ],
                notallowed=[
                    '\;',
                    'addE',
                    'addV',
                    'property',
                    'drop'
                ]
            )

            ret, log, err = pyexec.pyeval(
                query.strip()
            )

            if not ret:
                return [] # , log, err

            return ret # , log, err

            # except:
            #     logging.error(' GremlinFS: readFolder custom query exception ')
            #     traceback.print_exc()
            #     return [], None, None

        elif default:
            return default.valueMap(True).toList() # , None, None

        return [] # , None, None

    def eval(self, command, node = None, default = None):

        if node and command:

            # try:

            pyexec = PyExec.instance(
                environment={
                    "g": self.ro(),
                    "self": node,
                    # "node": node,
                    "config": self.config()
                },
                blacklist=[],
                allowed=[
                ],
                notallowed=[
                    '\;'
                    'addE',
                    'addV',
                    'property',
                    'drop'
                ]
            )

            ret, log, err = pyexec.pyeval(
                command
            )

            if not ret:
                return default, log, err

            return ret, log, err

            # except:
            #     logging.error(' GremlinFS: readFolder custom query exception ')
            #     traceback.print_exc()
            #     return default, None, None

        return default, None, None

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    # def utils(self):
    #     return GremlinFSUtils.utils()

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
        self._ro = None
        self._mq = None

        self._config = None

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

        logging.info( ' GremlinFS: AMQP DETAILS: ' )
        logging.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
        logging.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
        logging.info( ' GremlinFS: MQ ROUTING KEYS: ' )
        logging .info(mqroutingkeys)
        if mqroutingkey:
            logging.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )

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

            logging.info( ' GremlinFS: AMQP DETAILS: ' )
            logging.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
            logging.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
            logging.info( ' GremlinFS: MQ ROUTING KEYS: ' )
            logging.info(mqroutingkeys)
            if mqroutingkey:
                logging.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )

            try:
                logging.info(
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
                    logging.info(
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
                logging.info(
                    ' GremlinFS: Setting up AMQP queue consumer: ' + 
                    mqqueuename)
                mqchannel.basic_consume(
                    queue = mqqueuename,
                    auto_ack = True,
                    on_message_callback = callback
                )
                logging.info(
                    ' GremlinFS: Consuming AMQP queue: ' + 
                    mqqueuename)
                mqchannel.start_consuming()
                logging.info(
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
                logging.error(' GremlinFS: Caught an AMQP connection error: {} '.format(err))
                break

            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                logging.info(' GremlinFS: AMQP connection was closed, reconnecting ')
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

    def mq(self):

        if self._mq:
            return self._mq

        mqchannel = self.mqchannel()
        self._mq = mqchannel

        return self._mq

    def mqevent(self, event, **kwargs):

        import simplejson as json

        # data = event.toJSON()

        data = {
           "event": kwargs.get("event")
        }

        if "node" in kwargs and kwargs.get("node"):
            data["node"] = kwargs.get("node").all()

        # This one fails to serialize
        # if "link" in kwargs and kwargs.get("link"):
        #     data["link"] = kwargs.get("link").all()

        if "source" in kwargs and kwargs.get("source"):
            data["source"] = kwargs.get("source").all()

        if "target" in kwargs and kwargs.get("target"):
            data["target"] = kwargs.get("target").all()

        mqexchangename = self.config("mq_exchange")
        mqexchangetype = self.config("mq_exchange_type")

        mqqueuename = self.config("client_id") + '-' + self.config("fs_ns")

        mqroutingkeys = self.config("mq_routing_keys")
        mqroutingkey = self.config("mq_routing_key")

        logging.info( ' GremlinFS: AMQP DETAILS: ' )
        logging.info( ' GremlinFS: MQ EXCHANGE NAME: ' + mqexchangename + ", TYPE: " + mqexchangetype )
        logging.info( ' GremlinFS: MQ QUEUE NAME: ' + mqqueuename )
        logging.info( ' GremlinFS: MQ ROUTING KEYS: ' )
        logging.info(mqroutingkeys)
        if mqroutingkey:
            logging.info( ' GremlinFS: MQ DEFAULT ROUTING KEY: ' + mqroutingkey )
            logging.info(' GremlinFS: OUTBOUND AMQP/RABBIT EVENT: routing: ' + mqroutingkey + ' @ exchange: ' + mqexchangename)

        logging.info(' event: ')
        logging.info(data)

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

            logging.info(' GremlinFS: Outbound AMQP/RABBIT event, connection was closed, retry ')

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
            logging.error(' GremlinFS: Outbound AMQP/RABBIT event error: {} '.format(err))
            return

        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:

            logging.info(' GremlinFS: Outbound AMQP/RABBIT event, connection was closed, retry ')

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

        except:
            logging.error(' GremlinFS: MQ/AMQP send exception ')
            traceback.print_exc()

    def mqonevent(self, node, event, chain = [], data = {}, propagate = True):

        logging.info(' GremlinFS: INBOUND AMQP/RABBIT ON EVENT ')

        try:

            if node:
                node.event(
                    event = event, 
                    chain = chain, 
                    data = data, 
                    propagate = True
                )

        except:
            logging.error(' GremlinFS: INBOUND AMQP/RABBIT ON EVENT EXCEPTION ')
            traceback.print_exc()

    def mqonmessage(self, ch, method, properties, body):

        logging.info(' GremlinFS: INBOUND AMQP/RABBIT ON MESSAGE ')

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

        except:
            logging.error(' GremlinFS: INBOUND AMQP/RABBIT ON MESSAGE EXCEPTION ')
            traceback.print_exc()

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

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
            # "gremlin_password": self.gremlin_password,
            "gremlin_url": self.gremlin_url,

            "rabbitmq_host": self.rabbitmq_host,
            "rabbitmq_port": self.rabbitmq_port,
            # "rabbitmq_username": self.rabbitmq_username,
            "rabbitmq_password": self.rabbitmq_password,

            "mq_exchange": GremlinFSUtils.conf('mq_exchange', 'gfs-exchange'),
            "mq_exchange_type": GremlinFSUtils.conf('mq_exchange_type', 'topic'),
            "mq_routing_key": GremlinFSUtils.conf('mq_routing_key', 'gfs1.info'),
            "mq_routing_keys": GremlinFSUtils.conf('mq_routing_keys', ['gfs1.*']),

            "log_level": GremlinFSUtils.conf('log_level', logging.DEBUG),

            "client_id": GremlinFSUtils.conf('client_id', "0010"),
            "fs_ns": GremlinFSUtils.conf('fs_ns', "gfs1"),
            "fs_root": self.getfs(
                GremlinFSUtils.conf('fs_root', None),
                GremlinFSUtils.conf('fs_root_init', False)
            ),
            "fs_root_init": GremlinFSUtils.conf('fs_root_init', False),

            "folder_label": GremlinFSUtils.conf('folder_label', 'group'),
            "ref_label": GremlinFSUtils.conf('ref_label', 'ref'),
            "in_label": GremlinFSUtils.conf('in_label', 'ref'),
            "self_label": GremlinFSUtils.conf('self_label', 'ref'),
            "template_label": GremlinFSUtils.conf('template_label', 'template'),

            "in_name": GremlinFSUtils.conf('in_name', 'in0'),
            "self_name": GremlinFSUtils.conf('self_name', 'self0'),

            "vertex_folder": GremlinFSUtils.conf('vertex_folder', '.V'),
            "edge_folder": GremlinFSUtils.conf('edge_folder', '.E'),

            "uuid_property": GremlinFSUtils.conf('uuid_property', 'uuid'),
            "name_property": GremlinFSUtils.conf('name_property', 'name'),
            "data_property": GremlinFSUtils.conf('data_property', 'data'),
            "template_property": GremlinFSUtils.conf('template_property', 'template'),

            "default_uid": GremlinFSUtils.conf('default_uid', 1001),
            "default_gid": GremlinFSUtils.conf('default_gid', 1001),
            "default_mode": GremlinFSUtils.conf('default_mode', 0o777),

            "labels": GremlinFSUtils.conf('labels', [])

        }

        # Build label regexes
        if "labels" in config:
            for label_config in config["labels"]:
                if "pattern" in label_config:
                    try:
                        label_config["compiled"] = re.compile(label_config["pattern"])
                    except:
                        logging.error(' GremlinFS: failed to compile pattern %s ' % (
                            label_config["pattern"]
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

        except:
            logging.error(' GremlinFS: Failed to register ')
            traceback.print_exc()
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

        except:
            logging.error(' GremlinFS: Failed to register ')
            traceback.print_exc()

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

        created = False

        try:

            targetmatch = GremlinFSPath.match(target)
            sourcematch = GremlinFSPath.match(source)

            targetmatch.enter("link", target, source)
            if targetmatch and sourcematch:
                if not targetmatch.isFound():
                    created = targetmatch.createLink(sourcematch)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except:
            logging.error(' GremlinFS: link exception ')
            logging.error(sys.exc_info()[0])
            traceback.print_exc()
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

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
            logging.error(' GremlinFS: symlink exception ')
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



class GremlinFSCachingOperations(GremlinFSOperations):

    def __init__(
        self,
        **kwargs):
        super().__init__(**kwargs)

        self.caching = True # False
        self.cache = Dict() # {}

    # 

    def lookupCache(self, path, oper):

        from datetime import datetime
        logging.debug("CACHE: lookup: path: %s, oper: %s", path, oper)

        pick = False
        cachehit = self.cache[path][oper]
        if cachehit:
            if cachehit['expire']:
                if cachehit['expire'] > datetime.now():
                    logging.debug("CACHE: lookup: cachehit: found entry with expire, not expired")
                    pick = cachehit
                else:
                    logging.debug("CACHE: lookup: cachehit: found entry with expire, is expired")
            else:
                pick = cachehit

        if pick:
            cachehit = pick
            exp = ""
            if cachehit['expire']:
                exp = str(cachehit['expire'])
            logging.debug("CACHE: lookup: cachehit: PATH: %s, OPER: %s, CREATED: %s, EXPIRE: %s", 
                cachehit.path, cachehit.oper, str(cachehit.created), exp
            )
            return cachehit.data

        else:
            return False

    def prepareCache(self, path, oper, expire_seconds = None):

        from datetime import datetime, timedelta
        logging.debug("CACHE: prepare: path: %s, oper: %s", path, oper)

        expire = None
        if expire_seconds:
            time = datetime.now()
            expire = time + timedelta(0, expire_seconds) # days, seconds, then other fields.

        self.cache[path][oper]['path'] = path
        self.cache[path][oper]['oper'] = oper
        self.cache[path][oper]['flags'] = 1 # Indicate not yet active cache entry
        if expire:
            self.cache[path][oper]['expire'] = expire

        return self.cache[path][oper]

    def finalizeCache(self, path, oper, data):

        logging.debug("CACHE: finalize: path: %s, oper: %s", path, oper)
        self.cache[path][oper]['data'] = data
        self.cache[path][oper]['flags'] = 0 # Indicate active cache entry

        return self.cache[path][oper]

    def readCache(self, path, oper):

        cachepath = path
        cacheoper = oper
        expire = 60

        if cachepath and self.caching:
            try:
                cachedata = self.lookupCache(cachepath, cacheoper)
                if cachedata:
                    return cachedata
            except:
                e = sys.exc_info()[0]
                logging.warning("Client call not fatal error: lookup cache error: exception: %s" % ( str(e) ))
                logging.warning(e)
                traceback.print_exc()

        if cachepath and self.caching:
            cache = None
            try:
                cache = self.prepareCache(cachepath, cacheoper, expire)
            except:
                e = sys.exc_info()[0]
                logging.warning("Client call not fatal error: prepare cache error: exception: %s" % ( str(e) ))
                logging.warning(e)
                traceback.print_exc()

    def updateCache(self, path, oper, data):

        cachepath = path
        cacheoper = oper

        if data:
            if cachepath and self.caching:
                try:
                    self.finalizeCache(cachepath, cacheoper, data)
                except:
                    e = sys.exc_info()[0]
                    logging.warning("Client call not fatal error: finalize cache error: exception: %s" % ( str(e) ))
                    logging.warning(e)
                    traceback.print_exc()

        return data

    def clearCache(self, path):

        cachepath = path

        logging.info("CACHE: clear: path: %s", path)
        del self.cache[path]

    # 

    def chmod(self, path, mode):
        ret = super().chmod(path, mode)
        self.clearCache(path)
        return ret

    def chown(self, path, uid, gid):
        ret = super().chown(path, uid, gid)
        self.clearCache(path)
        return ret

    def create(self, path, mode, fi=None):
        ret = super().create(path, mode, fi)
        self.clearCache(path)
        return ret

    # def destroy(self, path):
    #     pass

    def flush(self, path, fh):
        ret = super().flush(path, fh)
        self.clearCache(path)
        return ret

    def fsync(self, path, datasync, fh):
        ret = super().fsync(path, datasync, fh)
        self.clearCache(path)
        return ret

    # def fsyncdir(self, path, datasync, fh):
    #     pass

    def getattr(self, path, fh=None):

        # logging.info(' !! getattr !! ')

        cachepath = path
        cacheoper = 'getattr'

        cachedata = self.readCache(cachepath, cacheoper)
        if cachedata:
            return cachedata

        ret = super().getattr(path, fh)
        self.updateCache(cachepath, cacheoper, ret)

        return ret

    # def getxattr(self, path, name, position=0):
    #     pass

    # def init(self, path):
    #     pass

    # def ioctl(self, path, cmd, arg, fip, flags, data):
    #     pass

    def link(self, target, source):
        ret = super().link(target, source)
        self.clearCache(target)
        self.clearCache(source)
        return ret

    # def listxattr(self, path):
    #     pass

    # 

    def mkdir(self, path, mode):
        ret = super().mkdir(path, mode)
        self.clearCache(path)
        return ret

    def mknod(self, path, mode, dev):
        ret = super().mknod(path, mode, dev)
        self.clearCache(path)
        return ret

    def open(self, path, flags):
        ret = super().open(path, flags)
        return ret

    # def opendir(self, path):
    #     pass

    def read(self, path, size, offset, fh):
        ret = super().read(path, size, offset, fh)
        return ret

    def readdir(self, path, fh):

        # logging.info(' !! readdir !! ')

        cachepath = path
        cacheoper = 'readdir'

        cachedata = self.readCache(cachepath, cacheoper)
        if cachedata:
            return cachedata

        ret = super().readdir(path, fh)
        self.updateCache(cachepath, cacheoper, ret)

        return ret

    def readlink(self, path):
        ret = super().readlink(path)
        return ret

    def release(self, path, fh):
        ret = super().release(path, fh)
        return ret

    # def releasedir(self, path, fh):
    #     pass

    # def removexattr(self, path, name):
    #     pass

    def rename(self, old, new):
        ret = super().rename(old, new)
        self.clearCache(old)
        self.clearCache(new)
        return ret

    def rmdir(self, path):
        ret = super().rmdir(path)
        self.clearCache(path)
        return ret

    # def setxattr(self, path, name, value, options, position=0):
    #     pass

    def statfs(self, path):
        ret = super().statfs(path)
        return ret

    def symlink(self, target, source):
        ret = super().symlink(target, source)
        self.clearCache(target)
        self.clearCache(source)
        return ret

    def truncate(self, path, length, fh=None):
        ret = super().truncate(path, length, fh)
        self.clearCache(path)
        return ret

    def unlink(self, path):
        ret = super().unlink(path)
        self.clearCache(path)
        return ret

    def utimens(self, path, times=None):
        ret = super().utimens(path, times)
        return ret

    def write(self, path, data, offset, fh):
        ret = super().write(path, data, offset, fh)
        self.clearCache(path)
        return ret



class GremlinFS(object):

    __operations = None

    @staticmethod
    def operations():

        if GremlinFS.__operations == None:
            GremlinFS.__operations = GremlinFSCachingOperations()

        return GremlinFS.__operations 

# 
# https://softwareengineering.stackexchange.com/questions/191623/best-practices-for-execution-of-untrusted-code
# https://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
# http://lucumr.pocoo.org/2011/2/1/exec-in-python/
# 


class PyExec(object):

    @classmethod
    def instance(clazz, environment={}, whitelist={}, blacklist=[], allowed=[], notallowed=[], defaults=None):
        instance = clazz(environment=environment, whitelist=whitelist, blacklist=blacklist, allowed=allowed, notallowed=notallowed, defaults=defaults)
        return instance

    def __init__(self, environment={}, whitelist={}, blacklist=[], allowed=[], notallowed=[], defaults=None):
        self.logger = logging.getLogger("PyExec")
        if not defaults:
            defaults = self.defaults()
        allalloweds = self.allowed()
        if allowed:
            allalloweds.extend(allowed)
        self.alloweds = []
        for allowed in allalloweds:
            self.alloweds.append(re.compile(allowed))
        allnotalloweds = self.notallowed()
        if notallowed:
            allnotalloweds.extend(notallowed)
        self.notalloweds = []
        for notallowed in allnotalloweds:
            self.notalloweds.append(re.compile(notallowed))
        definitions = self.definitions(whitelist, blacklist, defaults)
        self.globalenv = self.globals(environment, definitions)
        self.localenv = self.locals(environment, definitions)

    def defaults(self):
        return {
            "True": True,
            "False": False,
            "eval": eval,
            "len": len
        }

    def allowed(self):
        return []

    def notallowed(self):
        # Prevent using os, system and introspective __ objects
        return [
            '[\"\']+os[\"\']+',
            '(os)?\.system',
            '__[a-zA-Z]+__'
        ]

    def environment(self):
        return self.localenv

    def definitions(self, whitelist={}, blacklist=[], defaults=None):
        definitions = {}
        if defaults:
            definitions = dict(definitions, **defaults)
        if whitelist:
            definitions = dict(definitions, **whitelist)
        if blacklist:
            for key in blacklist:
                if key in definitions:
                    del definitions[key]
        return definitions

    def globals(self, environment={}, definitions={}):
        # Disable builtin functions, 
        # place needed and safe builtins into defaults or whitelist
        return {
            "__builtins__": {}
        }

    def locals(self, environment={}, definitions={}):
        locals = {}
        if environment:
            locals = dict(locals, **environment)
        if definitions:
            locals = dict(locals, **definitions)
        return locals

    # # https://stackoverflow.com/questions/3906232/python-get-the-print-output-in-an-exec-statement
    # @contextlib.contextmanager
    # def stdoutIO(stdout=None):
    #     old = sys.stdout
    #     if stdout is None:
    #         stdout = StringIO.StringIO()
    #     sys.stdout = stdout
    #     yield stdout
    #     sys.stdout = old

    def pyeval(self, command):
        ret = None
        # with stdoutIO() as s:
        # from cStringIO import StringIO
        try:
            # from StringIO import StringIO ## for Python 2
            from cStringIO import StringIO
        except ImportError:
            from io import StringIO ## for Python 3
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        redirected_output = sys.stdout = StringIO()
        redirected_error = sys.stderr = StringIO()
        if not command:
            # print "Empty line"
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return ret, redirected_output.getvalue(), redirected_error.getvalue()
        if self.notalloweds:
            for notallowed in self.notalloweds:
                if notallowed.search(command):
                    # print "Illegal line"
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
                    return ret, redirected_output.getvalue(), redirected_error.getvalue()
        if self.alloweds:
            ok = False
            for allowed in self.alloweds:
                if allowed.search(command):
                    ok = True
            if not ok:
                # print "Illegal line"
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                return ret, redirected_output.getvalue(), redirected_error.getvalue()
        try:
            ret = eval(
                command,
                self.globalenv,
                self.localenv
            )
        except:
            # print "Exception"
            traceback.print_exc()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return ret, redirected_output.getvalue(), redirected_error.getvalue()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        return ret, redirected_output.getvalue(), redirected_error.getvalue()

    def pyexec(self, command):
        # with stdoutIO() as s:
        # from cStringIO import StringIO
        try:
            # from StringIO import StringIO ## for Python 2
            from cStringIO import StringIO
        except ImportError:
            from io import StringIO ## for Python 3
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        redirected_output = sys.stdout = StringIO()
        redirected_error = sys.stderr = StringIO()
        if not command:
            # print "Empty line"
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return None, redirected_output.getvalue(), redirected_error.getvalue()
        if self.notalloweds:
            for notallowed in self.notalloweds:
                if notallowed.search(command):
                    # print "Illegal line"
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
                    return None, redirected_output.getvalue(), redirected_error.getvalue()
        if self.alloweds:
            ok = False
            for allowed in self.alloweds:
                if allowed.search(command):
                    ok = True
            if not ok:
                # print "Illegal line"
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                return None, redirected_output.getvalue(), redirected_error.getvalue()
        try:
            exec(
                command,
                self.globalenv,
                self.localenv
            )
        except:
            # print "Exception"
            traceback.print_exc()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return None, redirected_output.getvalue(), redirected_error.getvalue()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        return None, redirected_output.getvalue(), redirected_error.getvalue()

    def pyrun(self, command, execfn="eval"):
        if execfn == "eval":
            return self.pyeval(command)
        elif execfn == "exec":
            self.pyexec(command)



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

        # AMQP THREAD
        import threading

        amqp_rcve_thread = threading.Thread(
            target = amqp_rcve,
            kwargs = dict(
                operations = operations
            ),
            daemon = True
        )
        amqp_rcve_thread.start()

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
