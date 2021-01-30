# 
# Copyright (c) 2019, 2020, John Grundback
# All rights reserved.
# 

from gfs.common.log import GFSLogger
from gfs.common.base import GFSBase

from gfs.gfs import GremlinFS
from gfs.lib.util import GremlinFSUtils
from gfs.lib.event import GremlinFSEvent



class GFSNode(GFSBase):

    logger = GFSLogger.getLogger("GFSNode")

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
