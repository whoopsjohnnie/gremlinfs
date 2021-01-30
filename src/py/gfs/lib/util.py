# 
# Copyright (c) 2019, 2020, John Grundback
# All rights reserved.
# 

from gfs.common.log import GFSLogger
from gfs.common.base import GFSBase

from gfs.error.error import GFSError
from gfs.error.error import GFSExistsError
from gfs.error.error import GFSNotExistsError
from gfs.error.error import GFSIsFileError
from gfs.error.error import GFSIsFolderError

from gfs.gfs import GremlinFS



class GremlinFSUtils(GFSBase):

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
            raise GFSExistsError()

    @classmethod
    def found(clazz, value):
        if not value:
            raise GFSNotExistsError()
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
