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

# 
from time import time

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# 
# from fuse import FUSE
# from fuse import Operations
# from fuse import FuseOSError

# 3.3.0
# http://tinkerpop.apache.org/docs/3.3.0-SNAPSHOT/reference/#gremlin-python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 3.3.4?
# https://pypi.org/project/aiogremlin/
# import asyncio
# from aiogremlin import DriverRemoteConnection, Graph
# from gremlin_python.process.graph_traversal import __

# 
import pika

# 
# 
from gremlinfs.gremlinfslog import GremlinFSLogger

from gremlinfs.gremlinfslib import GremlinFSError
from gremlinfs.gremlinfslib import GremlinFSExistsError
from gremlinfs.gremlinfslib import GremlinFSNotExistsError
from gremlinfs.gremlinfslib import GremlinFSIsFileError
from gremlinfs.gremlinfslib import GremlinFSIsFolderError

from gremlinfs.gremlinfslib import GremlinFSPath
from gremlinfs.gremlinfslib import GremlinFSUtils
from gremlinfs.gremlinfslib import GremlinFS

#
# 
# import config



class GremlinFSOperationsUtils(GremlinFSUtils):

    logger = GremlinFSLogger.getLogger("GremlinFSOperationsUtils") # __name__)

    def tobytes(self, data):
        # return data

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
        # return data

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
        # return data
        data = base64.b64decode(data)
        return data

    def encode(self, data, encoding = "base64"):
        import base64
        # return data
        data = base64.b64encode(data)
        return data

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



class GremlinFSOperations(GremlinFS): # , Operations):

    logger = GremlinFSLogger.getLogger("GremlinFSOperations") # __name__)

    def __init__(
        self,
        **kwargs):

        super().__init__()

        self._utils = GremlinFSOperationsUtils()

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

        super().configure(

            mount_point,

            gremlin_host,
            gremlin_port,
            gremlin_username,
            gremlin_password,

            rabbitmq_host,
            rabbitmq_port,
            rabbitmq_username,
            rabbitmq_password,

        )

        self._utils = GremlinFSOperationsUtils()

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

        mqconnection = self.mqconnection()
        mqchannel = mqconnection.channel()
        mqchannel.queue_declare(
            queue = self.config("mq_queue"),
            durable = True
        )

        return mqchannel

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
        mqchannel.queue_declare(
            queue = self.config("mq_queue"),
            durable = True
        )

        self._mq = mqchannel

        return self._mq

    def mqevent(self, event, **kwargs):

        import simplejson as json

        data = event.toJSON()

        logging.info(' GremlinFS: OUTBOUND AMQP/RABBIT EVENT ')
        logging.info(data)

        try:

            self.mq().basic_publish(
                exchange = self.config("mq_exchange"),
                routing_key = self.config("fs_ns"),
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
                exchange = self.config("mq_exchange"),
                routing_key = self.config("fs_ns"),
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
                exchange = self.config("mq_exchange"),
                routing_key = self.config("fs_ns"),
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

#         except:
#             logging.error(' GremlinFS: MQ/AMQP send exception ')
#             traceback.print_exc()

    def mqonevent(self, node, event, chain = [], data = {}, propagate = True):
        pass

    def mqonmessage(self, ch, method, properties, body):
        pass

    def query(self, query, node = None, _default_ = None):
        return self.utils().query(query, node, _default_)

    def eval(self, command, node = None, _default_ = None):
        return self.utils().eval(command, node, _default_)

    def config(self, key=None, _default_=None):
        return self._config.get(key, _default_)

    def utils(self):
        return self._utils

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

    # 

    # JS jump:
    # UnsupportedSyntaxError: Having both param accumulator and keyword args is unsupported
    def enter(self, functioname, *args): # , **kwargs):
        pass

    # 



# 
# https://softwareengineering.stackexchange.com/questions/191623/best-practices-for-execution-of-untrusted-code
# https://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
# http://lucumr.pocoo.org/2011/2/1/exec-in-python/
# 


class PyExec():

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

        operations = GremlinFSOperations()

        operations.configure(
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
        GremlinFS.instance(operations)

        print(" >> G ")
        print(GremlinFS.instance().g())

        try:

            clean0 = GremlinFSPath.match("/folder1")
            clean0.deleteFolder()

            clean1 = GremlinFSPath.match("/folder1/test1")
            clean1.deleteFile()

            clean2 = GremlinFSPath.match("/folder1/test2")
            clean2.deleteFile()

        except:
            pass

        print("  ")
        print(" CREATING folder 1 ")
        match0 = GremlinFSPath.match("/folder1")
        match0.createFolder()
        match0 = GremlinFSPath.match("/folder1")
        print(match0.node())

        print("  ")
        print(" CREATING test 1 ")
        match1 = GremlinFSPath.match("/folder1/test1")
        match1.createFile()
        match1 = GremlinFSPath.match("/folder1/test1")
        print(match1.node())

        print("  ")
        print(" CREATING test 2 ")
        match2 = GremlinFSPath.match("/folder1/test2")
        match2.createFile()
        match2 = GremlinFSPath.match("/folder1/test2")
        print(match2.node())

        # match1 = GremlinFSPath.match("/folder1/.V")
        # print(match1.readFolder())

        match1 = GremlinFSPath.match("/folder1/.V/test1/OUT/link1")

        print("  ")
        print(" CREATING link 1 ")
        link1 = match1.createLink(match2)
        print(link1)

        match1 = GremlinFSPath.match("/folder1/.V/test1/OUT")
        print(match1.readFolder())

        match1 = GremlinFSPath.match("/folder1/.V/test1/OUT/link1")
        print(match1.readFile())
        match1.deleteLink()

        match1 = GremlinFSPath.match("/folder1/.V/test1/name")
        print(match1.readFile())

        match1 = GremlinFSPath.match("/folder1/.V/test1/uuid")
        print(match1.readFile())

        data = "HELLOE"
        data = data.encode(
            encoding='utf-8', 
            errors='strict'
        )

        match2 = GremlinFSPath.match("/folder1/test2")
        match2.writeFile(data);

        match2 = GremlinFSPath.match("/folder1/test2")
        print(match2.readFile());

        print("  ")
        print(" DELETING test 1 ")
        match1 = GremlinFSPath.match("/folder1/test1")
        match1.deleteFile()

        print("  ")
        print(" DELETING test 2 ")
        match2 = GremlinFSPath.match("/folder1/test2")
        match2.deleteFile()

    except:
        # self.logger.error(' GremlinFS: main/init exception ')
        traceback.print_exc()


def sysarg(
    args,
    index,
    default = None):
    if args and len(args) > 0 and index >= 0 and index < len(args):
        return args[index]
    return default


if __name__ == '__main__':

    mount_point = "/home/project"

    gremlin_host = "localhost"
    gremlin_port = "8182"
    gremlin_username = "root"
    gremlin_password = "root"

    rabbitmq_host = "localhost"
    rabbitmq_port = "5672"
    rabbitmq_username = "rabbitmq"
    rabbitmq_password = "rabbitmq"

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
