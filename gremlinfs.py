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

# 
from time import time

# 
from fuse import FUSE
from fuse import Operations
from fuse import FuseOSError


# 3.4.2
# from gremlin_python import statics
# from gremlin_python.process.anonymous_traversal import traversal
# from gremlin_python.process.traversal import traversal
# from gremlin_python.process.graph_traversal import __
# from gremlin_python.process.strategies import *
# from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
# from gremlin_python.process.traversal import T
# from gremlin_python.process.traversal import Order
# from gremlin_python.process.traversal import Cardinality
# from gremlin_python.process.traversal import Column
# from gremlin_python.process.traversal import Direction
# from gremlin_python.process.traversal import Operator
# from gremlin_python.process.traversal import P
# from gremlin_python.process.traversal import Pop
# from gremlin_python.process.traversal import Scope
# from gremlin_python.process.traversal import Barrier
# from gremlin_python.process.traversal import Bindings
# from gremlin_python.process.traversal import WithOptions

# 3.3.7
# http://tinkerpop.apache.org/docs/3.3.7-SNAPSHOT/reference/#gremlin-python
# from gremlin_python import statics
# from gremlin_python.process.anonymous_traversal import traversal
# from gremlin_python.process.graph_traversal import __
# from gremlin_python.process.strategies import *
# from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 3.3.0
# http://tinkerpop.apache.org/docs/3.3.0-SNAPSHOT/reference/#gremlin-python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


# 
logging.basicConfig(level=logging.DEBUG)
# logging.debug(' *** GremlinFS *** ')



class GremlinFSOperations(Operations):

    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    # 

    def enter(self, functioname, *args, **kwargs):
        logging.debug(' *** GremlinFS: ENTER: %s ' % (functioname))
        logging.debug(args)
        logging.debug(kwargs)

    # 

    def __call__(self, op, *args):
        if not hasattr(self, op):
            raise FuseOSError(errno.EFAULT)
        return getattr(self, op)(*args)

    def access(self, path, amode):
        self.enter("access", path, amode)
        node = self.path(self.expand(path))
        return 0

    # 

    def chmod(self, path, mode):
        self.enter("chmod", path, mode)
        self.notReadOnly()
        node = self.path(self.expand(path))
        self.setMode(path, node, mode)

    def chown(self, path, uid, gid):
        self.enter("chown", path, uid, gid)
        self.notReadOnly()
        node = self.path(self.expand(path))
        self.setOwner(path, node, uid, gid)

    def create(self, path, mode, fi=None):
        self.enter("create", path, mode)
        self.notReadOnly()
        node = self.createPath(path)
        self.setMode(path, node, mode)

    # def destroy(self, path):
    #     pass

    def flush(self, path, fh):
        self.enter("flush", path)
        return 0

    def fsync(self, path, datasync, fh):
        self.enter("fsync", path)
        return 0

    # def fsyncdir(self, path, datasync, fh):
    #     return 0

    # FOLDER: posix.stat_result(st_mode=16877, st_ino=13165180, st_dev=86, st_nlink=2, st_uid=0, st_gid=0, st_size=64, st_atime=1561859575, st_mtime=1561859513, st_ctime=1561859513)
    # FILE    posix.stat_result(st_mode=33188, st_ino=13168670, st_dev=86, st_nlink=1, st_uid=0, st_gid=0, st_size=0, st_atime=1561877418, st_mtime=1561877418, st_ctime=1561877418)
    def getattr(self, path, fh=None):
        self.enter("getattr", path)
        mode = 0
        links = 0
        owner = 0
        group = 0
        size = 0
        node = self.path(self.expand(path))
        if self.isFolder(path, node):
            node = self.folder(path, node)
            mode = (stat.S_IFDIR | 0o755)
            links = 2
            size = 1024
        elif self.isFile(path, node):
            node = self.file(path, node)
            mode = (stat.S_IFREG | 0o755)
            links = 1
            size = len("CONTENTS".encode("utf8"))
        attrs = {
            "st_mode": mode,
            "st_nlink": links,
            "st_uid": owner,
            "st_gid": group,
            "st_size": size,
            "st_atime": time(),
            "st_mtime": time(),
            "st_ctime": time(),
        }
        return attrs

    # def getxattr(self, path, name, position=0):
    #     raise FuseOSError(ENOTSUP)

    # def init(self, path):
    #     pass

    # def ioctl(self, path, cmd, arg, fip, flags, data):
    #     raise FuseOSError(errno.ENOTTY)

    def link(self, target, source):
        self.enter("link", target, source)
        self.notReadOnly()
        node = self.path(self.expand(source))
        link = self.createLink(source, node, target)

    # def listxattr(self, path):
    #     return []

    # 

    def mkdir(self, path, mode):
        self.enter("mkdir", path, mode)
        self.notReadOnly()
        # node = self.createPath(path)
        node = self.createFolder(path)
        self.setMode(path, node, mode)

    def mknod(self, path, mode, dev):
        self.enter("mknod", path, mode)
        self.notReadOnly()
        node = self.createPath(path)
        self.setMode(path, node, mode)

    def open(self, path, flags):
        self.enter("open", path, flags)
        node = self.path(self.expand(path))
        node = self.file(path, node)
        return 0

    # def opendir(self, path):
    #     return 0

    def read(self, path, size, offset, fh):
        self.enter("read", path, size, offset)
        node = self.path(self.expand(path))
        node = self.file(path, node)
        data = "CONTENTS".encode("utf8") # "FILE CONTENTS"
        return data

    # ['.', '..', u'file.name']
    def readdir(self, path, fh):
        self.enter("readdir", path)
        node = self.path(self.expand(path))
        logging.debug( node )
        node = self.folder(path, node)
        logging.debug( node )
        # return [
        #     '.', 
        #     '..',
        #     'file.name'
        # ]
        # gremlin> self.g().V('#14:5').inE('in').outV().values('name')
        # ==>one
        # ==>two
        # ==>three

        # logging.debug( " *** FOLDER CONTENTS *** " )
        # names = self.g().V('#14:5').inE('in') # .outV().values('name')

        # logging.debug( self.g().V('#14:5').inE('in').toList() )
        # logging.debug( self.g().V('#14:5').inE('in').outV().toList() )
        # logging.debug( self.g().V('#14:5').inE('in').outV().values('name') )
        # logging.debug( self.g().V('#14:5').inE('in').outV().values('name').toList() )
        names = self.g().V( node.id['@value'] ).inE('in').outV().values('name').toList()
        logging.debug( names )

        entries =  [
            '.',
            '..'
        ]

        entries.extend(names)

        return entries

    def readlink(self, path):
        self.enter("readlink", path)
        raise FuseOSError(errno.ENOENT)

    def release(self, path, fh):
        self.enter("release", path)
        return 0

    # def releasedir(self, path, fh):
    #     return 0

    # def removexattr(self, path, name):
    #     raise FuseOSError(ENOTSUP)

    def rename(self, old, new):
        self.enter("rename", old, new)
        self.notReadOnly()
        node = self.path(self.expand(old))
        self.movePath(old, node, new)

    def rmdir(self, path):
        self.enter("rmdir", path)
        self.notReadOnly()
        node = self.path(self.expand(path))
        node = self.folder(path, node)
        self.removePath(path, node)

    # def setxattr(self, path, name, value, options, position=0):
    #     raise FuseOSError(ENOTSUP)

    def statfs(self, path):
        self.enter("statfs", path)
        return {}

    def symlink(self, target, source):
        self.enter("symlink", target, source)
        self.notReadOnly()
        node = self.path(self.expand(source))
        link = self.createLink(source, node, target)

    def truncate(self, path, length, fh=None):
        self.enter("truncate", path, length)
        self.notReadOnly()

    def unlink(self, path):
        self.enter("unlink", path)
        self.notReadOnly()
        node = self.path(self.expand(path))
        self.removePath(path, node)

    def utimens(self, path, times=None):
        self.enter("utimens", path)
        return 0

    def write(self, path, data, offset, fh):
        self.enter("write", path, data, offset)
        self.notReadOnly()
        node = self.path(self.expand(path))
        node = self.file(path, node)
        return len(data.encode("utf8"))

    # 

    def isReadOnly(self):
        return False

    def notReadOnly(self):
        if self.isReadOnly():
            raise FuseOSError(errno.EROFS)
        return True

    # 

    def expand(self, path):
        if path == "/":
            return [""]
        elms = (os.path.normpath(os.path.splitdrive(path)[1]))
        # elms = elms.split('/')
        return elms.strip().split('/')

    def path(self, path):
        return None

    def isFolder(self, path, node):
        if path and path.find('.') != -1:
            # raise FuseOSError(errno.ENOENT)
            # raise FuseOSError(errno.ENOTDIR)
            return False
        return True

    def folder(self, path, node):
        # node = self.path(self.expand(path))
        if not self.isFolder(path, node):
            raise FuseOSError(errno.ENOENT)
        return node

    def isFile(self, path, node):
        if path and path.find('.') == -1:
            # raise FuseOSError(errno.EISDIR)
            return False
        return True

    def file(self, path, node):
        # node = self.path(self.expand(path))
        if self.isFolder(path, node):
            raise FuseOSError(errno.EISDIR)
        elif not self.isFile(path, node):
            raise FuseOSError(errno.ENOENT)
        return node

    # 

    def createPath(self, path):
        pass

    def movePath(self, old, node, new):
        pass

    def removePath(self, path, node):
        pass

    def createFolder(self, path):
        pass

    def createFile(self, path):
        pass

    def createLink(self, source, node, target):
        pass

    def setMode(self, path, node, mode):
        pass

    def setOwner(self, path, node, uid, gid):
        pass



class GremlinFS(GremlinFSOperations):

    def __init__(
        self,

        gremlin_host,
        gremlin_port,
        gremlin_username,
        gremlin_password,

        rabbitmq_host = None,
        rabbitmq_port = None,
        rabbitmq_username = None,
        rabbitmq_password = None,

        **kwargs):

        self.filesystem_init = True
        self.filesystem_name = "firstfs"
        self.filesystem_id = None

        # self.filesystem_class = "fs" # "filesystem"
        # self.filesystem_folder_class = "fsdir" # "folder"
        # self.filesystem_file_class = "fsfile" # "file"

        self.filesystem_label = "group"
        self.folder_label = "group"

        # self.filesystem_root_class = # "root"
        # self.filesystem_self_class = "fsself" # "self"
        # self.filesystem_reference_class = "fsref" # "parent"

        # self.in_folder_label = ...

        self.gremlin_host = gremlin_host
        self.gremlin_port = gremlin_port
        self.gremlin_username = gremlin_username
        self.gremlin_password = gremlin_password

        self.gremlin_url = "ws://" + self.gremlin_host + ":" + self.gremlin_port + "/gremlin";

        logging.debug(' *** GremlinFS gremlin host: %s' % (str(self.gremlin_host)))
        logging.debug(' *** GremlinFS gremlin port: %s' % (str(self.gremlin_port)))
        logging.debug(' *** GremlinFS gremlin username: %s' % (str(self.gremlin_username)))
        logging.debug(' *** GremlinFS gremlin password: %s' % (str(self.gremlin_password)))
        logging.debug(' *** GremlinFS gremlin URL: %s' % (str(self.gremlin_url)))

        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_username = rabbitmq_username
        self.rabbitmq_password = rabbitmq_password

        logging.debug(' *** GremlinFS rabbitmq host: %s' % (str(self.rabbitmq_host)))
        logging.debug(' *** GremlinFS rabbitmq port: %s' % (str(self.rabbitmq_port)))
        logging.debug(' *** GremlinFS rabbitmq username: %s' % (str(self.rabbitmq_username)))
        logging.debug(' *** GremlinFS rabbitmq password: %s' % (str(self.rabbitmq_password)))

        self.__graph = None
        self.__g = None

    def connection(self):

        if self.__g:
            return self.__g

        # 3.4.2
        # g = traversal().withRemote(DriverRemoteConnection(
        #     self.gremlin_url,
        #     'g',
        #     username = self.gremlin_username,
        #     password = self.gremlin_password
        # ))

        # 3.3.0
        graph = Graph()
        g = graph.traversal().withRemote(DriverRemoteConnection(
            self.gremlin_url,
            'g',
            username = self.gremlin_username,
            password = self.gremlin_password
        ))

        self.__graph = graph
        self.__g = g

        return self.__g

    def graph(self):

        if not self.__graph:
            g = self.connection()            

        return self.__graph

    def g(self):

        if not self.__g:
            g = self.connection()            

        return self.__g

    def initfs(self):

        # Drop all vertices in the graph to create a new one
        # self.g().V().drop().iterate()

        filesystem = None

        # if self.filesystem_name and not self.filesystem_id and self.filesystem_init:
        if self.filesystem_name and self.filesystem_init:

            # https://stackoverflow.com/questions/36608572/can-i-qualify-on-orientdb-vertex-edge-class-in-a-gremlin-query
            # https://stackoverflow.com/questions/35447890/orientdb-gremlin-retrieve-vertex-for-a-class-in-gremlin-not-hitting-indexes?rq=1
            # self.g().createVertexType('Person','V')
            # self.g().V.has('@class','Person').has('name','John')
            # self.g().V.has('@class','Person').has('name',T.eq,'John')
            # self.g().V.has('@class','Person').filter{it.name.contains('John')}
            # self.g().V.has('@class','Person').filter{it.name == 'John'}


            # filesystem = self.g().addV(self.filesystem_class).property('name', self.filesystem_name).next()
            # root = self.g().addV(self.filesystem_folder_class).property('name', '%s root' % (self.filesystem_name)).next()
            # # self.g().V(filesystem).addE(self.filesystem_root_class).to(__.V(root)).next()
            # self.g().V(filesystem).addE(self.filesystem_root_class).to(g.V(root)).iterate()


            # filesystem = self.g().addV(self.filesystem_class).property('name',self.filesystem_name).next()
            # root = self.g().addV(self.filesystem_folder_class).property('name','%s root' % (self.filesystem_name)).next()
            filesystem = self.g().addV(self.filesystem_label).property('name',self.filesystem_name).next()

            # Must look up FS here to get read ID??
            # filesystem_1a =  self.g().V().hasLabel(self.filesystem_class).has('name',self.filesystem_name).next()
            # root_2a = self.g().V().hasLabel(self.filesystem_folder_class).has('name','%s root' % (self.filesystem_name)).next()
            filesystem_1a =  self.g().V().hasLabel(self.filesystem_label).has('name',self.filesystem_name).next()

            # logging.debug( " FS IDS " )
            # logging.debug( filesystem_1a )
            # # logging.debug( root_2a )
            # logging.debug( filesystem_1a.id )
            # # logging.debug( root_2a.id )
            # logging.debug( filesystem_1a.id["@value"] )
            # # logging.debug( root_2a.id["@value"] )

            filesystem_1a_ID = filesystem_1a.id["@value"]
            # root_2a_ID = root_2a.id["@value"]

            # self.g().V( filesystem_1a_ID ).addE(self.filesystem_root_class).to(__.V( root_2a_ID )).next()
            # self.g().V( root_2a_ID ).addE(self.filesystem_parent_class).to(__.V( filesystem_1a_ID )).next()
            # self.g().V( filesystem_1a_ID ).addE(self.filesystem_reference_class).property('name','root').to(__.V( root_2a_ID )).next()
            # self.g().V( root_2a_ID ).addE(self.filesystem_reference_class).property('name','self').to(__.V( root_2a_ID )).next()

            self.filesystem_name = self.filesystem_name
            self.filesystem_id = filesystem_1a_ID

        return self.filesystem_id

    def getfs(self):

        # vtxs = self.g().V().toList()
        # logging.debug( vtxs )

        if self.filesystem_id:
            return self.filesystem_id

        filesystem = None

        # logging.debug( self.g().V().project('props','id','label').by(valueMap()).by(id).by(label) )
        # logging.debug( self.g().V().project('props','id','label').valueMap() ) # .toList() )
        # logging.debug( self.g().V().valueMap(True).toList() )

        # if self.filesystem_id and self.filesystem_name:
        #     logging.debug(' *** GremlinFS: get fs with ID: %s and name: %s' % (
        #         self.filesystem_id,
        #         self.filesystem_name
        #     ))
        #     # try:
        #     filesystem = self.g().V(self.filesystem_id).hasLabel(self.filesystem_label).has('name', self.filesystem_name) # .next()
        #     # except:
        #     #     logging.error(' *** GremlinFS: Exception ')
        # elif self.filesystem_id:
        #     logging.debug(' *** GremlinFS: get fs with ID: %s ' % (
        #         self.filesystem_id
        #     ))
        #     # try:
        #     filesystem = self.g().V(self.filesystem_id).hasLabel(self.filesystem_label) # .next()
        #     # except:
        #     #     logging.error(' *** GremlinFS: Exception ')
        # el
        if self.filesystem_name:
            # logging.debug(' *** GremlinFS: get fs with name: %s ' % (
            #     self.filesystem_name
            # ))
            try:
                filesystem = self.g().V().hasLabel(self.filesystem_label).has('name', self.filesystem_name) # .next()
                # logging.debug( filesystem )
                # fs = filesystem.next()
                # logging.debug( fs.id )
                # logging.debug( fs.label )

            except:
                logging.error(' *** GremlinFS: Exception ')
                filesystem = None

        try:
            filesystem = filesystem.next()
            filesystem = filesystem.id["@value"]
            self.filesystem_id = filesystem
            # logging.debug( filesystem )
        except:
            logging.error(' *** GremlinFS: Exception ')
            filesystem = None

        # if not filesystem and self.filesystem_name and not self.filesystem_id and self.filesystem_init:
        #     logging.debug(' *** GremlinFS: create fs with name: %s ' % (
        #         self.filesystem_name
        #     ))
        #     # try:
        # filesystem2 = self.initfs();
        #     logging.debug( filesystem )
        #     # except:
        #     #     logging.error(' *** GremlinFS: Exception ')

        if not filesystem and self.filesystem_name and self.filesystem_init:
            filesystem = self.initfs();

        # logging.debug( filesystem )

        # if not filesystem:
        #     logging.debug(' *** GremlinFS: no fs found ')
        #     return None

        return filesystem

    # 

    def expand(self, path):
        tmp = self.__parsePathInGroups(path);
        # logging.debug(' *** GremlinFS: EXPAND PATH: ')
        # logging.debug( path )
        # logging.debug( tmp )
        return tmp

    # def path(self, path):
    #     # raise FuseOSError(errno.ENOENT)
    #     # raise FuseOSError(errno.EACCES)
    #     groupIDs = (os.path.normpath(os.path.splitdrive(path)[1]))
    #     groupIDs = groupIDs.split('/')
    #     logging.debug(' *** GremlinFS: path elements: %s ' % (path))
    #     logging.debug(groupIDs)

    #     # # user_1 = self.g().addV('people').property('name', 'John').next()
    #     # # user_2 = self.g().addV('people').property('name', 'Mary').next()

    #     # filesystem = self.g().addV('FileSystem').property('name', 'FileSystem').next()
    #     # root = self.g().addV('FileSystemFolder').property('name', 'Root').next()

    #     # # self.g().V(filesystem).addE('FileSystemRoot').to(root)
    #     # # self.g().V(filesystem).addE('FileSystemRoot').to(root).iterate() # or .next() or .toList()
    #     # self.g().V(filesystem).addE('FileSystemRoot').to(__.V(root)).next()

    #     filesystem = self.getfs()
    #     # if not filesystem:
    #     #     raise FuseOSError(errno.ENOENT)
    #     logging.debug( filesystem )
    #     logging.debug( filesystem.toList() )
    #     logging.debug( filesystem.outE().toList() )
    #     # logging.debug( filesystem.outE(self.filesystem_root_class) )
    #     logging.debug( filesystem.outE(self.filesystem_root_class).toList() )

    #     # logging.debug( self.g().V().hasLabel(self.filesystem_class).has('name', self.filesystem_name) )
    #     # logging.debug( self.g().V().hasLabel(self.filesystem_class).has('name', self.filesystem_name).outE() )

    #     # logging.debug( filesystem.inE() )
    #     # logging.debug( filesystem.outE() )
    #     # logging.debug( filesystem.outE('root') )

    #     if path == "/":
    #         return filesystem


    # def path(self, path):

    #     # Drop all vertices in the graph to create a new one
    #     # self.g().V().drop().iterate()
    #     # self.g().E().drop().iterate()



    #     # user_1 = self.g().addV('people').property('name','John').next()
    #     # user_2 = self.g().addV('people').property('name','Mary').next()
    #     # self.g().V(user_1).addE('knows').to(user_2).next()


    #     # self.g().E().id().next()
    #     # self.g().E(g.E().id().next()).id().next()


    #     user_1 = self.g().addV('people').property('name','John').next()
    #     user_2 = self.g().addV('people').property('name','Mary').next()

    #     user_1a = self.g().V().has('name','John').next()
    #     user_2a = self.g().V().has('name','Mary').next()

    #     logging.debug( " USER IDS " )
    #     logging.debug( user_1a )
    #     logging.debug( user_2a )
    #     logging.debug( user_1a.id )
    #     logging.debug( user_2a.id )
    #     logging.debug( user_1a.id["@value"] )
    #     logging.debug( user_2a.id["@value"] )

    #     user_1a_ID = user_1a.id["@value"]
    #     user_2a_ID = user_2a.id["@value"]

    #     # knows_2_1 = self.g().V( __.V(user_2) ).addE('knows').to( __.V(user_1) ).next()
    #     # knows_1_2 = self.g().V( user_1a ).addE('read').to( __.V( user_2a ) ).toList() # .iterate() # .next()
    #     # knows_1_2 = self.g().V(user_2).addE('knows').from_( __.V(user_1) ) # .next()
    #     # knows_a_b = self.g().V(user_1).as_('a').V(user_2).as_('b').addE('knows').from_('a').to('b') # .next()

    #     # knows = self.g().V( self.g().V('#19:18').id() ).next().

    #     # 
    #     # Works in gremlin 3.3.0, client and server
    #     # Client: gremlin> Gremlin.version() ==>3.3.0
    #     # Server: Installed GREMLIN language v.3.3.0 - graph.pool.max=50 [OGraphServerHandler]
    #     # 
    #     # gremlin> self.g().V('#19:19').next()
    #     # ==>v[#19:19]
    #     # gremlin> self.g().V('#19:18').next()
    #     # ==>v[#19:18]
    #     # gremlin> self.g().V('#19:18').addE('test').to(__.V('#19:19')).next()
    #     # ==>e[#22:-2][#19:18-test->#19:19]
    #     # gremlin> self.g().V('#19:18').next().addEdge('test', self.g().V('#19:19').next())
    #     # ==>e[#21:-2][#19:18-test->#19:19]
    #     # 

    #     # logging.debug( user_1a_ID )
    #     # logging.debug( user_2a_ID )

    #     self.g().V('#19:18').addE('test').to(__.V('#19:19')).next()
    #     self.g().V( user_1a_ID ).addE('test').to(__.V( user_2a_ID )).next()
    #     # self.g().V('#19:18').next().addEdge('test', self.g().V('#19:19').next())


    #     # people = self.g().V().valueMap().toList()
    #     # connections = self.g().E().valueMap().toList()

    #     # logging.debug(people)
    #     # logging.debug(connections)

    #     raise FuseOSError(errno.ENOENT)


    # def path(self, path):

    #     fsid = self.getfs()
    #     logging.debug( " *** FSID *** " )
    #     logging.debug( fsid )
    #     fs = self.g().V(fsid).next()
    #     # # [{<T.label: 3>: u'group', u'name': [u'firstfs'], <T.id: 1>: {u'@type': u'orient:ORecordId', u'@value': u'#13:0'}}]
    #     # fsvals = self.g().V(fsid).valueMap(True).toList()
    #     # logging.debug( fsvals )
    #     # logging.debug( fsvals[0] )
    #     # fsvalslabel = None # fsvals[0]['<T.label: 3>']
    #     # fsvalsname = None # fsvals[0]['name']
    #     # fsvalsid = None # fsvals[0]['<T.id: 1>']['@value']
    #     # for key, val in fsvals[0].items():
    #     #     # logging.debug( key )
    #     #     logging.debug( str(key) )
    #     #     logging.debug( val )
    #     #     if key and "label" in str(key):
    #     #         fsvalslabel = val
    #     #     elif key and "id" in str(key):
    #     #         fsvalsid = val['@value']
    #     #     elif key and "name" in str(key):
    #     #         fsvalsname = val[0]
    #     # logging.debug( " *** FS *** " )
    #     # logging.debug( fs )
    #     # logging.debug( fs.id )
    #     # logging.debug( fs.label )
    #     # logging.debug( fsvalslabel )
    #     # logging.debug( fsvalsname )
    #     # logging.debug( fsvalsid )


    #     # logging.debug( self.g().V('#13:0').next().label )
    #     # logging.debug( self.g().V('#13:0').label() )
    #     # logging.debug( self.g().V('#13:0').label )
    #     # logging.debug( self.g().V('#13:0').next().id["@value"] )
    #     # logging.debug( self.g().V('#13:0').next().id["@class"] )
    #     # logging.debug( " *** FS VALUEMAP *** " )
    #     # logging.debug( self.g().V('#13:0').valueMap(True).toList() )

    #     # logging.debug( fs.class )
    #     # logging.debug( type(fs).__name__ )
    #     # logging.debug( type(fs).__class__.__name__ )
    #     # rootref = self.g().V(fsid).outE('fsref').has('name', 'root').next()
    #     # root = rootref.inV
    #     # parent = rootref.outV
    #     # logging.debug( " *** RT *** " )
    #     # logging.debug( rootref )
    #     # logging.debug( root )
    #     # logging.debug( parent )
    #     # logging.debug( rootref.label )
    #     # logging.debug( rootref.inV )
    #     # logging.debug( rootref.outV )
    #     # # logging.debug( rootref.class )
    #     # logging.debug( type(rootref).__name__ )
    #     # logging.debug( type(rootref).__class__.__name__ )

    #     # gremlin> self.g().V('#15:0').outE('fsref').has('name', 'root').next().property('name')
    #     # ==>p[name->root]

    #     # gremlin> self.g().V('#15:0').outE('fsref').has('name', 'root').inV().next().label()
    #     # ==>fsdir
    #     # gremlin> self.g().V('#15:0').outE('fsref').has('name', 'root').outV().next().label()
    #     # ==>fs

    #     # gremlin> self.g().V('#15:0').outE('fsref').has('name', 'root').inV().next().property('name')
    #     # ==>vp[name->firstfs root]
    #     # gremlin> self.g().V('#15:0').outE('fsref').has('name', 'root').outV().next().property('name')
    #     # ==>vp[name->firstfs]

    #     if not path:
    #         logging.debug( " *** AT ROOT *** " )
    #         return fs # rootref # root            

    #     elif path and len(path) == 1 and path[0] == "":
    #         logging.debug( " *** AT ROOT *** " )
    #         return fs # rootref # root

    #     node = fs
    #     for elem in path:

    #         logging.debug( " *** PATH ELEM " )
    #         logging.debug( elem )
    #         logging.debug( node )

    #         if not self.isFolder(None, node):
    #             logging.debug( " *** PATH ELEM IS NOT FOLDER " )
    #             break

    #         # logging.debug( self.g().V('#14:5').inE('in').toList() )
    #         # logging.debug( self.g().V('#14:5').inE('in').outV().toList() )
    #         # logging.debug( self.g().V('#14:5').inE('in').outV().values('name') )
    #         # logging.debug( self.g().V('#14:5').inE('in').outV().values('name').toList() )
    #         entries = self.g().V( node.id['@value'] ).inE('in').outV().toList()
    #         logging.debug( " *** PATH ELEM ENTRIES " )
    #         logging.debug( entries )
    #         if entries:
    #             for entry in entries:

    #                 logging.debug( " *** PATH ELEM ENTRY " )
    #                 # logging.debug( entry.property('name') )
    #                 # logging.debug( entry.values('name') )

    #                 entryid = entry.id['@value']
    #                 entryvals = self.g().V(entryid).valueMap(True).toList()
    #                 entrylabel = None
    #                 entryname = None
    #                 # nodeid = None
    #                 for key, val in entryvals[0].items():
    #                     if key and "label" in str(key):
    #                         entrylabel = val
    #                     # elif key and "id" in str(key):
    #                     #     entryid = val['@value']
    #                     elif key and "name" in str(key):
    #                         entryname = val[0]

    #                 logging.debug( entryid )
    #                 logging.debug( entrylabel )
    #                 logging.debug( entryname )

    #                 if entryname == elem:
    #                     node = entry
    #                     break

    #     raise FuseOSError(errno.ENOENT)


    def path(self, path, node = None):

        # logging.debug( " *** PATH " )
        # logging.debug( path )

        if not node:
            fsid = self.getfs()
            # logging.debug( " *** FSID *** " )
            # logging.debug( fsid )
            fs = self.g().V(fsid).next()
            node = fs

        if not path:
            # logging.debug( " *** AT ROOT *** " )
            return node # fs # rootref # root            

        elif path and len(path) == 1 and path[0] == "":
            # logging.debug( " *** AT ROOT *** " )
            return node # fs # rootref # root

        elem = path[0]

        # logging.debug( " *** PATH ELEM " )
        # logging.debug( elem )
        # logging.debug( node )

        if not self.isFolder(None, node):
            # logging.debug( " *** PATH ELEM IS NOT FOLDER " )
            # break
            raise FuseOSError(errno.ENOENT)

        # logging.debug( self.g().V('#14:5').inE('in').toList() )
        # logging.debug( self.g().V('#14:5').inE('in').outV().toList() )
        # logging.debug( self.g().V('#14:5').inE('in').outV().values('name') )
        # logging.debug( self.g().V('#14:5').inE('in').outV().values('name').toList() )
        entries = self.g().V( node.id['@value'] ).inE('in').outV().toList()
        # logging.debug( " *** PATH ELEM ENTRIES " )
        # logging.debug( entries )
        if entries:
            for entry in entries:

                # logging.debug( " *** PATH ELEM ENTRY " )
                # logging.debug( entry.property('name') )
                # logging.debug( entry.values('name') )

                entryid = entry.id['@value']
                entryvals = self.g().V(entryid).valueMap(True).toList()
                entrylabel = None
                entryname = None
                # nodeid = None
                for key, val in entryvals[0].items():
                    if key and "label" in str(key):
                        entrylabel = val
                    # elif key and "id" in str(key):
                    #     entryid = val['@value']
                    elif key and "name" in str(key):
                        entryname = val[0]

                # logging.debug( entryid )
                # logging.debug( entrylabel )
                # logging.debug( entryname )

                if entryname == elem:
                    # node = entry
                    # break
                    return self.path(path[1:], entry)

        # Did not find anything
        raise FuseOSError(errno.ENOENT)


    def isFolder(self, path, node):

        # nodelabel = node.label
        # # nodetype = type(node).__name__
        # nodeclass = type(node).__name__ # type(node).__class__.__name__
        # # downnode = node.inV
        # # downnodelabel = downnode.label
        # # upnode = node.outV
        # # upnodelabel = upnode.label

        # logging.debug( " *** IS FOLDER *** " )
        # logging.debug( nodelabel )
        # logging.debug( nodeclass )
        # # logging.debug( downnodelabel )
        # # logging.debug( downnode )
        # # logging.debug( type(downnode).__name__ )
        # # logging.debug( type(downnode).__class__.__name__ )
        # # logging.debug( upnodelabel )
        # # logging.debug( upnode )
        # # logging.debug( type(upnode).__name__ )
        # # logging.debug( type(upnode).__class__.__name__ )

        if not node:
            logging.debug( " *** IS FOLDER: No node supplied *** " )
            return False

        nodeid = node.id['@value']
        nodevals = self.g().V(nodeid).valueMap(True).toList()
        nodelabel = None
        nodename = None
        # nodeid = None
        for key, val in nodevals[0].items():
            if key and "label" in str(key):
                nodelabel = val
            # elif key and "id" in str(key):
            #     nodeid = val['@value']
            elif key and "name" in str(key):
                nodename = val[0]

        # logging.debug( " *** IS FOLDER *** " )
        # logging.debug( nodeid )
        # logging.debug( nodelabel )
        # logging.debug( nodename )

        # # if path and path.find('.') != -1:
        # if downnode and downnodelabel != self.filesystem_folder_class:
        #     # raise FuseOSError(errno.ENOENT)
        #     # raise FuseOSError(errno.ENOTDIR)
        #     return False
        if nodelabel != "group":
            # logging.debug( " *** IS FOLDER FALSE *** " )
            return False
        # logging.debug( " *** IS FOLDER TRUE *** " )
        return True

    def folder(self, path, node):
        # node = self.path(self.expand(path))
        if not self.isFolder(path, node):
            raise FuseOSError(errno.ENOENT)
        return node

    def isFile(self, path, node):

        # nodelabel = node.label
        # # nodetype = type(node).__name__
        # nodeclass = type(node).__name__ # type(node).__class__.__name__
        # # downnode = node.inV
        # # downnodelabel = downnode.label
        # # upnode = node.outV
        # # upnodelabel = upnode.label

        # logging.debug( " *** IS FILE *** " )
        # logging.debug( nodelabel )
        # logging.debug( nodeclass )
        # # logging.debug( downnodelabel )
        # # logging.debug( downnode )
        # # logging.debug( upnodelabel )
        # # logging.debug( upnode )

        if not node:
            logging.debug( " *** IS FILE: No node supplied *** " )
            return False

        nodeid = node.id['@value']
        nodevals = self.g().V(nodeid).valueMap(True).toList()
        nodelabel = None
        nodename = None
        # nodeid = None
        for key, val in nodevals[0].items():
            if key and "label" in str(key):
                nodelabel = val
            # elif key and "id" in str(key):
            #     nodeid = val['@value']
            elif key and "name" in str(key):
                nodename = val[0]

        # logging.debug( " *** IS FILE *** " )
        # logging.debug( nodeid )
        # logging.debug( nodelabel )
        # logging.debug( nodename )

        # if path and path.find('.') == -1:
        # if downnode and downnodelabel != self.filesystem_file_class:
        #     # raise FuseOSError(errno.EISDIR)
        #     return False
        if nodelabel == "group":
            # logging.debug( " *** IS FILE FALSE *** " )
            return False
        # logging.debug( " *** IS FILE TRUE *** " )
        return True

    def file(self, path, node):
        # node = self.path(self.expand(path))
        if self.isFolder(path, node):
            raise FuseOSError(errno.EISDIR)
        elif not self.isFile(path, node):
            raise FuseOSError(errno.ENOENT)
        return node

    # 

    def createPath(self, path):
        logging.debug( " *** createPath *** " )
        path = self.expand(path)
        name = path[-1]
        path = path[0:-1]
        node = self.path(path)

        pathuuid = uuid.uuid1()

        # txn = self.graph().tx()

        newpath = self.g().addV().property('name',name).property('uuid',str(pathuuid)).addE("in").to(__.V( node.id["@value"] )).next()
        return newpath

        # self.graph().tx().commit()

    def movePath(self, old, node, new):
        logging.debug( " *** movePath *** " )

    def removePath(self, path, node):
        logging.debug( " *** movePath *** " )

    def createFolder(self, path):
        # logging.debug( " *** createFolder *** " )
        path = self.expand(path)
        name = path[-1]
        path = path[0:-1]
        # logging.debug( " name: " )
        # logging.debug( name )
        # logging.debug( " path: " )
        # logging.debug( path )
        node = self.path(path)
        # logging.debug( " node: " )
        # logging.debug( node )

        pathuuid = uuid.uuid1()

        # txn = self.graph().tx()

        # newfolder = self.g().addV(self.folder_label).property('name',name).next()
        newfolder = self.g().addV(self.folder_label).property('name',name).property('uuid',str(pathuuid)).addE("in").to(__.V( node.id["@value"] )).next()
        # logging.debug( " newfolder: " )
        # logging.debug( newfolder )
        # logging.debug( newfolder.id )
        # logging.debug( newfolder.id["@value"] )

        # Must look up FS here to get read ID??
        newfolder = self.g().V().hasLabel(self.folder_label).has('uuid',str(pathuuid)).next()
        # logging.debug( " newfolder: " )
        # logging.debug( newfolder )
        # logging.debug( newfolder.id )
        # logging.debug( newfolder.id["@value"] )

        # self.g().V( newfolder.id["@value"] ).addE("in").to(__.V( node.id["@value"] )).next()
        self.g().V( newfolder.id["@value"] ).addE("self").to(__.V( newfolder.id["@value"] )).next()

        # self.graph().tx().commit()

    def createFile(self, path):
        return self.createPath(path)

    def createLink(self, source, node, target):
        pass

    def setMode(self, path, node, mode):
        pass

    def setOwner(self, path, node, uid, gid):
        pass

    ### ###

    def __parsePathInGroups(self, path):
        
        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
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
                    raise ValueError("There was an error parsing path [{}].".format(path))

    # def __isGroup(self, groupId):
    #     # if not Group.select(self.graph, groupId).first() is None:
    #     #     return True

    #     return False

    # def __isFile(self, fileId):
    #     # if not File.select(self.graph, fileId).first() is None:
    #     #     return True

    #     return False
    
    # def __verifyPath(self, path, lastElementMustExist = False):
    #     """
    #     A path is valid if all the elements apart the last one are existing groups.
    #     If lastElementMustExist is True, then the last element must be either a Group or a File
    #     """
    #     elementsIDs = self.__parsePathInGroups(path)

    #     if elementsIDs is None:
    #         return True
    #     elif lastElementMustExist:
    #         if len(elementsIDs) > 1:
    #             if all(self.__isGroup(element) for element in elementsIDs[:-1]) \
    #                 and (self.__isGroup(elementsIDs[-1]) \
    #                     or self.__isFile(elementsIDs[-1])):
    #                 return True
    #             else:
    #                 return False
    #         else:
    #             if self.__isGroup(elementsIDs[-1]) \
    #                 or self.__isFile(elementsIDs[-1]):
    #                 return True
    #             else:
    #                 return False
    #     else:    
    #         if len(elementsIDs) > 1:
    #             if all(self.__isGroup(element) for element in elementsIDs[:-1]):
    #                 return True
    #             else:
    #                 return False
    #         else:
    #             return True

    ### ###



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
        logging.error(' *** GremlinFS: Exception ')


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
