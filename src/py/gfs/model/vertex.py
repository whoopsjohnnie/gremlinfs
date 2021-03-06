
# 
# Copyright (c) 2019, 2020, 2021, John Grundback
# All rights reserved.
# 

from gfs.common.log import GFSLogger

from gfs.error.error import GFSError
from gfs.error.error import GFSExistsError
from gfs.error.error import GFSNotExistsError
from gfs.error.error import GFSIsFileError
from gfs.error.error import GFSIsFolderError

from gfs.model.node import GFSNode
from gfs.model.edge import GFSEdge

from gfs.gfs import GremlinFS
from gfs.lib.util import GremlinFSUtils
from gfs.lib.event import GremlinFSEvent



class GFSVertex(GFSNode):

    logger = GFSLogger.getLogger("GFSVertex")

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
                    return GFSVertex.fromV(
                        GremlinFS.operations().api().vertices(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GFSVertex.fromV(
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
                    return GFSVertex.fromV(
                        GremlinFS.operations().api().vertices(
                            None, 
                            {
                                "uuid": parts["uuid"]
                            }
                        )
                    )
                else:
                    return GFSVertex.fromV(
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
                return GFSVertex.fromV(
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
                return GFSVertex.fromV(
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
            # return GFSVertex.fromVal(
            return GFSVertex.fromMap(
                v # .valueMap(*names).next()
            )

        else:
            return GFSVertex.fromMap(
                v # .valueMap(True).next()
            )

    @classmethod
    def fromVs(clazz, vs, names = []):
        if names:
            # return GFSVertex.fromVals(
            return GFSVertex.fromMaps(
                vs # .valueMap(*names).toList()
            )

        else:
            return GFSVertex.fromMaps(
                vs # .valueMap(True).toList()
            )

    def edges(self, edgeid = None, ine = True):

        node = self

        label = GFSEdge.infer("label", edgeid, None)
        name = GFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                return GFSEdge.fromEs(
                    self.api().inEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            else:
                return GFSEdge.fromEs(
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
                return GFSEdge.fromEs(
                    self.api().inEdges(
                        node.get("id"), 
                        label
                    )
                )

            else:
                return GFSEdge.fromEs(
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
                return GFSEdge.fromEs(
                    self.api().inEdges(
                        node.get("id")
                    )
                )

            else:
                return GFSEdge.fromEs(
                    self.api().outEdges(
                        node.get("id")
                    )
                )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

    def edge(self, edgeid, ine = True):

        node = self

        label = GFSEdge.infer("label", edgeid, None)
        name = GFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                return GFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            else:
                return GFSEdge.fromE(
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
                return GFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label
                    )
                )

            else:
                return GFSEdge.fromE(
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

        label = GFSEdge.infer("label", edgeid, None)
        name = GFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                if inv:
                    return GFSVertex.fromVs(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

            else:
                if inv:
                    return GFSVertex.fromVs(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GFSVertex.fromVs(
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
                    return GFSVertex.fromVs(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label
                        )
                    )

            else:
                if inv:
                    return GFSVertex.fromVs(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GFSVertex.fromVs(
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
                    return GFSVertex.fromVs(
                        self.api().inEdgesInVertices(
                            node.get("id")
                        )
                    )

                else:
                    return GFSVertex.fromVs(
                        self.api().inEdgesOutVertices(
                            node.get("id")
                        )
                    )

            else:
                if inv:
                    return GFSVertex.fromVs(
                        self.api().outEdgesInVertices(
                            node.get("id")
                        )
                    )

                else:
                    return GFSVertex.fromVs(
                        self.api().outEdgesOutVertices(
                            node.get("id")
                        )
                    )

            # except Exception as e:
            #     # self.logger.exception(' GremlinFS: edge from path ID exception ', e)
            #     return None

    def edgenode(self, edgeid, ine = True, inv = True):

        node = self

        label = GFSEdge.infer("label", edgeid, None)
        name = GFSEdge.infer("name", edgeid, None)

        if not label and name:
            label = name
            name = None

        if node and label and name:

            # try:

            if ine:
                if inv:
                    return GFSVertex.fromV(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GFSVertex.fromV(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

            else:
                if inv:
                    return GFSVertex.fromV(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label, {
                                "name", name
                            }
                        )
                    )

                else:
                    return GFSVertex.fromV(
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
                    return GFSVertex.fromV(
                        self.api().inEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GFSVertex.fromV(
                        self.api().inEdgesOutVertices(
                            node.get("id"), 
                            label
                        )
                    )

            else:
                if inv:
                    return GFSVertex.fromV(
                        self.api().outEdgesInVertices(
                            node.get("id"), 
                            label
                        )
                    )

                else:
                    return GFSVertex.fromV(
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

        newnode = GFSVertex.fromV(
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

        newfolder = GFSVertex.fromV(
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
            newedge = GFSEdge.fromE(
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
            newedge = GFSEdge.fromE(
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
                return GFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

            else:
                return GFSEdge.fromE(
                    self.api().outEdges(
                        node.get("id"), 
                        label, {
                            "name", name
                        }
                    )
                )

        else:
            if ine:
                return GFSEdge.fromE(
                    self.api().inEdges(
                        node.get("id"), 
                        label
                    )
                )

            else:
                return GFSEdge.fromE(
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
                # GFSEdge.fromE(
                # nodeid, edgelabel, edgeprops
                self.api().deleteVertexInEdge(
                    node.get("id"),
                    label, {
                        'name', name
                    }
                )
                # )

            else:
                # GFSEdge.fromE(
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
                # GFSEdge.fromE(
                self.api().deleteVertexInEdge(
                    node.get("id"),
                    label
                )
                # )

            else:
                # GFSEdge.fromE(
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

        return GFSVertex.fromMap(
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
