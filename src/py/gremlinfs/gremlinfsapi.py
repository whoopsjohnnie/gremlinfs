# 
# Copyright (c) 2020, John Grundback
# All rights reserved.
# 

import requests

import simplejson as json

from .gremlinfslog import GremlinFSLogger



class GremlinFSAPIError(Exception):

    def __init__(self, error):
        pass



class GremlinFSAPI():

    logger = GremlinFSLogger.getLogger("GremlinFSAPI")

    def __init__(
        self,

        gfs_host,
        gfs_port,
        gfs_username,
        gfs_password,

        **kwargs):

        self.gfs_host = gfs_host
        self.gfs_port = gfs_port
        self.gfs_username = gfs_username
        self.gfs_password = gfs_password

        self.api_version = "api/v1.0"
        self.api_namespace = "gfs1"

    def decode(self, data):

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

    def json(self, data):

        if not data:
            return None

        return json.loads(data)

    def apiurl(self, resource, properties = {}):

        apiurl = "http://" + self.gfs_host + ":" + self.gfs_port + "/" + self.api_version + "/" + self.api_namespace + "/" + resource
        # self.logger.debug(apiurl)

        if properties:
            apiurl = apiurl + "?"
            for name in properties:
                apiurl = apiurl + name + "=" + properties.get(name) + "&"

        # self.logger.debug(apiurl)

        self.logger.debug(' GremlinFSAPI: apiurl for resource: ' + resource + ': ' + apiurl)

        return apiurl

    def apiget(self, resource, properties = {}):
        url = self.apiurl(
            resource, 
            properties
        )
        resp = requests.get(
            url
        )
        if resp.status_code != 200:
            raise GremlinFSAPIError(
                '{} {} {}'.format(
                    "GET",
                    url,
                    resp.status_code
                )
            )
        return self.decode(
            resp.text
        )

    def apipost(self, resource, data = {}):
        url = self.apiurl(
            resource
        )
        self.logger.debug(' GremlinFSAPI: POSTing to URL: ' + url)
        self.logger.debug(data)
        resp = requests.post(
            url,
            json = data
        )
        self.logger.debug(resp)
        if resp.status_code != 200:
            raise GremlinFSAPIError(
                '{} {} {}'.format(
                    "POST",
                    url,
                    resp.status_code
                )
            )
        return self.decode(
            resp.text
        )

    def apiput(self, resource, data = {}):
        url = self.apiurl(
            resource
        )
        self.logger.debug(' GremlinFSAPI: PUTing to URL: ' + url)
        self.logger.debug(data)
        resp = requests.put(
            url,
            json = data
        )
        self.logger.debug(resp)
        if resp.status_code != 200:
            raise GremlinFSAPIError(
                '{} {} {}'.format(
                    "PUT",
                    url,
                    resp.status_code
                )
            )
        return self.decode(
            resp.text
        )

    def apidelete(self, resource):
        url = self.apiurl(
            resource
        )
        resp = requests.delete(
            url
        )
        if resp.status_code != 200:
            raise GremlinFSAPIError(
                '{} {} {}'.format(
                    "PUT",
                    url,
                    resp.status_code
                )
            )
        return self.decode(
            resp.text
        )

    #
    #
    #

    def query(self, resource, match = {}, fields = []):
        self.logger.debug(' GremlinFSAPI: query ')
        properties = match
        if resource:
            properties["label"] = resource
        data = self.apiget(
            "vertex",
            properties
        )
        ret = []
        for item in data:
            ret.append({
                key: value for key, value in item.get("@value", {}).get("properties").items() if key in fields
            })

        return ret

    def get(self, resource, resourceid, property = None, fields = []):
        if property:
            data = self.apiget(
                "vertex/" + resourceid.replace("#", "") + "/property/" + property
            )

            return data.get("@value", {}).get("value").replace("\"", "")

        else:
            data = self.apiget(
                "vertex/" + resourceid.replace("#", "")
            )

            return {
                key: value for key, value in data.get("@value", {}).get("properties").items() if key in fields
            }

    def create(self, resource, data = {}, fields = []):
        pass

    def update(self, resource, resourceid, data = {}, fields = []):
        pass

    def delete(self, resource, resourceid):
        pass

    def set(self, resource, resourceid, property, value):
        if property:
            data = self.apiput(
                "vertex/" + resourceid.replace("#", "") + "/property/" + property,
                data = {
                    "@type": "g:VertexProperty",
                    "@value": {
                        "label": property,
                        "value": value
                    }
                }
            )

            return data.get("@value", {}).get("value").replace("\"", "")

    def unset(self, resource, resourceid, property):
        if property:
            try:
                self.apidelete(
                    "vertex/" + resourceid.replace("#", "") + "/property/" + property
                )

            except Exception as e:
                return False

            return True

    #
    #
    #

    def vertices(self, vlabel = None, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: vertices ')
        properties = vproperties
        if vlabel:
            properties["label"] = vlabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex",
                properties
            ))

        except Exception as e:
            return []

        return data

    def verticesWithEdge(self, elabel, elvalue = None):
        self.logger.debug(' GremlinFSAPI: verticesWithEdge ')
        properties = {} # vproperties
        # if elabel:
        #     properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                # "vertex/" + elvalue.replace("#", "") + "/outedge/" + elabel + "/invertex",
                "vertex/" + elvalue.replace("#", "") + "/inedge/" + elabel + "/outvertex",
                properties
            ))

        except Exception as e:
            return []

        return data

    def verticesWithoutEdge(self, elabel):
        self.logger.debug(' GremlinFSAPI: verticesWithoutEdge ')
        properties = {} # vproperties
        # if elabel:
        #     properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex",
                properties
            ))

        except Exception as e:
            return []

        return data

    def vertex(self, vid = None):
        self.logger.debug(' GremlinFSAPI: vertex ')
        data = self.json(self.apiget(
            "vertex/" + vid.replace("#", "")
        ))

        return data

    def createVertex(self, vlabel = None, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: createVertex ')
        properties = {
            "properties": vproperties
        }
        if vlabel:
            properties["label"] = vlabel
        data = self.json(self.apipost(
            "vertex", {
                "@type": "g:Vertex",
                "@value": properties
            }
        ))

        return data

    def updateVertex(self, vid, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: updateVertex ')
        properties = {
            "properties": vproperties
        }
        data = self.json(self.apiput(
            "vertex/" + vid.replace("#", ""), {
                "@type": "g:Vertex",
                "@value": properties
            }
        ))

        return data

    def deleteVertex(self, vid):
        self.logger.debug(' GremlinFSAPI: deleteVertex ')
        data = self.apidelete(
            "vertex/" + vid.replace("#", "")
        )

        return data

    def edges(self, elabel = None, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: edges ')
        properties = eproperties
        if elabel:
            properties["label"] = elabel
        data = self.json(self.apiget(
            "edge",
            properties
        ))

        return data

    def edge(self, vid = None):
        self.logger.debug(' GremlinFSAPI: edge ')
        data = self.json(self.apiget(
            "edge/" + vid.replace("#", "")
        ))

        return data

    def createEdge(self, svid, tvid, elabel = None, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: createEdge ')
        properties = {
            "inVLabel": None,
            "outVLabel": None,
            "inV": tvid,
            "outV": svid,
            "properties": eproperties
        }
        if elabel:
            properties["label"] = elabel
        data = self.json(self.apipost(
            "edge", {
                "@type": "g:Edge",
                "@value": properties
            }
        ))

        return data

    def updateEdge(self, eid, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: updateEdge ')
        properties = {
            "properties": eproperties
        }
        # if elabel:
        #     properties["label"] = elabel
        data = self.json(self.apiput(
            "edge/" + eid.replace("#", ""), {
                "@type": "g:Edge",
                "@value": properties
            }
        ))

        return data

    def deleteEdge(self, eid):
        self.logger.debug(' GremlinFSAPI: deleteEdge ')
        data = self.json(self.apidelete(
            "edge/" + eid.replace("#", "")
        ))

        return data

    def deleteVertexInEdge(self, vid, elabel, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: deleteVertexInEdge ')

    def deleteVertexOutEdge(self, vid, elabel, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: deleteVertexOutEdge ')


    def inEdges(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: inEdges ')
        properties = eproperties
        if elabel:
            properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/inedge/" + elabel
            ))

        except Exception as e:
            return []

        return data

    def outEdges(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: outEdges ')
        properties = eproperties
        if elabel:
            properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/outedge/" + elabel
            ))

        except Exception as e:
            return []

        return data


    def deleteInEdges(self, vid, elabel = None):
        self.logger.debug(' GremlinFSAPI: deleteInEdges ')
        data = self.json(self.apidelete(
            "vertex/" + vid.replace("#", "") + "/inedge/" + elabel
        ))

        return data

    def deleteOutEdges(self, vid, elabel = None):
        self.logger.debug(' GremlinFSAPI: deleteOutEdges ')
        data = self.json(self.apidelete(
            "vertex/" + vid.replace("#", "") + "/outedge/" + elabel
        ))

        return data


    def inEdgesInVertices(self, vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: inEdgeInVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/inedge/" + elabel + "/invertex"
            ))

        except Exception as e:
            return []

        return data

    def inEdgesOutVertices(self, vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: inEdgeOutVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/inedge/" + elabel + "/outvertex"
            ))

        except Exception as e:
            return []

        return data

    def outEdgesInVertices(self,vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: outEdgeInVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/outedge/" + elabel + "/invertex"
            ))

        except Exception as e:
            return []

        return data

    def outEdgesOutVertices(self, vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: outEdgeOutVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/outedge/" + elabel + "/outvertex"
            ))

        except Exception as e:
            return []

        return data


    def inVertices(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: inVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/edge/" + elabel + "/invertex"
            ))

        except Exception as e:
            return []

        return data

    def outVertices(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GremlinFSAPI: outVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + vid.replace("#", "") + "/edge/" + elabel + "/outvertex"
            ))

        except Exception as e:
            return []

        return data


    def vertexProperties(self, vid, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: vertexProperties ')

    def vertexProperty(self, vid, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: vertexProperty ')

    def setVertexProperties(self, vid, vproperties = {}):
        self.logger.debug(' GremlinFSAPI: setVertexProperties ')

    def setVertexProperty(self, vid, name, value):
        self.logger.debug(' GremlinFSAPI: setVertexProperty ')
        data = self.json(self.apiput(
            "vertex/" + vid.replace("#", "") + "/property/" + name, {
                "@type": "g:VertexProperty",
                "@value": {
                    # "id": vid + "_" + name,
                    "label": name,
                    "value": value
                }
            }
        ))

        return data

    def unsetVertexProperty(self, vid, name):
        self.logger.debug(' GremlinFSAPI: unsetVertexProperty ')
        data = self.json(self.apidelete(
            "vertex/" + vid.replace("#", "") + "/property/" + name
        ))

        return data


    def context(self, vid):
        self.logger.debug(' GremlinFSAPI: context ')
        return self.json(self.apiget(
            "context/" + vid.replace("#", "")
        ))

    def render(self, vid):
        self.logger.debug(' GremlinFSAPI: render ')
        return self.apiget(
            "render/" + vid.replace("#", "")
        )
