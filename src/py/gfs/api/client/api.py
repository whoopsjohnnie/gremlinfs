# 
# Copyright (c) 2020, John Grundback
# All rights reserved.
# 

import requests

import simplejson as json

import contextlib
import addict
from addict import Dict

from .common.log import GremlinFSLogger

# vertices get, post
# vertices properties get
# vertices property get
# vertex get, put, delete
# vertex properties get, post
# vertex property get, put, delete
# vertex edge get
# vertex edge by label get
# vertex out edge get
# vertex out edge by label get
# vertex in edge get
# vertex in edge by label get



class GFSAPIError(Exception):

    def __init__(self, error):
        pass



class GFSAPI():

    logger = GremlinFSLogger.getLogger("GFSAPI")

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

    def apiid(self, resourceid):
        if resourceid and type(resourceid) == str:
            return str(resourceid.replace("#", ""))
        return str(resourceid)

    def apiurl(self, resource, properties = {}):

        apiurl = "http://" + self.gfs_host + ":" + self.gfs_port + "/" + self.api_version + "/" + self.api_namespace + "/" + resource
        # self.logger.debug(apiurl)

        if properties:
            apiurl = apiurl + "?"
            for name in properties:
                apiurl = apiurl + name + "=" + properties.get(name) + "&"

        # self.logger.debug(apiurl)

        self.logger.debug(' GFSAPI: apiurl for resource: ' + resource + ': ' + apiurl)

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
            raise GFSAPIError(
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
        self.logger.debug(' GFSAPI: POSTing to URL: ' + url)
        self.logger.debug(data)
        resp = requests.post(
            url,
            json = data
        )
        self.logger.debug(resp)
        if resp.status_code != 200:
            raise GFSAPIError(
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
        self.logger.debug(' GFSAPI: PUTing to URL: ' + url)
        self.logger.debug(data)
        resp = requests.put(
            url,
            json = data
        )
        self.logger.debug(resp)
        if resp.status_code != 200:
            raise GFSAPIError(
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
            raise GFSAPIError(
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
        self.logger.debug(' GFSAPI: query ')
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
                "vertex/" + self.apiid(resourceid) + "/property/" + property
            )

            return data.get("@value", {}).get("value").replace("\"", "")

        else:
            data = self.apiget(
                "vertex/" + self.apiid(resourceid)
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
                "vertex/" + self.apiid(resourceid) + "/property/" + property,
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
                    "vertex/" + self.apiid(resourceid) + "/property/" + property
                )

            except Exception as e:
                return False

            return True

    #
    #
    #

    def vertices(self, vlabel = None, vproperties = {}):
        self.logger.debug(' GFSAPI: vertices ')
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
        self.logger.debug(' GFSAPI: verticesWithEdge ')
        properties = {} # vproperties
        # if elabel:
        #     properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                # "vertex/" + self.apiid(elvalue) + "/outedge/" + elabel + "/invertex",
                "vertex/" + self.apiid(elvalue) + "/inedge/" + elabel + "/outvertex",
                properties
            ))

        except Exception as e:
            return []

        return data

    def verticesWithoutEdge(self, elabel):
        self.logger.debug(' GFSAPI: verticesWithoutEdge ')
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
        self.logger.debug(' GFSAPI: vertex ')
        data = self.json(self.apiget(
            "vertex/" + self.apiid(vid)
        ))

        return data

    def createVertex(self, vlabel = None, vproperties = {}):
        self.logger.debug(' GFSAPI: createVertex ')
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
        # .get(
        #     "@value", {}
        # ).get(
        #     "properties", {}
        # )

    def updateVertex(self, vid, vproperties = {}):
        self.logger.debug(' GFSAPI: updateVertex ')
        properties = {
            "properties": vproperties
        }
        data = self.json(self.apiput(
            "vertex/" + self.apiid(vid), {
                "@type": "g:Vertex",
                "@value": properties
            }
        ))

        return data
        # .get(
        #     "@value", {}
        # ).get(
        #     "properties", {}
        # )

    def deleteVertex(self, vid):
        self.logger.debug(' GFSAPI: deleteVertex ')
        data = self.apidelete(
            "vertex/" + self.apiid(vid)
        )

        return data

    def edges(self, elabel = None, eproperties = {}):
        self.logger.debug(' GFSAPI: edges ')
        properties = eproperties
        if elabel:
            properties["label"] = elabel
        data = self.json(self.apiget(
            "edge",
            properties
        ))

        return data

    def edge(self, vid = None):
        self.logger.debug(' GFSAPI: edge ')
        data = self.json(self.apiget(
            "edge/" + self.apiid(vid)
        ))

        return data

    def createEdge(self, svid, tvid, elabel = None, eproperties = {}):
        self.logger.debug(' GFSAPI: createEdge ')
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
        # .get(
        #     "@value", {}
        # ).get(
        #     "properties", {}
        # )

    def updateEdge(self, eid, eproperties = {}):
        self.logger.debug(' GFSAPI: updateEdge ')
        properties = {
            # "inVLabel": None,
            # "outVLabel": None,
            # "inV": tvid,
            # "outV": svid,
            "properties": eproperties
        }
        # if elabel:
        #     properties["label"] = elabel
        data = self.json(self.apiput(
            "edge/" + self.apiid(eid), {
                "@type": "g:Edge",
                "@value": properties
            }
        ))

        return data
        # .get(
        #     "@value", {}
        # ).get(
        #     "properties", {}
        # )

    def deleteEdge(self, eid):
        self.logger.debug(' GFSAPI: deleteEdge ')
        data = self.json(self.apidelete(
            "edge/" + self.apiid(eid)
        ))

        return data

    def deleteVertexInEdge(self, vid, elabel, eproperties = {}):
        self.logger.debug(' GFSAPI: deleteVertexInEdge ')

    def deleteVertexOutEdge(self, vid, elabel, eproperties = {}):
        self.logger.debug(' GFSAPI: deleteVertexOutEdge ')


    def inEdges(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GFSAPI: inEdges ')
        properties = eproperties
        if elabel:
            properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/inedge/" + elabel
            ))

        except Exception as e:
            return []

        return data

    def outEdges(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GFSAPI: outEdges ')
        properties = eproperties
        if elabel:
            properties["label"] = elabel

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/outedge/" + elabel
            ))

        except Exception as e:
            return []

        return data


    def deleteInEdges(self, vid, elabel = None):
        self.logger.debug(' GFSAPI: deleteInEdges ')
        data = self.json(self.apidelete(
            "vertex/" + self.apiid(vid) + "/inedge/" + elabel
        ))

        return data

    def deleteOutEdges(self, vid, elabel = None):
        self.logger.debug(' GFSAPI: deleteOutEdges ')
        data = self.json(self.apidelete(
            "vertex/" + self.apiid(vid) + "/outedge/" + elabel
        ))

        return data


    def inEdgesInVertices(self, vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GFSAPI: inEdgeInVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/inedge/" + elabel + "/invertex"
            ))

        except Exception as e:
            return []

        return data

    def inEdgesOutVertices(self, vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GFSAPI: inEdgeOutVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/inedge/" + elabel + "/outvertex"
            ))

        except Exception as e:
            return []

        return data

    def outEdgesInVertices(self,vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GFSAPI: outEdgeInVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/outedge/" + elabel + "/invertex"
            ))

        except Exception as e:
            return []

        return data

    def outEdgesOutVertices(self, vid, elabel = None, eproperties = {}, vlabel = None, vproperties = {}):
        self.logger.debug(' GFSAPI: outEdgeOutVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/outedge/" + elabel + "/outvertex"
            ))

        except Exception as e:
            return []

        return data


    def inVertices(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GFSAPI: inVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/edge/" + elabel + "/invertex"
            ))

        except Exception as e:
            return []

        return data

    def outVertices(self, vid, elabel = None, eproperties = {}):
        self.logger.debug(' GFSAPI: outVertices ')

        data = None

        # 404 throws exception above
        try:
            data = self.json(self.apiget(
                "vertex/" + self.apiid(vid) + "/edge/" + elabel + "/outvertex"
            ))

        except Exception as e:
            return []

        return data


    def vertexProperties(self, vid, vproperties = {}):
        self.logger.debug(' GFSAPI: vertexProperties ')

    def vertexProperty(self, vid, vproperties = {}):
        self.logger.debug(' GFSAPI: vertexProperty ')

    def setVertexProperties(self, vid, vproperties = {}):
        self.logger.debug(' GFSAPI: setVertexProperties ')

    def setVertexProperty(self, vid, name, value):
        self.logger.debug(' GFSAPI: setVertexProperty ')
        data = self.json(self.apiput(
            "vertex/" + self.apiid(vid) + "/property/" + name, {
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
        self.logger.debug(' GFSAPI: unsetVertexProperty ')
        data = self.json(self.apidelete(
            "vertex/" + self.apiid(vid) + "/property/" + name
        ))

        return data


    def context(self, vid):
        self.logger.debug(' GFSAPI: context ')
        return self.json(self.apiget(
            "context/" + self.apiid(vid)
        ))

    def render(self, vid):
        self.logger.debug(' GFSAPI: render ')
        return self.apiget(
            "render/" + self.apiid(vid)
        )



class GFSCachingAPI(GFSAPI):

    logger = GremlinFSLogger.getLogger("GFSCachingAPI")

    def __init__(
        self,

        gfs_host,
        gfs_port,
        gfs_username,
        gfs_password,

        **kwargs):

        super().__init__(

            gfs_host,
            gfs_port,
            gfs_username,
            gfs_password,

            **kwargs
        )

        self.caching = True # False
        self.cache = Dict() # {}

    # 

    def lookupCache(self, path, oper):

        from datetime import datetime
        self.logger.debug("CACHE: lookup: path: %s, oper: %s", path, oper)

        pick = False
        cachehit = self.cache[path][oper]
        if cachehit:
            if cachehit['expire']:
                if cachehit['expire'] > datetime.now():
                    self.logger.debug("CACHE: lookup: cachehit: found entry with expire, not expired")
                    pick = cachehit
                else:
                    self.logger.debug("CACHE: lookup: cachehit: found entry with expire, is expired")
            else:
                pick = cachehit

        if pick:
            cachehit = pick
            exp = ""
            if cachehit['expire']:
                exp = str(cachehit['expire'])
            self.logger.debug("CACHE: lookup: cachehit: PATH: %s, OPER: %s, CREATED: %s, EXPIRE: %s", 
                cachehit.path, cachehit.oper, str(cachehit.created), exp
            )
            return cachehit.data

        else:
            return False

    def prepareCache(self, path, oper, expire_seconds = None):

        from datetime import datetime, timedelta
        self.logger.debug("CACHE: prepare: path: %s, oper: %s", path, oper)

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

        self.logger.debug("CACHE: finalize: path: %s, oper: %s", path, oper)
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
            except Exception as e:
                self.logger.warning("Client call not fatal error: lookup cache error: exception: %s" % ( str(e) ))
                self.logger.warning(e)

        if cachepath and self.caching:
            cache = None
            try:
                cache = self.prepareCache(cachepath, cacheoper, expire)
            except Exception as e:
                self.logger.warning("Client call not fatal error: prepare cache error: exception: %s" % ( str(e) ))
                self.logger.warning(e)

    def updateCache(self, path, oper, data):

        cachepath = path
        cacheoper = oper

        if data:
            if cachepath and self.caching:
                try:
                    self.finalizeCache(cachepath, cacheoper, data)
                except Exception as e:
                    self.logger.warning("Client call not fatal error: finalize cache error: exception: %s" % ( str(e) ))
                    self.logger.warning(e)

        return data

    def clearCache(self, path = None):

        cachepath = path

        if cachepath:
            self.logger.debug("CACHE: clear: path: %s", cachepath)
            del self.cache[path]

        else:
            self.logger.debug("CACHE: clear full")
            self.cache = Dict() # {}

    # 

    def apiget(self, resource, properties = {}):

        url = self.apiurl(
            resource, 
            properties
        )

        cachepath = url
        cacheoper = 'GET'

        # resp = None
        data = None

        cachedata = self.readCache(cachepath, cacheoper)
        if cachedata:
            # resp = cachedata
            data = cachedata

        else:
            resp = requests.get(
                url
            )
            data = {
                "status": resp.status_code,
                "data": self.decode(resp.text)
            }
            self.updateCache(cachepath, cacheoper, data)

        # if resp.status_code != 200:
        if data.get("status", 0) != 200:
            raise GFSAPIError(
                '{} {} {}'.format(
                    "GET",
                    url,
                    data.get("status", 0)
                )
            )

        return data.get("data", None)

    def apipost(self, resource, data = {}):
        ret = super().apipost(resource, data)
        # Clear full cache for now
        self.clearCache()
        return ret

    def apiput(self, resource, data = {}):
        ret = super().apiput(resource, data)
        # Clear full cache for now
        self.clearCache()
        return ret

    def apidelete(self, resource):
        ret = super().apidelete(resource)
        # Clear full cache for now
        self.clearCache()
        return ret
