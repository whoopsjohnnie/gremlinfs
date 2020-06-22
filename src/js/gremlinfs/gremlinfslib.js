import {GremlinFSLogger} from './gremlinfslog';
import {GremlinFSObj} from './gremlinfsobj';
import {gfslist} from './gremlinfsobj';
import {gfsmap} from './gremlinfsobj';
var _pj;
function _pj_snippets(container) {
    function in_es6(left, right) {
        if (((right instanceof Array) || ((typeof right) === "string"))) {
            return (right.indexOf(left) > (- 1));
        } else {
            if (((right instanceof Map) || (right instanceof Set) || (right instanceof WeakMap) || (right instanceof WeakSet))) {
                return right.has(left);
            } else {
                return (left in right);
            }
        }
    }
    function set_properties(cls, props) {
        var desc, value;
        var _pj_a = props;
        for (var p in _pj_a) {
            if (_pj_a.hasOwnProperty(p)) {
                value = props[p];
                if (((((! ((value instanceof Map) || (value instanceof WeakMap))) && (value instanceof Object)) && ("get" in value)) && (value.get instanceof Function))) {
                    desc = value;
                } else {
                    desc = {"value": value, "enumerable": false, "configurable": true, "writable": true};
                }
                Object.defineProperty(cls.prototype, p, desc);
            }
        }
    }
    container["in_es6"] = in_es6;
    container["set_properties"] = set_properties;
    return container;
}
_pj = {};
_pj_snippets(_pj);
class GremlinFSError extends Exception {
    constructor(path = null) {
        this.path = path;
    }
}
class GremlinFSExistsError extends GremlinFSError {
    constructor(path = null) {
        this.path = path;
    }
}
class GremlinFSNotExistsError extends GremlinFSError {
    constructor(path = null) {
        this.path = path;
    }
}
class GremlinFSIsFileError extends GremlinFSError {
    constructor(path = null) {
        this.path = path;
    }
}
class GremlinFSIsFolderError extends GremlinFSError {
    constructor(path = null) {
        this.path = path;
    }
}
class GremlinFSBase extends GremlinFSObj {
    constructor(kwargs = {}) {
        super();
        this.setall(kwargs);
    }
    property(name, _default_ = null, prefix = null) {
        return this.get(name, _default_, prefix);
    }
}
_pj.set_properties(GremlinFSBase, {"logger": GremlinFSLogger.getLogger("GremlinFSBase")});
class GremlinFSPath extends GremlinFSBase {
    static paths() {
        return {"root": {"type": "folder", "debug": false}, "atpath": {"type": null, "debug": false}, "vertex_labels": {"type": "folder", "debug": false}, "vertex_label": {"type": "file", "debug": false}, "vertexes": {"type": "folder", "debug": false}, "vertex": {"type": "folder", "debug": false}, "vertex_properties": {"type": "folder", "debug": false}, "vertex_folder_property": {"type": "folder", "debug": false}, "vertex_property": {"type": "file", "debug": false}, "vertex_edges": {"type": "folder", "debug": false}, "vertex_in_edges": {"type": "folder", "debug": false}, "vertex_out_edges": {"type": "folder", "debug": false}, "vertex_edge": {"type": "link", "debug": false}, "vertex_in_edge": {"type": "link", "debug": false}, "vertex_out_edge": {"type": "link", "debug": false}, "create_vertex": {"type": "file", "debug": false}};
    }
    static path(path) {
        var paths;
        paths = GremlinFSPath.paths();
        if ((paths && _pj.in_es6(path, paths))) {
            return paths[path];
        }
        return null;
    }
    static expand(path) {
        return GremlinFS.operations().utils().splitpath(path);
    }
    static atpath(path, node = null) {
        var elem, nodes, root;
        if ((! node)) {
            root = null;
            if (GremlinFS.operations().config("fs_root", null)) {
                root = GremlinFSVertex.load(GremlinFS.operations().config("fs_root", null));
            }
            node = root;
        }
        if ((! path)) {
            return node;
        } else {
            if ((path && (path.length === 0))) {
                return node;
            } else {
                if (((path && (path.length === 1)) && (path[0] === ""))) {
                    return node;
                }
            }
        }
        elem = path[0];
        nodes = null;
        if (node) {
            nodes = node.readFolderEntries();
        } else {
            nodes = GremlinFSVertex.fromVs(new GremlinFS.operations().g().V().where(GremlinFS.operations().a().out(GremlinFS.operations().config("in_label", "in")).count().is(0)));
        }
        if (nodes) {
            for (var cnode, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                cnode = _pj_a[_pj_c];
                if ((cnode.toid(true) === elem)) {
                    return GremlinFSPath.atpath(path.slice(1), cnode);
                }
            }
        }
        return null;
    }
    static pathnode(nodeid, parent, path) {
        var node, nodes;
        node = null;
        if ((parent && nodeid)) {
            nodes = parent.readFolderEntries();
            if (nodes) {
                for (var cnode, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                    cnode = _pj_a[_pj_c];
                    if ((cnode && (cnode.get("name") === nodeid))) {
                        node = cnode;
                        break;
                    }
                }
            }
        } else {
            if (nodeid) {
                node = GremlinFSVertex.load(nodeid);
            } else {
                if (path) {
                    node = GremlinFSPath.atpath(path);
                }
            }
        }
        return node;
    }
    static pathparent(path = []) {
        var parent, root, vindex;
        root = null;
        if (GremlinFS.operations().config("fs_root", null)) {
            root = GremlinFSVertex.load(GremlinFS.operations().config("fs_root", null));
        }
        parent = root;
        if ((! path)) {
            return parent;
        }
        vindex = 0;
        for (var item, _pj_c = 0, _pj_a = path, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
            item = _pj_a[_pj_c];
            if ((item === GremlinFS.operations().config("vertex_folder", ".V"))) {
                break;
            }
            vindex += 1;
        }
        if (vindex) {
            if ((vindex > 0)) {
                parent = GremlinFSPath.atpath(path.slice(0, vindex));
            }
        } else {
            parent = GremlinFSPath.atpath(path);
        }
        return parent;
    }
    static match(path) {
        var clazz, debug, expanded, match, node, parent, vindex;
        clazz = this;
        match = gfsmap({"path": null, "full": null, "parent": null, "node": null, "name": null, "vertexlabel": "vertex", "vertexid": null, "vertexuuid": null, "vertexname": null, "vertexproperty": null, "vertexedge": null});
        match.update({"full": GremlinFS.operations().utils().splitpath(path)});
        expanded = match.get("full", []);
        if ((! expanded)) {
            match.update({"path": "root"});
        } else {
            if ((expanded && (expanded.length === 0))) {
                match.update({"path": "root"});
            } else {
                if ((expanded && _pj.in_es6(GremlinFS.operations().config("vertex_folder", ".V"), expanded))) {
                    vindex = 0;
                    for (var item, _pj_c = 0, _pj_a = expanded, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                        item = _pj_a[_pj_c];
                        if ((item === GremlinFS.operations().config("vertex_folder", ".V"))) {
                            break;
                        }
                        vindex += 1;
                    }
                    if ((expanded.length === (vindex + 1))) {
                        parent = GremlinFSPath.pathparent(expanded);
                        if (parent) {
                            match.update({"parent": parent});
                        }
                        match.update({"path": "vertexes"});
                    } else {
                        if ((expanded.length === (vindex + 2))) {
                            parent = GremlinFSPath.pathparent(expanded);
                            if (parent) {
                                match.update({"parent": parent});
                            }
                            node = GremlinFSPath.pathnode(expanded[(vindex + 1)], match.get("parent", null), match.get("full", null));
                            if (node) {
                                match.update({"node": node});
                            }
                            match.update({"path": "vertex", "vertexid": GremlinFSUtils.found(expanded[(vindex + 1)])});
                        } else {
                            if ((expanded.length === (vindex + 3))) {
                                parent = GremlinFSPath.pathparent(expanded);
                                if (parent) {
                                    match.update({"parent": parent});
                                }
                                node = GremlinFSPath.pathnode(expanded[(vindex + 1)], match.get("parent", null), match.get("full", null));
                                if (node) {
                                    match.update({"node": node});
                                }
                                if ((expanded[(vindex + 2)] === GremlinFS.operations().config("in_edge_folder", "EI"))) {
                                    match.update({"path": "vertex_in_edges", "vertexid": GremlinFSUtils.found(expanded[(vindex + 1)])});
                                } else {
                                    if ((expanded[(vindex + 2)] === GremlinFS.operations().config("out_edge_folder", "EO"))) {
                                        match.update({"path": "vertex_out_edges", "vertexid": GremlinFSUtils.found(expanded[(vindex + 1)])});
                                    } else {
                                        match.update({"path": "vertex_property", "vertexid": GremlinFSUtils.found(expanded[(vindex + 1)]), "vertexproperty": GremlinFSUtils.found(expanded[(vindex + 2)])});
                                    }
                                }
                            } else {
                                if ((expanded.length === (vindex + 4))) {
                                    parent = GremlinFSPath.pathparent(expanded);
                                    if (parent) {
                                        match.update({"parent": parent});
                                    }
                                    node = GremlinFSPath.pathnode(expanded[(vindex + 1)], match.get("parent", null), match.get("full", null));
                                    if (node) {
                                        match.update({"node": node});
                                    }
                                    if ((expanded[(vindex + 2)] === GremlinFS.operations().config("in_edge_folder", "EI"))) {
                                        match.update({"path": "vertex_in_edge", "vertexid": GremlinFSUtils.found(expanded[(vindex + 1)]), "vertexedge": GremlinFSUtils.found(expanded[(vindex + 3)])});
                                    } else {
                                        if ((expanded[(vindex + 2)] === GremlinFS.operations().config("out_edge_folder", "EO"))) {
                                            match.update({"path": "vertex_out_edge", "vertexid": GremlinFSUtils.found(expanded[(vindex + 1)]), "vertexedge": GremlinFSUtils.found(expanded[(vindex + 3)])});
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if ((expanded && (expanded.length === 1))) {
                        match.update({"path": "atpath", "name": expanded[0], "parent": null, "node": GremlinFSPath.pathnode(match.get("vertexid", null), null, match.get("full", null))});
                    } else {
                        if ((expanded && (expanded.length === 2))) {
                            match.update({"path": "atpath", "name": expanded[1], "parent": GremlinFSPath.pathparent([expanded[0]])});
                            match.update({"node": GremlinFSPath.pathnode(match.get("vertexid", null), match.get("parent", null), match.get("full", null))});
                        } else {
                            if ((expanded && (expanded.length > 2))) {
                                match.update({"path": "atpath", "name": expanded.slice((- 1))[0], "parent": GremlinFSPath.pathparent(expanded.slice(0, (- 1)))});
                                match.update({"node": GremlinFSPath.pathnode(match.get("vertexid", null), match.get("parent", null), match.get("full", null))});
                            }
                        }
                    }
                }
            }
        }
        match.update(GremlinFSPath.path(match.get("path")));
        debug = false;
        if ((match && match.get("debug", false))) {
            debug = true;
        }
        return new GremlinFSPath({"type": match.get("type"), "debug": debug, "path": match.get("path"), "full": match.get("full"), "parent": match.get("parent"), "node": match.get("node"), "name": match.get("name"), "vertexlabel": match.get("vertexlabel"), "vertexid": match.get("vertexid"), "vertexuuid": match.get("vertexuuid"), "vertexname": match.get("vertexname"), "vertexproperty": match.get("vertexproperty"), "vertexedge": match.get("vertexedge")});
    }
    constructor(kwargs = {}) {
        super();
        this.setall(kwargs);
    }
    g() {
        return GremlinFS.operations().g();
    }
    ro() {
        return GremlinFS.operations().ro();
    }
    a() {
        return GremlinFS.operations().a();
    }
    mq() {
        return GremlinFS.operations().mq();
    }
    mqevent(event) {
        return GremlinFS.operations().mqevent(event);
    }
    query(query, node = null, _default_ = null) {
        return this.utils().query(query, node, _default_);
    }
    eval(command, node = null, _default_ = null) {
        return this.utils().eval(command, node, _default_);
    }
    config(key = null, _default_ = null) {
        return GremlinFS.operations().config(key, _default_);
    }
    utils() {
        return GremlinFS.operations().utils();
    }
    enter(functioname, ...args) {
    }
    root() {
        var root;
        root = null;
        if (this.config("fs_root")) {
            root = GremlinFSVertex.load(this.config("fs_root"));
        }
        return root;
    }
    node() {
        return this._node;
    }
    parent() {
        return this._parent;
    }
    isFolder() {
        var _default_, node;
        _default_ = false;
        if ((this._type && (this._type === "folder"))) {
            _default_ = true;
        }
        if ((this._path === "atpath")) {
            node = this.node();
            if ((node && node.isFolder())) {
                return true;
            }
            return false;
        }
        return _default_;
    }
    isFile() {
        var _default_, node;
        _default_ = false;
        if ((this._type && (this._type === "file"))) {
            _default_ = true;
        }
        if ((this._path === "atpath")) {
            node = this.node();
            if ((node && node.isFile())) {
                return true;
            }
            return false;
        }
        return _default_;
    }
    isLink() {
        var _default_, node;
        _default_ = false;
        if ((this._type && (this._type === "link"))) {
            _default_ = true;
        }
        if ((this._path === "atpath")) {
            node = this.node();
            if ((node && node.isLink())) {
                return true;
            }
            return false;
        }
        return _default_;
    }
    isFound() {
        var _default_, node;
        _default_ = false;
        if (this._type) {
            _default_ = true;
        }
        if ((this._path === "atpath")) {
            node = this.node();
            if (node) {
                return true;
            }
            return false;
        } else {
            if ((this._path === "vertex")) {
                node = this.node();
                if (node) {
                    return true;
                }
                return false;
            } else {
                if ((this._path === "vertex_property")) {
                    node = GremlinFSUtils.found(this.node());
                    if (node.has(this._vertexproperty)) {
                        return true;
                    } else {
                        if (node.edge(this._vertexproperty, false)) {
                            return true;
                        }
                    }
                    return false;
                } else {
                    if ((this._path === "vertex_in_edge")) {
                        node = GremlinFSUtils.found(this.node());
                        if (node.edge(this._vertexedge, true)) {
                            return true;
                        }
                        return false;
                    } else {
                        if ((this._path === "vertex_out_edge")) {
                            node = GremlinFSUtils.found(this.node());
                            if (node.edge(this._vertexedge, false)) {
                                return true;
                            }
                            return false;
                        }
                    }
                }
            }
        }
        return _default_;
    }
    createFolder(mode = null) {
        var _default_, newfile, newfolder, newlabel, newname, newuuid, parent;
        if (this.isFound()) {
            throw new GremlinFSExistsError(this);
        }
        if (this.isFile()) {
            throw new GremlinFSIsFileError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            newname = GremlinFSVertex.infer("name", this._name);
            newlabel = GremlinFSVertex.infer("label", this._name, GremlinFS.operations().defaultFolderLabel());
            newlabel = GremlinFSVertex.label(newname, newlabel, "folder", GremlinFS.operations().defaultFolderLabel());
            newuuid = GremlinFSVertex.infer("uuid", this._name);
            parent = this.parent();
            if ((! newname)) {
                throw new GremlinFSNotExistsError(this);
            }
            parent = this.parent();
            newfolder = GremlinFSVertex.make(newname, newlabel, newuuid).createFolder(parent, mode);
            this.mqevent(new GremlinFSEvent({"event": "create_node", "node": newfolder}));
            return true;
        } else {
            if ((this._path === "vertex")) {
                newname = GremlinFSVertex.infer("name", this._name);
                newlabel = GremlinFSVertex.infer("label", this._name, "vertex");
                newlabel = GremlinFSVertex.label(newname, newlabel, "file", "vertex");
                newuuid = GremlinFSVertex.infer("uuid", this._name);
                parent = this.parent();
                if ((label !== "vertex")) {
                    if ((label !== newlabel)) {
                        throw new GremlinFSNotExistsError(this);
                    }
                }
                if ((! newname)) {
                    throw new GremlinFSNotExistsError(this);
                }
                if (GremlinFS.operations().isFolderLabel(newlabel)) {
                    newfolder = GremlinFSVertex.make(newname, newlabel, newuuid).createFolder(null, mode);
                    this.mqevent(new GremlinFSEvent({"event": "create_node", "node": newfolder}));
                } else {
                    newfile = GremlinFSVertex.make(newname, newlabel, newuuid).create(null, mode);
                    this.mqevent(new GremlinFSEvent({"event": "create_node", "node": newfile}));
                }
                return true;
            }
        }
        return _default_;
    }
    readFolder() {
        var _short_, entries, label, labels, node, nodeid, nodes, parent, root;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        entries = gfslist([]);
        if ((this._path === "root")) {
            entries.extend([this.config("vertex_folder")]);
            root = this.root();
            nodes = null;
            if (root) {
                nodes = root.readFolderEntries();
            } else {
                nodes = GremlinFSVertex.fromVs(new this.g().V().where(GremlinFS.operations().a().out(this.config("in_label")).count().is(0)));
            }
            if (nodes) {
                for (var node, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                    node = _pj_a[_pj_c];
                    nodeid = node.toid(true);
                    if (nodeid) {
                        entries.append(nodeid);
                    }
                }
            }
            return entries.tolist();
        } else {
            if ((this._path === "atpath")) {
                entries.extend([this.config("vertex_folder")]);
                parent = this.node();
                nodes = parent.readFolderEntries();
                if (nodes) {
                    for (var node, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                        node = _pj_a[_pj_c];
                        nodeid = node.toid(true);
                        if (nodeid) {
                            entries.append(nodeid);
                        }
                    }
                }
                return entries.tolist();
            } else {
                if ((this._path === "vertex_labels")) {
                    labels = new this.g().V().label().dedup();
                    if (labels) {
                        for (var label, _pj_c = 0, _pj_a = labels, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                            label = _pj_a[_pj_c];
                            if (label) {
                                entries.append(label);
                            }
                        }
                    }
                    return entries.tolist();
                } else {
                    if ((this._path === "vertexes")) {
                        label = this._vertexlabel;
                        if ((! label)) {
                            label = "vertex";
                        }
                        _short_ = false;
                        parent = this.parent();
                        nodes = null;
                        if (parent) {
                            _short_ = true;
                            if ((label === "vertex")) {
                                nodes = GremlinFSVertex.fromVs(new this.g().V(parent.get("id")).inE(this.config("in_label")).has("name", this.config("in_name")).outV());
                            } else {
                                nodes = GremlinFSVertex.fromVs(new this.g().V(parent.get("id")).inE(this.config("in_label")).has("name", this.config("in_name")).outV().hasLabel(label));
                            }
                        } else {
                            if ((label === "vertex")) {
                                nodes = GremlinFSVertex.fromVs(new this.g().V());
                            } else {
                                nodes = GremlinFSVertex.fromVs(new this.g().V().hasLabel(label));
                            }
                        }
                        nodes = GremlinFSUtils.found(nodes);
                        for (var node, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                            node = _pj_a[_pj_c];
                            nodeid = node.toid(_short_);
                            if (nodeid) {
                                entries.append(nodeid);
                            }
                        }
                        return entries.tolist();
                    } else {
                        if ((this._path === "vertex")) {
                            label = this._vertexlabel;
                            if ((! label)) {
                                label = "vertex";
                            }
                            node = GremlinFSUtils.found(this.node());
                            entries.extend(node.keys());
                            entries.extend([GremlinFS.operations().config("in_edge_folder", "EI"), GremlinFS.operations().config("out_edge_folder", "EO")]);
                            return entries.tolist();
                        } else {
                            if ((this._path === "vertex_in_edges")) {
                                label = this._vertexlabel;
                                if ((! label)) {
                                    label = "vertex";
                                }
                                node = GremlinFSUtils.found(this.node());
                                nodes = GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE());
                                if (nodes) {
                                    for (var cnode, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                                        cnode = _pj_a[_pj_c];
                                        if ((cnode.get("label") && cnode.get("name"))) {
                                            entries.append(((cnode.get("name") + "@") + cnode.get("label")));
                                        } else {
                                            if (cnode.get("label")) {
                                                entries.append(cnode.get("label"));
                                            }
                                        }
                                    }
                                }
                                return entries.tolist();
                            } else {
                                if ((this._path === "vertex_out_edges")) {
                                    label = this._vertexlabel;
                                    if ((! label)) {
                                        label = "vertex";
                                    }
                                    node = GremlinFSUtils.found(this.node());
                                    nodes = GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE());
                                    if (nodes) {
                                        for (var cnode, _pj_c = 0, _pj_a = nodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                                            cnode = _pj_a[_pj_c];
                                            if ((cnode.get("label") && cnode.get("name"))) {
                                                entries.append(((cnode.get("name") + "@") + cnode.get("label")));
                                            } else {
                                                if (cnode.get("label")) {
                                                    entries.append(cnode.get("label"));
                                                }
                                            }
                                        }
                                    }
                                    return entries.tolist();
                                }
                            }
                        }
                    }
                }
            }
        }
        return entries;
    }
    renameFolder(newmatch) {
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        return this.moveNode(newmatch);
    }
    deleteFolder() {
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        return this.deleteNode();
    }
    createFile(mode = null, data = null) {
        var _default_, newfile, newlabel, newname, newuuid, node, parent;
        if (this.isFound()) {
            throw new GremlinFSExistsError(this);
        }
        if (this.isFolder()) {
            throw new GremlinFSIsFolderError(this);
        }
        if ((! data)) {
            data = "";
        }
        _default_ = data;
        if (this._type) {
            _default_ = data;
        }
        if ((this._path === "atpath")) {
            newname = GremlinFSVertex.infer("name", this._name);
            newlabel = GremlinFSVertex.infer("label", this._name, "vertex");
            newlabel = GremlinFSVertex.label(newname, newlabel, "file", "vertex");
            newuuid = GremlinFSVertex.infer("uuid", this._name);
            parent = this.parent();
            if ((! newname)) {
                throw new GremlinFSNotExistsError(this);
            }
            newfile = GremlinFSVertex.make(newname, newlabel, newuuid).create(parent, mode);
            this.mqevent(new GremlinFSEvent({"event": "create_node", "node": newfile}));
            return true;
        } else {
            if ((this._path === "vertex_property")) {
                node = GremlinFSUtils.found(this.node());
                node.setProperty(this._vertexproperty, data);
                this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
                return true;
            }
        }
        return _default_;
    }
    readFile(size = 0, offset = 0) {
        var data;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        data = this.readNode(size, offset);
        if (data) {
            return data;
        }
        return null;
    }
    readFileLength() {
        var data;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        data = this.readNode();
        if (data) {
            try {
                return data.length;
            } catch(e) {
            }
        }
        return 0;
    }
    writeFile(data, offset = 0) {
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        return this.writeNode(data, offset);
    }
    clearFile() {
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        return this.clearNode();
    }
    renameFile(newmatch) {
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        return this.moveNode(newmatch);
    }
    deleteFile() {
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        return this.deleteNode();
    }
    createLink(sourcematch, mode = null) {
        var _default_, label, name, newlink, node, source, target;
        if (this.isFound()) {
            throw new GremlinFSExistsError(this);
        }
        if ((! sourcematch.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            return _default_;
        } else {
            if ((this._path === "vertex_in_edge")) {
                node = this.node();
                source = sourcematch.node();
                target = node;
                label = GremlinFSEdge.infer("label", this._vertexedge, null);
                name = GremlinFSEdge.infer("name", this._vertexedge, null);
                if (((! label) && name)) {
                    label = name;
                    name = null;
                }
                newlink = source.createLink({"target": target, "label": label, "name": name, "mode": mode});
                this.mqevent(new GremlinFSEvent({"event": "create_link", "link": newlink, "source": source, "target": target}));
                return true;
            } else {
                if ((this._path === "vertex_out_edge")) {
                    node = this.node();
                    source = node;
                    target = sourcematch.node();
                    label = GremlinFSEdge.infer("label", this._vertexedge, null);
                    name = GremlinFSEdge.infer("name", this._vertexedge, null);
                    if (((! label) && name)) {
                        label = name;
                        name = null;
                    }
                    newlink = source.createLink({"target": target, "label": label, "name": name, "mode": mode});
                    this.mqevent(new GremlinFSEvent({"event": "create_link", "link": newlink, "source": source, "target": target}));
                    return true;
                }
            }
        }
        return _default_;
    }
    readLink() {
        var _default_, edgenode, newpath, node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            return _default_;
        } else {
            if ((this._path === "vertex_in_edge")) {
                node = GremlinFSUtils.found(this.node());
                edgenode = node.edgenode(this._vertexedge, true, false);
                newpath = this.utils().nodelink(edgenode);
                return newpath;
            } else {
                if ((this._path === "vertex_out_edge")) {
                    node = GremlinFSUtils.found(this.node());
                    edgenode = node.edgenode(this._vertexedge, false, true);
                    newpath = this.utils().nodelink(edgenode);
                    return newpath;
                }
            }
        }
        return _default_;
    }
    deleteLink() {
        var _default_, label, link, name, node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            return _default_;
        } else {
            if ((this._path === "vertex_in_edge")) {
                node = this.node();
                label = GremlinFSEdge.infer("label", this._vertexedge, null);
                name = GremlinFSEdge.infer("name", this._vertexedge, null);
                if (((! label) && name)) {
                    label = name;
                    name = null;
                }
                if ((label && name)) {
                    link = node.getLink({"label": label, "name": name, "ine": true});
                    node.deleteLink({"label": label, "name": name, "ine": true});
                    this.mqevent(new GremlinFSEvent({"event": "delete_link", "link": link}));
                } else {
                    if (label) {
                        link = node.getLink({"label": label, "name": null, "ine": true});
                        node.deleteLink({"label": label, "name": null, "ine": true});
                        this.mqevent(new GremlinFSEvent({"event": "delete_link", "link": link}));
                    }
                }
                return true;
            } else {
                if ((this._path === "vertex_out_edge")) {
                    node = this.node();
                    label = GremlinFSEdge.infer("label", this._vertexedge, null);
                    name = GremlinFSEdge.infer("name", this._vertexedge, null);
                    if (((! label) && name)) {
                        label = name;
                        name = null;
                    }
                    if ((label && name)) {
                        link = node.getLink({"label": label, "name": name, "ine": false});
                        node.deleteLink({"label": label, "name": name, "ine": false});
                        this.mqevent(new GremlinFSEvent({"event": "delete_link", "link": link}));
                    } else {
                        if (label) {
                            link = node.getLink({"label": label, "name": null, "ine": false});
                            node.deleteLink({"label": label, "name": null, "ine": false});
                            this.mqevent(new GremlinFSEvent({"event": "delete_link", "link": link}));
                        }
                    }
                    return true;
                }
            }
        }
        return _default_;
    }
    readNode(size = 0, offset = 0) {
        var _default_, data, node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            node = this.node().file();
            data = node.render();
            if (data) {
                data = this.utils().tobytes(data);
            }
            if (((data && (size > 0)) && (offset > 0))) {
                return data.slice(offset, (offset + size));
            } else {
                if ((data && (offset > 0))) {
                    return data.slice(offset);
                } else {
                    if ((data && (size > 0))) {
                        return data.slice(0, size);
                    } else {
                        return data;
                    }
                }
            }
        } else {
            if ((this._path === "vertex_property")) {
                node = GremlinFSUtils.found(this.node());
                data = node.readProperty(this._vertexproperty, "");
                data = this.utils().tobytes(data);
                if (((size > 0) && (offset > 0))) {
                    return data.slice(offset, (offset + size));
                } else {
                    if ((offset > 0)) {
                        return data.slice(offset);
                    } else {
                        if ((size > 0)) {
                            return data.slice(0, size);
                        } else {
                            return data;
                        }
                    }
                }
            }
        }
        return _default_;
    }
    writeNode(data, offset = 0) {
        var _default_, _new_, label_config, node, old, writefn;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = data;
        if (this._type) {
            _default_ = data;
        }
        if ((this._path === "atpath")) {
            node = this.node().file();
            label_config = node.labelConfig();
            writefn = null;
            old = node.readProperty(this.config("data_property"), null, "base64");
            old = this.utils().tobytes(old);
            _new_ = GremlinFSUtils.irepl(old, data, offset);
            _new_ = this.utils().tostring(_new_);
            node.writeProperty(this.config("data_property"), _new_, "base64");
            this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
            try {
                if ((label_config && _pj.in_es6("writefn", label_config))) {
                    writefn = label_config["writefn"];
                }
            } catch(e) {
            }
            try {
                if (writefn) {
                    writefn({"node": node, "data": data});
                }
            } catch(e) {
            }
            return data;
        } else {
            if ((this._path === "vertex_property")) {
                node = GremlinFSUtils.found(this.node());
                old = node.readProperty(this._vertexproperty, null);
                old = this.utils().tobytes(old);
                _new_ = GremlinFSUtils.irepl(old, data, offset);
                _new_ = this.utils().tostring(_new_);
                node.writeProperty(this._vertexproperty, _new_);
                this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
                return data;
            }
        }
        return _default_;
    }
    clearNode() {
        var _default_, node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if ((this._path === "atpath")) {
            node = this.node().file();
            node.writeProperty(this.config("data_property"), "");
            this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
            return null;
        } else {
            if ((this._path === "vertex_property")) {
                node = GremlinFSUtils.found(this.node());
                node.writeProperty(this._vertexproperty, "");
                this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
                return null;
            }
        }
        return _default_;
    }
    moveNode(newmatch) {
        var _default_, data, newdata, newname, newnode, node, oldname, oldnode, parent;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            node = GremlinFSUtils.found(this.node());
            parent = newmatch.parent();
            node.rename(newmatch._name);
            node.move(parent);
            this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
            return true;
        } else {
            if ((this._path === "vertex_property")) {
                oldnode = GremlinFSUtils.found(this.node());
                oldname = this._vertexproperty;
                newnode = newmatch.node();
                newname = newmatch._vertexproperty;
                data = "";
                data = oldnode.readProperty(oldname, "");
                newnode.writeProperty(newname, data);
                this.mqevent(new GremlinFSEvent({"event": "update_node", "node": newnode}));
                newdata = newnode.readProperty(newname, "");
                if ((newdata === data)) {
                    oldnode.unsetProperty(oldname);
                    this.mqevent(new GremlinFSEvent({"event": "update_node", "node": oldnode}));
                }
                return true;
            }
        }
        return _default_;
    }
    deleteNode() {
        var _default_, node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        _default_ = null;
        if (this._type) {
            _default_ = null;
        }
        if ((this._path === "atpath")) {
            node = GremlinFSUtils.found(this.node());
            node.delete();
            this.mqevent(new GremlinFSEvent({"event": "delete_node", "node": node}));
            return true;
        } else {
            if ((this._path === "vertex_property")) {
                node = GremlinFSUtils.found(this.node());
                node.unsetProperty(this._vertexproperty);
                this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
                return true;
            }
        }
        return _default_;
    }
    setProperty(key, value) {
        var node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        if ((this._path === "atpath")) {
            node = this.node();
            if (node) {
                node.setProperty(key, value);
            }
            this.mqevent(new GremlinFSEvent({"event": "update_node", "node": node}));
        }
        return true;
    }
    getProperty(key, _default_ = null) {
        var node;
        if ((! this.isFound())) {
            throw new GremlinFSNotExistsError(this);
        }
        if ((this._path === "atpath")) {
            node = this.node();
            if (node) {
                return node.getProperty(key, _default_);
            }
        }
        return _default_;
    }
}
_pj.set_properties(GremlinFSPath, {"logger": GremlinFSLogger.getLogger("GremlinFSPath")});
class GremlinFSNode extends GremlinFSBase {
    static parse(id) {
        return null;
    }
    static infer(field, obj, _default_ = null) {
        var clazz, parts;
        clazz = this;
        parts = clazz.parse(obj);
        if ((! _pj.in_es6(field, parts))) {
            return _default_;
        }
        return parts.get(field, _default_);
    }
    static label(name, label, fstype = "file", _default_ = "vertex") {
        var compiled;
        if ((! name)) {
            return _default_;
        }
        if ((! label)) {
            return _default_;
        }
        for (var label_config, _pj_c = 0, _pj_a = GremlinFS.operations().config("labels", []), _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
            label_config = _pj_a[_pj_c];
            if ((_pj.in_es6("type", label_config) && (label_config["type"] === fstype))) {
                compiled = null;
                if (_pj.in_es6("compiled", label_config)) {
                    compiled = label_config["compiled"];
                } else {
                    compiled = GremlinFS.operations().utils().recompile(label_config["pattern"]);
                }
                if (compiled) {
                    if (compiled.search(name)) {
                        label = label_config.get("label", _default_);
                        break;
                    }
                }
            }
        }
        return label;
    }
    constructor(kwargs = {}) {
        super();
        this.setall(kwargs);
    }
    g() {
        return GremlinFS.operations().g();
    }
    ro() {
        return GremlinFS.operations().ro();
    }
    a() {
        return GremlinFS.operations().a();
    }
    mq() {
        return GremlinFS.operations().mq();
    }
    mqevent(event) {
        return GremlinFS.operations().mqevent(event);
    }
    query(query, node = null, _default_ = null) {
        return this.utils().query(query, node, _default_);
    }
    eval(command, node = null, _default_ = null) {
        return this.utils().eval(command, node, _default_);
    }
    config(key = null, _default_ = null) {
        return GremlinFS.operations().config(key, _default_);
    }
    utils() {
        return GremlinFS.operations().utils();
    }
    labelConfig() {
        var config, node;
        node = this;
        config = null;
        for (var label_config, _pj_c = 0, _pj_a = GremlinFS.operations().config("labels", []), _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
            label_config = _pj_a[_pj_c];
            if ((_pj.in_es6("label", label_config) && (label_config["label"] === node.get("label", null)))) {
                config = label_config;
            }
        }
        return config;
    }
    toid(_short_ = false) {
        var mapid, maplabel, mapname, mapuuid, node;
        node = this;
        mapid = node.get("id", null);
        mapuuid = node.get("uuid", null);
        maplabel = node.get("label", null);
        mapname = node.get("name", null);
        if ((((mapname && mapuuid) && maplabel) && (! _short_))) {
            if ((maplabel === "vertex")) {
                return ((mapname + "@") + mapuuid);
            } else {
                return ((((mapname + "@") + maplabel) + "@") + mapuuid);
            }
        } else {
            if (((mapname && maplabel) && _short_)) {
                return mapname;
            } else {
                if ((mapname && maplabel)) {
                    return mapname;
                } else {
                    if (mapname) {
                        return mapname;
                    } else {
                        if (mapuuid) {
                            return mapuuid;
                        }
                    }
                }
            }
        }
    }
    matches(inmap) {
        var mapid, maplabel, mapname, mapuuid, node;
        node = this;
        mapid = inmap.get("id", null);
        mapuuid = inmap.get("uuid", null);
        maplabel = inmap.get("label", null);
        mapname = inmap.get("name", null);
        if ((((mapname && (mapname === node.get("name", null))) && maplabel) && (maplabel === node.get("label", null)))) {
            return true;
        }
        return false;
    }
    hasProperty(name, prefix = null) {
        var data, node;
        node = this;
        if ((! node)) {
            return false;
        }
        data = node.has(name, prefix);
        if ((! data)) {
            return false;
        }
        return true;
    }
    getProperty(name, _default_ = null, encoding = null, prefix = null) {
        var data, node;
        node = this;
        if ((! node)) {
            return _default_;
        }
        data = node.get(name, null, prefix);
        if ((! data)) {
            return _default_;
        }
        if (encoding) {
            data = this.utils().tobytes(data);
            data = this.utils().decode(data, encoding);
            data = this.utils().tostring(data);
        }
        return data;
    }
    setProperty(name, data = null, encoding = null, prefix = null) {
        var node, nodeid;
        node = this;
        if ((! node)) {
            return;
        }
        if ((! data)) {
            data = "";
        }
        nodeid = node.get("id");
        if (encoding) {
            data = this.utils().tobytes(data);
            data = this.utils().encode(data, encoding);
            data = this.utils().tostring(data);
        }
        node.set(name, data, prefix);
        if (prefix) {
            name = ((prefix + ".") + name);
        }
        new this.g().V(nodeid).property(name, data).next();
        return data;
    }
    unsetProperty(name, prefix = null) {
        var node, nodeid;
        node = this;
        if ((! node)) {
            return;
        }
        nodeid = node.get("id");
        node.set(name, null, prefix);
        if (prefix) {
            name = ((prefix + ".") + name);
        }
        try {
            new this.g().V(nodeid).properties(name).drop().next();
        } catch(e) {
        }
    }
    setProperties(properties, prefix = null) {
        var existing, node, value;
        node = this;
        existing = gfsmap({});
        existing.update(node.all(prefix));
        if (existing) {
            var _pj_a = existing;
            for (var key in _pj_a) {
                if (_pj_a.hasOwnProperty(key)) {
                    value = existing[key];
                    if ((! _pj.in_es6(key, properties))) {
                        node.unsetProperty(key, prefix);
                    }
                }
            }
        }
        if (properties) {
            var _pj_a = properties;
            for (var key in _pj_a) {
                if (_pj_a.hasOwnProperty(key)) {
                    value = properties[key];
                    try {
                        node.setProperty(key, value, null, prefix);
                    } catch(e) {
                        this.logger.exception(" GremlinFS: setProperties exception ", e);
                    }
                }
            }
        }
    }
    getProperties(prefix = null) {
        var node, properties;
        node = this;
        properties = gfsmap({});
        properties.update(node.all(prefix));
        return properties.tomap();
    }
    readProperty(name, _default_ = null, encoding = null, prefix = null) {
        return this.getProperty(name, _default_, encoding, prefix);
    }
    writeProperty(name, data, encoding = null, prefix = null) {
        return this.setProperty(name, data, encoding, prefix);
    }
    invoke(handler, event, chain = [], data = {}) {
    }
    event(event, chain = [], data = {}, propagate = true) {
    }
}
_pj.set_properties(GremlinFSNode, {"logger": GremlinFSLogger.getLogger("GremlinFSNode")});
class GremlinFSVertex extends GremlinFSNode {
    static parse(id) {
        var matcher, nodelbl, nodenme, nodetpe, nodeuid;
        if ((! id)) {
            return {};
        }
        matcher = GremlinFS.operations().utils().rematch("^(.+)\\.(.+)\\@(.+)\\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$", id);
        if (matcher) {
            nodenme = matcher.group(1);
            nodetpe = matcher.group(2);
            nodelbl = matcher.group(3);
            nodeuid = matcher.group(4);
            return {"name": ((nodenme + ".") + nodetpe), "type": nodetpe, "label": nodelbl, "uuid": nodeuid};
        }
        matcher = GremlinFS.operations().utils().rematch("^(.+)\\@(.+)\\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$", id);
        if (matcher) {
            nodenme = matcher.group(1);
            nodelbl = matcher.group(2);
            nodeuid = matcher.group(3);
            return {"name": nodenme, "label": nodelbl, "uuid": nodeuid};
        }
        matcher = GremlinFS.operations().utils().rematch("^(.+)\\.(.+)\\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$", id);
        if (matcher) {
            nodenme = matcher.group(1);
            nodetpe = matcher.group(2);
            nodeuid = matcher.group(3);
            return {"name": ((nodenme + ".") + nodetpe), "type": nodetpe, "uuid": nodeuid};
        }
        matcher = GremlinFS.operations().utils().rematch("^(.+)\\@([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$", id);
        if (matcher) {
            nodenme = matcher.group(1);
            nodeuid = matcher.group(2);
            return {"name": nodenme, "uuid": nodeuid};
        }
        matcher = GremlinFS.operations().utils().rematch("^(.+)\\.(.+)$", id);
        if (matcher) {
            nodenme = matcher.group(1);
            nodetpe = matcher.group(2);
            return {"name": ((nodenme + ".") + nodetpe), "type": nodetpe};
        }
        matcher = GremlinFS.operations().utils().rematch("^([0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$", id);
        if (matcher) {
            nodeuid = matcher.group(1);
            return {"uuid": nodeuid};
        }
        return {"name": id};
    }
    static make(name, label, uuid = null) {
        return new GremlinFSVertex({"name": name, "label": label, "uuid": uuid});
    }
    static load(id) {
        var clazz, parts;
        clazz = this;
        parts = GremlinFSVertex.parse(id);
        if ((((parts && _pj.in_es6("uuid", parts)) && _pj.in_es6("name", parts)) && _pj.in_es6("label", parts))) {
            try {
                if ((parts["label"] === "vertex")) {
                    return GremlinFSVertex.fromV(new GremlinFS.operations().g().V().has("uuid", parts["uuid"]));
                } else {
                    return GremlinFSVertex.fromV(new GremlinFS.operations().g().V().hasLabel(parts["label"]).has("uuid", parts["uuid"]));
                }
            } catch(e) {
                return null;
            }
        } else {
            if (((parts && _pj.in_es6("uuid", parts)) && _pj.in_es6("label", parts))) {
                try {
                    if ((parts["label"] === "vertex")) {
                        return GremlinFSVertex.fromV(new GremlinFS.operations().g().V().has("uuid", parts["uuid"]));
                    } else {
                        return GremlinFSVertex.fromV(new GremlinFS.operations().g().V().hasLabel(parts["label"]).has("uuid", parts["uuid"]));
                    }
                } catch(e) {
                    return null;
                }
            } else {
                if ((parts && _pj.in_es6("uuid", parts))) {
                    try {
                        return GremlinFSVertex.fromV(new GremlinFS.operations().g().V().has("uuid", parts["uuid"]));
                    } catch(e) {
                        return null;
                    }
                } else {
                    if ((id && _pj.in_es6(":", id))) {
                        try {
                            return GremlinFSVertex.fromV(new GremlinFS.operations().g().V(id));
                        } catch(e) {
                            return null;
                        }
                    }
                }
            }
        }
        return null;
    }
    static fromMap(map) {
        var node;
        node = new GremlinFSVertex();
        node.fromobj(map);
        return node;
    }
    static fromMaps(maps) {
        var node, nodes;
        nodes = gfslist([]);
        for (var map, _pj_c = 0, _pj_a = maps, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
            map = _pj_a[_pj_c];
            node = new GremlinFSVertex();
            node.fromobj(map);
            nodes.append(node);
        }
        return nodes.tolist();
    }
    static fromV(v) {
        var clazz;
        clazz = this;
        return GremlinFSVertex.fromMap(v.valueMap(true).next());
    }
    static fromVs(vs) {
        var clazz;
        clazz = this;
        return GremlinFSVertex.fromMaps(vs.valueMap(true).toList());
    }
    edges(edgeid = null, ine = true) {
        var label, name, node;
        node = this;
        label = GremlinFSEdge.infer("label", edgeid, null);
        name = GremlinFSEdge.infer("name", edgeid, null);
        if (((! label) && name)) {
            label = name;
            name = null;
        }
        if (((node && label) && name)) {
            try {
                if (ine) {
                    return GremlinFSEdge.fromEs(new this.g().V(node.get("id")).inE(label).has("name", name));
                } else {
                    return GremlinFSEdge.fromEs(new this.g().V(node.get("id")).outE(label).has("name", name));
                }
            } catch(e) {
                return null;
            }
        } else {
            if ((node && label)) {
                try {
                    if (ine) {
                        return GremlinFSEdge.fromEs(new this.g().V(node.get("id")).inE(label));
                    } else {
                        return GremlinFSEdge.fromEs(new this.g().V(node.get("id")).outE(label));
                    }
                } catch(e) {
                    return null;
                }
            } else {
                if (node) {
                    try {
                        if (ine) {
                            return GremlinFSEdge.fromEs(new this.g().V(node.get("id")).inE());
                        } else {
                            return GremlinFSEdge.fromEs(new this.g().V(node.get("id")).outE());
                        }
                    } catch(e) {
                        return null;
                    }
                }
            }
        }
    }
    edge(edgeid, ine = true) {
        var label, name, node;
        node = this;
        label = GremlinFSEdge.infer("label", edgeid, null);
        name = GremlinFSEdge.infer("name", edgeid, null);
        if (((! label) && name)) {
            label = name;
            name = null;
        }
        if (((node && label) && name)) {
            try {
                if (ine) {
                    return GremlinFSEdge.fromE(new this.g().V(node.get("id")).inE(label).has("name", name));
                } else {
                    return GremlinFSEdge.fromE(new this.g().V(node.get("id")).outE(label).has("name", name));
                }
            } catch(e) {
                return null;
            }
        } else {
            if ((node && label)) {
                try {
                    if (ine) {
                        return GremlinFSEdge.fromE(new this.g().V(node.get("id")).inE(label));
                    } else {
                        return GremlinFSEdge.fromE(new this.g().V(node.get("id")).outE(label));
                    }
                } catch(e) {
                    return null;
                }
            }
        }
        return null;
    }
    edgenodes(edgeid = null, ine = true, inv = true) {
        var label, name, node;
        node = this;
        label = GremlinFSEdge.infer("label", edgeid, null);
        name = GremlinFSEdge.infer("name", edgeid, null);
        if (((! label) && name)) {
            label = name;
            name = null;
        }
        if (((node && label) && name)) {
            try {
                if (ine) {
                    if (inv) {
                        return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE(label).has("name", name).inV());
                    } else {
                        return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE(label).has("name", name).outV());
                    }
                } else {
                    if (inv) {
                        return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE(label).has("name", name).inV());
                    } else {
                        return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE(label).has("name", name).outV());
                    }
                }
            } catch(e) {
                return null;
            }
        } else {
            if ((node && label)) {
                try {
                    if (ine) {
                        if (inv) {
                            return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE(label).inV());
                        } else {
                            return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE(label).outV());
                        }
                    } else {
                        if (inv) {
                            return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE(label).inV());
                        } else {
                            return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE(label).outV());
                        }
                    }
                } catch(e) {
                    return null;
                }
            } else {
                if (node) {
                    try {
                        if (ine) {
                            if (inv) {
                                return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE().inV());
                            } else {
                                return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).inE().outV());
                            }
                        } else {
                            if (inv) {
                                return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE().inV());
                            } else {
                                return GremlinFSVertex.fromVs(new this.g().V(node.get("id")).outE().outV());
                            }
                        }
                    } catch(e) {
                        return null;
                    }
                }
            }
        }
    }
    edgenode(edgeid, ine = true, inv = true) {
        var label, name, node;
        node = this;
        label = GremlinFSEdge.infer("label", edgeid, null);
        name = GremlinFSEdge.infer("name", edgeid, null);
        if (((! label) && name)) {
            label = name;
            name = null;
        }
        if (((node && label) && name)) {
            try {
                if (ine) {
                    if (inv) {
                        return GremlinFSVertex.fromV(new this.g().V(node.get("id")).inE(label).has("name", name).inV());
                    } else {
                        return GremlinFSVertex.fromV(new this.g().V(node.get("id")).inE(label).has("name", name).outV());
                    }
                } else {
                    if (inv) {
                        return GremlinFSVertex.fromV(new this.g().V(node.get("id")).outE(label).has("name", name).inV());
                    } else {
                        return GremlinFSVertex.fromV(new this.g().V(node.get("id")).outE(label).has("name", name).outV());
                    }
                }
            } catch(e) {
                return null;
            }
        } else {
            if ((node && label)) {
                try {
                    if (ine) {
                        if (inv) {
                            return GremlinFSVertex.fromV(new this.g().V(node.get("id")).inE(label).inV());
                        } else {
                            return GremlinFSVertex.fromV(new this.g().V(node.get("id")).inE(label).outV());
                        }
                    } else {
                        if (inv) {
                            return GremlinFSVertex.fromV(new this.g().V(node.get("id")).outE(label).inV());
                        } else {
                            return GremlinFSVertex.fromV(new this.g().V(node.get("id")).outE(label).outV());
                        }
                    }
                } catch(e) {
                    return null;
                }
            }
        }
    }
    inbound(edgeid = null) {
        var nodes;
        nodes = this.edgenodes(edgeid, true, false);
        if ((! nodes)) {
            return [];
        }
        return nodes;
    }
    outbound(edgeid = null) {
        var nodes;
        nodes = this.edgenodes(edgeid, false, true);
        if ((! nodes)) {
            return [];
        }
        return nodes;
    }
    follow(edgeid) {
        return this.outbound(edgeid);
    }
    isFolder() {
        var node;
        node = this;
        if ((! node)) {
            return false;
        }
        if ((! GremlinFS.operations().isFolderLabel(node.get("label")))) {
            return false;
        }
        return true;
    }
    folder() {
        var node;
        node = this;
        if ((! node)) {
            throw new GremlinFSNotExistsError(this);
        }
        if ((! this.isFolder())) {
            throw new GremlinFSNotExistsError(this);
        }
        return node;
    }
    isFile() {
        var node;
        node = this;
        if ((! node)) {
            return false;
        }
        if ((! GremlinFS.operations().isFileLabel(node.get("label")))) {
            return false;
        }
        return true;
    }
    file() {
        var node;
        node = this;
        if ((! node)) {
            throw new GremlinFSNotExistsError(this);
        }
        if (this.isFolder()) {
            throw new GremlinFSIsFolderError(this);
        } else {
            if ((! this.isFile())) {
                throw new GremlinFSNotExistsError(this);
            }
        }
        return node;
    }
    create(parent = null, mode = null, owner = null, group = null, namespace = null) {
        var UUID, label, name, newnode, node, pathtime, pathuuid;
        node = this;
        UUID = node.get("uuid", null);
        label = node.get("label", null);
        name = node.get("name", null);
        if ((! name)) {
            return null;
        }
        if ((! mode)) {
            mode = GremlinFS.operations().config("default_mode", 420);
        }
        if ((! owner)) {
            owner = GremlinFS.operations().config("default_uid", 0);
        }
        if ((! group)) {
            group = GremlinFS.operations().config("default_gid", 0);
        }
        if ((! namespace)) {
            namespace = GremlinFS.operations().config("fs_ns");
        }
        newnode = null;
        try {
            pathuuid = this.utils().genuuid(UUID);
            pathtime = this.utils().gentime();
            newnode = null;
            if (label) {
                newnode = this.g().addV(label);
            } else {
                newnode = this.g().addV();
            }
            newnode.property("name", name).property("uuid", pathuuid.toString()).property("namespace", namespace).property("created", Number.parseInt(pathtime)).property("modified", Number.parseInt(pathtime)).property("mode", mode).property("owner", owner).property("group", group);
            if (parent) {
                newnode.addE(this.config("in_label")).property("name", this.config("in_name")).property("uuid", this.utils().genuuid().toString()).to(new GremlinFS.operations().a().V(parent.get("id"))).next();
            } else {
                newnode.next();
            }
            newnode = GremlinFSVertex.fromV(new this.g().V().has("uuid", pathuuid.toString()));
        } catch(e) {
            this.logger.exception(" GremlinFS: create exception ", e);
            return null;
        }
        return newnode;
    }
    rename(name) {
        var newnode, node;
        node = this;
        if ((! node)) {
            return null;
        }
        newnode = null;
        if (name) {
            try {
                newnode = GremlinFSVertex.fromV(new this.g().V(node.get("id")).property("name", name));
            } catch(e) {
                this.logger.exception(" GremlinFS: rename exception ", e);
                return null;
            }
        }
        try {
            newnode = GremlinFSVertex.fromV(new this.g().V(node.get("id")));
        } catch(e) {
            this.logger.exception(" GremlinFS: rename exception ", e);
            return null;
        }
        return newnode;
    }
    move(parent = null) {
        var newnode, node;
        node = this;
        if ((! node)) {
            return null;
        }
        newnode = null;
        try {
            newnode = GremlinFSVertex.fromV(new this.g().V(node.get("id")).outE(this.config("in_label")).has("name", this.config("in_name")).drop());
        } catch(e) {
        }
        if (parent) {
            try {
                newnode = GremlinFSVertex.fromV(new this.g().V(node.get("id")).addE(this.config("in_label")).property("name", this.config("in_name")).property("uuid", this.utils().genuuid().toString()).to(new GremlinFS.operations().a().V(parent.get("id"))));
            } catch(e) {
                this.logger.exception(" GremlinFS: move exception ", e);
                return null;
            }
        }
        try {
            newnode = GremlinFSVertex.fromV(new this.g().V(node.get("id")));
        } catch(e) {
            this.logger.exception(" GremlinFS: move exception ", e);
            return null;
        }
        return newnode;
    }
    delete() {
        var node;
        node = this;
        if ((! node)) {
            return null;
        }
        try {
            new this.g().V(node.get("id")).drop().next();
        } catch(e) {
            return false;
        }
        return true;
    }
    render() {
        var data, found, haslabel, label_config, node, ps, readfn, template, templatectx, templatectxi, templatenodes, v2id, vs, vs2;
        node = this;
        data = "";
        label_config = node.labelConfig();
        template = null;
        readfn = null;
        data = node.readProperty(this.config("data_property"), "", "base64");
        try {
            templatenodes = node.follow(this.config("template_label"));
            if ((templatenodes && (templatenodes.length >= 1))) {
                template = templatenodes[0].readProperty(this.config("data_property"), "", "base64");
            } else {
                if (node.hasProperty(this.config("template_property"))) {
                    template = node.getProperty(this.config("template_property"), "");
                } else {
                    if ((label_config && _pj.in_es6("template", label_config))) {
                        template = label_config["template"];
                    } else {
                        if ((label_config && _pj.in_es6("readfn", label_config))) {
                            readfn = label_config["readfn"];
                        }
                    }
                }
            }
        } catch(e) {
            this.logger.exception(" GremlinFS: readNode template exception ", e);
        }
        try {
            ps = new GremlinFS.operations().g().V(node.get("id")).emit().repeat(GremlinFS.operations().a().inE().outV()).until(GremlinFS.operations().a().inE().count().is(0).or().loops().is(P.gt(10))).path().toList();
            vs = GremlinFSVertex.fromVs(new GremlinFS.operations().g().V(node.get("id")).emit().repeat(GremlinFS.operations().a().inE().outV()).until(GremlinFS.operations().a().inE().count().is(0).or().loops().is(P.gt(10))));
            vs2 = gfsmap({});
            for (var v, _pj_c = 0, _pj_a = vs, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                v = _pj_a[_pj_c];
                vs2.set(v.get("id"), v);
            }
            templatectx = gfsmap(vs2.get(node.get("id")).all());
            templatectxi = templatectx;
            for (var v, _pj_c = 0, _pj_a = ps, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                v = _pj_a[_pj_c];
                templatectxi = templatectx;
                haslabel = false;
                for (var v2, _pj_f = 0, _pj_d = v.objects, _pj_e = _pj_d.length; (_pj_f < _pj_e); _pj_f += 1) {
                    v2 = _pj_d[_pj_f];
                    v2id = v2.id["@value"];
                    if ((v2 instanceof Vertex)) {
                        if (haslabel) {
                            found = null;
                            for (var ctemplatectxi, _pj_i = 0, _pj_g = templatectxi.all(), _pj_h = _pj_g.length; (_pj_i < _pj_h); _pj_i += 1) {
                                ctemplatectxi = _pj_g[_pj_i];
                                if ((ctemplatectxi.get("id") === v2id)) {
                                    found = ctemplatectxi;
                                }
                            }
                            if (found) {
                                templatectxi = found;
                            } else {
                                templatectxi.append(gfsmap(vs2.get(v2id).all()));
                                templatectxi = templatectxi.all().slice((- 1))[0];
                            }
                        }
                    } else {
                        if ((v2 instanceof Edge)) {
                            haslabel = true;
                            if (templatectxi.has(v2.label)) {
                            } else {
                                templatectxi.set(v2.label, gfslist([]));
                            }
                            templatectxi = templatectxi.get(v2.label);
                        }
                    }
                }
            }
            if (template) {
                data = this.utils().render(template, templatectx.tomap());
            } else {
                if (readfn) {
                    data = readfn(node, templatectx.tomap(), data);
                }
            }
        } catch(e) {
            this.logger.exception(" GremlinFS: readNode render exception ", e);
        }
        return data;
    }
    createFolder(parent = null, mode = null, owner = null, group = null, namespace = null) {
        var UUID, label, name, newfolder, node;
        node = this;
        UUID = node.get("uuid", null);
        label = node.get("label", null);
        name = node.get("name", null);
        if ((! name)) {
            return null;
        }
        if ((! label)) {
            label = GremlinFS.operations().defaultFolderLabel();
        }
        if ((! mode)) {
            mode = GremlinFS.operations().config("default_mode", 420);
        }
        if ((! owner)) {
            owner = GremlinFS.operations().config("default_uid", 0);
        }
        if ((! group)) {
            group = GremlinFS.operations().config("default_gid", 0);
        }
        if ((! namespace)) {
            namespace = GremlinFS.operations().config("fs_ns");
        }
        newfolder = this.create(parent, mode, owner, group, namespace);
        try {
            GremlinFSVertex.fromV(new this.g().V(newfolder.get("id")).property("type", this.config("folder_label")).property("in_label", this.config("in_label")).property("in_name", this.config("in_name")).property("query", (((((((("g.V('" + newfolder.get("id").toString()) + "').has('uuid', '") + newfolder.get("uuid").toString()) + "').has('type', '") + "group") + "').inE('") + this.config("in_label")) + "').outV()")));
            GremlinFSVertex.fromV(new this.g().V(newfolder.get("id")).addE(this.config("self_label")).property("name", this.config("self_name")).property("uuid", this.utils().genuuid().toString()).to(new GremlinFS.operations().a().V(newfolder.get("id"))));
        } catch(e) {
            this.logger.exception(" GremlinFS: createFolder exception ", e);
            return null;
        }
        return newfolder;
    }
    createLink(target, label, name = null, mode = null, owner = null, group = null) {
        var newedge, source;
        source = this;
        if ((! source)) {
            return null;
        }
        if ((! target)) {
            return null;
        }
        if ((! label)) {
            return null;
        }
        if ((! mode)) {
            mode = GremlinFS.operations().config("default_mode", 420);
        }
        if ((! owner)) {
            owner = GremlinFS.operations().config("default_uid", 0);
        }
        if ((! group)) {
            group = GremlinFS.operations().config("default_gid", 0);
        }
        newedge = null;
        try {
            if (name) {
                newedge = GremlinFSEdge.fromE(new this.g().V(source.get("id")).addE(label).property("name", name).property("uuid", this.utils().genuuid().toString()).to(new GremlinFS.operations().a().V(target.get("id"))));
            } else {
                newedge = GremlinFSEdge.fromE(new this.g().V(source.get("id")).addE(label).property("uuid", this.utils().genuuid().toString()).to(new GremlinFS.operations().a().V(target.get("id"))));
            }
        } catch(e) {
            this.logger.exception(" GremlinFS: createLink exception ", e);
            return null;
        }
        return newedge;
    }
    getLink(label, name = null, ine = true) {
        var node;
        node = this;
        if ((! node)) {
            return null;
        }
        if ((! label)) {
            return null;
        }
        try {
            if (name) {
                if (ine) {
                    return GremlinFSEdge.fromE(new this.g().V(node.get("id")).inE(label).has("name", name));
                } else {
                    return GremlinFSEdge.fromE(new this.g().V(node.get("id")).outE(label).has("name", name));
                }
            } else {
                if (ine) {
                    return GremlinFSEdge.fromE(new this.g().V(node.get("id")).inE(label));
                } else {
                    return GremlinFSEdge.fromE(new this.g().V(node.get("id")).outE(label));
                }
            }
        } catch(e) {
        }
        return null;
    }
    deleteLink(label, name = null, ine = true) {
        var newedge, node;
        node = this;
        if ((! node)) {
            return null;
        }
        if ((! label)) {
            return null;
        }
        newedge = null;
        try {
            if (name) {
                if (ine) {
                    newedge = GremlinFSEdge.fromE(new this.g().V(node.get("id")).inE(label).has("name", name).drop());
                } else {
                    newedge = GremlinFSEdge.fromE(new this.g().V(node.get("id")).outE(label).has("name", name).drop());
                }
            } else {
                if (ine) {
                    newedge = GremlinFSEdge.fromE(new this.g().V(node.get("id")).inE(label).drop());
                } else {
                    newedge = GremlinFSEdge.fromE(new this.g().V(node.get("id")).outE(label).drop());
                }
            }
        } catch(e) {
        }
        return true;
    }
    parent() {
        var node;
        node = this;
        try {
            return GremlinFSVertex.fromMap(new this.g().V(node.get("id")).outE(this.config("in_label")).inV().valueMap(true).next());
        } catch(e) {
            return null;
        }
    }
    parents(_list_ = []) {
        var node, parent;
        node = this;
        if ((! _list_)) {
            _list_ = gfslist([]);
        } else {
            _list_ = gfslist(_list_);
        }
        parent = node.parent();
        if (((parent && parent.get("id")) && (parent.get("id") !== node.get("id")))) {
            _list_.append(parent);
            return parent.parents(_list_.tolist());
        }
        return _list_.tolist();
    }
    path() {
        return function () {
    var _pj_a = [], _pj_b = reversed(this.parents([this]));
    for (var _pj_c = 0, _pj_d = _pj_b.length; (_pj_c < _pj_d); _pj_c += 1) {
        var ele = _pj_b[_pj_c];
        _pj_a.push(ele);
    }
    return _pj_a;
}
.call(this);
    }
    children() {
        var node;
        node = this;
        if ((! node)) {
            return GremlinFSVertex.fromMaps(new this.g().V().where(GremlinFS.operations().a().out(this.config("in_label")).count().is(0)).valueMap(true).toList());
        } else {
            return GremlinFSVertex.fromMaps(new this.g().V(node.get("id")).inE(this.config("in_label")).outV().valueMap(true).toList());
        }
        return [];
    }
    readFolderEntries() {
        return this.children();
    }
}
_pj.set_properties(GremlinFSVertex, {"logger": GremlinFSLogger.getLogger("GremlinFSVertex")});
class GremlinFSEdge extends GremlinFSNode {
    static parse(id) {
        var matcher, nodelbl, nodenme;
        if ((! id)) {
            return {};
        }
        matcher = GremlinFS.operations().utils().rematch("^(.+)\\@(.+)$", id);
        if (matcher) {
            nodenme = matcher.group(1);
            nodelbl = matcher.group(2);
            return {"name": nodenme, "label": nodelbl};
        }
        return {"label": id};
    }
    static make(name, label, uuid = null) {
        return new GremlinFSEdge({"name": name, "label": label, "uuid": uuid});
    }
    static load(id) {
        var clazz, parts;
        clazz = this;
        parts = GremlinFSEdge.parse(id);
        if ((((parts && _pj.in_es6("uuid", parts)) && _pj.in_es6("name", parts)) && _pj.in_es6("label", parts))) {
            try {
                if ((parts["label"] === "vertex")) {
                    return GremlinFSEdge.fromE(new GremlinFS.operations().g().E().has("uuid", parts["uuid"]));
                } else {
                    return GremlinFSEdge.fromE(new GremlinFS.operations().g().E().hasLabel(parts["label"]).has("uuid", parts["uuid"]));
                }
            } catch(e) {
                return null;
            }
        } else {
            if (((parts && _pj.in_es6("uuid", parts)) && _pj.in_es6("label", parts))) {
                try {
                    if ((parts["label"] === "vertex")) {
                        return GremlinFSEdge.fromE(new GremlinFS.operations().g().E().has("uuid", parts["uuid"]));
                    } else {
                        return GremlinFSEdge.fromE(new GremlinFS.operations().g().E().hasLabel(parts["label"]).has("uuid", parts["uuid"]));
                    }
                } catch(e) {
                    return null;
                }
            } else {
                if ((parts && _pj.in_es6("uuid", parts))) {
                    try {
                        return GremlinFSEdge.fromE(new GremlinFS.operations().g().E().has("uuid", parts["uuid"]));
                    } catch(e) {
                        return null;
                    }
                } else {
                    if ((id && _pj.in_es6(":", id))) {
                        try {
                            return GremlinFSEdge.fromE(new GremlinFS.operations().g().E(id));
                        } catch(e) {
                            return null;
                        }
                    }
                }
            }
        }
        return null;
    }
    static fromMap(map) {
        var node;
        node = new GremlinFSEdge();
        node.fromobj(map);
        return node;
    }
    static fromMaps(maps) {
        var node, nodes;
        nodes = gfslist([]);
        for (var map, _pj_c = 0, _pj_a = maps, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
            map = _pj_a[_pj_c];
            node = new GremlinFSEdge();
            node.fromobj(map);
            nodes.append(node);
        }
        return nodes.tolist();
    }
    static fromE(e) {
        var clazz;
        clazz = this;
        return GremlinFSEdge.fromMap(e.valueMap(true).next());
    }
    static fromEs(es) {
        var clazz;
        clazz = this;
        return GremlinFSEdge.fromMaps(es.valueMap(true).toList());
    }
    node(inv = true) {
        var edge;
        edge = this;
        if (edge) {
            try {
                if (inv) {
                    return GremlinFSVertex.fromV(new this.g().E(edge.get("id")).inV());
                } else {
                    return GremlinFSVertex.fromV(new this.g().E(edge.get("id")).outV());
                }
            } catch(e) {
                return null;
            }
        }
    }
    delete() {
        var node;
        node = this;
        if ((! node)) {
            return null;
        }
        try {
            new this.g().E(node.get("id")).drop().next();
        } catch(e) {
            return false;
        }
        return true;
    }
}
_pj.set_properties(GremlinFSEdge, {"logger": GremlinFSLogger.getLogger("GremlinFSEdge")});
class GremlinFSNodeWrapper extends GremlinFSBase {
    constructor(node) {
        super();
        this.node = node;
    }
    __getattr__(attr) {
        var data, edgenodes, node, ret;
        node = this.node;
        try {
            data = null;
            if (((attr === "content") || (attr === "contents"))) {
                data = node.render();
            }
            if (data) {
                return data;
            }
            edgenodes = null;
            if ((attr === "inbound")) {
                edgenodes = node.inbound();
            } else {
                if ((attr === "outbound")) {
                    edgenodes = node.outbound();
                } else {
                    if ((attr && attr.startswith("inbound__"))) {
                        edgenodes = node.inbound(attr.replace("inbound__", ""));
                    } else {
                        if ((attr && attr.startswith("outbound__"))) {
                            edgenodes = node.outbound(attr.replace("outbound__", ""));
                        } else {
                            edgenodes = node.outbound(attr);
                        }
                    }
                }
            }
            if (edgenodes) {
                if ((edgenodes.length > 1)) {
                    ret = gfslist([]);
                    for (var edgenode, _pj_c = 0, _pj_a = edgenodes, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                        edgenode = _pj_a[_pj_c];
                        ret.append(new GremlinFSNodeWrapper(edgenode));
                    }
                    return ret.tolist();
                } else {
                    if ((edgenodes.length === 1)) {
                        return new GremlinFSNodeWrapper(edgenodes[0]);
                    }
                }
            }
        } catch(e) {
        }
        return this.get(attr);
    }
    all(prefix = null) {
        var datasources, dsprefix, err, existing, log, node, props, ret, value;
        node = this.node;
        dsprefix = "ds";
        if (prefix) {
            dsprefix = ("ds." + prefix);
        }
        existing = gfsmap({});
        existing.update(node.all(prefix));
        datasources = gfsmap({});
        datasources.update(node.all(dsprefix));
        props = gfsmap({});
        var _pj_a = existing;
        for (var key in _pj_a) {
            if (_pj_a.hasOwnProperty(key)) {
                if ((key && (! key.startswith("ds.")))) {
                    try {
                        if (_pj.in_es6(key, datasources)) {
                            [ret, log, err] = GremlinFS.operations().eval(datasources.get(key), this);
                            if (ret) {
                                props[key] = ret.toString();
                            }
                        } else {
                            if (_pj.in_es6(key, existing)) {
                                value = existing.get(key);
                                if (value) {
                                    props[key] = value.toString();
                                }
                            }
                        }
                    } catch(e) {
                        this.logger.exception(" GremlinFS: all exception ", e);
                    }
                }
            }
        }
        return props.tomap();
    }
    keys(prefix = null) {
        return this.all(prefix).keys();
    }
    has(key, prefix = null) {
    }
    set(key, value, prefix = null) {
    }
    get(key, _default_ = null, prefix = null) {
        var datasource, dsprefix, err, existing, log, node, prop, ret;
        node = this.node;
        key = key.replace("__", ".");
        dsprefix = "ds";
        if (prefix) {
            dsprefix = ("ds." + prefix);
        }
        existing = null;
        if (node.has(key, prefix)) {
            existing = node.get(key, _default_, prefix);
        }
        datasource = null;
        if (node.has(key, dsprefix)) {
            datasource = node.get(key, _default_, dsprefix);
        }
        prop = null;
        if (datasource) {
            try {
                [ret, log, err] = GremlinFS.operations().eval(datasource, this);
                if (ret) {
                    prop = ret.toString();
                }
            } catch(e) {
                this.logger.exception(" GremlinFS: get exception ", e);
            }
        } else {
            prop = existing;
        }
        return prop;
    }
    property(name, _default_ = null, prefix = null) {
    }
}
_pj.set_properties(GremlinFSNodeWrapper, {"logger": GremlinFSLogger.getLogger("GremlinFSNodeWrapper")});
class GremlinFSUtils extends GremlinFSBase {
    static missing(value) {
        if (value) {
            throw new GremlinFSExistsError();
        }
    }
    static found(value) {
        if ((! value)) {
            throw new GremlinFSNotExistsError();
        }
        return value;
    }
    static irepl(old, data, index = 0) {
        var _new_, infix, linfix, lprefix, lsuffix, offset, prefix, suffix;
        offset = index;
        if ((! old)) {
            if ((data && (index === 0))) {
                return data;
            }
            return null;
        }
        if ((! data)) {
            return old;
        }
        if ((index < 0)) {
            return old;
        }
        if ((offset > old.length)) {
            return old;
        }
        _new_ = "";
        prefix = "";
        lprefix = 0;
        infix = data;
        linfix = data.length;
        suffix = null;
        lsuffix = 0;
        if (((offset > 0) && (offset <= old.length))) {
            prefix = old.slice(0, offset);
            lprefix = prefix.length;
        }
        if ((old.length > (lprefix + linfix))) {
            suffix = old.slice((lprefix + linfix));
            lsuffix = old.length;
        }
        if ((((lprefix > 0) && (linfix > 0)) && (lsuffix > 0))) {
            _new_ = ((prefix + infix) + suffix);
        } else {
            if (((lprefix > 0) && (linfix > 0))) {
                _new_ = (prefix + infix);
            } else {
                if (((linfix > 0) && (lsuffix > 0))) {
                    _new_ = (infix + suffix);
                } else {
                    _new_ = infix;
                }
            }
        }
        return _new_;
    }
    static link(path) {
    }
    static utils() {
        return new GremlinFSUtils();
    }
    constructor(kwargs = {}) {
        super();
        this.setall(kwargs);
    }
    g() {
        return GremlinFS.operations().g();
    }
    ro() {
        return GremlinFS.operations().ro();
    }
    a() {
        return GremlinFS.operations().a();
    }
    mq() {
        return GremlinFS.operations().mq();
    }
    mqevent(event) {
        return GremlinFS.operations().mqevent(event);
    }
    query(query, node = null, _default_ = null) {
    }
    eval(command, node = null, _default_ = null) {
    }
    config(key = null, _default_ = null) {
        return GremlinFS.operations().config(key, _default_);
    }
    nodelink(node, path = null) {
        var nodename, nodepath;
        nodepath = "";
        if (node) {
            path = node.path();
            if (path) {
                for (var node, _pj_c = 0, _pj_a = path, _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                    node = _pj_a[_pj_c];
                    nodename = node.get("name", null);
                    nodepath += ("/" + nodename);
                }
            }
        }
        return this.linkpath(nodepath);
    }
    linkpath(path) {
        if ((! path)) {
            return null;
        }
        return (this.config("mount_point") + path);
    }
    tobytes(data) {
        return data;
    }
    tostring(data) {
        return data;
    }
    decode(data, encoding = "base64") {
        return data;
    }
    encode(data, encoding = "base64") {
        return data;
    }
    render(template, templatectx) {
    }
    splitpath(path) {
        var elems;
        if ((path === "/")) {
            return null;
        } else {
            if ((! _pj.in_es6("/", path))) {
                return [path];
            }
        }
        elems = path.split("/");
        if (((elems[0] === "") && (elems.length > 1))) {
            return elems.slice(1);
        }
        return elems;
    }
    rematch(pattern, data) {
    }
    recompile(pattern) {
    }
    genuuid(UUID = null) {
    }
    gentime() {
    }
}
_pj.set_properties(GremlinFSUtils, {"logger": GremlinFSLogger.getLogger("GremlinFSUtils")});
class GremlinFSEvent extends GremlinFSBase {
    constructor(kwargs = {}) {
        super();
        this.setall(kwargs);
    }
    toJSON() {
        var data;
        data = {"event": this.get("event")};
        if ((this.has("node") && this.get("node"))) {
            data["node"] = this.get("node").all();
        }
        if ((this.has("link") && this.get("link"))) {
            data["link"] = this.get("link").all();
        }
        if ((this.has("source") && this.get("source"))) {
            data["source"] = this.get("source").all();
        }
        if ((this.has("target") && this.get("target"))) {
            data["target"] = this.get("target").all();
        }
        return data;
    }
}
_pj.set_properties(GremlinFSEvent, {"logger": GremlinFSLogger.getLogger("GremlinFSEvent")});
class GremlinFSConfig extends GremlinFSBase {
    static defaults() {
        return {"mount_point": null, "gremlin_host": null, "gremlin_port": null, "gremlin_username": null, "gremlin_url": null, "rabbitmq_host": null, "rabbitmq_port": null, "rabbitmq_password": null, "mq_exchange": "gfs-exchange", "mq_queue": "gfs-queue", "log_level": GremlinFSLogger.getLogLevel(), "fs_ns": "gfs1", "fs_root": null, "fs_root_init": false, "folder_label": "group", "ref_label": "ref", "in_label": "in", "self_label": "self", "template_label": "template", "in_name": "in0", "self_name": "self0", "vertex_folder": ".V", "edge_folder": ".E", "in_edge_folder": "IN", "out_edge_folder": "OUT", "uuid_property": "uuid", "name_property": "name", "data_property": "data", "template_property": "template", "default_uid": 1001, "default_gid": 1001, "default_mode": 511, "labels": []};
    }
    constructor(kwargs = {}) {
        super();
        this.setall(GremlinFSConfig.defaults());
        this.setall(kwargs);
        if (this.has("labels")) {
            for (var label_config, _pj_c = 0, _pj_a = this.get("labels"), _pj_b = _pj_a.length; (_pj_c < _pj_b); _pj_c += 1) {
                label_config = _pj_a[_pj_c];
                if (_pj.in_es6("pattern", label_config)) {
                    try {
                        label_config["compiled"] = re.compile(label_config["pattern"]);
                    } catch(e) {
                        this.logger.exception((" GremlinFS: failed to compile pattern " + label_config["pattern"]), e);
                    }
                }
            }
        }
    }
}
_pj.set_properties(GremlinFSConfig, {"logger": GremlinFSLogger.getLogger("GremlinFSConfig")});
class GremlinFS {
    /*
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a GremlinFSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    */
    static instance(instance = null) {
        if (instance) {
            GremlinFS.__instance = instance;
        }
        return GremlinFS.__instance;
    }
    static operations() {
        return GremlinFS.__instance;
    }
    constructor(kwargs = {}) {
        this._g = null;
        this._ro = null;
        this._mq = null;
        this._config = null;
    }
    configure(mount_point, gremlin_host, gremlin_port, gremlin_username, gremlin_password, rabbitmq_host = null, rabbitmq_port = null, rabbitmq_username = null, rabbitmq_password = null, kwargs = {}) {
        this.mount_point = mount_point;
        this.logger.info((" GremlinFS mount point: " + this.mount_point));
        this.gremlin_host = gremlin_host;
        this.gremlin_port = gremlin_port;
        this.gremlin_username = gremlin_username;
        this.gremlin_password = gremlin_password;
        this.gremlin_url = (((("ws://" + this.gremlin_host) + ":") + this.gremlin_port) + "/gremlin");
        this.logger.info((" GremlinFS gremlin host: " + this.gremlin_host));
        this.logger.info((" GremlinFS gremlin port: " + this.gremlin_port));
        this.logger.info((" GremlinFS gremlin username: " + this.gremlin_username));
        this.logger.info((" GremlinFS gremlin URL: " + this.gremlin_url));
        this.rabbitmq_host = rabbitmq_host;
        this.rabbitmq_port = rabbitmq_port;
        this.rabbitmq_username = rabbitmq_username;
        this.rabbitmq_password = rabbitmq_password;
        this.logger.info((" GremlinFS rabbitmq host: " + this.rabbitmq_host));
        this.logger.info((" GremlinFS rabbitmq port: " + this.rabbitmq_port));
        this.logger.info((" GremlinFS rabbitmq username: " + this.rabbitmq_username));
        this._g = null;
        this._ro = null;
        this._mq = null;
        this._config = new GremlinFSConfig({"mount_point": mount_point, "gremlin_host": gremlin_host, "gremlin_port": gremlin_port, "gremlin_username": gremlin_username, "gremlin_password": gremlin_password, "rabbitmq_host": rabbitmq_host, "rabbitmq_port": rabbitmq_port, "rabbitmq_username": rabbitmq_username, "rabbitmq_password": rabbitmq_password});
        this._utils = new GremlinFSUtils();
        return this;
    }
    connection(ro = false) {
    }
    mqconnection() {
    }
    mqchannel() {
    }
    g() {
    }
    ro() {
    }
    a() {
    }
    mq() {
    }
    mqevent(event) {
    }
    mqonevent(node, event, chain = [], data = {}, propagate = true) {
    }
    mqonmessage(ch, method, properties, body) {
    }
    query(query, node = null, _default_ = null) {
    }
    eval(command, node = null, _default_ = null) {
    }
    config(key = null, _default_ = null) {
        return this._config.get(key, _default_);
    }
    utils() {
        return this._utils;
    }
    getfs(fsroot, fsinit = false) {
        return fsroot;
    }
    defaultLabel() {
        return "vertex";
    }
    defaultFolderLabel() {
        return this.config("folder_label");
    }
    isFileLabel(label) {
        if (this.isFolderLabel(label)) {
            return false;
        }
        return true;
    }
    isFolderLabel(label) {
        if ((label === this.defaultFolderLabel())) {
            return true;
        }
        return false;
    }
}
_pj.set_properties(GremlinFS, {"__instance": null, "logger": GremlinFSLogger.getLogger("GremlinFS")});
export {GremlinFSBase, GremlinFSPath, GremlinFSNode, GremlinFSVertex, GremlinFSEdge, GremlinFSNodeWrapper, GremlinFSUtils, GremlinFSEvent, GremlinFSConfig, GremlinFS};
export default GremlinFS;

//# sourceMappingURL=gremlinfslib.js.map
