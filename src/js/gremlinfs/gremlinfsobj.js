class GremlinFSObj {
    constructor(kwargs = {}) {
        this.setall(kwargs);
    }
    fromobj(obj) {
    }
    setall(_dict_ = {}, prefix = null) {
        var value;
        var _pj_a = _dict_;
        for (var key in _pj_a) {
            if (_pj_a.hasOwnProperty(key)) {
                value = _dict_[key];
                this.set(key, value, prefix);
            }
        }
    }
    getall(prefix = null) {
    }
    all(prefix = null) {
        return this.getall(prefix);
    }
    keys(prefix = null) {
        return this.getall(prefix).keys();
    }
    has(key, prefix = null) {
        if (prefix) {
            key = ((prefix + ".") + key);
        }
        if ((("_" + key) in this)) {
            return true;
        }
        return false;
    }
    set(key, value, prefix = null) {
        if ((key !== "__class__")) {
            if (prefix) {
                key = ((prefix + ".") + key);
            }
            this[("_" + key)] = value;
        }
    }
    get(key, _default_ = null, prefix = null) {
        var value;
        if ((! this.has(key, prefix))) {
            return _default_;
        }
        if (prefix) {
            key = ((prefix + ".") + key);
        }
        value = (this[("_" + key)] || _default_);
        return value;
    }
}
class GremlinFSList {
    constructor(kwargs = {}) {
        this._list = [];
    }
    toString() {
        return this._list.toString();
    }
    fromlist(list) {
        this._list = list;
    }
    tolist() {
        return this._list;
    }
    append(item) {
        this._list.append(item);
        return this._list;
    }
    extend(list) {
        this._list.extend(list);
        return this._list;
    }
}
class GremlinFSMap extends GremlinFSObj {
    constructor(kwargs = {}) {
        super();
        this.setall(kwargs);
    }
    toString() {
        return this.toString();
    }
    frommap(map) {
        this.setall(map);
    }
    tomap() {
        return this.getall();
    }
    update(map) {
        this.setall(map);
    }
}
function gfslist(list = []) {
    var gfslist;
    gfslist = new GremlinFSList();
    gfslist.fromlist(list);
    return gfslist;
}
function gfsmap(map = {}) {
    var gfsmap;
    gfsmap = new GremlinFSMap();
    gfsmap.frommap(map);
    return gfsmap;
}
export {GremlinFSObj, GremlinFSList, gfslist, gfsmap};
export default GremlinFSObj;

//# sourceMappingURL=gremlinfsobj.js.map
