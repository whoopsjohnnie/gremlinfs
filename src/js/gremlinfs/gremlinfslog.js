class GremlinFSLogger {
    static getLogger(name) {
        return new GremlinFSLogger(name);
    }
    static getLogLevel() {
        return null;
    }
    constructor(name, kwargs = {}) {
        this._name = name;
    }
    debug(...args) {
    }
    info(...args) {
    }
    warning(...args) {
    }
    error(...args) {
    }
    critical(...args) {
    }
    exception(...args) {
    }
    log(lvl, ...args) {
    }
}
export {GremlinFSLogger};
export default GremlinFSLogger;

//# sourceMappingURL=gremlinfslog.js.map
