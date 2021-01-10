
import logging



gremlinfs = dict(

    log_level = logging.INFO,

    mq_exchange = 'gfs-exchange',
    mq_exchange_type = 'topic',
    # mq_queue = 'gfs-queue',
    mq_routing_key = "gfs1.info",
    mq_routing_keys = ["gfs1.*"],

    client_id = "0010",
    fs_ns = "gfs1",
    fs_root = None,
    fs_root_init = False,

    folder_label = 'group',
    ref_label = 'ref',
    in_label = 'in',
    self_label = 'self',
    template_label = 'template',
    view_label = 'view',

    in_name = 'in0',
    self_name = 'self0',

    vertex_folder = '.V',
    edge_folder = '.E',
    in_edge_folder = 'IN', # 'EI',
    out_edge_folder = 'OUT', # 'EO',

    uuid_property = 'uuid',
    name_property = 'name',
    data_property = 'data',
    template_property = 'template',

    default_uid = 0,
    default_gid = 0,
    default_mode = 0o644,

    # 
    labels = [{
        "name": "json",
        "label": "json",
        "type": "file",
        "pattern": "^.*\.json$",
        "target": {
            "type": "file"
        },
        "match": {
            "type": "property",
            "property": "name",
            "pattern": {
                "type": "regex",
                "pattern": "^.*\.json$",
            }
        },
        # "readfn": ...,
        # "writefn": ...,
    }, {
        "name": "yaml",
        "label": "yaml",
        "type": "file",
        "pattern": "^.*\.yaml$",
        "target": {
            "type": "file"
        },
        "match": {
            "type": "property",
            "property": "name",
            "pattern": {
                "type": "regex",
                "pattern": "^.*\.yaml$",
            }
        },
        # "readfn": ...,
        # "writefn": ...,
    }, {
        "name": "group",
        "label": "group",
        "type": "folder",
        "default": True,
        "pattern": ".*",
        "target": {
            "type": "folder"
        },
        "match": {
            "type": "property",
            "property": "name",
            "pattern": {
                "type": "regex",
                "pattern": ".*",
            }
        },
    }, {
        "name": "vertex",
        "label": "vertex",
        "type": "file",
        "default": True,
        "pattern": ".*",
        "target": {
            "type": "file"
        },
        "match": {
            "type": "property",
            "property": "name",
            "pattern": {
                "type": "regex",
                "pattern": ".*",
            }
        },
    }],

)
