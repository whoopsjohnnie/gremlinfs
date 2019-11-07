
import logging



def render_json(node, wrapper, data = None):

    data = ""

    try:

        import json
        from flatten_json import unflatten

        jsondata = wrapper.all(prefix = "data")
        jsondata = unflatten(jsondata, '__') # '.')

        data = json.dumps(
            jsondata, 
            indent=4, 
            sort_keys=False
        )

    except:
        logging.error(' GremlinFS: render_json JSON exception ')
        traceback.print_exc()

    return data



def write_json(node, data):

    try:

        import json
        from flatten_json import flatten

        jsondata = json.loads(data)
        jsondata = flatten(jsondata, '__') # '.')

        node.setProperties(jsondata, 'data')

    except:
        logging.error(' GremlinFS: write_json JSON exception ')
        traceback.print_exc()



# def render_yaml(node, wrapper, data = None):
#     pass



def write_yaml(node, data):

    try:

        import yaml
        from flatten_json import flatten

        yamldata = yaml.safe_load(data)
        yamldata = flatten(yamldata, '__') # '.')

        node.setProperties(yamldata, 'data')

    except:
        logging.error(' GremlinFS: write_yaml YAML exception ')
        traceback.print_exc()



gremlinfs = dict(

    log_level = logging.INFO,

    fs_id = "gfs1",
    fs_root = None,
    fs_root_init = False,

    folder_label = 'group',
    ref_label = 'ref',
    in_label = 'in',
    self_label = 'self',
    template_label = 'template',

    in_name = 'in0',
    self_name = 'self0',

    vertex_folder = '.V',
    edge_folder = '.E',

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
        "readfn": render_json,
        "writefn": write_json,
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
        "writefn": write_yaml,
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
