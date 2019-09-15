
import logging

gremlinfs = dict(

	log_level = logging.INFO, # logging.DEBUG, # logging.INFO,

    folder_label = 'group',
    ref_label = 'ref',
    in_label = 'in',
    self_label = 'self',

    in_name = 'in0',
    self_name = 'self0',

    vertex_folder = '.V',
    edge_folder = '.E',

    uuid_property = 'uuid',
    name_property = 'name',
    data_property = 'data',

    default_uid = 0,
    default_gid = 0,
    default_mode = 0o644,

    folder_root = None,
    folder_root_init = False

)
