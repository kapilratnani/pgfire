"""
    Add routes here
"""
from .api import *
routes = [
    # ('path', handler, 'http_method')
    (r'/createdb', create_db, 'POST'),
    (r'/deletedb', delete_db, 'DELETE'),
    (r'/database/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_put, 'PUT'),
    (r'/database/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_get, 'GET'),
    (r'/database_events/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_sse_get, 'GET'),
    (r'/database/{db_name:[a-z0-9_\-]+}', db_get, 'GET'),
    (r'/database_events/{db_name:[a-z0-9_\-]+}', db_sse_get, 'GET'),
    (r'/database/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_post, 'POST'),
    (r'/database/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_del, 'DELETE'),
    (r'/database/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_patch, 'PATCH'),
    (r'/database/{db_name:[a-z0-9_\-]+}/{op_path:.*?}', db_head, 'HEAD'),
]
