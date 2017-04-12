"""
    Add routes here
"""
from .api import *

routes = [
    # ('path', handler, 'http_method')
    (r'/createdb', createdb, 'POST'),
    (r'/deletedb', deletedb, 'DELETE'),
    (r'/database/{dbname:[a-z0-9_\-]+}/{op_path:.*?}', dbput, 'PUT'),
    (r'/database/{dbname:[a-z0-9_\-]+}/{op_path:.*?}', dbget, 'GET'),
    (r'/database/{dbname:[a-z0-9_\-]+}/{op_path:.*?}', dbpost, 'POST'),
    (r'/database/{dbname:[a-z0-9_\-]+}/{op_path:.*?}', dbdel, 'DELETE'),
    (r'/database/{dbname:[a-z0-9_\-]+}/{op_path:.*?}', dbhead, 'HEAD'),
]
