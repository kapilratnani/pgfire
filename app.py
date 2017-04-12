"""
    REST app
"""

from aiohttp import web


def setup_config(app):
    from pgfire.conf import config
    app['config'] = config


async def setup_storage(app):
    from pgfire.engine.storage.postgres import PostgresJsonStorage
    dbconfig = app['config']['db']
    app['storage'] = PostgresJsonStorage(dbconfig)

async def close_storage(app):
    app['storage'].close()


def setup_routes(app):
    from pgfire.rest.routes import routes

    for route in routes:
        path = route[0]
        handler = route[1]
        method = route[2]
        app.router.add_route(method, path, handler)


def prepare_app():
    a = web.Application()
    setup_config(a)
    a.on_startup.append(setup_storage)
    a.on_cleanup.append(close_storage)
    setup_routes(a)
    return a

if __name__ == "__main__":
    app = prepare_app()
    web.run_app(app)
