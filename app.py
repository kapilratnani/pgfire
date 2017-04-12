"""
    REST app
"""
import argparse
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
    _app = web.Application()
    setup_config(_app)
    _app.on_startup.append(setup_storage)
    _app.on_cleanup.append(close_storage)
    setup_routes(_app)
    return _app

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="aiohttp server example")
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=8666)
    args = parser.parse_args()
    app = prepare_app()
    web.run_app(app, host=args.host, port=args.port)
