import asyncio
import json

from aiohttp import web
from aiohttp_sse import sse_response


async def create_db(request: web.Request):
    storage = request.app['storage']
    data = await request.json()
    db_name = data['db_name']
    json_db = storage.create_db(db_name)
    if json_db:
        return web.json_response(status=204)
    else:
        return web.json_response(status=400)


async def delete_db(request: web.Request):
    return web.Response(text="Hello, world")


async def db_put(request: web.Request):
    storage = request.app['storage']
    db_name = request.match_info['db_name']
    path = request.match_info['op_path']
    data = await request.json()
    json_db = storage.get_db(db_name)
    return web.json_response(data=json_db.put(path, data))


async def db_get(request: web.Request):
    storage = request.app['storage']
    db_name = request.match_info['db_name']
    path = request.match_info.get('op_path')
    json_db = storage.get_db(db_name)
    return web.json_response(data=json_db.get(path))


async def db_sse_get(request: web.Request):
    storage = request.app['storage']
    db_name = request.match_info['db_name']
    path = request.match_info.get('op_path')
    response = await sse_response(request)
    notifier = storage.get_notifier(db_name, path)
    async with response, notifier:
        stream = notifier.listen()
        while True:
            d = next(stream)
            if d:
                response.send(json.dumps(d))
            await asyncio.sleep(0)


async def db_patch(request: web.Request):
    storage = request.app['storage']
    db_name = request.match_info['db_name']
    path = request.match_info['op_path']
    data = await request.json()
    json_db = storage.get_db(db_name)
    return web.json_response(data=json_db.patch(path, data))


async def db_post(request: web.Request):
    storage = request.app['storage']
    db_name = request.match_info['db_name']
    path = request.match_info['op_path']
    data = await request.json()
    json_db = storage.get_db(db_name)
    return web.json_response(data=json_db.post(path, data))


async def db_del(request: web.Request):
    storage = request.app['storage']
    db_name = request.match_info['db_name']
    path = request.match_info['op_path']
    json_db = storage.get_db(db_name)
    return web.json_response(data=json_db.delete(path))


async def db_head(request: web.Request):
    return web.Response(status=405)
