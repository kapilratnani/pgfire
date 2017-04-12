from aiohttp import web


def createdb(request):
    return web.Response(text="Hello, world")


def deletedb(request):
    return web.Response(text="Hello, world")


def dbput(request):
    return web.Response(text="Hello, world")


def dbget(request):
    return web.Response(text="Hello, world")


def dbpost(request):
    return web.Response(text="Hello, world")


def dbdel(request):
    return web.Response(text="Hello, world")


def dbhead(request):
    return web.Response(text="Hello, world")
