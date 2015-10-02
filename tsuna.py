import asyncio

import asyncio_redis
import aiohttp

from uuid import uuid4


class Chrono:

    def __init__(self, redis, loop, args):
        self.redis = redis
        self.loop = loop
        self.args = args

    def __enter__(self):
        self.clock = self.loop.time()

    def __exit__(self, *args):
        self.loop.create_task(
            self.redis.lpush('stats', [
                "%s|%f" % ("|".join(self.args),
                           (self.loop.time() - self.clock))])
        )


class Session:

    def __init__(self, session, redis, loop):
        self.session = session
        self.redis = redis
        self.loop = loop
        self.uuid = uuid4().hex
        self._clock = self.loop.time()

    def chrono(self, *args):
        return Chrono(self.redis, self.loop, args)

    @asyncio.coroutine
    def get(self, tag, url, *args, **kargs):
        with self.chrono(self.uuid, tag):
            resp = yield from self.session.get(url, *args, **kargs)
            body = yield from resp.read()
        return resp, body

    def close(self):
        self.loop.create_task(self.redis.lpush(
                'stats', ["%s|scenario|%f" % (self.uuid,
                                              self.loop.time() - self._clock)])
        )
        self.session.close()


class Application(dict):

    def __init__(self, loop):
        self.loop = loop

    def chrono(self, *args):
        return Chrono(self['redis'], self.loop, args)

    def session(self):
        return Session(aiohttp.ClientSession(loop=self.loop),
                       self['redis'],
                       self.loop)
