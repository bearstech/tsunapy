import asyncio

import asyncio_redis
import aiohttp

from uuid import uuid4


class Chrono:

    def __init__(self, redis, loop, args):
        self.redis = redis
        self.loop = loop
        self.args = list(args)

    def tag(self, tag):
        self.args.append(tag)

    def __enter__(self):
        self.clock = self.loop.time()
        return self

    def __exit__(self, *args):
        self.loop.create_task(
            self.redis.lpush('stats', [
                "%s|%f|%f" % ("|".join(self.args),
                              (self.loop.time() - self.clock),
                              self.clock
                              )]))


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
        with self.chrono(self.uuid, tag) as c:
            resp = yield from self.session.get(url, *args, **kargs)
            c.tag(str(resp.status))
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
