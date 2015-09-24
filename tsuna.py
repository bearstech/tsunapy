import json
import asyncio

import asyncio_redis
import aiohttp


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

    def chrono(self, *args):
        return Chrono(self.redis, self.loop, args)

    @asyncio.coroutine
    def get(self, tags, url, *args, **kargs):
        with self.chrono(*tags):
            resp = yield from self.session.get(url, *args, **kargs)
            body = yield from resp.read()
        return resp, body


class Application(dict):

    def __init__(self, loop):
        self.loop = loop

    def finish(self):
        pass

    def chrono(self, *args):
        return Chrono(self['redis'], self.loop, args)

    def session(self):
        return Session(aiohttp.ClientSession(loop=self.loop), self['redis'], self.loop)

    @asyncio.coroutine
    def forever(self):
        redis = self['redis']
        ping = yield from redis.ping()
        assert ping.status == "PONG"
        subscriber = yield from redis.start_subscribe()
        chan = yield from subscriber.subscribe(['/tsunami'])
        print("SUBSCRIBE")
        def end_scenario(f):
            print("END OF TSUNAMI")
        while True:
            reply = yield from subscriber.next_published()
            print("TSUNAMI!", reply)
            batch = []
            for d in reply.value.split():
                for a in range(10):
                    batch.append(self.loop.create_task(self.handle(d)))
            self.loop.create_task(asyncio.wait(batch))\
                .add_done_callback(end_scenario)

    @asyncio.coroutine
    def handle(self, arg):
        raise NotImplemented()


@asyncio.coroutine
def application_factory(application=Application, loop=asyncio.get_event_loop()):
    app = application(loop)
    app['redis'] = yield from asyncio_redis.Pool.create(host='127.0.0.1',
                                                        port=6379, poolsize=10)
    return app
