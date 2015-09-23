import asyncio

import asyncio_redis
import aiohttp


class Application(dict):
    n = 0

    def finish(self):
        pass

    @asyncio.coroutine
    def forever(self):
        redis = self['redis']
        ping = yield from redis.ping()
        print("ping", ping)
        subscriber = yield from redis.start_subscribe()
        chan = yield from subscriber.subscribe(['/tsunami'])
        loop = asyncio.get_event_loop()
        print(subscriber)
        print("SUBSCRIBE")
        while True:
            reply = yield from subscriber.next_published()
            for d in reply.value.split():
                for a in range(10):
                    t = loop.create_task(self.handle(d))
            print(reply)

    @asyncio.coroutine
    def handle(self, arg):
        raise NotImplemented()


@asyncio.coroutine
def init(loop):
    app = Application()
    app['redis'] = yield from asyncio_redis.Pool.create(host='127.0.0.1',
                                                        port=6379, poolsize=10)
    return app
