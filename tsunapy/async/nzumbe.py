"""
It's a vaudou pattern, one bokor (named Boniface) command an army of nzumbes.

Nzumbes listen redis, Boniface talk to Redis.
"""
import asyncio

from asyncio_redis.exceptions import ConnectionLostError


class Boniface:
    """
    Like Boniface Malcoeur, the necromancer
    """

    def __init__(self, redis):
        self._redis = redis

    @asyncio.coroutine
    def publish(self, chan, msg):
        yield from self._redis.publish(chan, msg)

    @asyncio.coroutine
    def task(self, queue, msg):
        yield from self._redis.rpush(queue, [msg])


class Nzumbe:
    """
    Zombie in Kimbundu/kikongo
    See: https://en.wikipedia.org/wiki/Zombie_(folklore)
    """
    # TODO : implement message box?
    # TODO : multiplex messages, less connection for Redis, more for the load.

    def __init__(self, redis, chan=[], queue=[]):
        self._loop = redis._loop
        self._redis = redis
        self.chan = chan
        self.queue = queue
        self._pubsub = dict()
        self._queue = dict()
        self._task_queue = None
        self._running = False

    def subscribe(self, f, name=None):
        if name is None:
            name = f.__name__
        self.chan.append(name)
        self._pubsub[name] = f

    def list(self, f, name=None):
        if name is None:
            name = f.__name__
        self.queue.append(name)
        self._queue[name] = f

    @asyncio.coroutine
    def handle_pubsub_message(self, msg):
        yield from self._pubsub[msg.channel](msg.value)

    @asyncio.coroutine
    def handle_queue_message(self, msg):
        yield from self._queue[msg.list_name](msg.value)

    @asyncio.coroutine
    def loop_pubsub(self, subscriber):
        while self._running:
            reply = yield from subscriber.next_published()
            if reply is None:
                continue
            self._loop.create_task(self.handle_pubsub_message(reply))

    @asyncio.coroutine
    def loop_queue(self):
        while self._running:
            msg = yield from self._redis.blpop(self.queue)
            self._loop.create_task(self.handle_queue_message(msg))

    @asyncio.coroutine
    def init(self):
        ping = yield from self._redis.ping()
        assert ping.status == "PONG"
        self._running = True
        if len(self.chan):
            subscriber = yield from self._redis.start_subscribe()
            yield from subscriber.subscribe(self.chan)
            self._task_pubsub = self._loop.create_task(
                                                self.loop_pubsub(subscriber))

    @asyncio.coroutine
    def forever(self):
        if len(self.queue):
            try:
                self._task_queue = yield from self.loop_queue()
            except ConnectionLostError as e:
                if self._running:
                    raise e

    @asyncio.coroutine
    def close(self):
        self._running = False
        self._redis.close()


if __name__ == '__main__':
    import asyncio_redis

    loop = asyncio.get_event_loop()
    redis = loop.run_until_complete(asyncio_redis.Pool.create(host='localhost',
                                                              port=6379,
                                                              poolsize=10))
    z = Nzumbe(redis)

    @z.subscribe
    @asyncio.coroutine
    def the_chan(msg):
        print("Oh, a message :", msg)

    @z.list
    @asyncio.coroutine
    def first_queue(msg):
        print("Oh, a task to do :", msg)

    loop.run_until_complete(z.init())

    loop.create_task(z.forever())
    b = Boniface(redis)
    loop.run_until_complete(b.publish('the_chan', u'blah'))
    loop.run_until_complete(b.task('first_queue', u'hop'))
    loop.run_until_complete(z.close())
    loop.close()
