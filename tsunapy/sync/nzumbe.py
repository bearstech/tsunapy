class Boniface:
    """
    Like Boniface Malcoeur, the necromancer
    """

    def __init__(self, redis):
        self._redis = redis

    def publish(self, chan, msg):
        self._redis.publish(chan, msg)

    def task(self, queue, msg):
        self._redis.rpush(queue, [msg])
