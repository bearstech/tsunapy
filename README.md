Tsunapy
=======

Tsunapy is a load test tool for complex scenario.

Tsunapy use python 3.4+ and its asyncio. It's Python, it's standard, it's in Debian stable.

Tsunapy is distributed, your workers can use multiple process, on multiple server : it's cloud friendly.

Tsunapy primary target is HTTP, but you can load watherver [asyncio](http://asyncio.org) handles.
It will be hard to stress memcache or redis target, but you can play with websocket.

If you want to stress a monster, use [Basho bench](http://docs.basho.com/riak/latest/ops/building/benchmarking/), not this tool.

Status
------

Early violent code.

Licence
-------

3 Terms BSD Licence, © 2015, Mathieu Lecarme
