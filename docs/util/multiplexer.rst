***********
multiplexer
***********


:class:`~cloudly.util.multiplexer.Multiplexer` is a utility for distributing data elements to multiple concurrent or distributed workers.
Its implementation relies on the "locking" capability of :class:`~cloudly.upathlib.Upath`.

Suppose we perform some brute-force search on a cluster of machines;
there are 1000 grids, and the algorithm takes on one grid at a time.
Now, the grid is a "hyper-parameter" or "control parameter" that takes 1000 possible values.
We want to distribute these 1000 values to the workers.
This is the kind of use cases targeted by ``Multiplexer``.

Let's show its usage using local data and multiprocessing.
(For real work, we would use cloud storage and a cluster of machines.)
First, create a ``Multiplexer`` to hold the values to be distributed:

>>> from cloudly.upathlib import LocalUpath
>>> from cloudly.util.multiplexer import Multiplexer
>>> p = LocalUpath('/tmp/abc/mux')
>>> p.rmrf()
0
>>> hyper = Multiplexer.new(range(20), p)
>>> len(hyper)
20

Next, design an interesting worker function:

>>> import multiprocessing, random, time
>>>
>>> def worker(mux_id):
...     for x in Multiplexer(mux_id):
...         time.sleep(random.uniform(0.1, 0.2))  # doing a lot of things
...         print(x, 'done in', multiprocessing.current_process().name)

Back in the main process,

>>> mux_id = hyper.create_read_session()
>>> tasks = [multiprocessing.Process(target=worker, args=(mux_id,)) for _ in range(5)]
>>> for t in tasks:
...     t.start()
>>>
2 done in Process-13
0 done in Process-11
1 done in Process-12
4 done in Process-15
3 done in Process-14
6 done in Process-11
7 done in Process-12
8 done in Process-15
5 done in Process-13
9 done in Process-14
12 done in Process-15
13 done in Process-13
11 done in Process-12
10 done in Process-11
14 done in Process-14
15 done in Process-15
18 done in Process-11
16 done in Process-13
17 done in Process-12
19 done in Process-14
>>>
>>> for t in tasks:
...     t.join()
>>> hyper.done(mux_id)
True
>>> hyper.destroy()
>>>



.. automodule:: cloudly.util.multiplexer
