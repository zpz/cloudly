import multiprocessing
from time import sleep

from cloudly.util.multiplexer import Multiplexer, decode, encode


def test_encoding():
    print("")
    x = (
        "tom",
        {
            "tag": "adfoiqewr",
            "data": [23, 56, 0.81, ("a", "b", 22), "so you like it\n ehr?"],
        },
    )
    print(x)
    y = encode(x)
    z = decode(y)
    print(z)
    assert z == x


def mult_worker(mux_id, q):
    worker_id = multiprocessing.current_process().name
    total = 0
    for x in Multiplexer(mux_id, worker_id):
        print(worker_id, "got", x)
        total += x * x
        sleep(0.1)
    print(worker_id, "finishing with total", total)
    q.put(total)


def test_multiplexer(tmp_path):
    N = 30
    mux = Multiplexer.new(range(1, 1 + N), tmp_path)
    mux_id = mux.create_read_session()

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    workers = [ctx.Process(target=mult_worker, args=(mux_id, q)) for _ in range(5)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    total = 0
    while not q.empty():
        total += q.get()
    assert total == sum(x * x for x in range(1, 1 + N))

    s = mux.stat(mux_id)
    print(s)
    assert mux.done(mux_id)
