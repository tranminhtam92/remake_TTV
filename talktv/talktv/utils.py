import math
import multiprocessing as mp


def func_queue(func):
    def f(params, queue):
        result = []
        for n, param in enumerate(params):
            result.append(func(param))
        queue.put(result)

    return f


def parallel_map(func, params, n=None):
    if len(params) == 1:
        return [func(params[0]), ]

    n = mp.cpu_count() * 3 / 4 if not n else n
    params = params[:]
    queue = mp.Queue()
    processes = []

    while len(params) > 0:
        k = int(math.ceil(len(params) / float(n)))
        p = mp.Process(target=func_queue(func), args=(params[:k], queue))
        processes.append(p)
        p.start()
        params = params[k:]

    result = []
    for i in range(len(processes)):
        result.extend(queue.get())

    for p in processes:
        p.join()

    return result


def spawn_func(func):
    def f(params):
        result = []
        for n, param in enumerate(params):
            result.append(func(param))

    return f


def parallel_map_no_return(func, params, n=None):
    if len(params) == 1:
        return [func(params[0]), ]

    n = mp.cpu_count() * 3 / 4 if not n else n
    params = params[:]
    processes = []

    while len(params) > 0:
        k = int(math.ceil(len(params) / float(n)))
        p = mp.Process(target=spawn_func(func), args=(params[:k],))
        processes.append(p)
        p.start()
        params = params[k:]

    for p in processes:
        p.join()
