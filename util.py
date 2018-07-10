# encoding: utf8
import time
from functools import wraps


def time_the_process(func):
    @wraps(func)
    def time_wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print("{} - time elapsed: {:<.10f}ms".format(func.__name__, (end_time - start_time) * 1000))
        return result
    return time_wrapper


