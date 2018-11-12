import asyncio
from functools import partial
from threading import Thread, current_thread
from weakref import WeakKeyDictionary
from concurrent.futures import Future

class BackroundEventLoopProxyMeta(type):
    _instances = WeakKeyDictionary()

    def __call__(cls, ioloop, *args, **kwargs):
        if ioloop not in cls._instances:
            cls._instances[ioloop] = super(BackroundEventLoopProxyMeta, cls).__call__(ioloop, *args, **kwargs)
        return cls._instances[ioloop]


class BackroundEventLoopProxy(object, metaclass=BackroundEventLoopProxyMeta):
    def __init__(self, ioloop=None):
        super(BackroundEventLoopProxy, self).__init__()
        self._ioloop = ioloop or asyncio.new_event_loop()
        self._thread = None

    def thread(self):
        return self._thread

    def run_forever(self):
        self._thread = current_thread()
        self._ioloop.run_forever()

    def _do_create_task(self, future, coro, ioloop):
        try:
            ret = ioloop.create_task(coro)
            future.set_result(ret)
        except Exception as e:
            future.set_exception(e)

    def _do_cancel_task(self, future, task):
        try:
            ret = task.cancel()
            future.set_result(ret)
        except Exception as e:
            future.set_exception(e)

    def create_task_threadsafe(self, coro):
        ioloop = self._ioloop
        assert ioloop.is_running(), "%s.create_task_threadsafe() should be called after ioloop run" % (type(self),)
        if current_thread() != self._thread:
            future = Future()
            p = partial(self._do_create_task, future, coro, ioloop=ioloop)
            ioloop.call_soon_threadsafe(p)
            return future.result()
        else:
            return ioloop.create_task(coro)

    def cancel_task_threadsafe(self, task):
        ioloop = self._ioloop
        assert ioloop.is_running(), "%s.cancel_task_threadsafe() should be called after ioloop run" % (type(self),)
        future = Future()
        ioloop.call_soon_threadsafe(task.cancel)
