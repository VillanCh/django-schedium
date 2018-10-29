#!/usr/bin/env python3
# coding:utf-8
import logging
import time
import typing
import uuid
from functools import wraps
from threading import Thread, Event
from django.db import transaction, connections

from schedium import models

from .pool import Pool

logger = logging.getLogger(__name__)


class Schedium(object):

    def __init__(self, tick_interval=1, id=None, pool_size=5):
        self._id = id or uuid.uuid4().hex

        self._callbacks = {}

        self.pool = Pool(pool_size)

        # tick
        self.tick_interval = tick_interval
        self._tick_thread = None

        self._tick_start_event = Event()

        self._tick_count = 0

        self.start()


    def start(self):
        self._tick_thread = Thread(target=self._tick_loop)
        self._tick_thread.daemon = True
        self._tick_thread.start()

        self.pool.start()

    def _tick_loop(self):
        if not self._tick_start_event.is_set():
            self._tick_start_event.set()

        while self._tick_start_event.is_set():
            logger.warning("Schedium: {} tick: {}".format(self._id, time.time()))

            self._tick()

            time.sleep(self.tick_interval)
            self._tick_count += 1
            if self._tick_count >= 60:
                self._tick_count = 0

        connections.close_all()

    def _tick(self):
        for task_type, task_id, sched_id in self.safe_fetch_tasks():
            self.execute_task(task_type, task_id, sched_id)

    def execute_task(self, task_type, task_id, sched_id):
        if task_type not in self._callbacks:
            logger.warning("the task_type: {} is not existed/registered.".format(task_type))
            return

        target = self._callbacks[task_type]

        try:
            target(task_id)
        except Exception as e:
            logger.warning("exception: {} is occurred".format(e))
        finally:
            self.safe_release_task(task_id)

    @transaction.atomic
    def safe_release_task(self, sched_id):
        models.SchediumTask.objects.select_for_update().filter(
            sched_id=sched_id
        ).update(
            in_sched=False
        )

    @transaction.atomic
    def safe_fetch_tasks(self):
        now = time.time()
        queryset = models.SchediumTask.objects.select_for_update().filter(
            next_time__lte=now, in_sched=False
        )
        ret = list(queryset.values_list("task_type", "task_id", "sched_id", ))
        queryset.update(in_sched=True)
        return ret

    def register(self, task_type: str, callback: typing.Callable):
        self._callbacks[task_type] = callback

    # decorator for register callback.
    def register_task_callback(self, task_type):

        def register_callback(func):
            self.register(task_type, func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return register_callback

    def delay_task(self, task_type, task_id, delay, sched_id=None):
        sched_id = sched_id or uuid.uuid4()
        start_time = time.time()
        end_time = time.time() + delay

        task = self._create_task(sched_id, task_type, task_id,
                                 start_time=start_time, end_time=end_time,
                                 interval=None, next_time=end_time, first=True)

        return task

    def loop_task(self):
        pass

    def _create_task(self, sched_id, task_type, task_id,
                     start_time, end_time, interval, next_time, first):
        task = models.SchediumTask.objects.create(
            # basic
            sched_id=sched_id, task_type=task_type, task_id=task_id,
            # sched
            first=first, start_time=start_time, end_time=end_time, interval=interval,
            next_time=next_time, in_sched=False
        )
        return task

    def shutdown(self):
        self._tick_start_event.clear()
        self._tick_thread.join()

        self.pool.stop()

    def reset(self):
        self.shutdown()
        self.start()



schediumer = Schedium()
