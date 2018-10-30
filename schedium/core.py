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
        self._tasks = []
        self._callbacks = {}

        self.pool = Pool(pool_size)

        # tick
        self.tick_interval = tick_interval
        self._tick_thread = None

        self._tick_start_event = Event()
        self._update_in_next_tick = Event()

        self._tick_count = 0

        self.start()

    def start(self):
        self._tick_thread = Thread(target=self._tick_loop)
        self._tick_thread.daemon = True
        self._tick_thread.start()

        self.pool.start()

    @transaction.atomic
    def initial_schedium_database(self):
        models.SchediumTask.objects.select_for_update().filter(
            next_time__lte=time.time(),
            in_sched=True,
        ).update(
            in_sched=False
        )

    def _tick_loop(self):
        if not self._tick_start_event.is_set():
            self._tick_start_event.set()

        self.initial_schedium_database()

        self._tasks = self.sync_database()

        while self._tick_start_event.is_set():
            logger.debug("Schedium: {} tick: {}".format(self._id, time.time()))

            self._tick()

            time.sleep(self.tick_interval)
            self._tick_count += 1
            if self._tick_count >= 60:
                self._tick_count = 0

        connections.close_all()

    @transaction.atomic
    def sync_database(self, tasks: typing.List[models.SchediumTaskNamedTuple] = None):
        sched_ids = [task.sched_id for task in (tasks or self._tasks)]
        models.SchediumTask.objects.select_for_update().filter(
            sched_id__in=sched_ids
        ).update(
            in_sched=False
        )

        now = time.time()
        queryset = models.SchediumTask.objects.select_for_update().filter(
            next_time__lte=now + 10 * self.tick_interval, in_sched=False, is_finished=False
        )
        tasks = [task.dump_named_tuple() for task in queryset.all()]
        queryset.update(in_sched=True)

        return tasks

    def update_in_next_tick(self):
        if not self._update_in_next_tick.is_set():
            self._update_in_next_tick.set()

    def _tick(self):
        if self._tick_count % 10 == 0 or self._update_in_next_tick.is_set():
            self._update_in_next_tick.clear()

            self._tasks = self.sync_database()

        for task_type, task_id, sched_id in self.fetch_closed_tasks():
            self.pool.execute(
                self.execute_task,
                kwargs={
                    "task_type": task_type,
                    "task_id": task_id,
                    "sched_id": sched_id
                }
            )

    def fetch_closed_tasks(self, tasks=None):
        now = time.time()
        for task in list((tasks or self._tasks)):
            if task.next_time <= now:
                self._tasks.remove(task)
                yield task.task_type, task.task_id, task.sched_id

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
            self.safe_handle_executed_task(sched_id)
            self.update_in_next_tick()

    @transaction.atomic
    def safe_handle_executed_task(self, sched_id):
        now = time.time()
        try:
            job = models.SchediumTask.objects.select_for_update().get(
                sched_id=sched_id
            )

            job.last_executed_time = now

            # handle loop
            if job.interval:
                while job.next_time < now:
                    job.next_time += float(job.interval)

                # handle finished
                if job.end_time:
                    if job.next_time > job.end_time:
                        job.is_finished = True

            # handle delay
            else:
                job.is_finished = True

            job.in_sched = False
            job.save(update_fields=["in_sched", "next_time", "is_finished"])
        except models.SchediumTask.DoesNotExist:
            return

    @transaction.atomic
    def safe_release_task(self, sched_id):
        models.SchediumTask.objects.select_for_update().filter(
            sched_id=sched_id
        ).update(
            in_sched=False
        )

    @transaction.atomic
    def safe_release_task_bench(self, sched_ids):
        models.SchediumTask.objects.select_for_update().filter(
            sched_id__in=sched_ids
        ).update(
            in_sched=False
        )

    @transaction.atomic
    def safe_fetch_tasks(self):
        now = time.time()
        queryset = models.SchediumTask.objects.select_for_update().filter(
            next_time__lte=now + 10 * self.tick_interval, in_sched=False, is_finished=False
        )
        tasks = [task.dump_named_tuple() for task in queryset.all()]
        queryset.update(in_sched=True)
        return tasks

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
        sched_id = sched_id or uuid.uuid4().hex
        start_time = time.time()
        end_time = time.time() + delay

        task = self._create_task(sched_id, task_type, task_id,
                                 start_time=start_time, end_time=end_time,
                                 interval=None, next_time=end_time)

        return task

    def loop_task(self, task_type, task_id, loop_interval,
                  loop_start=None, loop_end=None, sched_id=None,
                  first=True):
        sched_id = sched_id or uuid.uuid4().hex
        loop_start = loop_start or time.time()
        loop_end = loop_end
        if first:
            next_time = loop_start
        else:
            next_time = loop_start + loop_interval

        return self._create_task(
            sched_id, task_type, task_id,
            start_time=loop_start, end_time=loop_end,
            interval=loop_interval, next_time=next_time
        )

    def _create_task(self, sched_id, task_type, task_id,
                     start_time, end_time, interval, next_time):
        task = models.SchediumTask.objects.create(
            # basic
            sched_id=sched_id, task_type=task_type, task_id=task_id,
            # sched
            start_time=start_time, end_time=end_time, interval=interval,
            next_time=next_time, in_sched=False
        )
        self.update_in_next_tick()
        return task

    def shutdown(self):
        self._tick_start_event.clear()
        self._tick_thread.join()

        self.pool.stop()

    def reset(self):
        self.shutdown()
        self.start()


schediumer = Schedium()
