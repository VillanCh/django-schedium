#!/usr/bin/env python3
# coding:utf-8
import logging
import time
import typing
import uuid
from datetime import datetime, timedelta
from threading import Timer, Thread, Event

from django.db import connections
from django.db.utils import ProgrammingError

from .handlers import DefaultTaskHandler, ScheduleModelTaskHandler


class _CurrentTask(Timer):

    def __init__(self, id: str, next_execute_time: datetime, target: typing.Callable, vargs: tuple = (),
                 kwargs: typing.Optional[typing.Mapping] = None):
        self.next_execute_time: datetime = next_execute_time
        interval = max(next_execute_time.timestamp() - time.time(), 0.1)
        self.id = id

        Timer.__init__(self, interval=interval, function=target, args=vargs, kwargs=kwargs)
        Timer.daemon = True


class Sched(object):

    def __init__(self, update_interval=30, task_handler=None, timezone=None):
        self._task_handler = task_handler or DefaultTaskHandler()
        self._current_task: typing.Optional[_CurrentTask] = None

        self._auto_update_thread: Thread = None
        self._auto_update_event = Event()
        self._update_interval = update_interval

        self._timezone = timezone
        self._daemon_thread = None

        self.update()

    def start_auto_update(self, empty_update_interval=10, normal_auto_update=30):
        thr = Thread(target=self._auto_update, kwargs={
            "empty_update_interval": empty_update_interval or self._update_interval,
            "normal_auto_update": normal_auto_update or self._update_interval,
        })
        thr.daemon = True
        self._auto_update_thread = thr
        thr.start()

    def join(self, timeout=None):
        if not self._auto_update_thread:
            self.start_auto_update()

        self._auto_update_thread.join(timeout=timeout)

    def stop(self):
        self._auto_update_event.clear()
        self._auto_update_thread = None

    def _auto_update(self, empty_update_interval, normal_auto_update):
        self._auto_update_event.set()
        while self._auto_update_event.is_set():
            self.update()
            if not self._current_task:
                time.sleep(min(empty_update_interval, normal_auto_update))
            else:
                time.sleep(max(empty_update_interval, normal_auto_update))

        if isinstance(self._task_handler, ScheduleModelTaskHandler):
            connections.close_all()

    def execute_later(self, after: typing.Union[int, float], target: typing.Callable, vargs: tuple = (),
                      kwargs: typing.Optional[typing.Mapping[str, str]] = None, id: str = None):
        _id = id or uuid.uuid4().hex
        now = datetime.now(tz=self._timezone)
        interval = timedelta(seconds=after)
        end = now + interval
        self._task_handler.add_task(target=target, vargs=vargs, kwargs=kwargs or {}, id=_id, start=now, end=end,
                                    interval=interval.total_seconds(), first=False)
        self.update()

    def execute_now(self, target: typing.Callable, vargs: typing.Optional[tuple] = None,
                    kwargs: dict = None):
        t = Timer(interval=0, function=target, args=vargs or (), kwargs=kwargs or {})
        t.daemon = True
        t.start()

    def execute_interval(self, interval: typing.Union[int, float], target: typing.Callable,
                         vargs: typing.Optional[tuple] = None,
                         kwargs: typing.Optional[typing.Mapping[str, str]] = None,
                         first: bool = True, start: typing.Optional[datetime] = None,
                         end: typing.Optional[datetime] = None, id=None):
        id = id or uuid.uuid4().hex

        if all([start, end]):
            if start > end:
                return

        self._task_handler.add_task(
            target=target,
            vargs=vargs or (),
            kwargs=kwargs or {},
            id=id,
            start=start,
            end=end,
            interval=interval,
            first=first
        )
        self.update()

    def update(self):
        try:
            return self._update()
        except ProgrammingError as e:
            logging.warning("Database Error in update sched with: {}".format(
                e
            ))
            logging.warning("If u are migrating database, ignore this.")

    def _update(self):
        task = self._task_handler.get_next_task()

        if not task:
            return

        if self._current_task:
            # still current task
            if task.id == self._current_task.id and task.next_time == self._current_task.next_execute_time:
                return

            if self._current_task.is_alive():
                self._current_task.cancel()
                self.delay_task(self._current_task.id)

        self._current_task = _CurrentTask(
            id=task.id, next_execute_time=task.next_time, target=self._execute,
            vargs=(), kwargs={
                "target": task.target,
                "vargs": task.vargs,
                "kwargs": task.kwargs,
            }
        )
        self._current_task.daemon = True
        self._current_task.start()

    def delay_task(self, schedium_id):
        self._task_handler.delay_task(schedium_id)

    def cancel(self, id):
        if self._current_task.id == id:
            self._current_task.cancel()
            self._current_task = None
        return self._task_handler.cancel(id)

    def _execute(self, target, vargs, kwargs):
        target(*vargs, **kwargs)
        self._current_task = None

        self.update()

        if isinstance(self._task_handler, ScheduleModelTaskHandler):
            connections.close_all()

    def shutdown(self):
        self._auto_update_event.clear()

        self._auto_update_thread.join()


schedium_model_task_handler = None


def _initial_schedium_model_task_handler():
    global schedium_model_task_handler
    if schedium_model_task_handler:
        raise RuntimeError("the ScheduleModelTaskHandler is initialed. donot initial again")
    else:
        schedium_model_task_handler = ScheduleModelTaskHandler()
        return schedium_model_task_handler
