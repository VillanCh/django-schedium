#!/usr/bin/env python3
# coding:utf-8
import traceback
import typing
from functools import wraps
from datetime import datetime, timedelta
from django.utils import timezone
from schedium import models

SCHEDIUM_MODEL_TASK_HANDLER_CALLBACKS = {

}


class SchediumTask(object):

    def __init__(self, target: typing.Callable, vargs: typing.Optional[tuple] = None,
                 kwargs: typing.Optional[dict] = None,
                 id: str = None, start: datetime = None, end: datetime = None,
                 interval: typing.Union[int, float] = None, first: bool = None,
                 next_time: datetime = None):
        self.target = target
        self.vargs = vargs
        self.kwargs = kwargs
        self.id = id
        self.start = start
        self.end = end
        self.interval = interval
        self.first = first

        self.next_time = next_time

    def is_finished(self):
        if self.end:
            return self.next_time > self.end
        else:
            return False

    def __repr__(self):
        return "<SchediumTask id:{}>".format(self.id)


class TaskHandlerBase(object):

    def get_next_task(self):
        raise NotImplementedError()

    def add_task(self, target: typing.Callable, vargs: typing.Optional[tuple] = None,
                 kwargs: typing.Optional[dict] = None,
                 id: str = None, start: datetime = None, end: datetime = None,
                 interval: typing.Union[int, float] = None, first: bool = None):
        raise NotImplementedError()

    def execute_target(self, target: typing.Callable, vargs: typing.Tuple, kwargs: typing.Mapping, id=None):
        raise NotImplementedError()

    def cancel(self, id):
        raise NotImplementedError()


class ScheduleModelTaskHandler(TaskHandlerBase):

    def __init__(self):
        self._callbacks = {}
        self._callbacks.update(SCHEDIUM_MODEL_TASK_HANDLER_CALLBACKS)

    def register(self, task_type: str, callback: typing.Callable):
        self._callbacks[task_type] = callback

    # decorator for register callback.
    def schedium_task_callback(self, task_type):

        def register_callback(func):
            self.register(task_type, func)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return register_callback

    def get_next_task(self):
        delay_task = None
        current_task = None

        try:
            delay_task = \
                models.SchediumDelayModelTask.objects.filter(is_finished=False).order_by("next_execute_datetime")[0]
            current_task = delay_task or None
        except IndexError:
            pass

        try:
            loop_task = \
                models.SchediumLoopModelTask.objects.filter(is_finished=False).order_by("next_execute_datetime")[0]
            if current_task:
                if delay_task:
                    current_task = loop_task if loop_task.next_execute_datetime <= delay_task.next_execute_datetime \
                        else delay_task
                else:
                    current_task = loop_task
            else:
                current_task = loop_task
        except IndexError:
            traceback.format_exc()

        if not current_task:
            return

        if current_task.task_type not in self._callbacks:
            current_task.delete()
            return

        callback = self._callbacks[current_task.task_type]

        if isinstance(current_task, models.SchediumDelayModelTask):
            return SchediumTask(
                self.execute_target, (), {
                    "target": callback,
                    "vargs": (),
                    "kwargs": {
                        "task_id": current_task.relative_id
                    },
                    "id": current_task.schedium_id,
                }, current_task.schedium_id, None, None, None, first=True,
                next_time=current_task.next_execute_datetime
            )
        elif isinstance(current_task, models.SchediumLoopModelTask):
            task = SchediumTask(
                self.execute_target, (), {
                    "target": callback,
                    "vargs": (),
                    "kwargs": {
                        "task_id": current_task.relative_id
                    },
                    "id": current_task.schedium_id,
                }, current_task.schedium_id, start=current_task.start_time,
                end=current_task.end_time, interval=current_task.interval_seconds,
                first=current_task.first,
                next_time=current_task.next_execute_datetime
            )
            return task

    def execute_target(self, target: typing.Callable, vargs: typing.Tuple, kwargs: typing.Mapping, id=None):
        try:
            target(*vargs, **kwargs)
        except Exception:
            traceback.print_exc()

        delay_task_queryset = models.SchediumDelayModelTask.objects.filter(is_finished=False)
        try:
            delay_task = delay_task_queryset.get(schedium_id=id)
            delay_task.is_finished = True
            delay_task.save()
        except delay_task_queryset.model.DoesNotExist:
            pass

        loop_task_queryset = models.SchediumLoopModelTask.objects.filter(is_finished=False)
        try:
            loop_task = loop_task_queryset.get(schedium_id=id)
            now = timezone.now()
            while loop_task.next_execute_datetime < now:
                loop_task.next_execute_datetime += timedelta(seconds=loop_task.interval_seconds)
            if loop_task.end_time:
                loop_task.is_finished = loop_task.next_execute_datetime > loop_task.end_time
            loop_task.save()
        except loop_task_queryset.model.DoesNotExist:
            pass

    def cancel(self, id):
        models.SchediumDelayModelTask.objects.filter(schedium_id=id).update({"is_finished": True})
        models.SchediumLoopModelTask.objects.filter(schedium_id=id).update({"is_finished": True})


class DefaultTaskHandler(TaskHandlerBase):

    def __init__(self):
        self.tasks = []
        self.tasks_table = {}

    def get_next_task(self):
        try:
            return self.tasks[0]
        except IndexError:
            return None

    def add_task(self, target: typing.Callable, vargs: typing.Optional[tuple] = None,
                 kwargs: typing.Optional[dict] = None,
                 id: str = None, start: datetime = None, end: datetime = None,
                 interval: typing.Union[int, float] = None, first: bool = None):
        if first:
            next_time = start or datetime.now()
        else:
            next_time = (start or datetime.now()) + timedelta(seconds=interval)

        task = SchediumTask(
            self.execute_target, (), {
                "target": target,
                "vargs": vargs,
                "kwargs": kwargs,
                "id": id,
            }, id, start, end, interval, first,
            next_time=next_time
        )
        self.tasks.append(task)
        self.tasks_table[task.id] = task

        self.update()

    def execute_target(self, target: typing.Callable, vargs: typing.Tuple, kwargs: typing.Mapping, id=None):
        # ret = threading.Thread(target=target, args=vargs, kwargs=kwargs)
        # ret.daemon = True
        # ret.start()
        try:
            target(*vargs, **kwargs)
        except Exception:
            traceback.print_exc()

        task: SchediumTask = self.tasks_table.get(id)
        if not task:
            return
        else:
            task.next_time += timedelta(seconds=task.interval)

            if task.is_finished():
                del self.tasks_table[task.id]
                if task in self.tasks:
                    self.tasks.remove(task)

            self.update()

    def update(self):
        self.tasks.sort(key=lambda x: x.next_time)

    def cancel(self, id):
        task = self.tasks_table.pop(id)
        if task in self.tasks:
            self.tasks.remove(task)
