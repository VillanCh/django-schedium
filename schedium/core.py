#!/usr/bin/env python3
# coding:utf-8
import time
from schedium import models
from .sched import handlers
from .sched.sched import Sched

_last_update = time.time()


def _update_schedium(sender=None, instance=None, **kwargs):
    print("Update is triggered.")
    global _last_update
    if time.time() - _last_update > 1:
        schediumer.sched.update()


class _Schediumer(object):

    def __init__(self):
        self.handler = handlers.ScheduleModelTaskHandler()

        self.sched = Sched(update_interval=30, task_handler=self.handler)

        self.auto_update(5)

    def register_task_handler(self, task_type):
        return self.handler.schedium_task_callback(task_type)

    def auto_update(self, interval=30):
        self.sched.start_auto_update(interval, interval)

    def shutdown(self):
        self.sched.shutdown()

    def reset(self, auto_update_interval=30):
        self.shutdown()
        self.auto_update(auto_update_interval)

    @property
    def callbacks(self):
        return self.handler.callbacks

    def create_delay_sched(self, task_type, delay_seconds, relative_id) -> models.SchediumDelayModelTask:
        task = models.SchediumDelayModelTask.objects.create(
            task_type=task_type,
            relative_id=relative_id,
            delay_seconds=int(delay_seconds)
        )
        # _update_schedium()
        return task

    def create_loop_sched(self, task_type, relative_id):
        pass


schediumer = _Schediumer()
