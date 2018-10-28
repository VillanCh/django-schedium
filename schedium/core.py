#!/usr/bin/env python3
# coding:utf-8
from .sched import handlers
from .sched.sched import Sched
from .sched.chan import SchediumChannel


class _Schediumer(object):

    def __init__(self):
        self.handler = handlers.ScheduleModelTaskHandler()
        self.sched = Sched(update_interval=30, task_handler=self.handler)
        self.channel = SchediumChannel()

        self.auto_update(1)

    def register_task_handler(self, task_type):
        return self.handler.schedium_task_callback(task_type)

    def auto_update(self, interval=30):
        self.sched.start_auto_update(interval, interval)

    def shutdown(self):
        self.sched.shutdown()

    def reset(self, auto_update_interval=30):
        self.shutdown()
        self.auto_update()



schediumer = _Schediumer()