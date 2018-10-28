#!/usr/bin/env python3
# coding:utf-8
import time
import threading
from django.test import TransactionTestCase, runner
from schedium.models import SchediumLoopModelTask
from django.db import connections
from schedium.core import schediumer

checker = {}


@schediumer.register_task_handler("test33")
def test(task_id):
    print('test function handler is executed.')
    checker["test"] = True


@schediumer.register_task_handler("test2")
def test2(task_id):
    print("this is test2")
    checker["test2"] = True


class NotifierTestCase(TransactionTestCase):

    def test_singleton(self):

        print("create task for test33")
        SchediumLoopModelTask.objects.create(
            task_type="test33", relative_id="dsasdfasdfasdf",
            interval_seconds=2
        )
        schediumer.shutdown()
        schediumer.sched.start_auto_update(1,1)
        print(schediumer.handler._callbacks)

        def t():
            assert SchediumLoopModelTask.objects.all().count() == 1
            connections.close_all()
        threading.Thread(target=t).start()

        time.sleep(3)
        self.assertIn("test", checker)

        SchediumLoopModelTask.objects.create(
            task_type="test2", relative_id="dsasddddfasdfasdf",
            interval_seconds=1
        )

        schediumer.shutdown()
