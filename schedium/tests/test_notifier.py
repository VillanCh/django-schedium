#!/usr/bin/env python3
# coding:utf-8
from django.test import TransactionTestCase, runner

from schedium.sched import Sched, handlers, _initial_schedium_model_task_handler

_initial_schedium_model_task_handler()

class NotifierTestCase(TransactionTestCase):

    def test_singleton(self):
        with self.assertRaises(RuntimeError):
            _initial_schedium_model_task_handler()
