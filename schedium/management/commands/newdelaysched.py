#!/usr/bin/env python3
# coding:utf-8
import logging
from schedium.core import schediumer
from argparse import ArgumentParser
from django.core.management import BaseCommand


class Command(BaseCommand):

    def add_arguments(self, parser: ArgumentParser):
        # parser.add_argument("-t")
        parser.add_argument("-t", "--type", help="Task Type for callbacks.")
        parser.add_argument("-d", "--delay", help="delay seconds")
        parser.add_argument("-i", "--id", help="task id")

    def handle(self, *args, **options):
        task_type = options.get("type")

        if not task_type:
            logging.error("No task_type is set.")
            exit(1)

        if task_type not in schediumer.handler.callbacks:
            logging.error("No task_type callback set.")
            exit(1)

        delay = options.get("delay")
        if not delay:
            logging.error("the delay should be set but empty.")
            exit(1)

        relative_id = options.get("id")
        if not relative_id:
            logging.error("u have to set id for task.")
            exit(1)

        print("[*] creating delay schedium")
        task = schediumer.create_delay_sched(
            task_type, delay, relative_id
        )
        print("[*] schedium sched is created")
        print("[*]    schedium-id: {}".format(task.schedium_id))
        print("[*]    delay: {}".format(delay))
        print("[*]    task_type: {}".format(task.task_type))
