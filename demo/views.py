import time
from django.shortcuts import render

# Create your views here.
from schedium.core import schediumer

print("test")


@schediumer.register_task_callback("test")
def test(task_id):
    print("task_id: {} is hold. now: {}".format(task_id, time.time()))
