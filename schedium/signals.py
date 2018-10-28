import time
from django.db.models.signals import post_save, post_delete
from schedium.models import SchediumLoopModelTask, SchediumDelayModelTask

from .core import schediumer


_last_update = time.time()

def _update_schedium(sender, instance, **kwargs):
    print("Update is triggered.")
    global _last_update
    if time.time() - _last_update > 1:
        schediumer.sched.update()

post_save.connect(_update_schedium, sender=SchediumLoopModelTask)
post_save.connect(_update_schedium, sender=SchediumLoopModelTask)
post_delete.connect(_update_schedium, sender=SchediumDelayModelTask)
post_delete.connect(_update_schedium, sender=SchediumDelayModelTask)