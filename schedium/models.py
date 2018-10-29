import time
import uuid
from django.db import models

# Create your models here.

class SchediumTask(models.Model):
    sched_id = models.CharField(max_length=200, null=False, primary_key=True, default=uuid.uuid4)
    task_type = models.CharField(max_length=2000, null=False, default="default")
    task_id = models.CharField(max_length=2000, null=False)

    # sched fields
    first = models.BooleanField(default=True)
    start_time = models.IntegerField(null=True)
    next_time = models.IntegerField(null=False)
    end_time = models.IntegerField(null=True)
    interval = models.IntegerField(null=True)

    is_finished = models.BooleanField(null=False, default=False)
    in_sched = models.BooleanField(null=False, default=False)

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):

        if not self.start_time:
            self.start_time = time.time()

        if self.end_time:
            if self.end_time < self.start_time:
                raise ValueError("end_time should be after start_time.")

        if not self.next_time:
            if self.first:
                self.next_time = self.start_time
            else:
                self.next_time = self.start_time + (self.interval or 0)

        return super().save(force_insert, force_update, using, update_fields)