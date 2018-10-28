from django.db import models
import uuid
from django.utils.timezone import timedelta, now


# Create your models here.
class SchediumDelayModelTask(models.Model):

    schedium_id = models.CharField(max_length=200, null=False, primary_key=True, default=uuid.uuid4)

    task_type = models.CharField(max_length=2000, null=False, default="default")
    relative_id = models.CharField(max_length=2000, null=False)

    start_time = models.DateTimeField(null=False)
    delay_seconds = models.PositiveIntegerField(null=False)

    next_execute_datetime = models.DateTimeField(null=False)

    is_finished = models.BooleanField(null=False, default=False)

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        if not self.start_time:
            self.start_time = now()

        if not self.next_execute_datetime:
            self.next_execute_datetime = self.start_time + timedelta(seconds=self.delay_seconds)

        return super().save(force_insert, force_update, using, update_fields)


class SchediumLoopModelTask(models.Model):

    schedium_id = models.CharField(max_length=200, null=False, primary_key=True, default=uuid.uuid4)
    task_type = models.CharField(max_length=2000, null=False, default="default")
    relative_id = models.CharField(max_length=2000, null=False)

    first = models.BooleanField(default=True)
    start_time = models.DateTimeField(null=True)
    interval_seconds = models.PositiveIntegerField()
    end_time = models.DateTimeField(null=True)

    next_execute_datetime = models.DateTimeField(null=False)

    is_finished = models.BooleanField(null=False, default=False)

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):

        if not self.start_time:
            self.start_time = now()

        if self.end_time:
            if self.end_time < self.start_time:
                raise ValueError("end_time should be after start_time.")

        if not self.next_execute_datetime:
            if self.first:
                self.next_execute_datetime = self.start_time
            else:
                self.next_execute_datetime = self.start_time + timedelta(seconds=self.interval_seconds)

        return super().save(force_insert, force_update, using, update_fields)

