import time
import uuid
import typing
from django.db import models
import pydantic


class SchediumTaskNamedTuple(pydantic.BaseModel):
    sched_id: str
    task_type: str
    task_id: str

    start_time: float
    next_time: float
    end_time: typing.Optional[float]
    interval: typing.Optional[float]
    last_executed_time: typing.Optional[float]

    is_finished: bool
    in_sched: bool


# Create your models here.
class SchediumTask(models.Model):
    sched_id = models.CharField(max_length=200, null=False, primary_key=True, default=uuid.uuid4)
    task_type = models.CharField(max_length=2000, null=False, default="default")
    task_id = models.CharField(max_length=2000, null=False)

    # sched fields
    start_time = models.IntegerField(null=False)
    next_time = models.IntegerField(null=False)
    end_time = models.IntegerField(null=True)
    interval = models.IntegerField(null=True)
    last_executed_time = models.IntegerField(null=True)

    is_finished = models.BooleanField(null=False, default=False)
    in_sched = models.BooleanField(null=False, default=False)

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):

        if not self.start_time:
            self.start_time = time.time()

        if self.end_time:
            if self.end_time < self.start_time:
                raise ValueError("end_time should be after start_time.")

        return super().save(force_insert, force_update, using, update_fields)

    def dump_named_tuple(self) -> SchediumTaskNamedTuple:
        return SchediumTaskNamedTuple(
            **self.__dict__
        )