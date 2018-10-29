import time
from django.test import TransactionTestCase
from schedium.core import schediumer

_check = {
    "loop": 0
}


@schediumer.register_task_callback(task_type="test")
def testfunction(task_id):
    print("test case with: {}".format(task_id))
    _check["delay"] = True

@schediumer.register_task_callback(task_type="loop")
def loopfunction(task_id):
    print("loop function with: {}".format(task_id))
    _check["loop"] += 1

# Create your tests here.
class TaskBasicUsecase(TransactionTestCase):

    def test_basic_test(self):

        schediumer.reset()

        sched_id = schediumer.delay_task(delay=2, task_type="test",
                                         task_id="teasdfasdf", sched_id=None)

        time.sleep(3)
        self.assertIn("delay", _check)

        self.assertEqual(_check["loop"], 0)
        sched_id = schediumer.loop_task(task_type="loop", task_id="asdfasdfloop",
                                        loop_interval=2, loop_start=None, loop_end=None,
                                        sched_id=None, first=True)
        time.sleep(1.5)
        self.assertEqual(_check["loop"], 1)

        time.sleep(2)
        self.assertEqual(_check["loop"], 2)

    def tearDown(self):
        schediumer.shutdown()
