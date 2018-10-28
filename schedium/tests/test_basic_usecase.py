import time
from django.test import TransactionTestCase
from schedium.sched.sched import Sched
from schedium.sched import handlers
from schedium import models
from schedium.core import schediumer

check_table = {
    "loop": 0
}


@schediumer.register_task_handler("test")
def test(task_id):
    print("task_id: {} is executed.".format(task_id))
    check_table['delay'] = True


print("Registered test function: {}".format(test))


@schediumer.register_task_handler("loop")
def looptest(task_id):
    print("loop task_id: {} is executed".format(task_id))
    check_table["loop"] += 1


def thread_ss():
    assert models.SchediumDelayModelTask.objects.all().count() > 0
    # assert models.SchediumLoopModelTask.objects.all().count() > 0


# Create your tests here.
class SchediumTestCase(TransactionTestCase):

    def setUp(self):
        self.assertTrue(models.SchediumLoopModelTask.objects.all().count() == 0)

        task = models.SchediumLoopModelTask.objects.create(
            task_type="loop", relative_id="test0-dasdfasdf",
            interval_seconds=2, first=False
        )
        print("LOOP TASK: id->{} in_sched->{}".format(task.schedium_id, task.in_sched))

        task = models.SchediumDelayModelTask.objects.create(
            task_type="test", relative_id="task-id",
            delay_seconds=2,
        )

    def test_schedium_delay_task(self):
        # self.assertTrue(models.SchediumLoopModelTask.objects.all().count() > 0)
        schediumer.reset(1)

        time.sleep(0.2)

        # ret = threading.Thread(target=thread_ss)
        # ret.start()

        self.assertNotIn("delay", check_table)

        self.assertEqual(check_table["loop"], 0)

        # self.assertTrue(models.SchediumLoopModelTask.objects.all().count() > 0)
        time.sleep(2.1)

        # self.assertTrue(models.SchediumLoopModelTask.objects.all().count() > 0)
        self.assertEqual(check_table["loop"], 1)

        self.assertIn("delay", check_table)

        #
        time.sleep(2)
        self.assertEqual(check_table["loop"], 2)
        time.sleep(2)
        self.assertEqual(check_table["loop"], 3)

    def tearDown(self):
        schediumer.shutdown()
        # connections.close_all()
