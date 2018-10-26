import time
from django.test import TransactionTestCase
from schedium.sched import Sched, handlers
from schedium import models

check_table = {
    "loop": 0
}


schedium_handler = handlers.ScheduleModelTaskHandler()

@schedium_handler.schedium_task_callback(task_type="test")
def test(task_id):
    print("task_id: {} is executed.".format(task_id))
    check_table['delay'] = True

print("Registered test function: {}".format(test))

def looptest(task_id):
    print("loop task_id: {} is executed".format(task_id))
    check_table["loop"] += 1


# schedium_handler.register(
#     "test", test
# )
schedium_handler.register(
    "loop", looptest
)


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
        task.save(force_update=True)


        task = models.SchediumDelayModelTask.objects.create(
            task_type="test", relative_id="test-id",
            delay_seconds=2,
        )
        task.save(force_update=True)

        models.SchediumLoopModelTask.objects.all().update()
        print(models.SchediumLoopModelTask.objects.all())

    def test_schedium_delay_task(self):
        # self.assertTrue(models.SchediumLoopModelTask.objects.all().count() > 0)

        self.sche = Sched(task_handler=schedium_handler)
        self.sche.start_auto_update(1, 1)
        self.sche.update()

        time.sleep(0.2)

        # ret = threading.Thread(target=thread_ss)
        # ret.start()

        self.assertNotIn("delay", check_table)

        self.assertEqual(check_table["loop"], 0)

        # self.assertTrue(models.SchediumLoopModelTask.objects.all().count() > 0)
        time.sleep(2.1)
        self.assertIn("delay", check_table)
        #
        # self.assertTrue(models.SchediumLoopModelTask.objects.all().count() > 0)
        self.assertEqual(check_table["loop"], 1)
        #
        time.sleep(2)
        self.assertEqual(check_table["loop"], 2)
        time.sleep(2)
        self.assertEqual(check_table["loop"], 3)

    def tearDown(self):
        self.sche.shutdown()
        # connections.close_all()
