from schedium.core import schediumer

# Create your views here.
@schediumer.register_task_handler("test")
def test(task_id):
    print("the task-type: test is called with id: {}".format(task_id))