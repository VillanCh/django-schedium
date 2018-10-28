from django.apps import AppConfig


class DemoConfig(AppConfig):
    name = 'demo'

    def ready(self):
        from . import tasks
        return tasks