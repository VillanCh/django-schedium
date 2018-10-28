from django.apps import AppConfig


class SchediumConfig(AppConfig):
    name = 'schedium'

    def ready(self):
        from . import core
        return core.schediumer
  