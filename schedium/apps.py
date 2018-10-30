from django.apps import AppConfig


class SchediumConfig(AppConfig):
    name = 'schedium'

    def ready(self):
        from .core import schediumer
        return schediumer
