#!/usr/bin/env python3
# coding:utf-8
from schediumdev import settings
import zmq


class SchediumChannel(object):

    def __init__(self, port=None):
        self._registered_addr = "tcp://127.0.0.1:{}".format(
            port if port else getattr(settings, "SCHEDIUM_CHANNEL_PORT", 4826)
        )

        self.context = zmq.Context()
        self._initial()


    def _initial(self):
        self._socket = self.context.socket(zmq.PULL)
        self._socket.bind(self._registered_addr)

