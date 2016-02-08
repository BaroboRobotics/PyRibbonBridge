#!/usr/bin/env python3

import concurrent.futures
import importlib.util
import inspect
import os
import sys
import asyncio
import itertools
import threading
import logging
import random

__path = sys.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from ribbonbridge import rpc_pb2 as rpc
sys.path = __path

class RpcProxy():
    def __init__(self, asyncio_loop):
        self._loop = asyncio_loop
        self._request_id = random.randint(100, 32000)
        self._open_convos = {}

    def _new_id(self):
        self._request_id = 0xffffffff & (self._request_id+1)
        return self._request_id

    def fire(self, procedure_name, payload):
        '''
        Fire a procedure 'procedure name' with bytestring payload 'payload'.

        '''
        r = rpc.Request()
        r.type = rpc.Request.FIRE
        r.fire.id = self.hash(procedure_name)
        r.fire.payload = payload
        cm = rpc.ClientMessage()
        cm.id = self._new_id()
        cm.request.CopyFrom(r)
        asyncio.run_coroutine_threadsafe(
                self.emit(cm.SerializeToString()),
                self._loop )
        fut = concurrent.futures.Future()
        self._open_convos[cm.id] = fut
        return fut

    def get_versions(self):
        r = rpc.Request()
        r.type = rpc.Request.CONNECT
        cm = rpc.ClientMessage()
        cm.id = self._new_id()
        logging.info('New VERSIONS message with id {}'.format(cm.id))
        cm.request.CopyFrom(r)
        asyncio.run_coroutine_threadsafe(
                self.emit(cm.SerializeToString()),
                self._loop )
        fut = concurrent.futures.Future()
        self._open_convos[cm.id] = fut
        return fut

    async def emit(self, bytestring):
        '''
        Overload this function.

        This function should transmit 'bytestring' to the server objects
        receiver.
        '''
        pass

    def deliver(self, bytestring):
        '''
        Pass all data from underlying transport to this function.
        '''
        r = rpc.ServerMessage()
        r.ParseFromString(bytestring)
        logging.info('Received message from server: {}'.format(r.type))
        logging.info(str(r))
        if r.type == rpc.ServerMessage.REPLY:
            self._process_reply(r.inReplyTo, r.reply)
        elif r.type == rpc.ServerMessage.BROADCAST:
            self._process_bcast(r.broadcast)

    def hash(self, s):
        h = 0
        for c in s:
            char = c.encode('ascii')[0]
            h = (101*h + char) & 0xffffffff
        return h

    def event(self, name, data):
        '''
        Overload this function. This callback is invoked when the RPC proxy
        receives a broadcast from the corresponding RPC server.
        '''
        pass

    def _process_reply(self, reply_id, reply):
        try:
            self._open_convos[reply_id].set_result(reply)
            print('Processed reply {}'.format(reply_id))
        except:
            logging.warning(
                    "Received reply to nonexistent conversation: {}"
                    .format(reply_id))

    def _process_bcast(self, broadcast):
        pass

