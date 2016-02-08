#!/usr/bin/env python3

import asyncio
import concurrent.futures
import functools
import importlib.util
import inspect
import itertools
import logging
import os
import random
import sys
import threading

__path = sys.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from ribbonbridge import rpc_pb2 as rpc
sys.path = __path

class RpcProxyImpl():
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
            if reply.type == rpc.Reply.RESULT:
                self._open_convos[reply_id].set_result(reply.result)
                print('Processed result {}'.format(reply_id))
            elif reply.type == rpc.Reply.VERSIONS:
                self._open_convos[reply_id].set_result(reply.versions)
                print('Procces reply of type: {}'.format(reply.type))
        except:
            logging.warning(
                    "Received reply to nonexistent conversation: {}"
                    .format(reply_id))

    def _process_bcast(self, broadcast):
        pass

class Proxy():
    def __init__(self, filename_pb2, asyncio_loop):
        ''' 
        Create a Ribbon Bridge Proxy object from a _pb2.py file generated from a
        .proto.
        '''
        self._loop = asyncio_loop

        filepath = os.path.abspath(filename_pb2)
        sys.path.append(os.path.dirname(filepath))

        basename = os.path.basename(filepath) 
        modulename = os.path.splitext(basename)[0]
        spec = importlib.util.spec_from_file_location(modulename,
                filepath)
        self.__pb2 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.__pb2)
        self._members = {}

        print('Creating member functions:')
        for name, m in inspect.getmembers(self.__pb2):
            if inspect.isclass(m):
                if hasattr(m, 'In') and hasattr(m, 'Result'):
                    self._members[name] = m
                    print(name)

        self._rpc = RpcProxyImpl(self._loop)
        self._rpc.emit = self.emit_to_server
        self._rpc.event = self._handle_bcast

    def connect(self):
        self._rpc.get_versions().result()
        logging.info('Connection established.')

    def deliver(self, bytestring):
        '''
        Pass all data incoming from underlying transport to this function.
        '''
        self._rpc.deliver(bytestring)

    async def emit_to_server(self, bytestring):
        '''
        Overload this function.

        This function should transmit 'bytestring' to the server objects
        receiver where 'bytestring' is the raw serialized protobuf message.
        '''
        pass

    def __getattr__(self, name):
        if name not in self._members:
            raise AttributeError('{} is not a method of this RPC proxy.'
                    .format(name))
        return functools.partial(self._handle_call, name)

    def get_args_obj(self, procedure_name):
        return self._members[procedure_name].In()

    def get_results_obj(self, procedure_name):
        return self._members[procedure_name].Result()

    def _handle_call(self, procedure_name, pb2_obj=None, **kwargs):
        '''
        Handle a call.
        '''
        if not pb2_obj:
            pb2_obj = self._members[procedure_name].In()
            for k,v in kwargs.items():
                setattr(pb2_obj, k, v)
        fut = self._rpc.fire(procedure_name, pb2_obj.SerializeToString())
        userfut = concurrent.futures.Future()
        fut.add_done_callback(
                functools.partial( self._handle_result,
                                   self.get_results_obj(procedure_name),
                                   userfut )
                )
        logging.info('Scheduled call to: {}'.format(procedure_name))
        return userfut

    def _handle_bcast(self, procedure_name, pb2_obj):
        if procedure_name in self:
            getattr(self, procedure_name)(pb2_obj)

    def _handle_result(self, rpc_result_obj, userfut, fut):
        logging.info('Proxy received result.')
        userfut.set_result(rpc_result_obj.ParseFromString(fut.result().payload))


