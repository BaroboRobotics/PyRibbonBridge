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

class _RpcProxyImpl():
    def __init__(self):
        self._request_id = random.randint(100, 32000)
        self._open_convos = {}
        self._bcast_handlers = {}

    def _new_id(self):
        self._request_id = 0xffffffff & (self._request_id+1)
        return self._request_id

    async def fire(self, procedure_name, payload):
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
        logging.info("Scheduled 'FIRE' op with id {}".format(cm.id))
        await self.emit(cm.SerializeToString())
        fut = asyncio.Future()
        self._open_convos[cm.id] = fut
        return fut

    async def get_versions(self):
        r = rpc.Request()
        r.type = rpc.Request.CONNECT
        cm = rpc.ClientMessage()
        cm.id = self._new_id()
        cm.request.CopyFrom(r)
        logging.info('EMITTING...')
        await self.emit(cm.SerializeToString())
        logging.info('EMITTING... DONE')
        fut = asyncio.Future()
        self._open_convos[cm.id] = fut
        logging.info('Scheduled call to get_versions()...')
        return fut

    async def emit(self, bytestring):
        '''
        Overload this function.

        This function should transmit 'bytestring' to the server objects
        receiver.
        '''
        pass

    def add_broadcast_handler(self, procedure_name, cb):
        self._bcast_handlers[self.hash(procedure_name)] = cb

    async def deliver(self, bytestring):
        '''
        Pass all data from underlying transport to this function.
        '''
        logging.info('Bytestring delivered to proxy impl from transport.')
        r = rpc.ServerMessage()
        r.ParseFromString(bytestring)
        if r.type == rpc.ServerMessage.REPLY:
            logging.info('Processing REPLY with id {}'.format(r.inReplyTo))
            await self._process_reply(r.inReplyTo, r.reply)
        elif r.type == rpc.ServerMessage.BROADCAST:
            logging.info('Processing BROADCAST')
            await self._process_bcast(r.broadcast)

    def hash(self, s):
        '''
        This function hashes plaintext procedure names into a 32 bit integers.
        '''
        h = 0
        for c in s:
            char = c.encode('ascii')[0]
            h = (101*h + char) & 0xffffffff
        return h

    async def event(self, name, data):
        '''
        Overload this function. This callback is invoked when the RPC proxy
        receives a broadcast from the corresponding RPC server.
        '''
        pass

    async def _process_reply(self, reply_id, reply):
        try:
            if reply.type == rpc.Reply.RESULT:
                self._open_convos[reply_id].set_result(reply.result)
            elif reply.type == rpc.Reply.VERSIONS:
                self._open_convos[reply_id].set_result(reply.versions)
        except:
            logging.warning(
                    "Received reply to nonexistent conversation: {}"
                    .format(reply_id))

    async def _process_bcast(self, broadcast):
        try:
            self._bcast_handlers[broadcast.id](broadcast.payload)
        except KeyError:
            logging.warning(
                    "Received unhandled broadcast. ID:{}".format(broadcast.id))

class Proxy():
    def __init__(self, filename_pb2):
        ''' 
        Create a Ribbon Bridge Proxy object from a _pb2.py file generated from a
        .proto.
        '''
        filepath = os.path.abspath(filename_pb2)
        sys.path.append(os.path.dirname(filepath))

        basename = os.path.basename(filepath) 
        modulename = os.path.splitext(basename)[0]
        spec = importlib.util.spec_from_file_location(modulename,
                filepath)
        self.__pb2 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.__pb2)
        self._members = {}

        for name, m in inspect.getmembers(self.__pb2):
            if inspect.isclass(m):
                if hasattr(m, 'In') and hasattr(m, 'Result'):
                    self._members[name] = m

        self._rpc = _RpcProxyImpl()
        self._rpc.emit = self.rb_emit_to_server
        self._rpc.event = self._handle_bcast

    async def rb_connect(self):
        fut = await self._rpc.get_versions()
        logging.info('Waiting for versions...')
        versions = await fut
        logging.info('Connection established: {}'.format(versions))

    def rb_procedures(self):
        return self._members.keys()

    async def rb_deliver(self, bytestring):
        '''
        Pass all data incoming from underlying transport to this function.
        '''
        logging.info('Bytestring delivered to proxy from transport.')
        await self._rpc.deliver(bytestring)

    async def rb_emit_to_server(self, bytestring):
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

    def rb_get_args_obj(self, procedure_name):
        return self._members[procedure_name].In()

    def rb_get_results_obj(self, procedure_name):
        return self._members[procedure_name].Result()

    async def _handle_call(self, procedure_name, pb2_obj=None, **kwargs):
        '''
        Handle a call.
        '''
        if not pb2_obj:
            pb2_obj = self._members[procedure_name].In()
            for k,v in kwargs.items():
                setattr(pb2_obj, k, v)
        result = await self._rpc.fire(procedure_name, pb2_obj.SerializeToString())
        user_fut = asyncio.Future()
        result.add_done_callback(
                functools.partial(
                    self._handle_reply,
                    procedure_name,
                    user_fut)
                )
        logging.info('Scheduled call to: {}'.format(procedure_name))
        return user_fut

    async def _handle_bcast(self, procedure_name, pb2_obj):
        if procedure_name in self:
            await getattr(self, procedure_name)(pb2_obj)

    def _handle_reply(self, procedure_name, user_fut, fut):
        result_obj = self.rb_get_results_obj(procedure_name)
        result_obj.ParseFromString(fut.result().payload)
        user_fut.set_result(result_obj)


