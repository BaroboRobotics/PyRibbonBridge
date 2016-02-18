#!/usr/bin/env python3

'''
PyRibbonBridge
==============

PyRibbonBridge is a pure Python implementation of
https://github.com/BaroboRobotics/ribbon-bridge . Ribbon-bridge is a Remote
Procedure Call (RPC) toolkit. Ribbon bridge uses NanoPB under the hood and is
built to be lightweight enough to be used on embedded systems, such as robots
with AVR ATmega microprocessors. 

If you are looking for a higher level, easier-to-set-up RPC toolkit, I might
recommend looking at other projects, such as Pyro4. If, however, you are looking
for a Python proxy to call procedures on a low level C server, this might be the
right place for you.

Synopsis
--------

PyRibbonBridge uses NanoPB under the hood to pass messages between the RPC proxy
and RPC server. Read about NanoPB here: http://koti.kapsi.fi/jpa/nanopb/ .
Currently, as of version 0.0.2, PyRibbonBridge is only able to implement RPC
proxies; not servers. For information on how to create a ribbon-bridge RPC
server, please visit https://github.com/BaroboRobotics/ribbon-bridge. 

The basic usage of this package is as follows:
First, a NanoPB ".proto" file is created which describes the procedures which
may be called on the server. Each procedure will have a corresponding
nanopb message type with a nested "In" message and "Result" message.
Here is a simple example of a server that implements a single procedure
called "func" that takes an integer as an argument and returns a float::

    message func {
        message In {
            required int32 arg = 1;
        }
        message Result {
            required float result = 1;
        }
    }

The names of the arguments and results are arbitrary. The names "In" and
"Result" are special, and the existence of the two signal to PyRibbonBridge that
"func" is an RPC.

Next, the ".proto" file must be compiled into a ".py" file by using the tools
"protoc" and NanoPB. Instructions are provided on NanoPB's at
http://koti.kapsi.fi/jpa/nanopb/ , as well as Google Protobuf's site at
https://developers.google.com/protocol-buffers/?csw=1 .

Now we create a :class:`ribbonbridge.Proxy` object. The Proxy object takes the
Python interface file generated by protoc as an argument. When the Proxy object
initializes, it inspects the Python interface file and morphs itself to expose
member functions that coincide with the RPC functions in the ".proto" file. 

Next, we will have to attach some callbacks and hooks to our Proxy object so
that it knows how to communicate with the Server object. First, all data coming
from the RPC server should be delivered to the function
:func:ribbonbridge.Proxy.rb_deliver . You will also have to override the
function :func:ribbonbridge.Proxy.rb_emit_to_server with a function that takes a
bytestring as an argument and sends it to the RPC server. 

Now you should be able to use the proxy::

    import asyncio
    import ribbonbridge

    async def task():
        proxy = ribbonbridge.Proxy('func_pb2.py')
        # Connect proxy.rb_deliver and proxy.rb_emit_to_server here
        await proxy.rb_connect()
        fut = proxy.func(5)
        print('func returned:', await fut)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(task)
'''

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

    def add_broadcast_handler(self, procedure_name, coroutine):
        self._bcast_handlers[self.hash(procedure_name)] = coroutine

    def remove_broadcast_handler(self, procedure_name):
        del self._bcast_handlers[self.hash(procedure_name)] 

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

    async def _process_reply(self, reply_id, reply):
        try:
            if reply.type == rpc.Reply.RESULT:
                fut = self._open_convos.pop(reply_id)
                fut.set_result(reply.result)
                
            elif reply.type == rpc.Reply.VERSIONS:
                fut = self._open_convos.pop(reply_id)
                fut.set_result(reply.versions)
        except:
            logging.warning(
                    "Received reply to nonexistent conversation: {}"
                    .format(reply_id))

    async def _process_bcast(self, broadcast):
        try:
            logging.info('Received bcast event.')
            await self._bcast_handlers[broadcast.id](broadcast.id, broadcast.payload)
            logging.info('bcast event handled.')
        except KeyError:
            logging.warning(
                    "Received unhandled broadcast. ID:{}".format(broadcast.id))
        except Exception as e:
            logging.error("Could not handle bcast event! {}".format(e))

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
        self._bcast_members = {}
        self._bcast_handlers = {}

        self._rpc = _RpcProxyImpl()
        self._rpc.emit = self.rb_emit_to_server

        for name, m in inspect.getmembers(self.__pb2):
            if inspect.isclass(m):
                self._bcast_members[name] = m
                if hasattr(m, 'In') and hasattr(m, 'Result'):
                    self._members[name] = m

    async def rb_connect(self):
        '''
        Handshake with the server object.
        '''
        fut = await self._rpc.get_versions()
        logging.info('Waiting for versions...')
        versions = await fut
        logging.info('Connection established: {}'.format(versions))

    def rb_add_broadcast_handler(self, procedure_name, coroutine):
        '''
        Add a handler coroutine to be called when we receive an RPC broadcast.

        :param procedure_name: This is the name of the event to handle. This
            name should match a message name in the RPC .proto file.
        :type procedure_name: str
        :param coroutine: This should be a coroutine to be called asynchronously
            when an RPC broadcast is received.
        :type coroutine: async def coroutine(payload) -> None . The payload is a
            ProtoBuf object.
        '''
        self._bcast_handlers[procedure_name] = coroutine
        self._rpc.add_broadcast_handler(
                procedure_name, 
                asyncio.coroutine(
                    functools.partial(
                        self._handle_bcast,
                        procedure_name )
                    )
                )

    def rb_remove_broadcast_handler(self, procedure_name):
        '''
        Remove a broadcast handler previously added by 
        :func:`ribbonbridge.Proxy.rb_add_broadcast_handler`
        '''
        del self._bcast_handlers[procedure_name]
        self._rpc.remove_broadcast_handler(procedure_name)

    def rb_procedures(self):
        '''
        Get a list of RPC procedures that are currently loaded from the protobuf
        interface file.
        '''
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
        '''
        Get a protobuf representation of the entire "In" message object.

        This function is useful if the "In" message of a ribbon-bridge RPC
        message is itself a nested message. Use this function to retrieve
        an object representing the entire message, which may be populated
        in the manner shown here:
        https://developers.google.com/protocol-buffers/docs/pythontutorial#writing-a-message
        '''
        return self._members[procedure_name].In()

    def rb_get_results_obj(self, procedure_name):
        return self._members[procedure_name].Result()

    def rb_get_bcast_obj(self, procedure_name):
        return self._bcast_members[procedure_name]()

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

    async def _handle_bcast(self, procedure_name, bcast_id, payload):
        try:
            # Parse the payload
            pb_obj = self.rb_get_bcast_obj(procedure_name)
            pb_obj.ParseFromString(payload)
            await self._bcast_handlers[procedure_name](pb_obj)
        except KeyError:
            logging.info('Warning: Could not handle broadcast: {}'
                    .format(procedure_name))
        except Exception as e:
            logging.error('Failed to handle {} bcast: {}'.format(procedure_name, e))

    def _handle_reply(self, procedure_name, user_fut, fut):
        result_obj = self.rb_get_results_obj(procedure_name)
        result_obj.ParseFromString(fut.result().payload)
        user_fut.set_result(result_obj)


