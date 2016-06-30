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
import functools
import importlib.util
import inspect
import logging
import os
import random
import sys

__path = sys.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from ribbonbridge import rpc_pb2 as rpc
sys.path = __path

def rb_hash(s):
    '''
    This function hashes plaintext procedure names into a 32 bit integers.
    '''
    h = 0
    for c in s:
        char = c.encode('ascii')[0]
        h = (101*h + char) & 0xffffffff
    return h

def _chain_futures(fut1, fut2, conv=lambda x: x):
    def handler(fut2, conv, fut1):
        if fut1.cancelled():
            fut2.cancel()
        else:
            fut2.set_result( conv(fut1.result()) )

    fut1.add_done_callback(
            functools.partial(
                handler,
                fut2,
                conv )
            )

class _RpcProxyImpl():
    def __init__(self, logger=None):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
        self._request_id = random.randint(100, 32000)
        self._open_convos = {}
        self._bcast_handlers = {}

    def _new_id(self):
        self._request_id = 0xffffffff & (self._request_id+1)
        return self._request_id

    def close(self):
        # Cancel all waiting futures
        for key, fut in self._open_convos.items():
            if not fut.done():
                fut.cancel()
            del self._open_convos[key]

    @asyncio.coroutine
    def fire(self, procedure_name, payload):
        '''
        Fire a procedure 'procedure name' with bytestring payload 'payload'.
        '''
        r = rpc.Request()
        r.type = rpc.Request.FIRE
        r.fire.id = rb_hash(procedure_name)
        r.fire.payload = payload
        cm = rpc.ClientMessage()
        cm.id = self._new_id()
        cm.request.CopyFrom(r)
        self.logger.info("Scheduled 'FIRE' op with id {}".format(cm.id))
        yield from self.emit(cm.SerializeToString())
        fut = asyncio.Future()
        self._open_convos[cm.id] = fut
        return fut

    @asyncio.coroutine
    def get_versions(self):
        r = rpc.Request()
        r.type = rpc.Request.CONNECT
        cm = rpc.ClientMessage()
        cm.id = self._new_id()
        cm.request.CopyFrom(r)
        yield from self.emit(cm.SerializeToString())
        fut = asyncio.Future()
        self._open_convos[cm.id] = fut
        self.logger.info('Scheduled call to rpc.get_versions()...')
        return fut

    @asyncio.coroutine
    def disconnect(self):
        r = rpc.Request()
        r.type = rpc.Request.DISCONNECT
        cm = rpc.ClientMessage()
        cm.id = self._new_id()
        cm.request.CopyFrom(r)
        yield from self.emit(cm.SerializeToString())
        fut = asyncio.Future()
        self._open_convos[cm.id] = fut
        self.logger.info('Scheduled call to rpc.disconnect()...')
        return fut

    @asyncio.coroutine
    def emit(self, bytestring):
        '''
        Overload this function.

        This function should transmit 'bytestring' to the server objects
        receiver.
        '''
        raise NotImplementedError

    def add_broadcast_handler(self, procedure_name, coroutine):
        self._bcast_handlers[rb_hash(procedure_name)] = coroutine

    def remove_broadcast_handler(self, procedure_name):
        del self._bcast_handlers[rb_hash(procedure_name)] 

    @asyncio.coroutine
    def deliver(self, bytestring):
        '''
        Pass all data from underlying transport to this function.
        '''
        r = rpc.ServerMessage()
        r.ParseFromString(bytestring)
        if r.type == rpc.ServerMessage.REPLY:
            self.logger.info('Processing REPLY with id {}'.format(r.inReplyTo))
            yield from self._process_reply(r.inReplyTo, r.reply)
        elif r.type == rpc.ServerMessage.BROADCAST:
            self.logger.info('Processing BROADCAST')
            yield from self._process_bcast(r.broadcast)

    @asyncio.coroutine
    def _process_reply(self, reply_id, reply):
        try:
            if reply.type == rpc.Reply.RESULT:
                fut = self._open_convos.pop(reply_id)
                fut.set_result(reply.result)
                
            elif reply.type == rpc.Reply.VERSIONS:
                fut = self._open_convos.pop(reply_id)
                fut.set_result(reply.versions)
            elif reply.type == rpc.Reply.STATUS:
                self.logger.info("Received STATUS: {}".format(reply.status.value))
            else:
                self.logger.warning("Warning: Unknown reply type: " + str(reply.type))
                self.logger.warning(str(reply))
        except:
            self.logger.info(
                    "Received reply to nonexistent conversation: {}"
                    .format(reply_id))

    @asyncio.coroutine
    def _process_bcast(self, broadcast):
        try:
            yield from self._bcast_handlers[broadcast.id](broadcast.id, broadcast.payload)
        except KeyError:
            self.logger.debug(
                    "Received unhandled broadcast. ID:{}".format(broadcast.id))
        except Exception as e:
            self.logger.error("Could not handle bcast event! {}".format(e))

class Proxy():
    def __init__(self, filename_pb2, logger=None):
        ''' 
        Create a Ribbon Bridge Proxy object from a _pb2.py file generated from a
        .proto.
        '''
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)

        filepath = os.path.abspath(filename_pb2)
        self.logger.info('Searching for pb2 file at: ' + filepath)
        sys.path.append(os.path.dirname(filepath))

        basename = os.path.basename(filepath) 
        modulename = os.path.splitext(basename)[0]
        if sys.version_info >= (3,5):
            spec = importlib.util.spec_from_file_location(modulename,
                    filepath)
            self.__pb2 = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.__pb2)
        else:
            from importlib.machinery import SourceFileLoader
            self.__pb2 = SourceFileLoader(modulename, filepath).load_module()
        self._members = {}
        self._bcast_members = {}
        self._bcast_handlers = {}

        self._rpc = _RpcProxyImpl(logger=self.logger)
        self._rpc.emit = self.rb_emit_to_server

        for name, m in inspect.getmembers(self.__pb2):
            if inspect.isclass(m):
                self._bcast_members[name] = m
                if hasattr(m, 'In') and hasattr(m, 'Result'):
                    self._members[name] = m

    @asyncio.coroutine
    def rb_connect(self):
        '''
        Handshake with the server object.
        '''
        fut = yield from self._rpc.get_versions()
        return fut

    @asyncio.coroutine
    def rb_disconnect(self):
        fut = yield from self._rpc.disconnect()
        return fut

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

    def rb_close(self):
        '''
        Gracefully close all open conversations
        '''
        self._rpc.close()

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

    @asyncio.coroutine
    def rb_deliver(self, bytestring):
        '''
        Pass all data incoming from underlying transport to this function.
        '''
        self.logger.info('{} bytes delivered from transport.'.format(len(bytestring)))
        yield from self._rpc.deliver(bytestring)

    @asyncio.coroutine
    def rb_emit_to_server(self, bytestring):
        '''
        Overload this function.

        This function should transmit 'bytestring' to the server objects
        receiver where 'bytestring' is the raw serialized protobuf message.
        '''
        raise NotImplementedError

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

    @asyncio.coroutine
    def _handle_call(self, procedure_name, pb2_obj=None, **kwargs):
        '''
        Handle a call.
        '''
        if not pb2_obj:
            pb2_obj = self._members[procedure_name].In()
            for k,v in kwargs.items():
                setattr(pb2_obj, k, v)
        result = yield from self._rpc.fire(procedure_name, pb2_obj.SerializeToString())
        user_fut = asyncio.Future()
        _chain_futures(result, user_fut, 
                functools.partial(
                    self._handle_reply,
                    procedure_name)
                )
        self.logger.info('Scheduled call to: {}'.format(procedure_name))
        return user_fut

    @asyncio.coroutine
    def _handle_bcast(self, procedure_name, bcast_id, payload):
        try:
            # Parse the payload
            pb_obj = self.rb_get_bcast_obj(procedure_name)
            pb_obj.ParseFromString(payload)
            yield from self._bcast_handlers[procedure_name](pb_obj)
        except KeyError:
            self.logger.info('Warning: Could not handle broadcast: {}'
                    .format(procedure_name))
        except Exception as e:
            self.logger.error('Failed to handle {} bcast: {}'.format(procedure_name, e))

    def _handle_reply(self, procedure_name, result):
        result_obj = self.rb_get_results_obj(procedure_name)
        result_obj.ParseFromString(result.payload)
        return result_obj

class Server():
    def __init__(self, logger=None):
        ''' 
        Create a Ribbon Bridge Proxy object from a _pb2.py file generated from a
        .proto.
        '''
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)

        self._members = {}
        self._bcast_members = {}
        self._bcast_handlers = {}
        for m in dir(self):
            self._members[rb_hash(m)] = m

    @asyncio.coroutine
    def inbox(self, data):
        ''' Data heading towards the server. These should be processed and the
            corresponding member function should be called. '''
        cm = rpc.ClientMessage()
        cm.ParseFromString(data)

        request_handlers = { rpc.Request.CONNECT : self._handle_connect,
                             rpc.Request.DISCONNECT : self._handle_disconnect,
                             rpc.Request.FIRE : self._handle_fire }

        reply = yield from request_handlers[cm.request.type](cm.request, cm.id)
        yield from self.deliver(reply)
    
    @asyncio.coroutine
    def deliver(self, data):
        raise NotImplementedError(
            '''Override this function so that it delivers the <data> argument
            to whatever transport the inbox is connected to. '''
            )
    
    @asyncio.coroutine    
    def _handle_connect(self, request, request_id):
        # Return the rpc version
        reply = rpc.ServerMessage()
        reply.type = rpc.ServerMessage.REPLY
        reply.inReplyTo = request_id
        reply.reply.versions.rpc.major = 0
        reply.reply.versions.rpc.minor = 3
        reply.reply.versions.rpc.patch = 0
        reply.reply.versions.interface.major = 0
        reply.reply.versions.interface.minor = 2
        reply.reply.versions.interface.patch = 2
        reply.reply.type = rpc.Reply.VERSIONS
        return reply.SerializeToString()

    @asyncio.coroutine
    def _handle_disconnect(self, request, request_id):
        raise NotImplementedError

    @asyncio.coroutine
    def _handle_fire(self, request, request_id):
        try:
            fname = self._members[request.fire.id]
        except KeyError:
            raise NotImplementedError(
                '''Unable to handle FIRE request: member with component id {}
                is not implemented.'''.format(request.fire.id) )

        result_payload = yield from getattr(self, fname)(request.fire.payload)
        reply = rpc.ServerMessage()
        reply.type = rpc.ServerMessage.REPLY
        reply.inReplyTo = request_id
        reply.reply.type = rpc.Reply.RESULT
        reply.reply.result.id = request.fire.id
        reply.reply.result.payload = result_payload
        return reply.SerializeToString()
