#!/usr/bin/env python3

import ribbonbridge as rb
#import sfp.asyncio
import asyncio
import logging
import websockets
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

class SfpProxy(rb.Proxy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_protocol(self, protocol):
        self._protocol = protocol

    @asyncio.coroutine
    def rb_emit_to_server(self, bytestring):
        print('Sending bytestring: ', bytestring)
        yield from self._protocol.send(bytestring)

@asyncio.coroutine
def consumer(source, dst):
    print('Consumer started.')
    while True:
        msg = yield from source.recv()
        print('Received message. Delivering...')
        yield from dst.rb_deliver(msg)

@asyncio.coroutine
def task():
    #coro = io.loop.create_connection(sfp.asyncio.SfpProtocol, 
    #        'localhost', '42000')

    protocol = yield from websockets.connect('ws://localhost:42000')
    #fut = asyncio.run_coroutine_threadsafe(coro, io.loop)
    #(transport, protocol) = fut.result()

    daemon_proxy = SfpProxy(
            '/home/dko/Projects/Barobo/PyLinkbot3/src/linkbot3/async/daemon_pb2.py')
    daemon_proxy.set_protocol(protocol)
    #daemon_proxy.rb_emit_to_server = protocol.write
    #protocol.deliver = daemon_proxy.rb_deliver
    asyncio.ensure_future(consumer(protocol, daemon_proxy))

    yield from asyncio.sleep(2)

    print('Connecting to daemon...')
    yield from daemon_proxy.rb_connect()

    args = daemon_proxy.rb_get_args_obj('sendRobotPing')
    destination = args.destinations.add()
    destination.value = 'DGKR'
    print('Sending robot ping...')
    fut = yield from daemon_proxy.sendRobotPing(args)
    print(fut)
    print('waiting for ping result...')
    result = yield from fut
    print("PING: ", result)

    args = daemon_proxy.rb_get_args_obj('resolveSerialId')
    print(args)
    args.serialId.value = 'LOCL'
    print("RESOLVE: ", daemon_proxy.resolveSerialId(args).result())

    '''
    proxy = SfpRpcProxy()
    proxy.connect('localhost', '42000')
    '''

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(task())
    loop.close()
#proxy = rb.Proxy('/home/dko/Projects/Barobo/PyLinkbot/rpc/robot_pb2.py')
