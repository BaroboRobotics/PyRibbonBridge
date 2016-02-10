#!/usr/bin/env python3

import ribbonbridge as rb
import sfp.asyncio
import asyncio
import threading
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

class IoCore():
    def __init__(self):
        self._initializing = True
        self._initializing_sig = threading.Condition()
        self.loop = None
        self._thread = threading.Thread(target=self.work)
        self._thread.start()

        self._initializing_sig.acquire()
        while self._initializing:
            logging.info('Wake: Sig is {}'.format(self._initializing))
            self._initializing_sig.wait(timeout=1)
        self._initializing_sig.release()

    def work(self):
        self.loop = asyncio.new_event_loop()
        self._initializing_sig.acquire()
        self._initializing = False
        self._initializing_sig.notify_all()
        self._initializing_sig.release()
        logging.info('Starting event loop.')
        self.loop.run_forever()

class SfpProxy(rb.Proxy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_protocol(self, protocol):
        self._protocol = protocol

    async def rb_emit_to_server(self, bytestring):
        self._protocol.write(bytestring)

def main():
    io = IoCore()
    
    coro = io.loop.create_connection(sfp.asyncio.SfpProtocol, 
            'localhost', '42000')
    fut = asyncio.run_coroutine_threadsafe(coro, io.loop)
    (transport, protocol) = fut.result()

    daemon_proxy = SfpProxy(
            '/home/dko/Projects/Barobo/PyLinkbot/rpc/daemon_pb2.py',
            io.loop)
    daemon_proxy.set_protocol(protocol)
    #daemon_proxy.rb_emit_to_server = protocol.write
    protocol.deliver = daemon_proxy.rb_deliver

    daemon_proxy.rb_connect()

    args = daemon_proxy.rb_get_args_obj('sendRobotPing')
    destination = args.destinations.add()
    destination.value = 'DGKR'
    print("PING: ", daemon_proxy.sendRobotPing(args).result())

    args = daemon_proxy.rb_get_args_obj('resolveSerialId')
    print(args)
    args.serialId.value = 'LOCL'
    print("RESOLVE: ", daemon_proxy.resolveSerialId(args).result())

    '''
    proxy = SfpRpcProxy()
    proxy.connect('localhost', '42000')
    '''

if __name__ == '__main__':
    main()
#proxy = rb.Proxy('/home/dko/Projects/Barobo/PyLinkbot/rpc/robot_pb2.py')
