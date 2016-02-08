#!/usr/bin/env python3

import ribbonbridge as rb
import sfp.asyncio
import asyncio
import threading
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

class SfpRpcProxy(rb.RpcProxyImpl):
    def __init__(self, *args, **kwargs):
        self._initializing = True
        self._initializing_sig = threading.Condition()
        self._io_thread = threading.Thread(target=self._io_thread_func)
        self._io_thread.start()

        self._initializing_sig.acquire()
        while self._initializing:
            logging.info('Wake: Sig is {}'.format(self._initializing))
            self._initializing_sig.wait(timeout=1)
        self._initializing_sig.release()
        logging.info('Calling super init.')
        super().__init__(*args, self._loop, **kwargs)

    def _io_thread_func(self):
        self._loop = asyncio.new_event_loop()
        self._initializing_sig.acquire()
        self._initializing = False
        self._initializing_sig.notify_all()
        self._initializing_sig.release()
        logging.info('Starting event loop.')
        self._loop.run_forever()

    def connect(self, host, port):
        logging.info('Connecting to remote host...')
        coro = self._loop.create_connection(sfp.asyncio.SfpProtocol, host, port)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        logging.info('Waiting for transport and protocol...')
        (transport, sfp_protocol) = fut.result()
        self._transport = transport
        self._sfp_protocol = sfp_protocol
        self._sfp_protocol.deliver = self.deliver
        logging.info('Getting remote versions...')
        fut = self.get_versions()
        print('Versions Result:', fut.result())

    async def emit(self, bytestring):
        self._sfp_protocol.write(bytestring)

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

    async def emit_to_server(self, bytestring):
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
    daemon_proxy.emit_to_server = protocol.write
    protocol.deliver = daemon_proxy.deliver

    args = daemon_proxy.get_args_obj('resolveSerialId')
    print(args)
    args.serialId.value = 'locl'
    print(daemon_proxy.resolveSerialId(args).result())

    '''
    proxy = SfpRpcProxy()
    proxy.connect('localhost', '42000')
    '''

if __name__ == '__main__':
    main()
#proxy = rb.Proxy('/home/dko/Projects/Barobo/PyLinkbot/rpc/robot_pb2.py')
