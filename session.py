import asyncio
import zmq
import zmq.asyncio
import pickle
import logging
import time
from enum import Enum

log = logging.getLogger(__name__)


CMD_CLIENT_URL = 'tcp://localhost:5555'
UPDATE_CLIENT_URL = 'tcp://localhost:5556'
LIFECYCLE_CLIENT_URL = 'tcp://localhost:5557'

CMD_SERVER_URL = 'tcp://*:5555'
UPDATE_SERVER_URL = 'tcp://*:5556'
LIFECYCLE_SERVER_URL = 'tcp://*:5557'
STATE_TOPIC = 'system.lifecycle.state'
HEARTBEAT_TOPIC = 'system.lifecycle.heartbeat'

CTX = zmq.asyncio.Context()


class ServerResetException(Exception):
    pass


class ServerState(Enum):
    UNKNOWN = 1
    START = 2
    STOP = 3


class PyZmqSocket:
    def __init__(self, zmqsocket, server_session_id=None):
        self.server_session_id = (server_session_id.encode('ascii') if server_session_id
                                  else b'None')
        self.sock = zmqsocket

    def check_server_session_id(self, server_session_id):
        self.server_session_id = (server_session_id
                                  if self.server_session_id == b'None'
                                  else self.server_session_id)
        if server_session_id != self.server_session_id:
            log.warning("Server session id has changed!")
            raise ServerResetException

    def __getattr__(self, attr):
        return getattr(self.sock, attr)


class PyRouterSocket(PyZmqSocket):

    async def send_py_multipart(self, identity, parts, *args, **kwargs):
        return await self.sock.send_multipart(
            (identity, self.server_session_id,
             *[pickle.dumps(_) for _ in parts]), *args, **kwargs)

    async def recv_py_multipart(self, *args, **kwargs):
        identity, server_session_id, *rest = await self.sock.recv_multipart(*args, **kwargs)
        if server_session_id != self.server_session_id:
            log.warning('Recieved a request from client that has a stale server_session_id')
        return (identity, *[pickle.loads(_) for _ in rest])


class PyDealerSocket(PyZmqSocket):

    async def send_py_multipart(self, parts, *args, **kwargs):
        return await self.sock.send_multipart(
            (self.server_session_id,
             *[pickle.dumps(_) for _ in parts]), *args, **kwargs)

    async def recv_py_multipart(self, *args, **kwargs):
        server_session_id, *rest = await self.sock.recv_multipart(*args, **kwargs)
        self.check_server_session_id(server_session_id)
        return [pickle.loads(_) for _ in rest]


class PyPubSubSocket(PyZmqSocket):

    async def send_py_multipart(self, topic, parts, *args, **kwargs):
        return await self.sock.send_multipart(
            (topic.encode('ascii'),
             self.server_session_id,
             *[pickle.dumps(_) for _ in parts]), *args, **kwargs)

    async def recv_py_multipart(self, *args, **kwargs):
        topic, server_session_id, *rest = await self.sock.recv_multipart(*args, **kwargs)
        self.check_server_session_id(server_session_id)
        return (topic.decode(), *[pickle.loads(_) for _ in rest])


class ClientSessionState(Enum):

    CLOSED = 0
    CONNECTING = 1
    CONNECTED = 2
    SUBSCRIBED = 3
    STOPPED = 4


class ClientSession:

    TIME_TO_STOP = False
    TIMEOUT = 10000  # Timeout for RPC responses (10s)

    @classmethod
    def install(cls):
        """Install the zmq event loop.
        
        This must be called exactly once in each thread
        and must be called before any other operations are performed on the event loop."""

        loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(loop)

    def __init__(self):
        self.client_state = ClientSessionState.CLOSED
        self.cmd_sock = None
        self.update_sock = None
        self.lifecycle_sock = None
        self.update_topic = None
        self.session_id = 0
        self.callbacks = []

    def start(self):
        """Setup the sockets.
        
        This must be called exactly once in a thread."""

        self.cmd_sock = PyDealerSocket(CTX.socket(zmq.DEALER))
        self.update_sock = PyPubSubSocket(CTX.socket(zmq.SUB))
        self.lifecycle_sock = PyPubSubSocket(CTX.socket(zmq.SUB))

        self.cmd_sock.connect(CMD_CLIENT_URL)
        self.update_sock.connect(UPDATE_CLIENT_URL)
        self.lifecycle_sock.connect(LIFECYCLE_CLIENT_URL)
        self.lifecycle_sock.subscribe('')

        self.client_state = ClientSessionState.CONNECTING

    async def stop(self):
        await self.on_stopped()
        for sock in (self.cmd_sock, self.update_sock, self.lifecycle_sock):
            if sock and not sock.closed:
                sock.close()

    def exit(self):
        self.__class__.TIME_TO_STOP = True

    async def restart(self):
        log.warning('Restarting connection to server.')
        await self.stop()
        self.start()

    async def cmd(self, cmd):
        """Send a command to the server and wait for the response."""

        self.session_id += 1  # Increment the counter for rpc call

        # Send the request to the server.
        # await self.cmd_sock.send_multipart([pickle.dumps(self.session_id), pickle.dumps(cmd)])
        try:
            await self.cmd_sock.send_py_multipart([self.session_id, cmd])

            while True:

                # Wait for a response to be ready
                events = await self.cmd_sock.poll(timeout=self.TIMEOUT)
                if events == 0:
                    log.warning(f"Timeout waiting for a response to cmd: {cmd}")
                    raise TimeoutError
                else:
                    reply_session, reply = await self.cmd_sock.recv_py_multipart()
                    if reply_session == self.session_id:
                        return reply
                    log.warning(f'Ignoring stale response: {reply_session} != {self.session_id} ({reply})')
        except ServerResetException:
            await self.restart()

    def register(self, topic, callback):
        """Register a callback function to be called when a message is received on the given topic."""
        self.callbacks.append(callback)

    async def on_connected(self):
        pass

    async def run(self):
        results = await asyncio.gather(
            self.process_update(),
            self.process_lifecycle(),
            return_exceptions=True)
        for e in results:
            if (isinstance(e, Exception) and
                    not isinstance(e, asyncio.CancelledError)):
                logging.error("Exception thrown during shutdown",
                              exc_info=(type(e), e, e.__traceback__))

    async def process_update(self):
        """Process updates received from the server."""

        while not self.__class__.TIME_TO_STOP:
            try:
                topic, update = await self.update_sock.recv_py_multipart()
                for cb in self.callbacks:
                    await cb(update)
            except asyncio.CancelledError:
                pass  # We get CanelledError when doing the restart logic.
            except ServerResetException:
                await self.restart()
            except:
                log.exception('Unhandled exception on process_update:', exc_info=True)

        await self.stop()

    async def process_lifecycle(self):
        """Process lifecycle messages received from the server."""

        while not self.__class__.TIME_TO_STOP:
            try:
                topic, state = await self.lifecycle_sock.recv_py_multipart()
                if topic == STATE_TOPIC:
                    if state == ServerState.STOP:
                        await self.restart()
                elif (topic == HEARTBEAT_TOPIC or
                        (topic == STATE_TOPIC and state == ServerState.START)):
                    if self.client_state == ClientSessionState.CONNECTING:
                        self.client_state = ClientSessionState.CONNECTED
                        await self.on_connected()
                else:
                    log.warning(f'Ignoring unknon topic on lifecycle socket {topic}')
            except asyncio.CancelledError:
                raise
            except ServerResetException:
                await self.restart()
            except:
                log.exception('Unhandled exception on process_update:', exc_info=True)

        await self.stop()


class ServerSession:

    TIME_TO_STOP = False

    @classmethod
    def install(cls):
        """Install the zmq event loop.

        This must be called exactly once in each thread
        and must be called before any other operations are performed on the event loop."""

        loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(loop)

    def __init__(self):
        self.cmd_sock = None
        self.update_sock = None
        self.lifecycle_sock = None
        self.state = ServerState.UNKNOWN
        self.server_session_id = 0

    async def start(self, server_session_id=None):
        """Setup the sockets.

        This must be called exactly once in a thread."""

        self.server_session_id = (server_session_id if server_session_id
                                  else str(time.time()))
        self.cmd_sock = PyRouterSocket(CTX.socket(zmq.ROUTER),
                                       self.server_session_id)
        self.update_sock = PyPubSubSocket(CTX.socket(zmq.PUB),
                                          self.server_session_id)
        self.lifecycle_sock = PyPubSubSocket(CTX.socket(zmq.PUB),
                                             self.server_session_id)

        self.cmd_sock.bind(CMD_SERVER_URL)
        self.update_sock.bind(UPDATE_SERVER_URL)
        self.lifecycle_sock.bind(LIFECYCLE_SERVER_URL)

        await self.set_state(ServerState.START)
        await self.on_start()

    async def stop(self):
        await self.on_stop()
        await self.set_state(ServerState.STOP)
        self.cmd_sock.close()
        self.update_sock.close()
        self.lifecycle_sock.close()

    async def restart(self):
        await self.stop()
        await self.start()

    def exit(self):
        self.__class__.TIME_TO_STOP = True

    def is_time_to_stop(self):
        return self.__class__.TIME_TO_STOP

    async def set_state(self, state):
        self.state = state
        log.debug(f'Set state: {state}')
        await self.lifecycle_sock.send_py_multipart(STATE_TOPIC, [state])

    async def run(self):
        results = await asyncio.gather(
            self.process_cmd(),
            self.process_heartbeat(),
            return_exceptions=True)
        for e in results:
            if (isinstance(e, Exception) and
                    not isinstance(e, asyncio.CancelledError)):
                logging.error("Exception thrown during shutdown",
                              exc_info=(type(e), e, e.__traceback__))

    async def process_heartbeat(self):
        while not self.TIME_TO_STOP:
            try:
                await asyncio.sleep(1)
                await self.lifecycle_sock.send_py_multipart(HEARTBEAT_TOPIC, [time.time()])
            except asyncio.CancelledError:
                raise
            # except:
            #     log.exception('Unhandled exception in process_heartbeat',
            #                   exc_info=True)
        await self.stop()

    async def process_cmd(self):
        previous_session_id = {}
        while not self.TIME_TO_STOP:
            try:
                identity, session_id, cmd = await self.cmd_sock.recv_py_multipart()
                await self.cmd_sock.send_py_multipart(identity, [session_id, cmd])
                if session_id != (previous_session_id.get(identity, 0) + 1):
                    log.warning(f"Missed session_id {previous_session_id.get(identity, 0)} "
                                f"current: {session_id}")
                previous_session_id[identity] = session_id
                if (previous_session_id[identity] % 1000) == 0:
                    log.info(f"{identity}, {session_id}")
            except asyncio.CancelledError:
                raise
            except:
                log.exception('Unhandled exception on process_cmd:', exc_info=True)
        await self.stop()

    async def publish(self, topic, msg):
        await self.update_sock.send_py_multipart(topic, [msg])
