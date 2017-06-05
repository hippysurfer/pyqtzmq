import asyncio
import zmq
import zmq.asyncio
import pickle
import logging

log = logging.getLogger(__name__)


CMD_CLIENT_URL = 'tcp://localhost:5555'
UPDATE_CLIENT_URL = 'tcp://localhost:5556'

CMD_SERVER_URL = 'tcp://*:5555'
UPDATE_SERVER_URL = 'tcp://*:5556'

CTX = zmq.asyncio.Context()


class ClientSession:

    @classmethod
    def install(cls):
        loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(loop)

    def __init__(self):
        self.cmd_sock = None
        self.update_sock = None
        self.session_id = 0

    def start(self):
        self.cmd_sock = CTX.socket(zmq.DEALER)
        self.update_sock = CTX.socket(zmq.SUB)

        self.cmd_sock.connect(CMD_CLIENT_URL)
        self.update_sock.connect(UPDATE_CLIENT_URL)

    async def cmd(self, cmd):
        self.session_id += 1
        await self.cmd_sock.send_multipart([pickle.dumps(self.session_id), pickle.dumps(cmd)])
        reply_session, reply = await self.cmd_sock.recv_multipart()
        reply_session = pickle.loads(reply_session)
        reply = pickle.loads(reply)
        if reply_session != self.session_id:
            log.warning(f'Ignoring stale response: {reply_session}, {reply}')
            return None
        print(f'{reply_session}, {reply}')
        return reply

    def register(self, topic, callback):
        pass


class ServerSession:

    @classmethod
    def install(cls):
        loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(loop)

    def __init__(self):
        self.cmd_sock = None
        self.update_sock = None

    def start(self):
        self.cmd_sock = CTX.socket(zmq.ROUTER)
        self.update_sock = CTX.socket(zmq.PUB)

        self.cmd_sock.bind(CMD_SERVER_URL)
        self.update_sock.bind(UPDATE_SERVER_URL)

    async def process_cmd(self):
        identity, session_id, cmd = await self.cmd_sock.recv_multipart()
        await self.cmd_sock.send_multipart([identity, session_id, cmd])

    def publish(self, topic, msg):
        pass
