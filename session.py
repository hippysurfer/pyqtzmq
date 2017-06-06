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
        self.callbacks = []

    def start(self):
        self.cmd_sock = CTX.socket(zmq.DEALER)
        self.update_sock = CTX.socket(zmq.SUB)

        self.cmd_sock.connect(CMD_CLIENT_URL)
        self.update_sock.connect(UPDATE_CLIENT_URL)

    async def cmd(self, cmd):
        reply = None

        self.session_id += 1
        await self.cmd_sock.send_multipart([pickle.dumps(self.session_id), pickle.dumps(cmd)])

        while True:
            events = await self.cmd_sock.poll(timeout=10000)
            if events == 0:
                # Timeout
                log.warning("timeout in cmd")
                break
            else:
                reply_session, reply = await self.cmd_sock.recv_multipart()
                reply_session = pickle.loads(reply_session)
                reply = pickle.loads(reply)
                if reply_session != self.session_id:
                    log.warning(f'Ignoring stale response: {reply_session} != {self.session_id} ({reply})')
                    reply = None
                else:
                    # print(f'{reply_session}, {reply}')
                    break
        return reply

    def register(self, topic, callback):
        self.update_sock.subscribe = topic
        self.callbacks.append(callback)

    async def process_update(self):
        while True:
            # try:
            topic, update = await self.update_sock.recv_multipart()
            update = pickle.loads(update)
            for cb in self.callbacks:
                await cb(update)
            # except (KeyboardInterrupt, asyncop.CancelledError):
            #     raise
            # except:
            #     log.exception('', exc_info=True)
            #     break


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
        previous_session_id = {}
        while True:
            # try:
            identity, session_id, cmd = await self.cmd_sock.recv_multipart()
            await self.cmd_sock.send_multipart([identity, session_id, cmd])
            session_id = int(pickle.loads(session_id))
            if session_id != (previous_session_id.get(identity, 0) + 1):
                log.warning(f"Missed session_id {previous_session_id.get(identity, 0)} "
                            "current: {session_id}")
            previous_session_id[identity] = session_id
            if (previous_session_id[identity] % 1000) == 0:
                log.info(f"{identity}, {session_id}")
            # except (KeyboardInterrupt, asyncop.CancelledError):
            #     raise
            # except:
            #     log.exception('', exc_info=True)
            #     break

    async def publish(self, topic, msg):
        await self.update_sock.send_multipart([topic.encode('ascii'), pickle.dumps(msg)])
