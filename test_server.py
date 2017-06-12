import asyncio
import logging
import session
import signal

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class ChattyServer(session.ServerSession):

    async def on_start(self):
        self.chat_task = asyncio.Task(self.chat())
        asyncio.ensure_future(self.chat_task)

    async def on_stop(self):
        if not self.chat_task.cancelled():
            self.chat_task.cancel()

    async def chat(self):
        count = 0
        try:
            while not self.is_time_to_stop():
                count += 1
                await asyncio.sleep(0)
                await self.publish('test.topic', count)
                if (count % 100000) == 0:
                    print(f'publish msg count {count}')
        except:
            log.exception('', exc_info=True)

s = ChattyServer()

s.install()

l = asyncio.get_event_loop()
l.run_until_complete(s.start())

async def stop(msg):
    log.debug(msg)
    await s.stop()
    tasks = asyncio.Task.all_tasks()
    for task in tasks:
        task.cancel()


l.add_signal_handler(signal.SIGINT, lambda msg: s.exit(), "Halting on SIGINT")

try:
    l.run_until_complete(s.run())
except asyncio.CancelledError:
    log.debug('CancelledError')
finally:
    l.close()