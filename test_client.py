import asyncio
import logging
import signal
import session

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

c = session.ClientSession()

c.install()
c.start()

async def r():
    count = 0
    while True:
        count += 1
        # print('Sending command ...')
        await asyncio.sleep(1)
        try:
            reply = await c.cmd(f'test_cmd {count}')
        except TimeoutError:
            log.warning(f"Timeout waiting for cmd: {cmd}")
        # if (count % 1000) == 0:
        print(f'cmd count {count}')


async def update(msg):
    if (msg % 100000) == 0:
        print(f'update: {msg}')


c.register('test.topic', update)

l = asyncio.get_event_loop()

async def main():
    results = await asyncio.gather(r(), c.process_update(), return_exceptions=True)
    for e in results:
        if isinstance(e, Exception) and not isinstance(e, asyncio.CancelledError):
            logging.error("Exception thrown during shutdown", exc_info=(type(e), e, e.__traceback__))


def stop(msg):
    log.debug(msg)
    tasks = asyncio.Task.all_tasks()
    for task in tasks:
        task.cancel()

l.add_signal_handler(signal.SIGINT, stop, "Halting on SIGINT")

try:
    l.run_until_complete(main())
except asyncio.CancelledError:
    log.debug('CancelledError')
finally:
    l.close()