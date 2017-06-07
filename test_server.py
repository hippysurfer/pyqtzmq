import asyncio
import logging
import session
import signal

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


async def chat(s):
    count = 0
    try:
        while True:
            count += 1
            await asyncio.sleep(0)
            await s.publish('test.topic', count)
            if (count % 100000) == 0:
                print(f'publish msg count {count}')
    except:
        log.exception('', exc_info=True)



s = session.ServerSession()

s.install()

l = asyncio.get_event_loop()
l.run_until_complete(s.start())

async def main():
    results = await asyncio.gather(chat(s), s.process_cmd(), return_exceptions=True)
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