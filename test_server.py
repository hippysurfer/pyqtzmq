import asyncio
import logging
import session

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


async def chat(s):
    count = 0
    while True:
        try:
            count += 1
            await asyncio.sleep(0.001)
            await s.publish('test.topic', count)
        except:
            log.exception('', exc_info=True)
            break


s = session.ServerSession()

s.install()
s.start()

l = asyncio.get_event_loop()
l.run_until_complete(asyncio.wait([chat(s), s.process_cmd()]))
# l.run_until_complete(asyncio.wait([s.process_cmd(), ]))