import asyncio
import logging
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
        await asyncio.sleep(0.001)
        reply = await c.cmd(f'test_cmd {count}')
        if (count % 1000) == 0:
            print(f'cmd count {count}')


async def update(msg):
    if (msg % 10000) == 0:
        print(f'update: {msg}')


c.register('test.topic', update)

l = asyncio.get_event_loop()

l.run_until_complete(asyncio.wait([r(), c.process_update()]))
