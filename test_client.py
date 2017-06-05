import asyncio
import logging
import session

logging.basicConfig(level=logging.DEBUG)

c = session.ClientSession()

c.install()
c.start()

async def r():
    print(await c.cmd('test_cmd'))

l = asyncio.get_event_loop()
l.run_until_complete(r())
