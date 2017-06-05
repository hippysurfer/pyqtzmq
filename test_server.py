import asyncio
import logging
import session


logging.basicConfig(level=logging.DEBUG)

s = session.ServerSession()

s.install()
s.start()

l = asyncio.get_event_loop()
l.run_until_complete(s.process_cmd())
