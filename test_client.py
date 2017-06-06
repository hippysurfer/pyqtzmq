import asyncio
import logging
import sys
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
tasks = asyncio.wait([r(), c.process_update()])

try:
    # Here `amain(loop)` is the core coroutine that may spawn any
    # number of tasks
    sys.exit(l.run_until_complete(tasks))
except KeyboardInterrupt:
    # Optionally show a message if the shutdown may take a while
    print("Attempting graceful shutdown, press Ctrl+C again to exit…", flush=True)

    # Do not show `asyncio.CancelledError` exceptions during shutdown
    # (a lot of these may be generated, skip this if you prefer to see them)
    def shutdown_exception_handler(l, context):
        if ( "exception" not in context or not
             isinstance(context["exception"], asyncio.CancelledError)):
            l.default_exception_handler(context)
    l.set_exception_handler(shutdown_exception_handler)

    # Handle shutdown gracefully by waiting for all tasks to be cancelled
    tasks = asyncio.gather(*asyncio.Task.all_tasks(loop=l), loop=l, return_exceptions=True)
    tasks.add_done_callback(lambda t: loop.stop())
    tasks.cancel()

    # Keep the event loop running until it is either destroyed or all
    # tasks have really terminated
    while not tasks.done() and not l.is_closed():
        l.run_forever()
finally:
    l.close()