# httppubsubclient

## PROJECT STAGE - PLANNING

This project is in the development stage 1 - planning. This means that most things
you see won't work yet and are in the process of being finalized. Star/watch this
repository to be notified when it reaches the next stage!

## Overview

This is the client library for [httppubsubserver](https://github.com/Tjstretchalot/httppubsubserver).
For more details on when and why you would use httppubsub, as well as
the terminology, see the server repository.

## Installation

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -U pip
pip install httppubsubclient
pip freeze > requirements.txt
```

## Usage

In order not to maintain any active connections, you must have a listen socket
accepting incoming HTTP(s) requests. This is the main benefit of this library,
however, for those occasions where a topic is highly active or for very
temporary subscriptions, you can also use a websocket connection which will
behave more like what you may be used to from e.g. Redis.

To avoid security mishaps, it's strongly recommended that the endpoints required
for the httppubsub subscribers are on a port which only accepts internal
traffic.

This client exposes a FastAPI APIRouter that you can bind however you want and
a default flow that uses uvicorn to run an ASGI app based on that router.

```python
from httppubsubclient.client import PubSubClient
import json

async def main():
    with open('httppubsubclient.json') as f:
        httppubsubclient_config = json.load(f)

    async with PubSubClient(**httppubsubclient_config) as client:
        print('Subscribing to foo/bar (exact match) until 1 message is received...')
        async with client.subscribe_exact(b'foo/bar') as subscription:
            async for message in subscription:
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print('Subscribing to multiple topics using glob pattern until 1 message is received...')
        async with client.subscribe_glob('foo/*') as subscription:
            async for message in subscription:
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print('Subscribing to a variety of topics until one message is received...')
        async with client.subscribe_multi() as subscription:
            await subscription.subscribe_exact(b'foo/bar')
            await subscription.subscribe_glob('foo/*')
            async for message in subscription:
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print('Subscribing to one exact topic until a message is received, with arbitrary timeout behavior...')
        timeout_task = asyncio.create_task(asyncio.sleep(5))
        async with client.subscribe_exact(b'foo/bar') as subscription:
            sub_iter = await subscription.__aiter__()
            message_task = asyncio.create_task(sub_iter.__anext__())
            await asyncio.wait({timeout_task, message_task}, return_when=asyncio.FIRST_COMPLETED)
            if not message_task.cancel():
                timeout_task.cancel()
                message = message_task.result()
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
            else:
                message_task.cancel()
                print('Timed out waiting for message')

        print('Subscribing to one exact topic with simple timeout behavior...')
        async with client.subscribe_exact(b'foo/bar') as subscription:
            async for message in subscription.with_timeout(5):
                if message is None:
                    print('Been 5 seconds without a message! Ending early')
                    break
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')


        print('Sending a notification to foo/baz with bytes...')
        result = await client.notify(topic=b'foo/baz', data=b'Hello, world!')
        print(f'Notified {result.notified} subscribers to foo/baz')

        print('Sending a notification to foo/baz with a file...')
        with open('hello.txt', 'rb') as f:
            result = await client.notify(topic=b'foo/baz', sync_file=f)
        print(f'Notified {result.notified} subscribers to foo/baz')
```
