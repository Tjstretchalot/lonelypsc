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
from httppubsubclient.client import HttpPubSubClient
from httppubsubclient.config import (
    HttpPubSubConfig,
    HttpPubSubBindUvicornConfig,
    get_auth_config_from_file
)
import json

def _build_config() -> HttpPubSubConfig:
    # subscriber-secrets.json is produced from the --setup command on the
    # server. generally, this configures HMAC signing authorization between the
    # subscribers and broadcasters; if you are using HTTPS you can use token
    # authorization instead, or if you have some other way to authorize the
    # connections (e.g., TLS mutual auth), or you are sufficiently satisfied
    # network communication is internal only, this can setup "none"
    # authorization. no matter what, the broadcasters and subscribers will need
    # to agree.
    send_auth_config, receive_auth_config = get_auth_config_from_file(
        'subscriber-secrets.json'
    )

    # if you want to use a websocket instead, use WebsocketPubSubConfig instead
    # (and use the WebsocketPubSubClient class)

    return HttpPubSubConfig(
        # configures how uvicorn is going to bind the listen socket
        # can also just use a plain dict as this is a TypedDict class
        # If you want to receive the fastapi APIRouter and bind it yourself,
        # you can do that via HttpPubSubBindManualConfig(type="manual", callback=lambda router: ...)
        bind=HttpPubSubBindUvicornConfig(
            type="uvicorn",
            address='0.0.0.0',
            port=3002
        ),
        # configures how the broadcaster is going to connect to us. This can include
        # a path, if you are prefixing our router with something, and it can include
        # a fragment, which will be used on all subscribe urls.
        # ex: you are serving the router's `/v1/receive` at `/pubsub/v1/receive`
        # and you are hosting multiple processes on this machine, and this has the
        # unique process id of 1, then you might use:
        # host="http://192.0.2.0:3002/pubsub#1"
        host='http://192.0.2.0:3002',
        # determines how we set the authorization header when reaching out to the broadcaster
        send_auth=send_auth_config,
        # determines how we validate the authorization header when receiving from the broadcaster
        receive_auth=receive_auth_config
    )


async def main():
    async with HttpPubSubClient(_build_config()) as client:
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

        print(
            'Subscribing to one exact topic until a message is received, '
            'with arbitrary timeout behavior...'
        )
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
