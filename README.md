# lonelypsc

## PROJECT STAGE - PRE-ALPHA

This project is in the development stage 2 - pre-alpha. This means that
the project is still in the early stages of development, and is not yet
stable. The current primary focus of the library is building the test
suite in `lonelypst`

## Overview

This is the client library for [lonelypss](https://github.com/Tjstretchalot/lonelypss).
For more details on when and why you would use lonelyps, as well as
the terminology, see the server repository.

## Installation

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -U pip
pip install lonelypsc[standard]
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
from lonelypsc.http_client import HttpPubSubClient
from lonelypsc.config.config import HttpPubSubConfig, make_http_pub_sub_config
from lonelypsc.config.auth_config import AuthConfigFromParts
from lonelypsc.config.file_config import get_auth_config_from_file
import json
import os

def _build_config() -> HttpPubSubConfig:
    # subscriber-secrets.json is produced from the --setup command on the
    # server. generally, this configures HMAC signing authorization between the
    # subscribers and broadcasters; if you are using HTTPS you can use token
    # authorization instead, or if you have some other way to authorize the
    # connections (e.g., TLS mutual auth), or you are sufficiently satisfied
    # network communication is internal only, this can setup "none"
    # authorization. no matter what, the broadcasters and subscribers will need
    # to agree.
    incoming_auth_config, outgoing_auth_config = get_auth_config_from_file(
        "subscriber-secrets.json"
    )

    return make_http_pub_sub_config(
        # configures how uvicorn is going to bind the listen socket
        bind={"type": "uvicorn", "host": "0.0.0.0", "port": 3002},
        # configures how the broadcaster is going to connect to us. This can include
        # a path, if you are prefixing our router with something, and it can include
        # a fragment, which will be used on all subscribe urls.
        # ex: you are serving the router's `/v1/receive` at `/pubsub/v1/receive`
        # and you are hosting multiple processes on this machine, and this has the
        # unique process id of 1, then you might use:
        # host="http://192.0.2.0:3002/pubsub#1"
        host="http://127.0.0.1:3002",
        # the broadcasters that we will try to connect to. note that broadcasters
        # are generally stateless, so there is no data loss if one goes down. hence,
        # a high-availability setup typically needs only 2 broadcasters to tolerate
        # 1 AZ failure.
        broadcasters=[
            {"host": "http://127.0.0.1:3003"},
        ],
        # how many attempts per broadcaster before giving up; if this is 2,
        # for example, we will try every broadcaster once, then we will try
        # them all one more time before giving up. will retry 502, 503, and 504
        # responses by default, plus network errors if outgoing_retry_network_errors
        # is True.
        outgoing_retries_per_broadcaster=2,
        # if receiving a message thats larger than this in bytes, it will be spooled to disk
        message_body_spool_size=1024 * 1024 * 10,
        # total timeout for a request to a broadcaster in seconds
        outgoing_http_timeout_total=30,
        # total timeout for connecting to a broadcaster in seconds
        outgoing_http_timeout_connect=None,
        # timeout for a single socket read from a broadcaster in seconds
        outgoing_http_timeout_sock_read=None,
        # timeout for a single socket connect to a broadcaster in seconds
        outgoing_http_timeout_sock_connect=5,
        # if we assume the broadcaster did not receive the message where ambiguous
        outgoing_retry_ambiguous=True,
        auth=AuthConfigFromParts(
            # determines how we validate the authorization header when receiving from the broadcaster
            incoming_auth_config,
            # determines how we set the authorization header when reaching out to the broadcaster
            outgoing_auth_config,
        ),
    )


async def main():
    async with HttpPubSubClient(_build_config()) as client:
        # entering/exiting the client involves setting up/tearing down a server socket, so
        # you should generally only have 1 at a time and avoid unnecessarily recreating them

        print('Subscribing to foo/bar (exact match) until 1 message is received...')
        async with client.subscribe_exact(b'foo/bar') as subscription:
            async for message in await subscription.messages():
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print('Subscribing to multiple topics using glob pattern until 1 message is received...')
        async with client.subscribe_glob('foo/*') as subscription:
            async for message in await subscription.messages():
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print('Subscribing to a variety of topics until one message is received...')
        async with client.subscribe(exact=[b'foo/bar'], glob=['baz/*']) as subscription:
            async for message in await subscription.messages():
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print('Subscribing to a variety of topics (alt syntax) until one message is received...')
        async with client.subscribe_multi() as subscription:
            await subscription.subscribe_exact(b'foo/bar')
            await subscription.subscribe_glob('baz/*')
            async for message in await subscription.messages():
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')
                break

        print(
            'Subscribing to one exact topic until a message is received, '
            'with arbitrary timeout behavior...'
        )
        timeout_task = asyncio.create_task(asyncio.sleep(5))
        async with client.subscribe_exact(b'foo/bar') as subscription:
            # implementation note: will error if you try to call messages() more than
            # once on a subscription
            sub_iter = await subscription.messages()
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
            async for message in await subscription.with_timeout(5):
                if message is None:
                    print('Been 5 seconds without a message! Ending early')
                    break
                print(f'Received message on {message.topic}: {message.data.read().decode('utf-8')}')


        print('Sending a notification to foo/baz with bytes...')
        result = await client.notify(topic=b'foo/baz', data=b'Hello, world!')
        print(f'Notified {result.notified} subscribers to foo/baz')

        print('Sending a notification to foo/baz with a file...')
        if not os.path.exists('hello.txt'):
            with open('hello.txt', 'w') as f:
                f.write('Hello, world!')

        with open('hello.txt', 'rb') as f:
            result = await client.notify(topic=b'foo/baz', sync_file=f)
        print(f'Notified {result.notified} subscribers to foo/baz')
```

## Duplicate messages

With sane usage, i.e., no overlapping glob patterns within a client, duplicate
messages are very unlikely. If using overlapping glob patterns (e.g.,
subscribing to foo/\* and foo/baz), duplicate messages are expected and will
behave unintuitively. Generally, you should:

- be resilient to duplicated messages
- avoid glob patterns that overlap with each other or with exact topics within
  the same client

In practice, exact subscriptions handle the vast majority of cases this library
is suitable for, with glob patterns primarily being for analytics/logging/debugging
on a separate client, so this is a relatively non-issue. Generally, just put the
analytics/logging/debugging that attaches to a glob pattern (e.g., `**` for everything)
on their own client (e.g via a websocket client).

## Notify-Only Clients

If you want to use this library to notify over http but don't need to bind, just
set your bind config to a no-op, e.g.,

```python
from fastapi import APIRouter
from lonelypsc.config.config import make_http_pub_sub_config

async def _noop_callback(router: APIRouter) -> None: ...

make_http_pub_sub_config(
    bind={"type": "manual", "callback": _noop_callback},
    # ... other args omitted for brevity ...
)
```
