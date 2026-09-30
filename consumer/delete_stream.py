"""
Utility Script: delete_stream.py
Description: This script connects to the local NATS server and forcibly deletes the 
JetStream "LocalCDCStream". This is used when the stream configuration gets corrupted 
or misconfigured by Debezium (e.g. 503 No Responders or API overlap errors) to give 
us a clean slate before recreating it.
"""
import asyncio
from nats.aio.client import Client as NATS
async def main():
    nc = NATS()
    await nc.connect("nats://127.0.0.1:4222")
    js = nc.jetstream()
    try:
        await js.delete_stream("LocalCDCStream")
        print("Deleted LocalCDCStream")
    except Exception as e:
        print(e)
    await nc.close()
if __name__ == '__main__':
    asyncio.run(main())
