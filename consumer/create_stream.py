"""
Utility Script: create_stream.py
Description: This script connects to the local NATS server and manually creates the 
JetStream "LocalCDCStream". It explicitly defines the subjects to prevent the 
"invalid jetstream ack" or overlap errors that occur when Debezium tries to 
auto-create the stream improperly.
"""
import asyncio
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig

async def main():
    nc = NATS()
    await nc.connect("nats://127.0.0.1:4222")
    js = nc.jetstream()
    try:
        await js.add_stream(name="LocalCDCStream", subjects=[
            "academics.enrollment.main.registrar-cvsu.student_profile",
            "academics.enrollment.main.registrar-cvsu.student_info",
            "academics.enrollment.main.registrar-cvsu.outbox_events",
            "academics.enrollment.main"
        ])
        print("Stream LocalCDCStream created successfully.")
    except Exception as e:
        print("Error:", e)
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
