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
            "academics.enrollment.main",
            "academics.enrollment.main.>",
            "academics.enrollment.students.main.>"
        ])
        print("Stream LocalCDCStream created successfully.")
    except Exception as e:
        # If already exists, update subjects
        await js.update_stream(name="LocalCDCStream", subjects=[
            "academics.enrollment.main",
            "academics.enrollment.main.>",
            "academics.enrollment.students.main.>"
        ])
        print("Stream LocalCDCStream updated successfully.")
    except Exception as e:
        print("Error:", e)
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
