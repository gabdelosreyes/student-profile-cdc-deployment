"""
Utility script to reset the registrar_legacy_sync durable consumer on remote NATS.
Running this forces a full backfill of all legacy records from sequence 1.
"""
import asyncio
import os
from nats.aio.client import Client as NATS

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\student-profile-cdc-deployment\univ-reg.creds")
STREAM_NAME = "EnrollmentData"
CONSUMER_NAME = "registrar_legacy_sync"

async def reset():
    nc = NATS()
    print(f"Connecting to NATS at {NATS_URL}...")
    try:
        await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
        js = nc.jetstream()
        print(f"Deleting durable consumer '{CONSUMER_NAME}' from stream '{STREAM_NAME}'...")
        await js.delete_consumer(STREAM_NAME, CONSUMER_NAME)
        print("Consumer successfully reset!")
        print("The legacy-sync service will recreate it and backfill all messages from sequence 1.")
    except Exception as e:
        print(f"Notice: {e}")
    finally:
        await nc.close()

if __name__ == '__main__':
    asyncio.run(reset())
