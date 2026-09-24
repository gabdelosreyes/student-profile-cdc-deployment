import asyncio
import os
import json
from nats.aio.client import Client as NATS

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")

async def main():
    nc = NATS()
    await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
    js = nc.jetstream()
    
    try:
        info = await js.stream_info("EnrollmentData")
        print("EnrollmentData Stream Configuration:")
        print(f"Subjects: {info.config.subjects}")
    except Exception as e:
        print(f"Error: {e}")

    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
