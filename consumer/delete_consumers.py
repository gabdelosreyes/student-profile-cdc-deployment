import asyncio
import os
from nats.aio.client import Client as NATS

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")

async def main():
    nc = NATS()
    print(f"Connecting to NATS at {NATS_URL}...")
    await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
    js = nc.jetstream()
    
    stream_name = "EnrollmentData"
    
    print(f"Fetching consumers for stream {stream_name}...")
    
    try:
        consumers = await js.consumers_info(stream_name)
        deleted_count = 0
        for consumer in consumers:
            c_name = consumer.name
            if c_name.startswith("mass_sync_consumer"):
                print(f"Deleting consumer: {c_name}...")
                await js.delete_consumer(stream_name, c_name)
                deleted_count += 1
                
        print(f"Successfully deleted {deleted_count} orphaned consumers!")
    except Exception as e:
        print(f"Failed: {e}")

    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
