import asyncio
import os
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run():
    nc = NATS()
    
    # Setup credentials
    creds = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")
    
    await nc.connect("nats://192.168.10.130:4222", user_credentials=creds)
    
    print("Connected to NATS! Listening for all academics.enrollment.>")
    
    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(f"Received a message on '{subject}': {data}")

    # Subscribe to all academics topics
    await nc.subscribe("academics.enrollment.>", cb=message_handler)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await nc.close()

if __name__ == '__main__':
    asyncio.run(run())
