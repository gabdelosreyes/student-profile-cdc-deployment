"""
Utility Script: purge_subject.py
Description: This script connects to the remote NATS server and purges ONLY the 
'academics.enrollment.main.student.profile' subject from the 'EnrollmentData' stream. 
This is used to safely clear out old or bad data from this specific subject without 
wiping the entire JetStream stream and losing metadata/subjects for other apps.
"""
import asyncio
from nats.aio.client import Client as NATS
import os

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")

async def main():
    nc = NATS()
    print(f"Connecting to {NATS_URL}...")
    await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
    js = nc.jetstream()
    
    stream_name = "EnrollmentData"
    subject_to_purge = "academics.enrollment.main.student.profile"
    
    print(f"Purging ONLY subject '{subject_to_purge}' from stream '{stream_name}'...")
    try:
        await js.purge_stream(name=stream_name, subject=subject_to_purge)
        print("Purge successful! Subject is now empty.")
    except Exception as e:
        print(f"Failed to purge: {e}")
        
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
