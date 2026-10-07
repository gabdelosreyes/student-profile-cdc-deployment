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
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\student-profile-cdc-deployment\univ-reg.creds")

import sys

async def main():
    nc = NATS()
    print(f"Connecting to {NATS_URL}...")
    await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
    js = nc.jetstream()
    
    stream_name = sys.argv[1] if len(sys.argv) > 1 else "STUDENTS"
    subject_to_purge = sys.argv[2] if len(sys.argv) > 2 else "academics.enrollment.students.main.profile"
    
    print(f"Purging subject '{subject_to_purge}' from stream '{stream_name}'...")
    try:
        info_before = await js.stream_info(stream_name)
        await js.purge_stream(name=stream_name, subject=subject_to_purge)
        info_after = await js.stream_info(stream_name)
        purged = info_before.state.messages - info_after.state.messages
        print(f"Purge successful! Purged {purged} messages from stream '{stream_name}'. Subject '{subject_to_purge}' is now empty.")
    except Exception as e:
        print(f"Failed to purge: {e}")
        
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
