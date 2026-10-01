"""
FastStream CDC Consumer: main.py
Description: This is the core FastStream application running inside the Docker container. 
It listens to the local NATS stream 'LocalCDCStream' for CDC events pushed by Debezium 
(whenever Laravel's student_profile or student_info tables change). It then combines 
the data and publishes the final, unified payload to the remote NATS JetStream 
subject 'academics.enrollment.main.student.profile'.
"""
import os
import json
from fastapi import FastAPI
from contextlib import asynccontextmanager
from faststream.nats.fastapi import NatsRouter
from sqlalchemy import create_engine, text
import datetime

LOCAL_NATS_URL = os.getenv("LOCAL_NATS_URL", "nats://127.0.0.1:4222")
REMOTE_NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")
MYSQL_URL = os.getenv("MYSQL_URL", "mysql+pymysql://root:@127.0.0.1/registrar-cvsu")

# Listen to LOCAL NATS where Debezium is publishing (no auth)
router = NatsRouter(LOCAL_NATS_URL)
engine = create_engine(MYSQL_URL, pool_recycle=3600)

TARGET_SUBJECT = "academics.enrollment.students.main.profile"

from nats.aio.client import Client as RawNATS
remote_nc = RawNATS()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Connecting to remote NATS {REMOTE_NATS_URL}...")
    await remote_nc.connect(REMOTE_NATS_URL, user_credentials=NATS_CREDS)
    print("Connected to remote NATS successfully!")
    async with router.lifespan_context(app):
        yield
    await remote_nc.close()

app = FastAPI(lifespan=lifespan)
app.include_router(router)

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "CDC Consumer via FastAPI"}

def get_student_payload(student_number: str) -> dict:
    query = text("""
        SELECT 
            sp.*, 
            si.first_name, si.mid_name, si.last_name, si.suffix, si.gender,
            si.regions_id, si.provinces_id, si.municipalities_id, si.brgys_id,
            si.street, si.zip_code, si.date_of_birth, si.religion_id, si.nationality,
            si.marital_status, si.email, si.cvsu_email, si.contact_no, si.is_pwd,
            si.photo_path
        FROM student_profile sp
        LEFT JOIN student_info si ON sp.student_number = si.student_number
        WHERE sp.student_number = :student_number
        LIMIT 1;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"student_number": student_number}).fetchone()
        
    if not result:
        return {"student_number": student_number}
        
    row = dict(result._mapping)
    
    # Format date of birth to string if it exists (since datetime.date is not JSON serializable natively by all libs)
    if row.get("date_of_birth"):
        row["date_of_birth"] = str(row["date_of_birth"])
        
    return row

@router.subscriber("academics.enrollment.main.registrar-cvsu.outbox_events", stream="LocalCDCStream", no_reply=True)
async def handle_outbox_cdc_event(msg: dict):
    payload = msg.get("payload", {})
    if not payload:
        payload = msg
        
    after = payload.get("after") or {}
    if not after:
        return
        
    # Extract clean event envelope from outbox record's payload field
    raw_event_payload = after.get("payload")
    if not raw_event_payload:
        return
        
    if isinstance(raw_event_payload, str):
        try:
            event_envelope = json.loads(raw_event_payload)
        except Exception as e:
            print(f"Error parsing outbox payload JSON: {e}")
            return
    elif isinstance(raw_event_payload, dict):
        event_envelope = raw_event_payload
    else:
        return
        
    student_number = event_envelope.get("aggregate_id") or after.get("aggregate_id")
    target_subject = event_envelope.get("subject")
    if not target_subject or target_subject == "academics.enrollment.main.student.profile":
        target_subject = TARGET_SUBJECT
    event_envelope["subject"] = target_subject
    op = event_envelope.get("op", "u")
    
    # Ensure changes list is present
    if "changes" not in event_envelope:
        changes = []
        if op == "u" and event_envelope.get("before") and event_envelope.get("after"):
            b = event_envelope["before"]
            a = event_envelope["after"]
            all_keys = set(b.keys()).union(set(a.keys()))
            for k in all_keys:
                b_val = b.get(k)
                a_val = a.get(k)
                if b_val != a_val and str(b_val or "") != str(a_val or ""):
                    changes.append(k)
        event_envelope["changes"] = sorted(changes)

    # Publish clean event envelope directly to the target subject on REMOTE NATS (STUDENTS stream)
    print(f"Publishing clean outbox student {student_number} (op={op}) to {target_subject}...")
    await remote_nc.publish(target_subject, json.dumps(event_envelope).encode('utf-8'))

# Legacy direct-table listener superseded by handle_outbox_cdc_event
# All student events are now unified through the outbox_events table.
# @router.subscriber("academics.enrollment.main.registrar-cvsu.student_info")
# @router.subscriber("academics.enrollment.main.registrar-cvsu.student_profile")
# async def handle_student_cdc_event(msg: dict):
#     ...


