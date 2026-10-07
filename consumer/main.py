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
import uuid
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
MAIN_SUBJECT = "academics.enrollment.main.student.profile"

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

@router.subscriber("academics.enrollment.main.student.profile", stream="LocalCDCStream", no_reply=True)
async def handle_outbox_cdc_event(msg: dict):
    # Support both envelope-wrapped payload (from Debezium additional.placement=envelope) and flat payloads
    if "payload" in msg and isinstance(msg["payload"], dict):
        event_id = msg.get("eventId") or str(uuid.uuid4())
        event_type = msg.get("eventType")
        payload = msg["payload"]
    else:
        payload = dict(msg)
        event_id = payload.pop("eventId", None) or str(uuid.uuid4())
        event_type = payload.pop("eventType", None)

    if "student_number" in payload or "aggregate_id" in payload or "changes" in payload:
        student_number = payload.get("student_number") or payload.get("aggregate_id")
        op = payload.get("op", "u")
        default_event_type = f"student.profile.{'created' if op == 'c' else 'updated' if op == 'u' else 'deleted'}"
        event_type = event_type or default_event_type

        headers = {
            "content-type": "application/json",
            "eventType": str(event_type),
            "Nats-Msg-Id": str(event_id),
        }

        print(f"Relaying live student change {student_number} (op={op}, eventType={event_type}, eventId={event_id}) with headers to remote {TARGET_SUBJECT} and {MAIN_SUBJECT}...")
        encoded = json.dumps(payload).encode('utf-8')
        await remote_nc.publish(TARGET_SUBJECT, encoded, headers=headers)
        await remote_nc.publish(MAIN_SUBJECT, encoded, headers=headers)
        return

# Legacy direct-table listener superseded by handle_outbox_cdc_event
# All student events are now unified through the outbox_events table.
# @router.subscriber("academics.enrollment.main.registrar-cvsu.student_info")
# @router.subscriber("academics.enrollment.main.registrar-cvsu.student_profile")
# async def handle_student_cdc_event(msg: dict):
#     ...


