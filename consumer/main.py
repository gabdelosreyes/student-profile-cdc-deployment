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
from faststream.nats import JStream
from faststream.nats.fastapi import NatsRouter
from sqlalchemy import create_engine, text
import datetime

from config import config

# Listen to LOCAL NATS where Debezium is publishing (no auth)
router = NatsRouter(config.local_nats_url)
engine = create_engine(config.mysql_url, pool_recycle=3600)

from nats.aio.client import Client as RawNATS
remote_nc = RawNATS()


async def ensure_local_cdc_stream():
    local_nc = RawNATS()
    try:
        print(f"Ensuring '{config.local_cdc_stream}' exists on local NATS ({config.local_nats_url})...")
        await local_nc.connect(config.local_nats_url)
        js = local_nc.jetstream()
        subjects = config.local_cdc_stream_subjects
        try:
            await js.add_stream(name=config.local_cdc_stream, subjects=subjects)
            print(f"Stream '{config.local_cdc_stream}' created successfully.")
        except Exception:
            try:
                await js.update_stream(name=config.local_cdc_stream, subjects=subjects)
                print(f"Stream '{config.local_cdc_stream}' verified/updated.")
            except Exception:
                pass
    except Exception as e:
        print(f"Warning: Could not connect to local NATS to verify {config.local_cdc_stream}: {e}")
    finally:
        if local_nc.is_connected:
            await local_nc.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await ensure_local_cdc_stream()
    print(f"Connecting to remote NATS {config.remote_nats_url}...")
    await remote_nc.connect(config.remote_nats_url, user_credentials=config.nats_creds)
    print("Connected to remote NATS successfully!")
    async with router.lifespan_context(app):
        yield
    if remote_nc.is_connected:
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

@router.subscriber(
    config.local_cdc_consume_subject,
    stream=JStream(name=config.local_cdc_stream, declare=False),
    durable=config.local_cdc_consumer_name,
    no_reply=True,
)
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

    # Route document request responses to academics.enrollment.documents.students.response.created
    if event_type == "document.request.response.created" or (
        payload.get("status") == "for_release" and "control_number" in payload
    ):
        headers = {
            "content-type": "application/json",
            "eventType": str(event_type or "document.request.response.created"),
            "Nats-Msg-Id": str(event_id),
        }
        control_num = payload.get("control_number")
        print(f"Relaying document request response {control_num} (eventId={event_id}) to remote {config.doc_response_subject}...")
        encoded = json.dumps(payload).encode("utf-8")
        await remote_nc.publish(config.doc_response_subject, encoded, headers=headers)
        return

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

        print(f"Relaying live student change {student_number} (op={op}, eventType={event_type}, eventId={event_id}) with headers to remote {config.target_student_subject} and {config.main_student_subject}...")
        encoded = json.dumps(payload).encode('utf-8')
        await remote_nc.publish(config.target_student_subject, encoded, headers=headers)
        await remote_nc.publish(config.main_student_subject, encoded, headers=headers)
        return

# Legacy direct-table listener superseded by handle_outbox_cdc_event
# All student events are now unified through the outbox_events table.
# @router.subscriber("academics.enrollment.main.registrar-cvsu.student_info")
# @router.subscriber("academics.enrollment.main.registrar-cvsu.student_profile")
# async def handle_student_cdc_event(msg: dict):
#     ...


