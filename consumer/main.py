"""
FastStream CDC Consumer: main.py
Description: This FastStream application bridges Debezium outbox CDC events from the local
NATS stream ('LocalCDCStream') to remote NATS JetStream, publishing to dynamically constructed
enterprise subjects: <domain>.<system>.<service>.<entity-subentity?>.<action?>
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
        event_type = msg.get("eventType") or msg.get("type")
        aggregate_type = msg.get("aggregateType") or msg.get("aggregatetype")
        payload = msg["payload"]
    else:
        payload = dict(msg)
        event_id = payload.pop("eventId", None) or payload.pop("id", None) or str(uuid.uuid4())
        event_type = payload.pop("eventType", None) or payload.pop("type", None)
        aggregate_type = payload.pop("aggregateType", None) or payload.pop("aggregatetype", None)

    # Infer aggregate_type if not explicitly set in envelope
    if not aggregate_type:
        if "control_number" in payload or (event_type in ("document.request.response.created", "updated") and "id" in payload):
            aggregate_type = "request.document"
        elif "student_number" in payload or "aggregate_id" in payload:
            aggregate_type = "record.student-profile"
        else:
            aggregate_type = "record.student-profile"

    # Infer action event_type if missing
    if not event_type:
        op = payload.get("op", "u")
        event_type = "created" if op == "c" else "updated" if op == "u" else "deleted"

    # Dynamic 3-tier enterprise subject assembly (<domain>.<system>.<service>.<entity-subentity?>.<action?>)
    target_subject = config.build_outbound_subject(aggregate_type, event_type)

    headers = {
        "content-type": "application/json",
        "eventType": str(event_type),
        "aggregateType": str(aggregate_type),
        "Nats-Msg-Id": str(event_id),
    }

    encoded = json.dumps(payload).encode("utf-8")
    print(f"Relaying live {aggregate_type} change (eventId={event_id}, action={event_type}) to remote subject '{target_subject}'...")
    await remote_nc.publish(target_subject, encoded, headers=headers)


