"""
Document Request Ingestion Worker: sync_document_requests.py
Description: This daemon subscribes to NATS JetStream ('DOCUMENTS' stream on subject
'academics.enrollment.documents.students.request.created') using a durable consumer.
It auto-provisions missing student profiles, resolves document types and purposes,
generates atomic registrar control numbers, and ingests document requests into
the MySQL database with status 'waiting_for_payment'.
"""
import asyncio
import json
import os
import re
import time
from datetime import datetime
from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\student-profile-cdc-deployment\univ-reg.creds")
MYSQL_URL = os.getenv("MYSQL_URL", "mysql+pymysql://root:@127.0.0.1/registrar-cvsu")

STREAM_NAME = "DOCUMENTS"
SUBJECT_FILTER = "academics.enrollment.documents.students.request.created"
DURABLE_CONSUMER = "registrar_doc_request_sync"

engine = create_engine(MYSQL_URL, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def parse_full_name(full_name: str) -> dict:
    """Splits a full name string into first_name, mid_name, last_name, and suffix."""
    if not full_name:
        return {"first_name": "", "mid_name": None, "last_name": "", "suffix": None}
    
    tokens = [t.strip() for t in full_name.strip().split() if t.strip()]
    if not tokens:
        return {"first_name": "", "mid_name": None, "last_name": "", "suffix": None}
    
    # Check for common suffixes at the end
    known_suffixes = {"JR", "JR.", "SR", "SR.", "II", "III", "IV", "V"}
    suffix = None
    if len(tokens) > 1 and tokens[-1].upper() in known_suffixes:
        suffix = tokens.pop()
        
    if len(tokens) == 1:
        return {"first_name": tokens[0], "mid_name": None, "last_name": "", "suffix": suffix}
    if len(tokens) == 2:
        return {"first_name": tokens[0], "mid_name": None, "last_name": tokens[1], "suffix": suffix}
    if len(tokens) == 3:
        return {"first_name": tokens[0], "mid_name": tokens[1], "last_name": tokens[2], "suffix": suffix}
    
    # 4+ tokens: assume first tokens are first name, second-to-last is middle name, last is last name
    return {
        "first_name": " ".join(tokens[:-2]),
        "mid_name": tokens[-2],
        "last_name": tokens[-1],
        "suffix": suffix,
    }

def generate_next_control_number(conn, doc_type_id: int, campus_id: int = 1) -> str:
    """Generates the next control number in format {YY}{MM}{DD}{campus:02d}{seq:05d}."""
    now = datetime.now()
    date_prefix = now.strftime("%y%m%d")
    campus_prefix = f"{campus_id:02d}"

    # Lock table rows for current date to compute sequence atomically
    query_latest = text("""
        SELECT control_number 
        FROM document_requests 
        WHERE DATE(created_at) = CURRENT_DATE() 
        ORDER BY id DESC 
        LIMIT 1 
        FOR UPDATE;
    """)
    latest = conn.execute(query_latest).fetchone()

    next_sequence = 1
    if latest and latest[0]:
        last_control = str(latest[0])
        last_num = last_control[-5:]
        if last_num.isdigit():
            next_sequence = int(last_num) + 1

    sequence_str = f"{next_sequence:05d}"
    base_control = f"{date_prefix}{campus_prefix}{sequence_str}"

    doc_type_code = conn.execute(
        text("SELECT code FROM document_types WHERE id = :id LIMIT 1"),
        {"id": doc_type_id}
    ).scalar()

    if doc_type_code and doc_type_code.upper() == "HON_DISMISSAL":
        return f"CVSU-{base_control}"

    return base_control

def ensure_student_exists(conn, payload: dict) -> None:
    """Ensures student_profile and student_info rows exist to satisfy foreign keys."""
    student_number = str(payload.get("student_number", "")).strip()
    if not student_number:
        return

    exists = conn.execute(
        text("SELECT id FROM student_profile WHERE student_number = :sn LIMIT 1;"),
        {"sn": student_number}
    ).fetchone()

    if exists:
        return

    # Parse names
    names = parse_full_name(payload.get("full_name", ""))
    campus_id = 1
    grad_date = payload.get("graduation_date")
    if grad_date:
        try:
            grad_date = datetime.strptime(str(grad_date).strip(), "%Y-%m-%d").date()
        except Exception:
            grad_date = None

    # Insert stub student_profile
    conn.execute(
        text("""
            INSERT INTO student_profile (student_number, campus_id, date_graduated)
            VALUES (:student_number, :campus_id, :date_graduated);
        """),
        {
            "student_number": student_number,
            "campus_id": campus_id,
            "date_graduated": grad_date,
        }
    )

    # Insert student_info
    conn.execute(
        text("""
            INSERT INTO student_info (
                student_number, first_name, mid_name, last_name, suffix,
                gender, email, contact_no, street
            ) VALUES (
                :student_number, :first_name, :mid_name, :last_name, :suffix,
                :gender, :email, :contact_no, :street
            );
        """),
        {
            "student_number": student_number,
            "first_name": names["first_name"],
            "mid_name": names["mid_name"],
            "last_name": names["last_name"],
            "suffix": names["suffix"],
            "gender": payload.get("sex"),
            "email": payload.get("email"),
            "contact_no": payload.get("contact_number"),
            "street": payload.get("complete_address"),
        }
    )
    print(f"Auto-provisioned student profile and info for {student_number} ({payload.get('full_name')}).")

def resolve_document_type_id(conn, doc_type_code: str) -> int:
    """Looks up document_type_id by code (e.g. 'TOR') or fallback."""
    code_clean = doc_type_code.strip() if doc_type_code else "TOR"
    row = conn.execute(
        text("SELECT id FROM document_types WHERE UPPER(code) = UPPER(:code) LIMIT 1;"),
        {"code": code_clean}
    ).fetchone()
    if row:
        return int(row[0])

    # Try name match
    row_name = conn.execute(
        text("SELECT id FROM document_types WHERE UPPER(name) LIKE UPPER(:pattern) LIMIT 1;"),
        {"pattern": f"%{code_clean}%"}
    ).fetchone()
    if row_name:
        return int(row_name[0])

    # Fallback to first available active document type
    first_active = conn.execute(
        text("SELECT id FROM document_types WHERE is_active = 1 LIMIT 1;")
    ).scalar()
    return int(first_active or 1)

def resolve_purpose_id(conn, purpose_text: str) -> int:
    """Resolves purpose_id from document_purpose, inserting dynamically if missing."""
    clean_purpose = purpose_text.strip() if purpose_text else "For General Purpose"
    row = conn.execute(
        text("SELECT id FROM document_purpose WHERE LOWER(TRIM(name)) = LOWER(TRIM(:name)) LIMIT 1;"),
        {"name": clean_purpose}
    ).fetchone()
    if row:
        return int(row[0])

    # Insert new purpose record
    res = conn.execute(
        text("INSERT INTO document_purpose (name, created_at, updated_at) VALUES (:name, NOW(), NOW());"),
        {"name": clean_purpose}
    )
    return int(res.lastrowid)

def get_active_term(conn) -> tuple:
    """Returns (academic_year_id, semester_id)."""
    ay_id = conn.execute(
        text("SELECT id FROM academic_years WHERE is_active = 1 LIMIT 1;")
    ).scalar()
    sem_id = conn.execute(
        text("SELECT id FROM semester WHERE is_active = 1 LIMIT 1;")
    ).scalar()
    return ay_id, sem_id

def process_document_request_payload(payload: dict) -> list:
    """
    Ingests an inbound document request payload into MySQL database.
    Returns a list of created document request control numbers.
    """
    external_id = str(payload.get("id", "")).strip()
    student_number = str(payload.get("student_number", "")).strip()
    purpose_str = payload.get("purpose", "")
    documents = payload.get("documents", [])

    if not documents:
        # Default to a single generic request if documents array is empty
        documents = [{"copies": 1, "document_type": "TOR"}]

    created_controls = []

    with engine.begin() as conn:
        # 1. Ensure student profile and info exist
        ensure_student_exists(conn, payload)

        # 2. Resolve term and purpose
        ay_id, sem_id = get_active_term(conn)
        purpose_id = resolve_purpose_id(conn, purpose_str)
        signatory_id = conn.execute(
            text("SELECT id FROM signatories WHERE is_active = 1 AND campus_id = 1 LIMIT 1;")
        ).scalar()

        # 3. Process each document item
        for item in documents:
            doc_type_code = item.get("document_type", "TOR")
            copies = int(item.get("copies", 1))
            doc_type_id = resolve_document_type_id(conn, doc_type_code)

            # Idempotency check: check if already exists
            existing = conn.execute(
                text("""
                    SELECT control_number FROM document_requests 
                    WHERE JSON_UNQUOTE(JSON_EXTRACT(details, '$.external_request_id')) = :ext_id 
                    AND document_type_id = :doc_type_id 
                    LIMIT 1;
                """),
                {"ext_id": external_id, "doc_type_id": doc_type_id}
            ).fetchone()

            if existing:
                print(f"Skipping duplicate document request {external_id} ({doc_type_code}) - already exists as {existing[0]}.")
                created_controls.append(existing[0])
                continue

            # Generate next control number
            control_number = generate_next_control_number(conn, doc_type_id, campus_id=1)

            details_data = {
                "external_request_id": external_id,
                "document_type_code": doc_type_code,
                "course_major": payload.get("course_major"),
                "high_school_name": payload.get("high_school_name"),
                "high_school_address": payload.get("high_school_address"),
                "attendance_entry_date": payload.get("attendance_entry_date"),
                "senior_high_school_name": payload.get("senior_high_school_name"),
                "senior_high_school_address": payload.get("senior_high_school_address"),
                "original_payload": payload,
            }

            conn.execute(
                text("""
                    INSERT INTO document_requests (
                        control_number, student_number, campus_id, document_type_id,
                        purpose_id, copies_requested, academic_year_id, semester_id,
                        status, requested_at, details, signatory_id, created_at, updated_at
                    ) VALUES (
                        :control_number, :student_number, :campus_id, :document_type_id,
                        :purpose_id, :copies_requested, :academic_year_id, :semester_id,
                        'waiting_for_payment', NOW(), :details, :signatory_id, NOW(), NOW()
                    );
                """),
                {
                    "control_number": control_number,
                    "student_number": student_number,
                    "campus_id": 1,
                    "document_type_id": doc_type_id,
                    "purpose_id": purpose_id,
                    "copies_requested": copies,
                    "academic_year_id": ay_id,
                    "semester_id": sem_id,
                    "details": json.dumps(details_data),
                    "signatory_id": signatory_id,
                }
            )
            print(f"Created document request {control_number} for student {student_number} (Type: {doc_type_code}, External ID: {external_id}).")
            created_controls.append(control_number)

    return created_controls

async def main():
    print(f"Starting Document Request Ingestion Worker daemon...")
    print(f"Target NATS: {NATS_URL}, Stream: {STREAM_NAME}, Subject: {SUBJECT_FILTER}")

    while True:
        nc = NATS()
        try:
            print(f"Connecting to NATS at {NATS_URL}...")
            await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
            js = nc.jetstream()

            print(f"Subscribing with durable consumer '{DURABLE_CONSUMER}' on stream '{STREAM_NAME}'...")
            sub = await js.pull_subscribe(
                SUBJECT_FILTER,
                durable=DURABLE_CONSUMER,
                stream=STREAM_NAME,
                config=ConsumerConfig(
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=60,
                ),
            )
            print(f"Durable subscription '{DURABLE_CONSUMER}' active. Polling messages...")

            while True:
                try:
                    msgs = await sub.fetch(50, timeout=2.0)
                    if not msgs:
                        await asyncio.sleep(1.0)
                        continue

                    print(f"Fetched {len(msgs)} document request message(s)...")
                    for msg in msgs:
                        try:
                            raw = msg.data.decode("utf-8")
                            payload = json.loads(raw)
                            created = process_document_request_payload(payload)
                            print(f"Processed message {msg.metadata.sequence.stream}: created {created}")
                            await msg.ack()
                        except Exception as item_err:
                            print(f"Error processing document request message {msg.metadata.sequence.stream}: {item_err}")
                            # ACK or handle dead letter to prevent poison pill loops
                            await msg.ack()
                except (TimeoutError, Exception) as e:
                    if "timeout" in str(e).lower() or isinstance(e, TimeoutError):
                        await asyncio.sleep(1.0)
                    else:
                        print(f"Fetch notice: {e}")
                        await asyncio.sleep(2.0)
        except Exception as e:
            print(f"Connection error in document request sync: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5.0)
        finally:
            try:
                await nc.close()
            except Exception:
                pass

if __name__ == "__main__":
    asyncio.run(main())
