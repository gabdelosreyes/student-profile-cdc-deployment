import os
import json
from faststream import FastStream
from faststream.nats import NatsBroker
from sqlalchemy import create_engine, text
import datetime

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")
MYSQL_URL = os.getenv("MYSQL_URL", "mysql+pymysql://root:@127.0.0.1/registrar-cvsu")

broker = NatsBroker(NATS_URL, user_credentials=NATS_CREDS)
app = FastStream(broker)
engine = create_engine(MYSQL_URL, pool_recycle=3600)

TARGET_SUBJECT = "academics.enrollment.main.student.profile"

def get_student_payload(student_number: str) -> dict:
    query = text("""
        SELECT 
            sp.student_number AS studentNumber,
            si.first_name AS firstName,
            si.last_name AS lastName,
            si.mid_name AS middleName,
            si.suffix AS suffix,
            si.street AS street,
            brgy.name AS barangay,
            mun.name AS municipality,
            prov.name AS province,
            si.date_of_birth AS dateOfBirth,
            si.gender AS gender,
            rel.religion AS religion,
            si.nationality AS citizenship,
            si.marital_status AS status,
            sg.guardian_name AS guardian,
            si.contact_no AS mobilePhone,
            si.email AS email,
            sp.year_admitted AS yearAdmitted,
            sp.sem_admitted AS SemesterAdmitted,
            prog.code AS course
        FROM student_profile sp
        LEFT JOIN student_info si ON sp.student_number = si.student_number
        LEFT JOIN geographic_codes brgy ON si.brgys_id = brgy.id
        LEFT JOIN geographic_codes mun ON si.municipalities_id = mun.id
        LEFT JOIN geographic_codes prov ON si.provinces_id = prov.id
        LEFT JOIN religions rel ON si.religion_id = rel.id
        LEFT JOIN student_guardians sg ON sp.student_number = sg.student_number
        LEFT JOIN programs prog ON sp.program_id = prog.id
        WHERE sp.student_number = :student_number
        LIMIT 1;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"student_number": student_number}).fetchone()
        
    if not result:
        return {"studentNumber": student_number}
        
    row = result._mapping
    
    # Calculate date of birth as days since epoch (Debezium DATE format)
    dob_days = 0
    if row.get("dateOfBirth"):
        delta = row["dateOfBirth"] - datetime.date(1970, 1, 1)
        dob_days = delta.days

    return {
        "studentNumber": row.get("studentNumber"),
        "firstName": row.get("firstName"),
        "lastName": row.get("lastName"),
        "middleName": row.get("middleName"),
        "suffix": row.get("suffix") or "N/A",
        "street": row.get("street"),
        "barangay": row.get("barangay"),
        "municipality": row.get("municipality"),
        "province": row.get("province"),
        "dateOfBirth": dob_days,
        "gender": row.get("gender"),
        "religion": row.get("religion"),
        "citizenship": row.get("citizenship"),
        "status": row.get("status"),
        "guardian": row.get("guardian"),
        "mobilePhone": row.get("mobilePhone"),
        "email": row.get("email"),
        "yearAdmitted": row.get("yearAdmitted"),
        "SemesterAdmitted": row.get("SemesterAdmitted"),
        "course": row.get("course"),
        
        # Legacy/Unmapped fields from the screenshot
        "cardNumber": "0",
        "studentincrement": 0,
        "lastupdate": "N/A",
        "highschool": "N/A",
        "curriculumid": 0,
    }

@broker.subscriber("academics.enrollment.main.registrar-cvsu.student_info")
@broker.subscriber("academics.enrollment.main.registrar-cvsu.student_profile")
async def handle_student_cdc_event(msg: dict):
    payload = msg.get("payload", {})
    if not payload:
        return
        
    op = payload.get("op")
    before = payload.get("before") or {}
    after = payload.get("after") or {}
    
    # Determine student number
    student_number = after.get("student_number") or before.get("student_number")
    if not student_number:
        return
        
    is_deleted = (op == "d")
    
    # Calculate changed fields for this specific event
    changed_fields = []
    if op == "u" and before and after:
        for k, v in after.items():
            if before.get(k) != v:
                changed_fields.append(k)
                
    # Fetch unified payload
    unified = get_student_payload(student_number)
    
    # Inject metadata
    unified["__deleted"] = "true" if is_deleted else "false"
    unified["__changed_fields"] = changed_fields
    
    # Publish to unified target topic
    print(f"Publishing unified student {student_number} to {TARGET_SUBJECT}...")
    await broker.publish(unified, TARGET_SUBJECT)
