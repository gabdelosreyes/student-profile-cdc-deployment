import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from nats.aio.client import Client as NATS
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")

# SQLAlchemy Setup
engine = create_engine("mysql+pymysql://root:@127.0.0.1/registrar-cvsu", pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def clean_address_text(text_val):
    if not text_val: return ""
    clean = text_val.strip()
    if clean in ("N/A", "NA", ""): return ""
    patterns = [
        r'(?i)^City\s+of\s+',
        r'(?i)^Municipality\s+of\s+',
        r'(?i)^Province\s+of\s+',
        r'(?i)^Brgy\.?\s+',
        r'(?i)^Barangay\s+',
        r'(?i)^Purok\s+\d+[\s,-]*',
        r'(?i)\s+City$',
        r'(?i)\s+Municipality$',
        r'(?i)\s+Province$'
    ]
    for p in patterns:
        clean = re.sub(p, '', clean)
    return clean.strip(" \t\n\r\x0B,.-")

def normalize_null(val):
    if not val: return None
    clean = str(val).strip()
    clean_lower = clean.lower()
    
    if clean_lower in ('n/a', 'na', 'none', 'null', '') or all(c in '-.' for c in clean):
        return None
    return clean

GEO_PROVS = []
GEO_MUNIS = []
GEO_BRGYS = []
GEO_REGS = []

# Grouped caches for O(1) subset lookups
MUN_BY_PROV_CODE = {}
BRGY_BY_MUN_CODE = {}

ACADEMIC_YEARS = []
SEMESTERS = []
PROGRAMS = []
RELIGIONS = []

def load_caches():
    global GEO_PROVS, GEO_MUNIS, GEO_BRGYS, GEO_REGS, ACADEMIC_YEARS, SEMESTERS, PROGRAMS, RELIGIONS
    global MUN_BY_PROV_CODE, BRGY_BY_MUN_CODE
    
    print("Loading lookup tables into memory for fast matching...")
    with engine.connect() as conn:
        geo = conn.execute(text("SELECT id, code, name, level FROM geographic_codes")).fetchall()
        GEO_REGS = [g for g in geo if g.level == 'REGION']
        GEO_PROVS = [g for g in geo if g.level == 'PROVINCE']
        GEO_MUNIS = [g for g in geo if g.level in ('CITY', 'MUNICIPALITY')]
        # Sort barangays by length desc to avoid matching short names inside longer words
        GEO_BRGYS = sorted([g for g in geo if g.level == 'BARANGAY'], key=lambda x: len(x.name), reverse=True)
        
        # Build optimized lookup dictionaries
        for m in GEO_MUNIS:
            p_code = m.code[:5]
            if p_code not in MUN_BY_PROV_CODE: MUN_BY_PROV_CODE[p_code] = []
            MUN_BY_PROV_CODE[p_code].append(m)
            
        for b in GEO_BRGYS:
            m_code = b.code[:7]
            if m_code not in BRGY_BY_MUN_CODE: BRGY_BY_MUN_CODE[m_code] = []
            BRGY_BY_MUN_CODE[m_code].append(b)
        
        ACADEMIC_YEARS = conn.execute(text("SELECT id, academic_year, is_active FROM academic_years")).fetchall()
        SEMESTERS = conn.execute(text("SELECT id, semester, is_active FROM semester")).fetchall()
        PROGRAMS = conn.execute(text("SELECT id, code, title FROM programs")).fetchall()
        RELIGIONS = conn.execute(text("SELECT id, religion FROM religions")).fetchall()

def process_batch(msgs):
    profiles = []
    infos = []
    legacies = []
    resolved_legacies = []
    
    # Active defaults
    default_ay = next((ay for ay in ACADEMIC_YEARS if ay.is_active), ACADEMIC_YEARS[-1] if ACADEMIC_YEARS else None)
    default_sem = next((sem for sem in SEMESTERS if sem.is_active), SEMESTERS[0] if SEMESTERS else None)
    default_prog = PROGRAMS[0] if PROGRAMS else None
    default_rel = next((r for r in RELIGIONS if r.religion == 'Roman Catholic'), RELIGIONS[0] if RELIGIONS else None)
    
    for msg in msgs:
        try:
            payload = json.loads(msg.data.decode())
            data = payload.get('after', payload)
        except Exception:
            continue
            
        is_deleted = data.get("__deleted") in ["true", True]
        if is_deleted:
            continue
            
        student_num = str(data.get('studentNumber', '')).strip()
        if not student_num:
            continue
            
        mismatches = {}
        
        # 1. Date Admitted
        raw_ya = str(data.get('yearAdmitted', '')).strip()
        raw_sa = str(data.get('SemesterAdmitted', data.get('semesterAdmitted', ''))).strip()
        
        ay = next((a for a in ACADEMIC_YEARS if a.academic_year == raw_ya), None)
        if not ay:
            ay = default_ay
            if raw_ya: mismatches['yearAdmitted'] = f"Academic Year [{raw_ya}] was not found."
            
        sem = next((s for s in SEMESTERS if str(s.semester).upper().startswith(raw_sa.upper())), None) if raw_sa else None
        if not sem:
            sem = default_sem
            if raw_sa: mismatches['semesterAdmitted'] = f"Semester [{raw_sa}] was not found."
            
        # 2. Date of Birth
        raw_dob = data.get('dateOfBirth')
        date_of_birth = None
        if raw_dob:
            try:
                if str(raw_dob).isdigit():
                    date_of_birth = datetime.fromtimestamp(int(raw_dob) * 86400, timezone.utc).strftime('%Y-%m-%d')
                else:
                    date_of_birth = datetime.strptime(str(raw_dob)[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
            except Exception:
                mismatches['dateOfBirth'] = f"Could not parse Date of Birth [{raw_dob}]."
        else:
            mismatches['dateOfBirth'] = 'Date of birth is missing.'
            
        # 3. Address
        raw_prov = normalize_null(data.get('province')) or ""
        raw_mun = normalize_null(data.get('municipality')) or ""
        raw_brgy = normalize_null(data.get('barangay')) or ""
        raw_street = normalize_null(data.get('street'))
        
        c_prov = clean_address_text(raw_prov)
        c_mun = clean_address_text(raw_mun)
        c_brgy = clean_address_text(raw_brgy)
        
        c_prov_lower = c_prov.lower() if c_prov else ""
        c_mun_lower = c_mun.lower() if c_mun else ""
        c_brgy_lower = c_brgy.lower() if c_brgy else ""
        
        prov = next((p for p in GEO_PROVS if c_prov_lower in p.name.lower()), None) if c_prov else None
        if not prov and not c_prov:
            prov = next((p for p in GEO_PROVS if p.name == 'Cavite'), None)
            
        prov_id = prov.id if prov else None
        reg_id = next((r.id for r in GEO_REGS if prov and r.code.startswith(prov.code[:2])), None)
        if not prov and c_prov:
            mismatches['province'] = f"Province [{raw_prov}] not found."
            
        mun = None
        if c_mun:
            if prov:
                muns_in_prov = MUN_BY_PROV_CODE.get(prov.code[:5], [])
                mun = next((m for m in muns_in_prov if c_mun_lower in m.name.lower()), None)
            if not mun:
                mun = next((m for m in GEO_MUNIS if c_mun_lower in m.name.lower()), None)
                
        mun_id = mun.id if mun else None
        if mun and not prov_id:
            prov = next((p for p in GEO_PROVS if p.code.startswith(mun.code[:5])), None)
            prov_id = prov.id if prov else None
            reg = next((r for r in GEO_REGS if r.code.startswith(mun.code[:2])), None)
            reg_id = reg.id if reg else None
        elif not mun and c_mun:
            mismatches['municipality'] = f"Municipality [{raw_mun}] not found."
            
        brgy = None
        if mun and c_brgy:
            brgys_in_mun = BRGY_BY_MUN_CODE.get(mun.code[:7], [])
            brgy = next((b for b in brgys_in_mun if c_brgy_lower in b.name.lower()), None)
            if not brgy:
                # Substring matching (regex bounded) on the small subset
                for b in brgys_in_mun:
                    if re.search(r'\b' + re.escape(b.name) + r'\b', c_brgy, re.IGNORECASE):
                        brgy = b
                        break
        brgy_id = brgy.id if brgy else None
        if not brgy and c_brgy:
            mismatches['barangay'] = f"Barangay [{raw_brgy}] not found."
            
        # 4. Program
        raw_prog = normalize_null(data.get('course')) or ""
        prog = None
        if raw_prog:
            prog = next((p for p in PROGRAMS if p.code == raw_prog or raw_prog.lower() in p.title.lower()), None)
        if not prog and raw_prog:
            mismatches['course'] = f"Course [{raw_prog}] not found."
            
        # 5. Religion
        raw_rel = normalize_null(data.get('religion')) or ""
        rel = next((r for r in RELIGIONS if raw_rel.lower() in r.religion.lower()), None) if raw_rel else None
        if not rel and raw_rel:
            mismatches['religion'] = f"Religion [{raw_rel}] not found."
            
        # Profile Data
        profiles.append({
            "student_number": student_num,
            "program_id": prog.id if prog else None,
            "major_id": None,
            "year_level": 1,
            "semester_id": sem.id if sem else 1,
            "academic_year_id": ay.id if ay else 1,
            "student_type_id": 1,
            "student_status_id": 1,
            "campus_id": 1,
            "year_admitted": raw_ya or None,
            "sem_admitted": raw_sa or None
        })
        
        # Info Data
        first = (normalize_null(data.get('firstName')) or 'UNKNOWN').upper()
        last = (normalize_null(data.get('lastName')) or 'UNKNOWN').upper()
        mid = normalize_null(data.get('middleName'))
        if mid: mid = mid.upper()
        suf = normalize_null(data.get('suffix'))
        if suf: suf = suf.upper()
        
        gender_raw = normalize_null(data.get('gender'))
        gender = gender_raw.upper() if gender_raw else 'MALE'
        if gender not in ('MALE', 'FEMALE'): gender = 'MALE'
        
        marital_raw = normalize_null(data.get('status'))
        marital = marital_raw.upper() if marital_raw else 'SINGLE'
        
        nat_raw = normalize_null(data.get('citizenship'))
        nat = nat_raw.upper() if nat_raw else 'FILIPINO'
        
        raw_email = normalize_null(data.get('email'))
        email = None
        cvsu_email = None
        if raw_email:
            if raw_email.lower().endswith('@cvsu.edu.ph'):
                cvsu_email = raw_email
            else:
                email = raw_email
                
        contact = normalize_null(data.get('mobilePhone'))
        if contact:
            if contact.startswith('+63'):
                pass
            elif contact.startswith('0'):
                contact = '+63' + contact[1:]
            else:
                contact = '+63' + contact
        
        infos.append({
            "student_number": student_num,
            "first_name": first,
            "mid_name": mid,
            "last_name": last,
            "suffix": suf,
            "gender": gender,
            "regions_id": reg_id,
            "provinces_id": prov_id,
            "municipalities_id": mun_id,
            "brgys_id": brgy_id,
            "street": raw_street.upper() if raw_street else None,
            "date_of_birth": date_of_birth,
            "religion_id": rel.id if rel else None,
            "nationality": nat,
            "marital_status": marital,
            "email": email,
            "cvsu_email": cvsu_email,
            "contact_no": contact,
            "is_pwd": 0
        })
        
        if mismatches:
            legacies.append({
                "student_number": student_num,
                "legacy_street": raw_street if 'street' in mismatches else None,
                "legacy_barangay": raw_brgy if 'barangay' in mismatches else None,
                "legacy_municipality": raw_mun if 'municipality' in mismatches else None,
                "legacy_province": raw_prov if 'province' in mismatches else None,
                "legacy_course": raw_prog if 'course' in mismatches else None,
                "legacy_religion": raw_rel if 'religion' in mismatches else None,
                "legacy_payload": json.dumps(data),
                "mismatch_fields": json.dumps(mismatches),
                "mismatch_summary": "; ".join(mismatches.values()),
                "is_resolved": 0
            })
        else:
            resolved_legacies.append(student_num)
            
    # Bulk execute
    with SessionLocal() as session:
        if profiles:
            session.execute(text("""
                INSERT INTO student_profile (student_number, program_id, major_id, year_level, semester_id, academic_year_id, student_type_id, student_status_id, year_admitted, sem_admitted, campus_id)
                VALUES (:student_number, :program_id, :major_id, :year_level, :semester_id, :academic_year_id, :student_type_id, :student_status_id, :year_admitted, :sem_admitted, :campus_id)
                ON DUPLICATE KEY UPDATE 
                program_id=VALUES(program_id), major_id=VALUES(major_id), year_level=VALUES(year_level), semester_id=VALUES(semester_id), academic_year_id=VALUES(academic_year_id), student_type_id=VALUES(student_type_id), student_status_id=VALUES(student_status_id), year_admitted=VALUES(year_admitted), sem_admitted=VALUES(sem_admitted), campus_id=VALUES(campus_id)
            """), profiles)
            
        if infos:
            session.execute(text("""
                INSERT INTO student_info (student_number, first_name, mid_name, last_name, suffix, gender, regions_id, provinces_id, municipalities_id, brgys_id, street, date_of_birth, religion_id, nationality, marital_status, email, cvsu_email, contact_no, is_pwd)
                VALUES (:student_number, :first_name, :mid_name, :last_name, :suffix, :gender, :regions_id, :provinces_id, :municipalities_id, :brgys_id, :street, :date_of_birth, :religion_id, :nationality, :marital_status, :email, :cvsu_email, :contact_no, :is_pwd)
                ON DUPLICATE KEY UPDATE 
                first_name=VALUES(first_name), mid_name=VALUES(mid_name), last_name=VALUES(last_name), suffix=VALUES(suffix), gender=VALUES(gender), regions_id=VALUES(regions_id), provinces_id=VALUES(provinces_id), municipalities_id=VALUES(municipalities_id), brgys_id=VALUES(brgys_id), street=VALUES(street), date_of_birth=VALUES(date_of_birth), religion_id=VALUES(religion_id), nationality=VALUES(nationality), marital_status=VALUES(marital_status), email=VALUES(email), cvsu_email=VALUES(cvsu_email), contact_no=VALUES(contact_no), is_pwd=VALUES(is_pwd)
            """), infos)
            
        if legacies:
            session.execute(text("""
                INSERT INTO legacy_student_info (student_number, legacy_street, legacy_barangay, legacy_municipality, legacy_province, legacy_course, legacy_religion, legacy_payload, mismatch_fields, mismatch_summary, is_resolved)
                VALUES (:student_number, :legacy_street, :legacy_barangay, :legacy_municipality, :legacy_province, :legacy_course, :legacy_religion, :legacy_payload, :mismatch_fields, :mismatch_summary, :is_resolved)
                ON DUPLICATE KEY UPDATE 
                legacy_payload=VALUES(legacy_payload), mismatch_fields=VALUES(mismatch_fields), mismatch_summary=VALUES(mismatch_summary)
            """), legacies)
            
        if resolved_legacies:
            # Delete any legacy records that are now perfectly matched
            # Split into chunks of 500 to avoid huge queries
            for i in range(0, len(resolved_legacies), 10000):
                chunk = resolved_legacies[i:i+10000]
                placeholders = ','.join([f"'{s}'" for s in chunk])
                session.execute(text(f"DELETE FROM legacy_student_info WHERE student_number IN ({placeholders})"))
                
        session.commit()

db_semaphore = None

async def process_and_ack(msgs):
    try:
        # 1. Wait for a free DB connection slot (max 5 concurrent batches)
        async with db_semaphore:
            await asyncio.to_thread(process_batch, msgs)
            
        # 2. Fire ACKs concurrently in background chunks
        for i in range(0, len(msgs), 500):
            await asyncio.gather(*(msg.ack() for msg in msgs[i:i+500]), return_exceptions=True)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Background task failed: {e}")

async def main():
    global db_semaphore
    db_semaphore = asyncio.Semaphore(1)
    
    load_caches()
    
    nc = NATS()
    print(f"Connecting to NATS at {NATS_URL}...")
    await nc.connect(NATS_URL, user_credentials=NATS_CREDS)
    js = nc.jetstream()
    
    print("Setting up hyper-fast pull consumer...")
    try:
        sub = await js.pull_subscribe("academics.enrollment.main.profile", f"mass_sync_consumer_{int(time.time())}", stream="EnrollmentData")
    except Exception as e:
        print(f"Failed to subscribe: {e}")
        await nc.close()
        return
        
    print("Subscription successful. Starting fast bulk sync!")
    synced_count = 0
    
    while True:
        try:
            # Fetch as many as NATS allows (usually capped at 1000-5000 by server)
            msgs = await sub.fetch(5000, timeout=2.0)
            if not msgs:
                continue
                
            synced_count += len(msgs)
            print(f"Fetched {synced_count} total records. Flushing to DB in background...")
            
            # FIRE AND FORGET:
            # We process and ACK this batch in the background so we can immediately fetch the next batch!
            asyncio.create_task(process_and_ack(msgs))
            
        except (TimeoutError, Exception) as e:
            if "timeout" in str(e).lower() or isinstance(e, TimeoutError):
                pass
            else:
                import traceback
                traceback.print_exc()
                print(f"Error fetching batch: {e}")

if __name__ == '__main__':
    asyncio.run(main())
