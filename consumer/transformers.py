import json
import re
from datetime import datetime, date, timezone
from typing import Dict, Any, Tuple, Optional, List
from sqlalchemy import text

from config import config


def clean_address_text(text_val: Optional[str]) -> str:
    """Cleans and standardizes raw address text by stripping common prefixes/suffixes."""
    if not text_val:
        return ""
    clean = str(text_val).strip()
    if clean in ("N/A", "NA", ""):
        return ""
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


def normalize_null(val: Any) -> Optional[str]:
    """Converts common null representations to None, otherwise returns stripped string."""
    if val is None:
        return None
    clean = str(val).strip()
    clean_lower = clean.lower()
    if clean_lower in ('n/a', 'na', 'none', 'null', '') or all(c in '-.' for c in clean):
        return None
    return clean


class LookupCaches:
    """Thread-safe in-memory cache for geographic and registrar reference tables."""
    def __init__(self):
        self.geo_regs = []
        self.geo_provs = []
        self.geo_munis = []
        self.geo_brgys = []
        self.mun_by_prov_code = {}
        self.brgy_by_mun_code = {}
        self.academic_years = []
        self.semesters = []
        self.programs = []
        self.religions = []
        self.default_ay = None
        self.default_sem = None
        self.default_prog = None
        self.default_rel = None

    def load(self, engine):
        print("Loading PSGC and registrar reference lookup tables into memory...")
        with engine.connect() as conn:
            geo = conn.execute(text("SELECT id, code, name, level FROM geographic_codes")).fetchall()
            self.geo_regs = [g for g in geo if g.level == 'REGION']
            self.geo_provs = [g for g in geo if g.level == 'PROVINCE']
            self.geo_munis = [g for g in geo if g.level in ('CITY', 'MUNICIPALITY')]
            # Sort barangays by length descending to prevent shorter names matching inside longer names
            self.geo_brgys = sorted([g for g in geo if g.level == 'BARANGAY'], key=lambda x: len(x.name), reverse=True)

            self.mun_by_prov_code = {}
            for m in self.geo_munis:
                p_code = m.code[:5]
                if p_code not in self.mun_by_prov_code:
                    self.mun_by_prov_code[p_code] = []
                self.mun_by_prov_code[p_code].append(m)

            self.brgy_by_mun_code = {}
            for b in self.geo_brgys:
                m_code = b.code[:7]
                if m_code not in self.brgy_by_mun_code:
                    self.brgy_by_mun_code[m_code] = []
                self.brgy_by_mun_code[m_code].append(b)

            self.academic_years = conn.execute(text("SELECT id, academic_year, is_active FROM academic_years")).fetchall()
            self.semesters = conn.execute(text("SELECT id, semester, is_active FROM semester")).fetchall()
            self.programs = conn.execute(text("SELECT id, code, title FROM programs")).fetchall()
            self.religions = conn.execute(text("SELECT id, religion FROM religions")).fetchall()

            self.default_ay = next((ay for ay in self.academic_years if ay.is_active), self.academic_years[-1] if self.academic_years else None)
            self.default_sem = next((sem for sem in self.semesters if sem.is_active), self.semesters[0] if self.semesters else None)
            self.default_prog = self.programs[0] if self.programs else None
            self.default_rel = next((r for r in self.religions if r.religion == 'Roman Catholic'), self.religions[0] if self.religions else None)

        print(f"Caches loaded: {len(self.geo_provs)} provinces, {len(self.geo_munis)} municipalities, {len(self.geo_brgys)} barangays, {len(self.programs)} programs.")


def transform_legacy_student(data: Dict[str, Any], caches: LookupCaches, default_campus_id: int = 1) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
    """
    Transforms a single raw legacy student record into normalized student_profile and student_info dicts.
    Returns: (profile_data, info_data, legacy_mismatch_data, resolved_student_number)
    """
    student_num = str(data.get('studentNumber', '')).strip()
    if not student_num:
        return None, None, None, None

    mismatches = {}

    # 1. Date / Term Admitted
    raw_ya = str(data.get('yearAdmitted', '')).strip()
    raw_sa = str(data.get('SemesterAdmitted', data.get('semesterAdmitted', ''))).strip()

    ay = next((a for a in caches.academic_years if a.academic_year == raw_ya), None)
    if not ay:
        ay = caches.default_ay
        if raw_ya:
            mismatches['yearAdmitted'] = f"Academic Year [{raw_ya}] was not found."

    sem = next((s for s in caches.semesters if str(s.semester).upper().startswith(raw_sa.upper())), None) if raw_sa else None
    if not sem:
        sem = caches.default_sem
        if raw_sa:
            mismatches['semesterAdmitted'] = f"Semester [{raw_sa}] was not found."

    # 2. Date of Birth
    raw_dob = data.get('dateOfBirth')
    date_of_birth = None
    if raw_dob:
        try:
            if isinstance(raw_dob, (datetime, date)):
                date_of_birth = raw_dob.strftime('%Y-%m-%d')
            elif str(raw_dob).isdigit():
                date_of_birth = datetime.fromtimestamp(int(raw_dob) * 86400, timezone.utc).strftime('%Y-%m-%d')
            else:
                date_of_birth = datetime.strptime(str(raw_dob)[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
        except Exception:
            mismatches['dateOfBirth'] = f"Could not parse Date of Birth [{raw_dob}]."
    else:
        mismatches['dateOfBirth'] = 'Date of birth is missing.'

    # 3. PSGC Address Resolution
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

    prov = next((p for p in caches.geo_provs if c_prov_lower in p.name.lower()), None) if c_prov else None
    if not prov and not c_prov:
        prov = next((p for p in caches.geo_provs if p.name == 'Cavite'), None)

    prov_id = prov.id if prov else None
    reg_id = next((r.id for r in caches.geo_regs if prov and r.code.startswith(prov.code[:2])), None)
    if not prov and c_prov:
        mismatches['province'] = f"Province [{raw_prov}] not found."

    mun = None
    if c_mun:
        if prov:
            muns_in_prov = caches.mun_by_prov_code.get(prov.code[:5], [])
            mun = next((m for m in muns_in_prov if c_mun_lower in m.name.lower()), None)
        if not mun:
            mun = next((m for m in caches.geo_munis if c_mun_lower in m.name.lower()), None)

    mun_id = mun.id if mun else None
    if mun and not prov_id:
        prov = next((p for p in caches.geo_provs if p.code.startswith(mun.code[:5])), None)
        prov_id = prov.id if prov else None
        reg = next((r for r in caches.geo_regs if r.code.startswith(mun.code[:2])), None)
        reg_id = reg.id if reg else None
    elif not mun and c_mun:
        mismatches['municipality'] = f"Municipality [{raw_mun}] not found."

    brgy = None
    if mun and c_brgy:
        brgys_in_mun = caches.brgy_by_mun_code.get(mun.code[:7], [])
        brgy = next((b for b in brgys_in_mun if c_brgy_lower in b.name.lower()), None)
        if not brgy:
            # Word boundary regex search on municipality barangays
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
        prog = next((p for p in caches.programs if p.code == raw_prog or raw_prog.lower() in p.title.lower()), None)
    if not prog and raw_prog:
        mismatches['course'] = f"Course [{raw_prog}] not found."

    # 5. Religion
    raw_rel = normalize_null(data.get('religion')) or ""
    rel = next((r for r in caches.religions if raw_rel.lower() in r.religion.lower()), None) if raw_rel else None
    if not rel and raw_rel:
        mismatches['religion'] = f"Religion [{raw_rel}] not found."

    # Construct Profile Data
    profile_data = {
        "student_number": student_num,
        "program_id": prog.id if prog else None,
        "major_id": None,
        "year_level": None,
        "semester_id": sem.id if sem else None,
        "academic_year_id": ay.id if ay else None,
        "student_type_id": None,
        "student_status_id": None,
        "campus_id": default_campus_id,
        "year_admitted": raw_ya or None,
        "sem_admitted": raw_sa or None
    }

    # Biographical Fields & Uppercase Formatting
    first_raw = normalize_null(data.get('firstName'))
    first = first_raw.upper() if first_raw else None

    last_raw = normalize_null(data.get('lastName'))
    last = last_raw.upper() if last_raw else None

    mid = normalize_null(data.get('middleName'))
    if mid:
        mid = mid.upper()

    suf = normalize_null(data.get('suffix'))
    if suf:
        suf = suf.upper()

    gender_raw = normalize_null(data.get('gender'))
    gender = gender_raw.upper() if gender_raw else None

    marital_raw = normalize_null(data.get('status'))
    marital = marital_raw.upper() if marital_raw else None

    nat_raw = normalize_null(data.get('citizenship'))
    nat = nat_raw.upper() if nat_raw else None

    # Emails: Check raw_email and gsuite_email
    raw_email = normalize_null(data.get('email'))
    gsuite_email = normalize_null(data.get('gsuite_email') or data.get('cvsu_email'))
    email = None
    cvsu_email = None

    if gsuite_email and gsuite_email.lower().endswith('@cvsu.edu.ph'):
        cvsu_email = gsuite_email

    if raw_email:
        if raw_email.lower().endswith('@cvsu.edu.ph'):
            if not cvsu_email:
                cvsu_email = raw_email
        else:
            email = raw_email

    # Contact Number Format (+63)
    contact = normalize_null(data.get('mobilePhone') or data.get('contact_no'))
    if contact:
        if contact.startswith('+63'):
            pass
        elif contact.startswith('0'):
            contact = '+63' + contact[1:]
        else:
            contact = '+63' + contact

    info_data = {
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
    }

    legacy_data = None
    resolved_num = None

    if mismatches:
        legacy_data = {
            "student_number": student_num,
            "legacy_street": raw_street if 'street' in mismatches else None,
            "legacy_barangay": raw_brgy if 'barangay' in mismatches else None,
            "legacy_municipality": raw_mun if 'municipality' in mismatches else None,
            "legacy_province": raw_prov if 'province' in mismatches else None,
            "legacy_course": raw_prog if 'course' in mismatches else None,
            "legacy_religion": raw_rel if 'religion' in mismatches else None,
            "legacy_payload": json.dumps(data, default=str),
            "mismatch_fields": json.dumps(mismatches),
            "mismatch_summary": "; ".join(mismatches.values()),
            "is_resolved": 0
        }
    else:
        resolved_num = student_num

    return profile_data, info_data, legacy_data, resolved_num
