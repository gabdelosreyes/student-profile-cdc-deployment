import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
from nats.js.errors import NotFoundError
from sqlalchemy import create_engine, text

from config import config
from transformers import LookupCaches, transform_legacy_student

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("legacy_student_sync")

class LegacyStudentSyncWorker:
    def __init__(self):
        self.running = True
        self.nc = NATS()
        self.engine = create_engine(config.mysql_url, pool_recycle=3600)
        self.caches = LookupCaches()

        consumer_cfg = config.get_consumer("legacy_student_sync")
        self.stream_name = consumer_cfg.get("stream")
        self.filter_subject = consumer_cfg.get("subject")
        self.durable_name = consumer_cfg.get("durable_name") or "registrar_legacy_student_sync"
        self.batch_size = consumer_cfg.get("batch_size", 1000)
        self.ack_wait_seconds = consumer_cfg.get("ack_wait_seconds", 60)
        self.auto_reset_on_empty = os.getenv("LEGACY_STUDENT_AUTO_RESET", "true").lower() in ("true", "1")

    def stop(self):
        logger.info("Stopping Legacy Student Sync Worker gracefully...")
        self.running = False

    async def connect_nats(self):
        logger.info(f"Connecting to remote NATS {config.remote_nats_url}...")
        await self.nc.connect(
            config.remote_nats_url,
            user_credentials=config.nats_creds,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
        )
        logger.info("Connected to remote NATS successfully.")

    def check_table_count(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT COUNT(*) AS total FROM student_profile")).fetchone()
            return row.total if row else 0

    async def setup_subscription(self, js):
        count = self.check_table_count()
        logger.info(f"Current student_profile table row count: {count}")

        # Check if consumer exists
        existing_consumer = None
        try:
            existing_consumer = await js.consumer_info(self.stream_name, self.durable_name)
        except NotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Could not inspect consumer {self.durable_name}: {e}")

        # If student_profile is empty and auto_reset_on_empty is True, recreate with ALL
        deliver_policy = DeliverPolicy.ALL
        if count == 0 and self.auto_reset_on_empty:
            logger.info("Database student_profile is empty. Ensuring DeliverPolicy.ALL for full replay...")
            if existing_consumer:
                try:
                    await js.delete_consumer(self.stream_name, self.durable_name)
                    logger.info(f"Deleted existing durable consumer '{self.durable_name}' for clean replay.")
                except Exception as e:
                    logger.warning(f"Failed to delete existing consumer: {e}")
        elif existing_consumer:
            logger.info(f"Using existing durable consumer '{self.durable_name}' (delivered: {existing_consumer.delivered.consumer_seq})")
            deliver_policy = None

        # Build consumer config
        c_cfg = ConsumerConfig(
            durable_name=self.durable_name,
            filter_subject=self.filter_subject,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=self.ack_wait_seconds,
            max_deliver=5,
        )
        if deliver_policy:
            c_cfg.deliver_policy = deliver_policy

        sub = await js.pull_subscribe(
            subject=self.filter_subject,
            durable=self.durable_name,
            stream=self.stream_name,
            config=c_cfg
        )
        logger.info(f"Subscribed to stream '{self.stream_name}' on subject '{self.filter_subject}' with durable '{self.durable_name}'.")
        return sub

    def persist_batch(
        self,
        profiles: List[Dict[str, Any]],
        infos: List[Dict[str, Any]],
        legacies: List[Dict[str, Any]],
        resolved_nums: List[str],
        emails: Optional[List[Dict[str, Any]]] = None,
    ):
        if not profiles and not emails:
            return

        with self.engine.begin() as conn:
            if profiles:
                # 1. Upsert student_profile
                profile_sql = text("""
                    INSERT INTO student_profile (
                        student_number, program_id, major_id, year_level, semester_id,
                        academic_year_id, student_type_id, student_status_id, campus_id,
                        year_admitted, sem_admitted
                    ) VALUES (
                        :student_number, :program_id, :major_id, :year_level, :semester_id,
                        :academic_year_id, :student_type_id, :student_status_id, :campus_id,
                        :year_admitted, :sem_admitted
                    ) ON DUPLICATE KEY UPDATE
                        program_id = VALUES(program_id),
                        major_id = VALUES(major_id),
                        year_level = VALUES(year_level),
                        semester_id = VALUES(semester_id),
                        academic_year_id = VALUES(academic_year_id),
                        campus_id = VALUES(campus_id),
                        year_admitted = VALUES(year_admitted),
                        sem_admitted = VALUES(sem_admitted)
                """)
                conn.execute(profile_sql, profiles)

                # 2. Upsert student_info
                info_sql = text("""
                    INSERT INTO student_info (
                        student_number, first_name, mid_name, last_name, suffix, gender,
                        regions_id, provinces_id, municipalities_id, brgys_id, street,
                        date_of_birth, religion_id, nationality, marital_status,
                        email, cvsu_email, contact_no, is_pwd
                    ) VALUES (
                        :student_number, :first_name, :mid_name, :last_name, :suffix, :gender,
                        :regions_id, :provinces_id, :municipalities_id, :brgys_id, :street,
                        :date_of_birth, :religion_id, :nationality, :marital_status,
                        :email, :cvsu_email, :contact_no, :is_pwd
                    ) ON DUPLICATE KEY UPDATE
                        first_name = VALUES(first_name),
                        mid_name = VALUES(mid_name),
                        last_name = VALUES(last_name),
                        suffix = VALUES(suffix),
                        gender = VALUES(gender),
                        regions_id = VALUES(regions_id),
                        provinces_id = VALUES(provinces_id),
                        municipalities_id = VALUES(municipalities_id),
                        brgys_id = VALUES(brgys_id),
                        street = VALUES(street),
                        date_of_birth = VALUES(date_of_birth),
                        religion_id = VALUES(religion_id),
                        nationality = VALUES(nationality),
                        marital_status = VALUES(marital_status),
                        email = COALESCE(VALUES(email), student_info.email),
                        cvsu_email = COALESCE(VALUES(cvsu_email), student_info.cvsu_email),
                        contact_no = VALUES(contact_no),
                        is_pwd = VALUES(is_pwd)
                """)
                conn.execute(info_sql, infos)

                # 3. Upsert legacy_student_info for mismatches
                if legacies:
                    legacy_sql = text("""
                        INSERT INTO legacy_student_info (
                            student_number, legacy_street, legacy_barangay, legacy_municipality,
                            legacy_province, legacy_course, legacy_religion, legacy_payload,
                            mismatch_fields, mismatch_summary, is_resolved, created_at, updated_at
                        ) VALUES (
                            :student_number, :legacy_street, :legacy_barangay, :legacy_municipality,
                            :legacy_province, :legacy_course, :legacy_religion, :legacy_payload,
                            :mismatch_fields, :mismatch_summary, :is_resolved, NOW(), NOW()
                        ) ON DUPLICATE KEY UPDATE
                            legacy_street = VALUES(legacy_street),
                            legacy_barangay = VALUES(legacy_barangay),
                            legacy_municipality = VALUES(legacy_municipality),
                            legacy_province = VALUES(legacy_province),
                            legacy_course = VALUES(legacy_course),
                            legacy_religion = VALUES(legacy_religion),
                            legacy_payload = VALUES(legacy_payload),
                            mismatch_fields = VALUES(mismatch_fields),
                            mismatch_summary = VALUES(mismatch_summary),
                            is_resolved = VALUES(is_resolved),
                            updated_at = NOW()
                    """)
                    conn.execute(legacy_sql, legacies)

                # 4. Clean up resolved student numbers
                if resolved_nums:
                    conn.execute(
                        text("DELETE FROM legacy_student_info WHERE student_number IN :snums AND is_resolved = 0"),
                        {"snums": tuple(resolved_nums)}
                    )

            # 5. Update institutional CvSU emails if present
            if emails:
                email_sql = text("""
                    UPDATE student_info
                    SET cvsu_email = :cvsu_email
                    WHERE student_number = :student_number;
                """)
                conn.execute(email_sql, emails)

    async def run(self):
        # 1. Load caches
        self.caches.load(self.engine)

        # 2. Connect to NATS
        await self.connect_nats()
        js = self.nc.jetstream()

        # Check if stream and subject are configured before subscribing
        while self.running and (not self.stream_name or not self.filter_subject):
            logger.warning(
                "Legacy student sync stream or subject is not configured. "
                "Please configure LEGACY_STUDENT_STREAM and LEGACY_STUDENT_SUBJECT in .env."
            )
            await asyncio.sleep(5.0)
            consumer_cfg = config.get_consumer("legacy_student_sync")
            self.stream_name = consumer_cfg.get("stream")
            self.filter_subject = consumer_cfg.get("subject")
            self.durable_name = consumer_cfg.get("durable_name") or self.durable_name

        if not self.running:
            return

        # 3. Setup pull subscription
        sub = await self.setup_subscription(js)

        logger.info(f"Legacy Student Sync Worker running. Polling for messages on {self.filter_subject}...")
        total_processed = 0

        while self.running:
            try:
                msgs = await sub.fetch(batch=self.batch_size, timeout=3.0)
            except (asyncio.TimeoutError, TimeoutError):
                await asyncio.sleep(1.0)
                continue
            except Exception as e:
                err_str = str(e).lower()
                if "serviceunavailableerror" in err_str or "timeout" in err_str or "503" in err_str or "408" in err_str:
                    await asyncio.sleep(1.0)
                    continue
                logger.error(f"Error fetching messages: {e}")
                await asyncio.sleep(2.0)
                continue

            if not msgs:
                await asyncio.sleep(1.0)
                continue

            profiles = []
            infos = []
            legacies = []
            resolved_nums = []
            emails = []
            ack_msgs = []

            for msg in msgs:
                try:
                    raw_str = msg.data.decode("utf-8")
                    data = json.loads(raw_str)
                    
                    # Unwrap payload if nested inside CDC or envelope
                    payload = data.get("after") or data.get("payload") or data
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except Exception:
                            pass

                    # 1. Check if student-email record (e.g. acad.enrollment.record.student-email.created)
                    s_email = payload.get("email")
                    s_num = payload.get("studentnumber") or payload.get("studentNumber")
                    if s_num and s_email and ("firstName" not in payload and "lastName" not in payload):
                        emails.append({
                            "student_number": str(s_num).strip(),
                            "cvsu_email": str(s_email).strip(),
                        })
                        ack_msgs.append(msg)
                        continue

                    # 2. Check if student-profile record (e.g. acad.enrollment.record.student-profile.created)
                    prof, inf, leg, res_num = transform_legacy_student(payload, self.caches)
                    if prof and inf:
                        profiles.append(prof)
                        infos.append(inf)
                        if leg:
                            legacies.append(leg)
                        if res_num:
                            resolved_nums.append(res_num)

                    ack_msgs.append(msg)
                except Exception as ex:
                    logger.error(f"Failed to transform message seq={msg.metadata.sequence.stream}: {ex}")
                    # Ack anyway to avoid poison message loop
                    ack_msgs.append(msg)

            if profiles or emails:
                try:
                    self.persist_batch(profiles, infos, legacies, resolved_nums, emails)
                    total_processed += len(profiles) + len(emails)
                    logger.info(f"Persisted batch of {len(profiles)} profiles, {len(emails)} emails (Total session: {total_processed}).")
                except Exception as ex:
                    logger.error(f"Failed to persist batch into database: {ex}")
                    await asyncio.sleep(2.0)
                    continue

            # Batch ack
            for msg in ack_msgs:
                try:
                    await msg.ack()
                except Exception:
                    pass

        if self.nc.is_connected:
            await self.nc.close()
        logger.info("Worker stopped.")

def main():
    worker = LegacyStudentSyncWorker()

    def handle_sig(sig, frame):
        worker.stop()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    asyncio.run(worker.run())

if __name__ == "__main__":
    main()
