import os
from pathlib import Path
from typing import List

env_path = Path(__file__).resolve().parent.parent / ".env"

# Zero-dependency .env reader
def _load_env_file(path: Path):
    if not path.is_file():
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass

# Try python-dotenv first, fallback to native reader
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
except ImportError:
    _load_env_file(env_path)


class CDCConfig:
    def __init__(self):
        # 1. Database Connection (Local Registrar MySQL)
        self.mysql_host = os.getenv("MYSQL_HOST", "host.docker.internal")
        self.mysql_port = int(os.getenv("MYSQL_PORT", "3306"))
        self.mysql_database = os.getenv("MYSQL_DATABASE", "registrar-cvsu")
        self.mysql_username = os.getenv("MYSQL_USERNAME", "root")
        self.mysql_password = os.getenv("MYSQL_PASSWORD", "")
        self._raw_mysql_url = os.getenv("MYSQL_URL")

        # 2. NATS Brokers & Authentication
        self.local_nats_host = os.getenv("LOCAL_NATS_HOST", "nats")
        self.local_nats_port = int(os.getenv("LOCAL_NATS_PORT", "4222"))
        self._raw_local_nats_url = os.getenv("LOCAL_NATS_URL")

        self.remote_nats_host = os.getenv("REMOTE_NATS_HOST", "192.168.10.130")
        self.remote_nats_port = int(os.getenv("REMOTE_NATS_PORT", "4222"))
        self._raw_remote_nats_url = os.getenv("REMOTE_NATS_URL")

        self.nats_creds = os.getenv("NATS_CREDS", "/app/univ-reg.creds")

        # 3. Local CDC JetStream (Debezium -> FastStream Bridge)
        self.local_cdc_stream = os.getenv("LOCAL_CDC_STREAM", "LocalCDCStream")
        raw_stream_subjects = os.getenv(
            "LOCAL_CDC_STREAM_SUBJECTS",
            "academics.enrollment.main,academics.enrollment.main.>,academics.enrollment.students.main.>"
        )
        self.local_cdc_stream_subjects: List[str] = [
            s.strip() for s in raw_stream_subjects.split(",") if s.strip()
        ]
        self.local_cdc_consume_subject = os.getenv(
            "LOCAL_CDC_CONSUME_SUBJECT", "academics.enrollment.main.student.profile"
        )
        self.local_cdc_consumer_name = os.getenv(
            "LOCAL_CDC_CONSUMER_NAME", "faststream_cdc_bridge"
        )

        # 4. Remote Outbound Publishing (FastStream -> Remote NATS)
        self.target_student_subject = os.getenv(
            "TARGET_STUDENT_SUBJECT", "academics.enrollment.students.main.profile"
        )
        self.main_student_subject = os.getenv(
            "MAIN_STUDENT_SUBJECT", "academics.enrollment.main.student.profile"
        )
        self.doc_response_subject = os.getenv(
            "DOC_RESPONSE_SUBJECT", "academics.enrollment.documents.students.response.created"
        )

        # 5. Remote Inbound Document Request Stream (Remote NATS -> Ingestion Worker)
        self.doc_request_stream = os.getenv("DOC_REQUEST_STREAM", "DOCUMENTS")
        self.doc_request_subject = os.getenv(
            "DOC_REQUEST_SUBJECT", "academics.enrollment.documents.students.request.created"
        )
        self.doc_request_consumer = os.getenv(
            "DOC_REQUEST_CONSUMER", "registrar_doc_request_sync"
        )

    @property
    def mysql_url(self) -> str:
        if self._raw_mysql_url:
            return self._raw_mysql_url
        user_pass = f"{self.mysql_username}:{self.mysql_password}" if self.mysql_password else self.mysql_username
        return f"mysql+pymysql://{user_pass}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"

    @property
    def local_nats_url(self) -> str:
        if self._raw_local_nats_url:
            return self._raw_local_nats_url
        return f"nats://{self.local_nats_host}:{self.local_nats_port}"

    @property
    def remote_nats_url(self) -> str:
        if self._raw_remote_nats_url:
            return self._raw_remote_nats_url
        return f"nats://{self.remote_nats_host}:{self.remote_nats_port}"


config = CDCConfig()
