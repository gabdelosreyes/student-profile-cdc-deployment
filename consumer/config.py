import os
from pathlib import Path
from typing import List, Dict, Any, Optional

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
        # 0. University Taxonomy Domain and System (<domain>.<system>)
        self.stream_domain_system = os.getenv("STREAM_DOMAIN_SYSTEM", "academic.univ-reg")

        # 1. Database Connection (Local Registrar MySQL)
        self.mysql_host = os.getenv("MYSQL_HOST", "host.docker.internal")
        if self.mysql_host == "host.docker.internal":
            try:
                import socket
                socket.gethostbyname("host.docker.internal")
            except Exception:
                self.mysql_host = "127.0.0.1"

        self.mysql_port = int(os.getenv("MYSQL_PORT", "3306"))
        self.mysql_database = os.getenv("MYSQL_DATABASE", "registrar-cvsu")
        self.mysql_username = os.getenv("MYSQL_USERNAME", "root")
        self.mysql_password = os.getenv("MYSQL_PASSWORD", "")
        self._raw_mysql_url = os.getenv("MYSQL_URL")

        # 2. NATS Brokers & Authentication
        self.local_nats_host = os.getenv("LOCAL_NATS_HOST", "nats")
        if self.local_nats_host == "nats":
            try:
                import socket
                socket.gethostbyname("nats")
            except Exception:
                self.local_nats_host = "127.0.0.1"

        self.local_nats_port = int(os.getenv("LOCAL_NATS_PORT", "4222"))
        self._raw_local_nats_url = os.getenv("LOCAL_NATS_URL")

        self.remote_nats_host = os.getenv("REMOTE_NATS_HOST", "192.168.10.130")
        self.remote_nats_port = int(os.getenv("REMOTE_NATS_PORT", "4222"))
        self._raw_remote_nats_url = os.getenv("REMOTE_NATS_URL")

        self.nats_creds = os.getenv("NATS_CREDS", "/app/univ-reg.creds")

        # 3. Local CDC JetStream (Debezium -> FastStream Bridge)
        self.local_cdc_stream = os.getenv("LOCAL_CDC_STREAM", "LocalCDCStream")
        raw_stream_subjects = os.getenv("LOCAL_CDC_STREAM_SUBJECTS", "cdc.outbox,cdc.outbox.>")
        self.local_cdc_stream_subjects: List[str] = [
            s.strip() for s in raw_stream_subjects.split(",") if s.strip()
        ]
        self.local_cdc_consume_subject = os.getenv("LOCAL_CDC_CONSUME_SUBJECT", "cdc.outbox")
        self.local_cdc_consumer_name = os.getenv("LOCAL_CDC_CONSUMER_NAME", "faststream_cdc_bridge")

    def get_consumer(self, name: str) -> Dict[str, Any]:
        """
        Dynamically retrieves consumer configuration from .env.
        Contains ZERO hardcoded remote stream names or subjects.
        """
        if name == "legacy_student_sync":
            stream = os.getenv("LEGACY_STUDENT_STREAM", "").strip() or None
            subject = os.getenv("LEGACY_STUDENT_SUBJECT", "").strip() or None
            consumer = os.getenv("LEGACY_STUDENT_CONSUMER", "registrar_legacy_student_sync").strip()
            batch_size = int(os.getenv("LEGACY_STUDENT_BATCH_SIZE", "1000"))
            ack_wait = int(os.getenv("LEGACY_STUDENT_ACK_WAIT", "60"))
        elif name == "document_request_sync":
            stream = os.getenv("DOC_REQUEST_STREAM", "").strip() or None
            subject = os.getenv("DOC_REQUEST_SUBJECT", "").strip() or None
            consumer = os.getenv("DOC_REQUEST_CONSUMER", "registrar_doc_request_sync").strip()
            batch_size = int(os.getenv("DOC_REQUEST_BATCH_SIZE", "100"))
            ack_wait = int(os.getenv("DOC_REQUEST_ACK_WAIT", "30"))
        else:
            prefix = name.upper().replace("-", "_")
            stream = os.getenv(f"{prefix}_STREAM", "").strip() or None
            subject = os.getenv(f"{prefix}_SUBJECT", "").strip() or None
            consumer = os.getenv(f"{prefix}_CONSUMER", f"registrar_{name}").strip()
            batch_size = int(os.getenv(f"{prefix}_BATCH_SIZE", "100"))
            ack_wait = int(os.getenv(f"{prefix}_ACK_WAIT", "30"))

        return {
            "name": name,
            "stream": stream,
            "subject": subject,
            "durable_name": consumer,
            "batch_size": batch_size,
            "ack_wait_seconds": ack_wait,
        }

    def get_consumer_config(self, name: str) -> Optional[Dict[str, Any]]:
        """Backward-compatibility alias for get_consumer."""
        return self.get_consumer(name)

    @property
    def legacy_student_stream(self) -> Optional[str]:
        return self.get_consumer("legacy_student_sync").get("stream")

    @property
    def legacy_student_subject(self) -> Optional[str]:
        return self.get_consumer("legacy_student_sync").get("subject")

    @property
    def legacy_student_consumer(self) -> Optional[str]:
        return self.get_consumer("legacy_student_sync").get("durable_name")

    @property
    def doc_request_stream(self) -> Optional[str]:
        return self.get_consumer("document_request_sync").get("stream")

    @property
    def doc_request_subject(self) -> Optional[str]:
        return self.get_consumer("document_request_sync").get("subject")

    @property
    def doc_request_consumer(self) -> Optional[str]:
        return self.get_consumer("document_request_sync").get("durable_name")

    @property
    def doc_response_subject(self) -> str:
        return os.getenv("DOC_RESPONSE_SUBJECT", "acad.studentportal.request.document.updated")

    def build_outbound_subject(self, aggregatetype: str, event_type: str) -> str:
        """
        Dynamically constructs the standardized enterprise subject:
        <domain>.<system>.<service>.<entity-subentity?>.<action?>
        Example: academic.univ-reg.record.student-profile.updated
                 acad.studentportal.request.document.updated
        """
        clean_aggregate = (aggregatetype or "record.student-profile").strip()
        clean_action = (event_type or "updated").strip()

        # If incoming event_type has dot-notation, extract the action word
        if "." in clean_action:
            clean_action = clean_action.split(".")[-1]

        # Route document request responses to configured DOC_RESPONSE_SUBJECT
        if clean_aggregate in ("request.document", "document_request", "request.student-document", "request.student"):
            return self.doc_response_subject

        # Normalize legacy aggregate types
        if clean_aggregate == "student_profile":
            clean_aggregate = "record.student-profile"

        return f"{self.stream_domain_system}.{clean_aggregate}.{clean_action}"

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
