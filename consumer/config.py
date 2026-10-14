import os
from pathlib import Path

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

try:
    from pydantic_settings import BaseSettings
    from pydantic import Field

    class CDCConfig(BaseSettings):
        local_nats_url: str = Field(default="nats://nats:4222", validation_alias="LOCAL_NATS_URL")
        remote_nats_url: str = Field(default="nats://192.168.10.130:4222", validation_alias="REMOTE_NATS_URL")
        nats_creds: str = Field(default="/app/univ-reg.creds", validation_alias="NATS_CREDS")
        mysql_url: str = Field(default="mysql+pymysql://root:@host.docker.internal/registrar-cvsu", validation_alias="MYSQL_URL")

        local_cdc_stream: str = Field(default="LocalCDCStream", validation_alias="LOCAL_CDC_STREAM")
        target_student_subject: str = Field(default="academics.enrollment.students.main.profile", validation_alias="TARGET_STUDENT_SUBJECT")
        main_student_subject: str = Field(default="academics.enrollment.main.student.profile", validation_alias="MAIN_STUDENT_SUBJECT")
        doc_response_subject: str = Field(default="academics.enrollment.documents.students.response.created", validation_alias="DOC_RESPONSE_SUBJECT")

        doc_request_stream: str = Field(default="DOCUMENTS", validation_alias="DOC_REQUEST_STREAM")
        doc_request_subject: str = Field(default="academics.enrollment.documents.students.request.created", validation_alias="DOC_REQUEST_SUBJECT")
        doc_request_consumer: str = Field(default="registrar_doc_request_sync", validation_alias="DOC_REQUEST_CONSUMER")

        model_config = {
            "env_file": str(env_path),
            "env_file_encoding": "utf-8",
            "extra": "ignore"
        }

    config = CDCConfig()

except Exception:
    class FallbackCDCConfig:
        def __init__(self):
            self.local_nats_url = os.getenv("LOCAL_NATS_URL", "nats://nats:4222")
            self.remote_nats_url = os.getenv("REMOTE_NATS_URL", os.getenv("NATS_URL", "nats://192.168.10.130:4222"))
            self.nats_creds = os.getenv("NATS_CREDS", "/app/univ-reg.creds")
            self.mysql_url = os.getenv("MYSQL_URL", "mysql+pymysql://root:@host.docker.internal/registrar-cvsu")

            self.local_cdc_stream = os.getenv("LOCAL_CDC_STREAM", "LocalCDCStream")
            self.target_student_subject = os.getenv("TARGET_STUDENT_SUBJECT", "academics.enrollment.students.main.profile")
            self.main_student_subject = os.getenv("MAIN_STUDENT_SUBJECT", "academics.enrollment.main.student.profile")
            self.doc_response_subject = os.getenv("DOC_RESPONSE_SUBJECT", "academics.enrollment.documents.students.response.created")

            self.doc_request_stream = os.getenv("DOC_REQUEST_STREAM", "DOCUMENTS")
            self.doc_request_subject = os.getenv("DOC_REQUEST_SUBJECT", "academics.enrollment.documents.students.request.created")
            self.doc_request_consumer = os.getenv("DOC_REQUEST_CONSUMER", "registrar_doc_request_sync")

    config = FallbackCDCConfig()
