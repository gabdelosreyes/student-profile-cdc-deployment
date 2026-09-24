import os
import httpx
from faststream import FastStream
from faststream.nats import NatsBroker

# Connect directly to your existing NATS server shown in the NUI screenshot
NATS_URL = os.getenv("NATS_URL", "nats://192.168.10.130:4222")
NATS_CREDS = os.getenv("NATS_CREDS", r"c:\laragon\www\nats-cli-deployment\univ-reg.creds")

# Make sure this points to your running Laravel app (Laragon usually uses port 80 locally)
# Adjust the domain if your local laragon URL is different.
LARAVEL_API_URL = os.getenv("LARAVEL_API_URL", "http://localhost:8000/admin/api/internal/students/sync-legacy")

broker = NatsBroker(NATS_URL, user_credentials=NATS_CREDS)
app = FastStream(broker)

http_client = httpx.AsyncClient()

# Subscribe to the new subjects
@broker.subscriber("academics.enrollment.registrar-cvsu.student_info")
@broker.subscriber("academics.enrollment.registrar-cvsu.student_profile")
async def handle_legacy_student_change(msg: dict):
    # Because of the Debezium ExtractChangedRecordState transform, 
    # the msg is ALREADY flattened! No need to look for 'payload.after'.
    
    is_deleted = msg.get("__deleted") in ["true", True]
    
    if not is_deleted:
        student_id = msg.get("studentNumber", "UNKNOWN")
        print(f"Captured CDC event. Syncing student {student_id} to Laravel...")
        
        try:
            response = await http_client.post(
                LARAVEL_API_URL,
                json=msg,  # Send the flat JSON directly!
                headers={"Accept": "application/json"}
            )
            
            if response.status_code in [200, 201]:
                print(f"Successfully synced student {student_id} to Laravel.")
            else:
                print(f"Failed to sync student {student_id}. HTTP {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"HTTP Request to Laravel failed: {e}")

@app.on_shutdown
async def shutdown():
    await http_client.aclose()
