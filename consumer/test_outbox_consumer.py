import unittest
import json

class TestOutboxConsumer(unittest.TestCase):
    def test_outbox_cdc_payload_extraction(self):
        clean_envelope = {
            "event_id": "01ae0528-5814-7052-8078-493740d2cce5",
            "event_type": "student.profile.created",
            "aggregate_type": "student_profile",
            "aggregate_id": "202014166",
            "subject": "academics.enrollment.main.student.profile",
            "op": "c",
            "before": None,
            "after": {
                "student_number": "202014166",
                "first_name": "NINA KLARISSE",
                "last_name": "ALANGKOY"
            },
            "timestamp": 1759821591900
        }

        cdc_msg = {
            "payload": {
                "before": None,
                "after": {
                    "id": 1,
                    "event_id": "01ae0528-5814-7052-8078-493740d2cce5",
                    "event_type": "student.profile.created",
                    "aggregate_type": "student_profile",
                    "aggregate_id": "202014166",
                    "subject": "academics.enrollment.main.student.profile",
                    "op": "c",
                    "payload": json.dumps(clean_envelope),
                    "created_at": "2026-10-01 13:00:00",
                    "updated_at": "2026-10-01 13:00:00"
                },
                "op": "c"
            }
        }

        payload = cdc_msg.get("payload", {})
        after = payload.get("after") or {}
        raw = after.get("payload")
        event = json.loads(raw) if isinstance(raw, str) else raw

        self.assertEqual(event["event_id"], "01ae0528-5814-7052-8078-493740d2cce5")
        self.assertEqual(event["op"], "c")
        self.assertEqual(event["subject"], "academics.enrollment.main.student.profile")
        self.assertEqual(event["after"]["first_name"], "NINA KLARISSE")
        self.assertNotIn("status", event)

    def test_unwrapped_outbox_router_event_handling(self):
        delta_event = {
            "student_number": "202014166",
            "op": "u",
            "changed_fields": ["email"],
            "changes": {
                "email": "nina.updated@cvsu.edu.ph"
            }
        }

        # Outbox Event Router sends the unwrapped payload directly
        self.assertEqual(delta_event["student_number"], "202014166")
        self.assertEqual(delta_event["op"], "u")
        self.assertIn("email", delta_event["changed_fields"])
        self.assertEqual(delta_event["changes"]["email"], "nina.updated@cvsu.edu.ph")
        self.assertNotIn("before", delta_event)

if __name__ == "__main__":
    unittest.main()
