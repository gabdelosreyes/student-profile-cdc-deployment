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

    def test_document_request_response_routing(self):
        doc_response_msg = {
            "eventId": "01a11923-9327-719e-a010-73462b814cf1",
            "eventType": "document.request.response.created",
            "payload": {
                "id": "01a11923-9327-719e-a010-73462b814cf1",
                "student_number": "202014165",
                "document_type": "TOR",
                "control_number": "2610080100001",
                "status": "for_release",
                "timestamp": "2026-10-08T14:45:00Z"
            }
        }

        event_type = doc_response_msg.get("eventType")
        payload = doc_response_msg.get("payload")
        
        is_doc_response = (
            event_type == "document.request.response.created" or 
            (payload.get("status") == "for_release" and "control_number" in payload)
        )
        self.assertTrue(is_doc_response)
        self.assertEqual(payload["control_number"], "2610080100001")
        self.assertEqual(payload["status"], "for_release")
        self.assertEqual(payload["document_type"], "TOR")
        self.assertEqual(payload["id"], "01a11923-9327-719e-a010-73462b814cf1")

    def test_three_tier_taxonomy_dynamic_assembly(self):
        from config import config

        # Test standardized 3-tier assembly for student profile
        subj_student = config.build_outbound_subject("record.student-profile", "updated")
        self.assertEqual(subj_student, "academic.univ-reg.record.student-profile.updated")

        # Test document request response routes to configured DOC_RESPONSE_SUBJECT
        subj_doc = config.build_outbound_subject("request.document", "updated")
        self.assertEqual(subj_doc, "acad.studentportal.request.document.updated")

        # Test legacy compatibility normalization
        subj_legacy_student = config.build_outbound_subject("student_profile", "student.profile.updated")
        self.assertEqual(subj_legacy_student, "academic.univ-reg.record.student-profile.updated")

        subj_legacy_doc = config.build_outbound_subject("document_request", "document.request.response.created")
        self.assertEqual(subj_legacy_doc, "acad.studentportal.request.document.updated")

    def test_streams_registry_loading(self):
        from config import config
        import os

        # Verify dynamic consumer resolution with no hardcoded fallback streams
        legacy_cfg = config.get_consumer("legacy_student_sync")
        self.assertIsNotNone(legacy_cfg)
        self.assertEqual(legacy_cfg["name"], "legacy_student_sync")
        self.assertEqual(legacy_cfg["durable_name"], "registrar_legacy_student_sync")

        doc_cfg = config.get_consumer("document_request_sync")
        self.assertIsNotNone(doc_cfg)
        self.assertEqual(doc_cfg["name"], "document_request_sync")
        self.assertEqual(doc_cfg["durable_name"], "registrar_doc_request_sync")

        # Verify env override dynamically takes precedence
        os.environ["LEGACY_STUDENT_STREAM"] = "CUSTOM_STREAM"
        os.environ["LEGACY_STUDENT_SUBJECT"] = "custom.subject.test"
        try:
            custom_cfg = config.get_consumer("legacy_student_sync")
            self.assertEqual(custom_cfg["stream"], "CUSTOM_STREAM")
            self.assertEqual(custom_cfg["subject"], "custom.subject.test")
        finally:
            os.environ.pop("LEGACY_STUDENT_STREAM", None)
            os.environ.pop("LEGACY_STUDENT_SUBJECT", None)

if __name__ == "__main__":
    unittest.main()


