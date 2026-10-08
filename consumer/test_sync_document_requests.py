import unittest
import json
from sync_document_requests import parse_full_name

class TestSyncDocumentRequests(unittest.TestCase):
    def test_parse_full_name_variations(self):
        # 1 token
        self.assertEqual(
            parse_full_name("CHER"),
            {"first_name": "CHER", "mid_name": None, "last_name": "", "suffix": None}
        )

        # 2 tokens
        self.assertEqual(
            parse_full_name("JUAN DELA CRUZ"),
            {"first_name": "JUAN", "mid_name": "DELA", "last_name": "CRUZ", "suffix": None}
        )

        # 4 tokens: "NINA KLARISSE AMBROSIO ULANGKAYA"
        parsed = parse_full_name("NINA KLARISSE AMBROSIO ULANGKAYA")
        self.assertEqual(parsed["first_name"], "NINA KLARISSE")
        self.assertEqual(parsed["mid_name"], "AMBROSIO")
        self.assertEqual(parsed["last_name"], "ULANGKAYA")
        self.assertIsNone(parsed["suffix"])

        # With suffix
        parsed_suffix = parse_full_name("JUAN SANTOS DELA CRUZ JR.")
        self.assertEqual(parsed_suffix["first_name"], "JUAN SANTOS")
        self.assertEqual(parsed_suffix["mid_name"], "DELA")
        self.assertEqual(parsed_suffix["last_name"], "CRUZ")
        self.assertEqual(parsed_suffix["suffix"], "JR.")

    def test_inbound_payload_structure(self):
        sample_payload = {
            "id": "01a11923-9327-719e-a010-73462b814cf1",
            "sex": "FEMALE",
            "email": "ninaklarisse.ulangkaya@cvsu.edu.ph",
            "purpose": "For School Requirements.",
            "documents": [
                {
                    "copies": 2,
                    "document_type": "TOR"
                }
            ],
            "full_name": "NINA KLARISSE AMBROSIO ULANGKAYA",
            "course_major": "BS Information Technology",
            "contact_number": "09165564433",
            "student_number": "202014165",
            "graduation_date": "2026-10-31",
            "complete_address": "BLK13 LOT15 VOLKS COR SUZ BANCAL CARMONA CAVITE",
            "high_school_name": "shalom-school",
            "high_school_address": "MAGUYAM",
            "attendance_entry_date": "2026-10-01",
            "senior_high_school_name": "perps-shs",
            "senior_high_school_address": "GMA"
        }

        self.assertEqual(sample_payload["student_number"], "202014165")
        self.assertEqual(sample_payload["id"], "01a11923-9327-719e-a010-73462b814cf1")
        self.assertEqual(len(sample_payload["documents"]), 1)
        self.assertEqual(sample_payload["documents"][0]["document_type"], "TOR")
        self.assertEqual(sample_payload["documents"][0]["copies"], 2)

if __name__ == "__main__":
    unittest.main()
