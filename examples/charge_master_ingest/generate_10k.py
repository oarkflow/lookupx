#!/usr/bin/env python3
"""Generate a deterministic synthetic 10,000-row charge master JSON fixture."""
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 20261116
ROW_COUNT = 10_000
CODES = [
    ("99202", "New patient office visit, straightforward"),
    ("99203", "New patient office visit, low complexity"),
    ("99204", "New patient office visit, moderate complexity"),
    ("99212", "Established patient office visit, straightforward"),
    ("99213", "Established patient office visit, low complexity"),
    ("99214", "Established patient office visit, moderate complexity"),
    ("93000", "Electrocardiogram complete"),
    ("80053", "Comprehensive metabolic panel"),
    ("85025", "Complete blood count with automated differential"),
    ("71046", "Chest radiograph, two views"),
    ("36415", "Collection of venous blood by venipuncture"),
    ("81001", "Urinalysis with microscopy"),
    ("83036", "Hemoglobin A1c test"),
    ("84443", "Thyroid stimulating hormone test"),
    ("96372", "Therapeutic injection administration"),
    ("90471", "Immunization administration"),
    ("87880", "Rapid infectious agent antigen test"),
    ("45378", "Diagnostic colonoscopy"),
    ("97110", "Therapeutic exercises, each 15 minutes"),
    ("97140", "Manual therapy techniques, each 15 minutes"),
]
DEPARTMENTS = ["primary_care", "cardiology", "laboratory", "radiology", "urgent_care", "orthopedics", "gastroenterology"]
PATIENT_STATUSES = [(1, "outpatient"), (2, "inpatient"), (3, "emergency")]
CHARGE_TYPES = ["professional", "facility", "ancillary"]
PAYERS = ["commercial", "medicare", "medicaid", "self_pay"]
MODIFIERS = ["", "25", "59", "TC", "26"]


def make_rows():
    rng = random.Random(SEED)
    first_day = date(2024, 1, 1)
    for i in range(1, ROW_COUNT + 1):
        code, description = rng.choice(CODES)
        dos = first_day + timedelta(days=rng.randrange(1050))
        patient_status_uid, patient_status = rng.choice(PATIENT_STATUSES)
        yield {
            "charge_master_uid": f"CM{i:08d}",
            "cpt_code": code,
            "client_proc_desc": description,
            "event_dos": dos.isoformat(),
            "work_item_id": 100000 + i,
            "patient_status_uid": patient_status_uid,
            "patient_status": patient_status,
            "charge_type": rng.choice(CHARGE_TYPES),
            "department": rng.choice(DEPARTMENTS),
            "modifier": rng.choice(MODIFIERS),
            "units": rng.randint(1, 4),
            "charge_amount": round(rng.uniform(18, 1450), 2),
            "place_of_service": rng.choice(["office", "hospital", "clinic", "outpatient_center"]),
            "payer_class": rng.choice(PAYERS),
            "effective_date": "2024-01-01",
            "end_effective_date": "2027-12-31",
        }


if __name__ == "__main__":
    output = Path(__file__).parent / "data" / "charge_master_cpt_10k.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(list(make_rows()), indent=2) + "\n")
    print(f"Wrote {ROW_COUNT:,} synthetic rows to {output}")
