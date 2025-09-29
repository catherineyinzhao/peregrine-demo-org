#!/usr/bin/env python3
"""
Generate EMS Reports from existing incidents/patients/medications.
- Loads JSON from data/json
- Uses EMSDataGenerator.generate_ems_report to build reports
- Optionally replaces report narrative via local Ollama for higher fidelity
- Saves to data/json/ems_reports.json
"""
import os
import json
import argparse
import time
from faker import Faker
from types import SimpleNamespace

from ems_data_generator import EMSDataGenerator

try:
    import requests  # For Ollama
except Exception:
    requests = None

BASE_DIR = os.path.join(os.path.dirname(__file__), 'data', 'json')
INCIDENTS_PATH = os.path.join(BASE_DIR, 'ems_incidents.json')
PATIENTS_PATH = os.path.join(BASE_DIR, 'ems_patients.json')
MEDS_PATH = os.path.join(BASE_DIR, 'ems_medications.json')
REPORTS_PATH = os.path.join(BASE_DIR, 'ems_reports.json')

# Default to a more verbose local model; can be overridden via env
OLLAMA_URL = os.environ.get('OLLAMA_URL', 'http://localhost:11434')
OLLAMA_MODEL = os.environ.get('OLLAMA_MODEL', 'mixtral:8x7b')


def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def to_ns(obj):
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: to_ns(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return [to_ns(v) for v in obj]
    return obj


def ensure_patient_fields(pat_ns: SimpleNamespace) -> SimpleNamespace:
    if not hasattr(pat_ns, 'patient_home_address'):
        geo = getattr(pat_ns, 'patient_home_address_geo', '') or ''
        pat_ns.patient_home_address = geo.split(',')[0].strip() if geo else ''
    if not hasattr(pat_ns, 'patient_ethnicity'):
        pat_ns.patient_ethnicity = 'UNKNOWN'
    if not hasattr(pat_ns, 'patient_gender') and hasattr(pat_ns, 'patient_sex'):
        pat_ns.patient_gender = pat_ns.patient_sex
    return pat_ns


def ensure_incident_fields(inc_ns: SimpleNamespace) -> SimpleNamespace:
    # Map timeline names the report expects
    if not hasattr(inc_ns, 'unit_enroute_datetime') and hasattr(inc_ns, 'en_route_datetime'):
        inc_ns.unit_enroute_datetime = inc_ns.en_route_datetime
    if not hasattr(inc_ns, 'unit_arrived_at_patient_datetime') and hasattr(inc_ns, 'arrive_datetime'):
        inc_ns.unit_arrived_at_patient_datetime = inc_ns.arrive_datetime
    # patient fields
    if not hasattr(inc_ns, 'patient_sex') and hasattr(inc_ns, 'patient_gender'):
        inc_ns.patient_sex = inc_ns.patient_gender
    if not hasattr(inc_ns, 'patient_home_address') and hasattr(inc_ns, 'patient_home_address_geo'):
        inc_ns.patient_home_address = inc_ns.patient_home_address_geo.split(',')[0]
    return inc_ns


def build_ollama_prompt(incident: SimpleNamespace, patient: SimpleNamespace, medications_list):
    context = {
        'incident': {
            'type': getattr(incident, 'incident_type_description', ''),
            'subtype': getattr(incident, 'incident_subtype', ''),
            'address': getattr(incident, 'address', ''),
            'city': getattr(incident, 'city', ''),
            'district': getattr(incident, 'district', ''),
            'datetime': getattr(incident, 'call_datetime', ''),
            'complaint_reported_by_dispatch': getattr(incident, 'complaint_reported_by_dispatch', ''),
            'complaint_reported_by_dispatch_code': getattr(incident, 'complaint_reported_by_dispatch_code', ''),
            'chief_complaint': getattr(incident, 'chief_complaint', ''),
            'primary_impression': getattr(incident, 'primary_impression', ''),
            'provider_primary_impression': getattr(incident, 'provider_primary_impression', ''),
            'cad_emd_code': getattr(incident, 'cad_emd_code', ''),
            'unit_call_sign': getattr(incident, 'unit_call_sign', ''),
            'unit_level_of_care': getattr(incident, 'unit_level_of_care', ''),
            'transportation_method': getattr(incident, 'transportation_method', ''),
            'disposition': getattr(incident, 'patient_disposition', ''),
            'destination': getattr(incident, 'transfer_destination', ''),
            'destination_type': getattr(incident, 'destination_type', ''),
            'patient_acuity': getattr(incident, 'patient_acuity', ''),
            'situation_patient_acuity': getattr(incident, 'situation_patient_acuity', ''),
            'ecg_findings': getattr(incident, 'ecg_findings', ''),
            'procedures_attempted': getattr(incident, 'attempted_procedures', []),
            'procedures_successful': getattr(incident, 'successful_procedures', []),
            'timeline': {
                'dispatch': getattr(incident, 'unit_notified_by_dispatch_datetime', ''),
                'enroute': getattr(incident, 'unit_enroute_datetime', ''),
                'arrival': getattr(incident, 'unit_arrived_at_patient_datetime', ''),
                'left_scene': getattr(incident, 'unit_left_scene_datetime', ''),
                'arrival_at_destination': getattr(incident, 'patient_arrived_at_destination_datetime', ''),
                'transfer_of_care': getattr(incident, 'transfer_of_ems_patient_care_datetime', ''),
                'back_in_service': getattr(incident, 'unit_back_in_service_datetime', ''),
            },
            'status': getattr(incident, 'incident_status', ''),
            'priority': getattr(incident, 'priority', ''),
        },
        'crew': {
            'member_name': getattr(incident, 'crew_member_name', ''),
            'member_level': getattr(incident, 'crew_member_level', ''),
            'crew_disposition': getattr(incident, 'crew_disposition', ''),
            'primary_patient_caregiver_on_scene': getattr(incident, 'primary_patient_caregiver_on_scene', ''),
        },
        'patient': {
            'name': getattr(patient, 'patient_full_name', ''),
            'age': getattr(patient, 'patient_age', ''),
            'gender': getattr(patient, 'patient_gender', ''),
            'race': getattr(patient, 'patient_race', ''),
            'weight': getattr(patient, 'patient_weight', ''),
        },
        'medications': [
            {
                'name': getattr(m, 'medication_name', ''),
                'dosage': f"{getattr(m, 'dosage', '')}{getattr(m, 'dosage_unit', '')}",
                'route': getattr(m, 'medication_administration_route', ''),
                'time': getattr(m, 'medication_administration_time', ''),
            }
            for m in (medications_list or [])
        ],
    }
    instructions = (
        "Write a professional EMS narrative (10–16 sentences) that strictly reflects only the provided data. "
        "Do not invent or infer details (no vitals, past history, allergies, unlisted symptoms, times, or locations). "
        "Cover, when available: dispatch complaint and chief complaint; incident type and time; patient age/gender/weight; "
        "primary/provider impression; attempted vs. successful procedures (and brief clinical rationale only if implied by the data); ECG findings if present; "
        "medications with dose/unit/route/time; unit level of care and crew actions; disposition, transport method/priority, destination; "
        "timeline markers (dispatch/enroute/arrival/left scene/arrival at destination/transfer/back in service) that exist in the context. "
        "You may add neutral connective phrasing to improve readability, but must not add facts. Avoid repetition and avoid restating field names."
    )
    return {
        'prompt': (
            f"You are an EMS reporting assistant.\n\nContext (JSON):\n{json.dumps(context, ensure_ascii=False)}\n\n"
            f"Task: {instructions}\n\nOutput only the narrative text."
        )
    }


def build_retry_prompt(prev_prompt: str) -> str:
    return prev_prompt + "\n\nThe previous narrative was too brief. Expand to 10–16 sentences and ensure each required item is covered explicitly, without adding new facts. Output only the narrative text."
def ollama_generate(prompt: str, model: str = OLLAMA_MODEL, timeout: int = 120) -> str:
    if requests is None:
        return ''
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                'model': model,
                'prompt': prompt,
                'stream': False,
                'options': {
                    'temperature': 0.3,
                    'top_p': 0.9,
                    'repeat_penalty': 1.07,
                    'num_predict': 1200
                }
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            return ''
        try:
            data = resp.json()
            return data.get('response', '') if isinstance(data, dict) else ''
        except ValueError:
            return resp.text
    except Exception:
        return ''


def count_sentences(text: str) -> int:
    # Count rough number of sentences by ., !, ?
    if not text:
        return 0
    return sum(text.count(x) for x in ['.', '!', '?'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-ollama', action='store_true', help='Generate report narrative via Ollama')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of reports to generate')
    args = parser.parse_args()

    fake = Faker()
    gen = EMSDataGenerator(fake)

    incidents = load_json(INCIDENTS_PATH)
    patients = load_json(PATIENTS_PATH)
    meds = load_json(MEDS_PATH)

    if not incidents or not patients:
        print('Missing incidents or patients; cannot generate reports.')
        return

    if args.limit is not None:
        incidents = incidents[: args.limit]

    patient_by_incident = {}
    for p in patients:
        inc_id = p.get('incident_id')
        if inc_id and inc_id not in patient_by_incident:
            patient_by_incident[inc_id] = p

    meds_by_incident = {}
    for m in meds:
        inc_id = m.get('incident_id')
        if not inc_id:
            continue
        meds_by_incident.setdefault(inc_id, []).append(m)

    reports = []
    skipped_no_patient = 0
    for i, inc in enumerate(incidents):
        if i % 200 == 0:
            print(f'Generating reports: {i}/{len(incidents)}')
        related_patient = patient_by_incident.get(inc.get('incident_id'))
        if not related_patient:
            skipped_no_patient += 1
            continue
        related_meds = meds_by_incident.get(inc.get('incident_id'), [])

        # Convert to attribute-access objects expected by generator
        inc_ns = ensure_incident_fields(to_ns(inc))
        pat_ns = ensure_patient_fields(to_ns(related_patient))
        meds_ns = to_ns(related_meds)

        report = gen.generate_ems_report(inc_ns, meds_ns, pat_ns)

        if args.use_ollama:
            prompt_obj = build_ollama_prompt(inc_ns, pat_ns, meds_ns)
            narrative = ollama_generate(prompt_obj['prompt'])
            if count_sentences(narrative) < 10:
                narrative = ollama_generate(build_retry_prompt(prompt_obj['prompt']))
            if narrative:
                report.report_narrative = narrative.strip()

        if report:
            reports.append(report.__dict__)
        # Gentle pacing if using Ollama
        if args.use_ollama and (i % 10 == 0):
            time.sleep(0.2)

    with open(REPORTS_PATH, 'w', encoding='utf-8') as f:
        json.dump(reports, f, indent=2)

    print(f'Saved {len(reports)} EMS reports to {REPORTS_PATH} (skipped {skipped_no_patient} without patient)')
    if args.use_ollama:
        print('Narratives generated via Ollama model:', OLLAMA_MODEL)


if __name__ == '__main__':
    main()
