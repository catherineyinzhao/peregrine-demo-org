import json as json_module
import os
import csv
import random
import sqlite3
from datetime import datetime, timedelta
from faker import Faker
import uuid
from collections import defaultdict, Counter
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, asdict, fields
import math
import time
import phonenumbers
from phonenumbers import PhoneNumberFormat
from faker_vehicle import VehicleProvider
import re, pathlib
import sys, types
sys.modules['sqlalchemy_mate'] = types.SimpleNamespace(ExtendedBase=object)

print("JSON module imported:", json_module)
print("JSON module type:", type(json_module))

# Initialize Faker with multiple providers
fake = Faker('en_US')
Faker.seed(42)
random.seed(42)

# Add additional providers (use classes/modules, not strings)
from faker.providers import automotive as fp_automotive
from faker.providers import bank as fp_bank
fake.add_provider(fp_automotive)
fake.add_provider(fp_bank)
fake.add_provider(VehicleProvider)

# Create locale-specific faker instances for consistent ethnicity-name mapping
faker_locales = {
    'WHITE': Faker('en_US'),  # Default English names
    'BLACK': Faker('en_US'),  # English names (many African American names are English-based)
    'HISPANIC': Faker('es_ES'),  # Spanish names
    'ASIAN': [Faker('ja_JP'), Faker('zh_CN'), Faker('ko_KR'), Faker('vi_VN'), Faker('en_IN')],  # Multiple Asian cultures
    'NATIVE_AMERICAN': Faker('en_US'),  # English names (most Native Americans use English names today)
    'OTHER': Faker('en_US')
}

# Set seeds for consistent results - use class method instead of instance method
for locale_fake in faker_locales.values():
    if isinstance(locale_fake, list):
        for fake_instance in locale_fake:
            Faker.seed(42)  # Use class method
    else:
        Faker.seed(42)  # Use class method

# Enhanced Configuration for comprehensive data generation
CONFIG = {
    'num_persons': 200_000,  # Increased for comprehensive coverage
    'num_vehicles': 250_000,
    'num_properties': 100_000,
    'num_police_incidents': 75_000,
    'num_fire_incidents': 20_000,
    'num_ems_incidents': 100_000,
    'num_fire_rms_incidents': 20_000,
    'num_fire_shifts': 5_000,
    'num_fire_personnel': 3_000,
    # Jail data generation controls
    'num_corrections_facilities': 3,
    'num_jail_programs': 8,
    'pct_bookings_with_sentence': 0.55,
    'pct_bookings_with_bail_bond': 0.45,
    'avg_jail_incidents_per_booking': 0.2,
    'num_arrests': 25_000,
    'num_jail_bookings': 20_000,
    'simulation_years': 5,
    'output_formats': ['json', 'csv', 'sqlite'],
    'enable_cross_agency_sharing': True,
    'generate_rms_data': True,
    'generate_cad_data': True,
    'generate_jail_data': True,
    'generate_fire_data': True,
    'generate_ems_data': True,
    'realistic_response_times': True,
    'inter_agency_mutual_aid': True,
    'include_seattle_neighborhoods': True,
    'include_bellevue_districts': False,
    'include_king_county_unincorporated': True,
}

# Curated Corrections Facilities for King County region
CORRECTIONS_FACILITY_CATALOG = [
    {
        'facility_id': 'FAC-KCCF',
        'name': 'King County Correctional Facility',
        'address': '500 5th Ave',
        'city': 'Seattle',
        'state': 'WA',
        'zip_code': '98104',
        'capacity': 1300,
        'security_level': 'MAXIMUM',
        'contact_phone': None,
        'contact_email': None,
        'warden_name': None,
    },
    {
        'facility_id': 'FAC-MRJC',
        'name': 'Maleng Regional Justice Center',
        'address': '401 4th Ave N',
        'city': 'Kent',
        'state': 'WA',
        'zip_code': '98032',
        'capacity': 700,
        'security_level': 'MEDIUM',
        'contact_phone': None,
        'contact_email': None,
        'warden_name': None,
    },
    {
        'facility_id': 'FAC-SCORE',
        'name': 'South Correctional Entity (SCORE) Jail',
        'address': '20817 17th Ave S',
        'city': 'Des Moines',
        'state': 'WA',
        'zip_code': '98198',
        'capacity': 800,
        'security_level': 'MEDIUM',
        'contact_phone': None,
        'contact_email': None,
        'warden_name': None,
    },
    {
        'facility_id': 'FAC-KIRKLAND',
        'name': 'Kirkland Justice Center Jail',
        'address': '11750 NE 118th St',
        'city': 'Kirkland',
        'state': 'WA',
        'zip_code': '98034',
        'capacity': 60,
        'security_level': 'MINIMUM',
        'contact_phone': None,
        'contact_email': None,
        'warden_name': None,
    },
]

# BCS (Bureau of Criminal Statistics) Code Mapping - Complete Set
BCS_CODES = {
    # Part I Offenses (Index Crimes)
    '01': 'Criminal Homicide',
    '02': 'Forcible Rape',
    '03': 'Robbery',
    '04': 'Aggravated Assault',
    '05': 'Burglary',
    '06': 'Larceny-Theft',
    '07': 'Motor Vehicle Theft',
    '08': 'Arson',
    
    # Part II Offenses (Non-Index Crimes)
    '09': 'Simple Assault',
    '10': 'Forgery and Counterfeiting',
    '11': 'Fraud',
    '12': 'Embezzlement',
    '13': 'Stolen Property; Buying, Receiving, Possessing',
    '14': 'Vandalism',
    '15': 'Weapons; Carrying, Possessing, etc.',
    '16': 'Prostitution and Commercialized Vice',
    '17': 'Sex Offenses (except forcible rape and prostitution)',
    '18': 'Drug Abuse Violations',
    '19': 'Gambling',
    '20': 'Offenses Against Family and Children',
    '21': 'Driving Under the Influence',
    '22': 'Liquor Laws',
    '23': 'Drunkenness',
    '24': 'Disorderly Conduct',
    '25': 'Vagrancy',
    '26': 'All Other Offenses',
    '27': 'Suspicion',
    '28': 'Curfew and Loitering Law Violations',
    '29': 'Runaway',
    
    # Additional BCS Codes (30-99)
    '30': 'Bad Checks',
    '31': 'Bribery',
    '32': 'Extortion',
    '33': 'Kidnapping/Abduction',
    '34': 'Negligent Manslaughter',
    '35': 'Non-Negligent Manslaughter',
    '36': 'Rape by Force',
    '37': 'Statutory Rape',
    '38': 'Assault with Deadly Weapon',
    '39': 'Assault with Intent to Kill',
    '40': 'Assault with Intent to Rape',
    '41': 'Assault with Intent to Rob',
    '42': 'Assault with Intent to Commit Other Felony',
    '43': 'Assault on Police Officer',
    '44': 'Assault on Firefighter',
    '45': 'Assault on Emergency Medical Personnel',
    '46': 'Assault on School Employee',
    '47': 'Assault on Transit Employee',
    '48': 'Assault on Other Public Employee',
    '49': 'Assault on Family Member',
    '50': 'Assault on Child',
    '51': 'Assault on Elderly',
    '52': 'Assault on Disabled Person',
    '53': 'Assault with Firearm',
    '54': 'Assault with Knife/Cutting Instrument',
    '55': 'Assault with Other Dangerous Weapon',
    '56': 'Assault with Hands/Fists/Feet',
    '57': 'Assault with Other Object',
    '58': 'Assault - Other/Unknown Weapon',
    '59': 'Burglary - Forcible Entry',
    '60': 'Burglary - Unlawful Entry (No Force)',
    '61': 'Burglary - Attempted Forcible Entry',
    '62': 'Burglary - Residential',
    '63': 'Burglary - Non-Residential',
    '64': 'Burglary - Attempted',
    '65': 'Larceny - From Motor Vehicle',
    '66': 'Larceny - Motor Vehicle Parts/Accessories',
    '67': 'Larceny - Bicycles',
    '68': 'Larceny - From Building',
    '69': 'Larceny - From Coin-Operated Machine',
    '70': 'Larceny - Pocket Picking',
    '71': 'Larceny - Purse Snatching',
    '72': 'Larceny - Shoplifting',
    '73': 'Larceny - From Motor Vehicle',
    '74': 'Larceny - Motor Vehicle Parts/Accessories',
    '75': 'Larceny - All Other',
    '76': 'Motor Vehicle Theft - Auto',
    '77': 'Motor Vehicle Theft - Truck/Bus',
    '78': 'Motor Vehicle Theft - Other Vehicle',
    '79': 'Motor Vehicle Theft - Attempted',
    '80': 'Arson - Structure',
    '81': 'Arson - Mobile Property',
    '82': 'Arson - Other Property',
    '83': 'Arson - Attempted',
    '84': 'Drug/Narcotic Violations - Sale/Manufacturing',
    '85': 'Drug/Narcotic Violations - Possession',
    '86': 'Drug/Narcotic Violations - Other',
    '87': 'Weapons Violations - Carrying/Possessing',
    '88': 'Weapons Violations - Sale/Manufacturing',
    '89': 'Weapons Violations - Other',
    '90': 'Prostitution - Assisting/Promoting',
    '91': 'Prostitution - Purchasing Prostitution',
    '92': 'Prostitution - Other',
    '93': 'Sex Offenses - Forcible',
    '94': 'Sex Offenses - Non-Forcible',
    '95': 'Sex Offenses - Other',
    '96': 'Gambling - Bookmaking',
    '97': 'Gambling - Numbers/Lottery',
    '98': 'Gambling - Other',
    '99': 'All Other Offenses'
}

# Enhanced Seattle/King County geography with realistic neighborhoods
SEATTLE_NEIGHBORHOODS = {
    'DOWNTOWN': {'population': 85000, 'crime_rate': 'HIGH', 'fire_stations': [2, 5, 10]},
    'CAPITOL_HILL': {'population': 35000, 'crime_rate': 'MEDIUM', 'fire_stations': [25, 26]},
    'BALLARD': {'population': 45000, 'crime_rate': 'MEDIUM', 'fire_stations': [18, 20]},
    'FREMONT': {'population': 25000, 'crime_rate': 'MEDIUM', 'fire_stations': [9, 21]},
    'WALLINGFORD': {'population': 20000, 'crime_rate': 'LOW', 'fire_stations': [22]},
    'GREENWOOD': {'population': 30000, 'crime_rate': 'MEDIUM', 'fire_stations': [16, 17]},
    'NORTHGATE': {'population': 25000, 'crime_rate': 'MEDIUM', 'fire_stations': [31, 32]},
    'LAKE_CITY': {'population': 20000, 'crime_rate': 'MEDIUM', 'fire_stations': [33]},
    'RAVENNA': {'population': 15000, 'crime_rate': 'LOW', 'fire_stations': [34]},
    'UNIVERSITY_DISTRICT': {'population': 40000, 'crime_rate': 'MEDIUM', 'fire_stations': [35, 36]},
    'WEDGWOOD': {'population': 15000, 'crime_rate': 'LOW', 'fire_stations': [37]},
    'MAGNOLIA': {'population': 20000, 'crime_rate': 'LOW', 'fire_stations': [28]},
    'QUEEN_ANNE': {'population': 30000, 'crime_rate': 'MEDIUM', 'fire_stations': [2, 8]},
    'INTERNATIONAL_DISTRICT': {'population': 15000, 'crime_rate': 'HIGH', 'fire_stations': [10]},
    'PIONEER_SQUARE': {'population': 10000, 'crime_rate': 'HIGH', 'fire_stations': [10]},
    'SOUTH_LAKE_UNION': {'population': 20000, 'crime_rate': 'MEDIUM', 'fire_stations': [2, 5]},
    'BEACON_HILL': {'population': 25000, 'crime_rate': 'MEDIUM', 'fire_stations': [13, 14]},
    'COLUMBIA_CITY': {'population': 20000, 'crime_rate': 'MEDIUM', 'fire_stations': [13]},
    'RAINIER_VALLEY': {'population': 35000, 'crime_rate': 'HIGH', 'fire_stations': [13, 14]},
    'WEST_SEATTLE': {'population': 40000, 'crime_rate': 'MEDIUM', 'fire_stations': [11, 12]},
    'GEORGETOWN': {'population': 15000, 'crime_rate': 'MEDIUM', 'fire_stations': [11]},
    'SOUTH_PARK': {'population': 10000, 'crime_rate': 'MEDIUM', 'fire_stations': [11]},
}


# Enhanced incident types with Seattle/King County specific patterns
ENHANCED_INCIDENT_TYPES = {
    # Law Enforcement
    'TRAFFIC_VIOLATION': {'weight': 35, 'agencies': ['KCSO'], 'citation_likely': 0.8},
    'DOMESTIC_VIOLENCE': {'weight': 15, 'agencies': ['KCSO'], 'arrest_likely': 0.6},
    'THEFT': {'weight': 20, 'agencies': ['KCSO'], 'report_only': 0.7},
    'ASSAULT': {'weight': 10, 'agencies': ['KCSO'], 'arrest_likely': 0.5},
    'DUI': {'weight': 8, 'agencies': ['KCSO'], 'arrest_likely': 0.95},
    'DRUG_POSSESSION': {'weight': 12, 'agencies': ['KCSO'], 'arrest_likely': 0.4},
    'BURGLARY': {'weight': 5, 'agencies': ['KCSO'], 'report_investigation': 0.9},
    'ROBBERY': {'weight': 3, 'agencies': ['KCSO'], 'arrest_likely': 0.3},
    'HOMELESS_ENCAMPMENT': {'weight': 8, 'agencies': ['KCSO'], 'citation_likely': 0.2},
    'MENTAL_HEALTH_CRISIS': {'weight': 10, 'agencies': ['KCSO'], 'crisis_team': 0.4},
    'OVERDOSE': {'weight': 6, 'agencies': ['KCSO'], 'medical_response': 1.0},
    'GANG_ACTIVITY': {'weight': 4, 'agencies': ['KCSO'], 'investigation': 1.0},
    'HATE_CRIME': {'weight': 2, 'agencies': ['KCSO'], 'investigation': 1.0},
    'SEXUAL_ASSAULT': {'weight': 3, 'agencies': ['KCSO'], 'special_investigation': 1.0},
    'HUMAN_TRAFFICKING': {'weight': 1, 'agencies': ['KCSO'], 'special_investigation': 1.0},
    
    # Fire/EMS
    'MEDICAL_EMERGENCY': {'weight': 40, 'agencies': ['SEATTLE_FD'], 'transport': 0.7},
    'FIRE_ALARM': {'weight': 20, 'agencies': ['SEATTLE_FD'], 'false_alarm': 0.6},
    'STRUCTURE_FIRE': {'weight': 8, 'agencies': ['SEATTLE_FD'], 'multi_unit': 0.8},
    'VEHICLE_FIRE': {'weight': 5, 'agencies': ['SEATTLE_FD'], 'total_loss': 0.4},
    'HAZMAT_INCIDENT': {'weight': 2, 'agencies': ['SEATTLE_FD'], 'specialist_required': 1.0},
    'WATER_RESCUE': {'weight': 3, 'agencies': ['SEATTLE_FD'], 'marine_unit': 1.0},
    'CARDIAC_ARREST': {'weight': 12, 'agencies': ['SEATTLE_FD'], 'als_required': 1.0},
    'OVERDOSE_EMS': {'weight': 8, 'agencies': ['SEATTLE_FD'], 'narcan_admin': 0.6},
    'TRAUMA': {'weight': 15, 'agencies': ['SEATTLE_FD'], 'trauma_alert': 0.3},
    'STROKE': {'weight': 6, 'agencies': ['SEATTLE_FD'], 'stroke_alert': 0.8},
    'DIABETIC_EMERGENCY': {'weight': 5, 'agencies': ['SEATTLE_FD'], 'medical': 1.0},
    'RESPIRATORY_DISTRESS': {'weight': 10, 'agencies': ['SEATTLE_FD'], 'medical': 1.0},
    'CHILD_BIRTH': {'weight': 2, 'agencies': ['SEATTLE_FD'], 'obstetric': 1.0},
    'PSYCHIATRIC_EMERGENCY': {'weight': 8, 'agencies': ['SEATTLE_FD'], 'crisis': 0.5},
    'FALL': {'weight': 12, 'agencies': ['SEATTLE_FD'], 'medical': 1.0},
    'MOTOR_VEHICLE_ACCIDENT': {'weight': 10, 'agencies': ['SEATTLE_FD'], 'trauma': 0.4},
    'ELECTROCUTION': {'weight': 1, 'agencies': ['SEATTLE_FD'], 'trauma': 1.0},
    'DROWNING': {'weight': 2, 'agencies': ['SEATTLE_FD'], 'water_rescue': 1.0},
    'HIGH_ANGLE_RESCUE': {'weight': 1, 'agencies': ['SEATTLE_FD'], 'technical_rescue': 1.0},
    'CONFINED_SPACE': {'weight': 1, 'agencies': ['SEATTLE_FD'], 'technical_rescue': 1.0},
}

# Seattle hospitals and medical facilities
SEATTLE_HOSPITALS = {
    'HARBORVIEW_MEDICAL_CENTER': {'type': 'TRAUMA_1', 'location': 'DOWNTOWN', 'capacity': 413},
    'UW_MEDICAL_CENTER': {'type': 'ACADEMIC', 'location': 'UNIVERSITY_DISTRICT', 'capacity': 450},
    'VIRGINIA_MASON_MEDICAL_CENTER': {'type': 'ACUTE_CARE', 'location': 'FIRST_HILL', 'capacity': 336},
    'SWEDISH_MEDICAL_CENTER_FIRST_HILL': {'type': 'ACUTE_CARE', 'location': 'FIRST_HILL', 'capacity': 697},
    'SWEDISH_MEDICAL_CENTER_CHERRY_HILL': {'type': 'ACUTE_CARE', 'location': 'CENTRAL_DISTRICT', 'capacity': 175},
    'SWEDISH_MEDICAL_CENTER_BALLARD': {'type': 'ACUTE_CARE', 'location': 'BALLARD', 'capacity': 217},
    'NORTHWEST_HOSPITAL': {'type': 'ACUTE_CARE', 'location': 'NORTHGATE', 'capacity': 281},
    'SEATTLE_CHILDRENS_HOSPITAL': {'type': 'PEDIATRIC', 'location': 'LAURELHURST', 'capacity': 407},
}


# Enhanced data structures
@dataclass
class Person:
    person_id: str
    ssn: str
    first_name: str
    last_name: str
    date_of_birth: str
    sex: str
    race: str
    height: str
    weight: int
    hair_color: str
    eye_color: str
    address: str
    city: str
    state: str
    zip_code: str
    phone: str
    email: str
    emergency_contact: Dict
    criminal_history: List[Dict]
    warrants: str
    agency: str  # Make sure this field exists
    updated_date: str
    updated_time: str
    is_transient: bool
    drivers_license_number: str
    drivers_license_state: str
    created_date: str
    created_by_agency: str
    dl_expiration: str
    dl_class: str
    aliases: List[Dict]          # [{first_name,last_name,source,confidence}]
    languages: List[str]         # e.g., ['EN','ES']
    veteran_status: bool
    homeless_since: Optional[str]
    # Inmate-specific extensions (populated for inmates only)
    incarceration_site: Optional[str]
    inmate_key: Optional[str]
    parole_status: Optional[str]
    gang_membership_description: Optional[str]
    inmate_received_date: Optional[str]
    earliest_release_date: Optional[str]
    release_datetime: Optional[str]
    parole_offense: Optional[str]
    ccis_offender_key: Optional[str]
    parole_timing: Optional[str]
    incarceration_status: Optional[str]
    probation_status: Optional[str]
    probation_officer_name: Optional[str]
    probation_officer_phone_number: Optional[str]
    is_multistate_offender: Optional[bool]

@dataclass
class Vehicle:
    vehicle_id: str
    vin: str
    license_plate: str
    state: str
    make: str
    model: str
    year: int
    color: str
    body_type: str
    owner_person_id: str
    registration_expiration: str
    insurance_status: str
    stolen_status: str
    created_date: str
    created_by_agency: str
    stolen_flag_dt: Optional[str]
    lienholder: Optional[str]

@dataclass
class PoliceIncident:
    incident_id: str
    incident_type: str
    incident_date: str
    incident_time: str
    call_datetime: str
    cad_id: str  # Add this missing attribute
    location: str
    latitude: float
    longitude: float
    district: str
    beat: str
    reporting_party: str
    reporting_party_phone: str
    incident_description: str
    primary_officer: str
    backup_officers: List[str]
    suspect_id: str
    victim_id: str
    witness_id: str
    evidence_collected: List[str]
    case_status: str
    caller_person_id: Optional[str]
    created_date: str
    created_by_agency: str

@dataclass
class Arrest:
    arrest_id: str
    cad_incident_id: str
    person_id: str
    arrest_datetime: str
    arrest_location: str
    arrest_latitude: float
    arrest_longitude: float
    arresting_officer: str
    backup_officers: List[str]
    arrest_reason: str
    charges: List[Dict]
    arrest_type: str
    arrest_method: str
    use_of_force: bool
    force_type: str
    injuries_sustained: bool
    injury_description: str
    arrestee_condition: str
    transport_method: str
    destination: str
    booking_datetime: str
    miranda_read: bool
    miranda_datetime: str
    search_authorization: str
    evidence_collected: List[str]
    evidence_property_ids: List[str]
    witness_statements: List[str]
    agency: str  # Add this missing attribute
    created_date: str
    created_by_agency: str
    pc_narrative: str
    intoxication_bac: Optional[float]
    restraint_details: List[str]         # ['HANDCUFFS','LEG_IRONS']
    transport_unit_id: Optional[str]

@dataclass
class JailBooking:
    booking_id: str
    person_id: str
    arrest_id: str
    booking_number: str
    inmate_number: str
    booking_datetime: str
    booking_officer: str
    booking_type: str  # NEW_ARREST, WARRANT, COURT_ORDER, TRANSFER
    housing_assignment: str
    housing_datetime: str
    classification_level: str  # MINIMUM, MEDIUM, MAXIMUM
    special_housing: List[str]  # MEDICAL, MENTAL_HEALTH, PROTECTIVE_CUSTODY
    medical_screening_datetime: str
    medical_screening_nurse: str
    medical_alerts: List[str]
    mental_health_screening: bool
    suicide_risk_level: str  # NONE, LOW, MEDIUM, HIGH
    charges_at_booking: List[Dict]
    personal_property: List[str]  # Property IDs
    visitors: List[Dict]  # Visitor logs
    phone_calls: List[Dict]  # Phone call logs
    disciplinary_actions: List[Dict]
    programs_enrolled: List[str]  # EDUCATION, SUBSTANCE_ABUSE, etc.
    court_dates: List[Dict]
    bail_amount: float
    bail_posted: bool
    release_datetime: str
    release_type: str  # BAIL, TIME_SERVED, DISMISSED, TRANSFER
    release_officer: str
    days_served: int
    created_date: str

@dataclass
class JailSentence:
    sentence_id: str
    person_id: str
    booking_id: Optional[str]
    court_case_number: str
    sentence_date: str
    offense_description: str
    statute: str
    orc_code: Optional[str]
    sentence_type: str            # JAIL_TIME, PROBATION, COMMUNITY_SERVICE, SUSPENDED
    total_days: int
    time_served_days: int
    start_date: str
    end_date: Optional[str]
    concurrent_with_case_numbers: List[str]
    good_time_eligible: bool
    notes: Optional[str]
    created_date: str
    # Additional sentencing attributes
    crime_date: Optional[str]
    definite_stated_term_years: Optional[float]
    minimum_years: Optional[float]
    maximum_years: Optional[float]
    return_violator_offender_years: Optional[float]
    major_drug_offender_years: Optional[float]
    gun_years: Optional[float]
    commitment_county_years: Optional[float]
    # Inmate identity snapshot
    inmate_first_name: Optional[str]
    inmate_middle_name: Optional[str]
    inmate_last_name: Optional[str]
    inmate_date_of_birth: Optional[str]
    inmate_key: Optional[str]

@dataclass
class CorrectionsFacility:
    facility_id: str
    name: str
    address: str
    city: str
    state: str
    zip_code: str
    capacity: int
    security_level: str           # MINIMUM, MEDIUM, MAXIMUM
    contact_phone: str
    contact_email: Optional[str]
    warden_name: Optional[str]
    created_date: str

@dataclass
class JailIncident:
    incident_id: str
    booking_id: Optional[str]
    person_id: Optional[str]
    incident_datetime: str
    incident_type: str            # ASSAULT, CONTRABAND, SELF_HARM, MEDICAL, PROPERTY_DAMAGE
    location: str                 # e.g., POD A, DAYROOM, CELL 2
    involved_inmates: List[str]   # person_ids
    involved_staff: List[str]
    description: str
    actions_taken: List[str]      # e.g., MEDICAL_EVAL, DISCIPLINARY, ISOLATION
    referral_to_outside_agency: Optional[str]
    created_date: str

@dataclass
class BailBond:
    bond_id: str
    person_id: str
    booking_id: Optional[str]
    bond_type: str                # CASH, SURETY, PROPERTY, PERSONAL_RECOGNIZANCE
    amount: float
    posted: bool
    posted_datetime: Optional[str]
    poster_name: Optional[str]
    bondsman_company: Optional[str]
    receipt_number: Optional[str]
    conditions: List[str]
    exonerated: bool
    exonerated_datetime: Optional[str]
    created_date: str

@dataclass
class JailProgram:
    program_id: str
    name: str
    category: str                 # EDUCATION, SUBSTANCE_ABUSE, VOCATIONAL, MENTAL_HEALTH
    provider: str
    location: str
    schedule: str                 # e.g., Mon/Wed/Fri 10:00-12:00
    capacity: int
    enrolled_person_ids: List[str]
    waitlist_person_ids: List[str]
    start_date: str
    end_date: Optional[str]
    active: bool
    created_date: str

@dataclass
class JailLog:
    log_id: str
    booking_id: str
    person_id: str
    log_datetime: str
    action_type: str            # BOOKED, HOUSING_CHANGE, MEDICAL_INTAKE, COURT_APPEARANCE, RELEASED, DISCIPLINARY, PROGRAM_ENROLL, PROGRAM_COMPLETE
    details: Dict               # free-form structured fields, e.g., {from: 'POD A', to: 'POD B'}
    actor: str                  # staff name or system
    created_date: str

@dataclass
class Property:
    property_id: str
    property_type: str  # EVIDENCE, FOUND, STOLEN, SEIZED
    case_number: str
    incident_number: str
    description: str
    category: str  # WEAPON, DRUG, ELECTRONICS, JEWELRY, etc.
    subcategory: str
    serial_number: str
    make_model: str
    value_estimated: float
    currency_amount: float
    quantity: int
    unit_of_measure: str
    found_location: str
    found_date: str
    found_by_officer: str
    owner_person_id: str
    chain_of_custody: List[Dict]  # Officer, date, purpose
    evidence_locker: str
    destruction_date: str
    disposition: str  # RELEASED, DESTROYED, AUCTION, HELD
    created_date: str
    agency: str
    barcode: str
    storage_bin: str
    photo_count: int
    lab_case_number: Optional[str]
    analysis_results: Optional[str]

@dataclass
class FireIncident:
    incident_id: str
    incident_number: str
    call_number: str
    incident_type: str
    incident_subtype: str
    nfirs_code: str
    alarm_datetime: str
    dispatch_datetime: str
    en_route_datetime: str
    arrive_datetime: str
    controlled_datetime: str
    last_unit_cleared_datetime: str
    response_time_seconds: int
    suppression_time_seconds: int
    on_scene_time_seconds: int
    total_time_seconds: int
    address: str
    city: str
    latitude: float
    longitude: float
    district: str
    first_due_station: int
    units_responding: List[str]
    incident_commander: str
    fire_cause: str
    fire_origin: str
    ignition_factor: str
    property_type: str
    property_use: str
    occupancy_type: str
    construction_type: str
    stories: int
    total_floor_area: int
    property_loss: float
    contents_loss: float
    casualties: List[Dict]
    fatalities: int
    injuries: int
    caller_person_id: Optional[str]
    created_date: str

@dataclass
class FireRMSIncident:
    incident_id: str
    incident_number: str
    incident_type_code: str
    incident_datetime: str
    initial_dispatch_code: str
    shift: str
    district: str
    district_name: str
    modified_datetime: str
    station: str
    action_descriptions: List[str]
    action_codes: List[str]
    final_incident_type: str
    incident_times: Dict[str, str]  # call, dispatch_notified, alarm, arrival, last_unit_cleared, response_time
    location: str
    wildland_address: bool
    location_type_code: str
    property_use_code: str
    location_details: str
    suppression_apparatus_count: int
    suppression_personnel_count: int
    ems_apparatus_count: int
    ems_personnel_count: int
    personnel_deployed: List[str]
    apparatus_deployed: List[str]
    aid_code: str
    aid_details: str
    resources_include_mutual_aid: bool
    is_locked: bool
    is_active: bool
    resource_form_used: bool

@dataclass
class FireShift:
    shift_id: str
    unit_name: str            # ENGINE 117
    unit_code: str            # E117
    station: str              # STATION_XX
    location: str             # address, city, state zip
    employee_ids: List[str]
    employee_names: List[str]
    qualifiers: List[str]     # paramedic, firefighter, company officer
    work_type: str            # regular, overtime
    scheduled_by: List[str]
    start_datetime: str
    end_datetime: str
    length_minutes: int

@dataclass
class FirePersonnel:
    personnel_id: str            # unique UUID
    employee_id: str             # department employee id
    full_name: str               # "First Last"
    first_name: str
    last_name: str
    group: str                   # station/group/team
    role: str                    # FIREFIGHTER, PARAMEDIC, CAPTAIN, BATTALION_CHIEF, etc.
    phone_mobile: str
    phone_work: Optional[str]
    email: str                   # first initial + last name @ firedepartment.org
    username: str                # same scheme as email local part
    user_id: str                 # login/user id (could mirror username)
    date_of_hire: str            # YYYY-MM-DD
    station: Optional[str]
    unit: Optional[str]
    shift: Optional[str]         # A/B/C
    active: bool
    certifications: List[str]
    created_date: str
    updated_date: str

@dataclass
class FireReport:
    report_id: str
    incident_number: str
    created_datetime: str
    report_writer_employee_id: str
    report_writer_name: str
    narrative: str

# Namespace-style grouping for fire-related dataclasses without breaking existing references
class Fire:
    Incident = FireIncident
    RMSIncident = FireRMSIncident
    Shift = FireShift
    Personnel = FirePersonnel
    Report = FireReport

# Namespace-style grouping for jail-related dataclasses
class Jail:
    Booking = JailBooking
    Sentence = JailSentence
    Facility = CorrectionsFacility
    Incident = JailIncident
    BailBond = BailBond
    Program = JailProgram
    Log = JailLog

@dataclass
class EMSIncident:
    incident_id: str
    incident_number: str
    call_number: str
    incident_type: str
    incident_subtype: str
    priority: str
    call_datetime: str
    dispatch_datetime: str
    en_route_datetime: str
    arrive_datetime: str
    transport_datetime: str
    hospital_arrival_datetime: str
    clear_datetime: str
    dispatch_to_enroute_seconds: int
    enroute_to_arrival_seconds: int
    arrival_to_transport_seconds: int
    transport_to_hospital_seconds: int
    total_scene_time_seconds: int
    total_incident_time_seconds: int
    address: str
    city: str
    state: str
    zip_code: str
    district: str
    responding_unit: str
    crew_members: List[str]
    patient_person_id: str
    patient_age: int
    patient_sex: str
    chief_complaint: str
    primary_impression: str
    vital_signs: Dict
    treatment_provided: List[str]
    medications_given: List[str]
    transport_destination: str
    transport_mode: str
    created_date: str
    # Enhanced EMS fields based on schema
    complaint_reported_by_dispatch: str
    patient_full_name: str
    unit_call_sign: str
    patient_contact: bool
    patient_disposition: str
    crew_disposition: str
    patient_evaluation_care_disposition: str
    transport_disposition: str
    transfer_destination: str
    destination_type: str
    transportation_method: str
    unit_level_of_care: str
    cad_emd_code: str
    prearrival_activation: str
    # Medical Details fields
    complaint_reported_by_dispatch_code: str
    patient_acuity: str
    attempted_procedures: List[str]
    successful_procedures: List[str]
    cardiac_arrest_datetime: Optional[str]
    cardiac_arrest_resuscitation_discontinuation_datetime: Optional[str]
    ecg_findings: str
    incident_emd_performed: str
    incident_emd_performed_code: str
    cad_level_of_care_provided: str
    incident_level_of_care_provided: str
    provider_primary_impression: str
    situation_patient_acuity: str
    # Crew Details fields
    crew_member_name: str
    crew_member_level: str
    crew_badge_number: str
    # Patient Details fields
    patient_id: str
    patient_date_of_birth: str
    patient_weight: float
    patient_home_address: str
    is_superuser: bool
    # Unit Details fields
    agency_number: str
    agency_name: str
    agency_affiliation: str
    primary_unit_role: str
    # Incident Dates/Times fields
    total_commit_time: int
    unit_notified_by_dispatch_datetime: str
    unit_arrived_at_patient_datetime: str
    transfer_of_ems_patient_care_datetime: str
    arrival_at_destination_landing_area_datetime: str
    unit_left_scene_datetime: str
    patient_arrived_at_destination_datetime: str
    unit_back_in_service_datetime: str
    last_modified: str
    # Ungrouped Properties fields
    incident_status: str
    created_by: str
    patient_pk: str
    primary_patient_caregiver_on_scene: str
    crew_with_als_pt_contact_response_role: str

# REMOVED: Duplicate CADIncident dataclass definition - using the one at line 1067

# REMOVED: Duplicate Case dataclass definition - using the one at line 1036

@dataclass
class Officer:
    officer_id: str
    badge_number: str
    first_name: str
    last_name: str
    rank: str
    unit_id: Optional[str]
    unit_name: Optional[str]
    call_sign: Optional[str]
    agency: str
    email: Optional[str]
    phone: Optional[str]
    hire_date: str
    active: bool
    certifications: List[str]
    trainings: List[str]
    specialties: List[str]
    current_assignment: Optional[str]
    created_date: str
    updated_date: str
    bodycam_id: Optional[str]
    shift_status: str            # ON_DUTY, OFF_DUTY, ON_CALL
    supervisor_officer_id: Optional[str]

@dataclass
class CallForService:
    cfs_id: str
    call_number: str
    call_type: str
    priority: str
    status: str
    call_received_datetime: str
    call_entered_datetime: str
    dispatch_datetime: str
    en_route_datetime: str
    on_scene_datetime: str
    clear_datetime: str
    caller_person_id: Optional[str]
    caller_phone: str
    address: str
    city: str
    state: str
    zip_code: str
    latitude: float
    longitude: float
    district: str
    beat: str
    units_assigned: List[str]
    primary_officer_id: Optional[str]
    backup_officer_ids: List[str]
    related_cad_id: Optional[str]
    related_police_incident_id: Optional[str]
    related_fire_incident_id: Optional[str]
    related_ems_incident_id: Optional[str]
    notes: Optional[str]
    created_date: str
    created_by_agency: str

@dataclass
class Citation:
    citation_id: str
    citation_number: str
    source: str  # e.g., "Mark43 RMS"
    agency: str
    citation_type: str  # e.g., "TRAFFIC", "PARKING", "CRIMINAL"
    is_commercial_driver: bool
    violation_summary: str  # e.g., "VC 21650 - drive wrong"
    citation_datetime: str
    date_entered: str
    address: str
    beat: str
    reporting_district: str
    officer: str  # officer name
    person_cited_name: str  # full name
    first_name: str
    middle_name: Optional[str]
    last_name: str
    date_of_birth: str
    sex: str
    race: str
    drivers_license_number: str
    dl_state: str
    residential_address: str
    license_plate_number: str
    vehicle_year: int
    vehicle_color: str
    vehicle_make: str
    vehicle_model: str
    approximate_speed: Optional[int]
    speed_limit: Optional[int]
    is_confidential: bool
    # Legacy fields for backward compatibility
    person_id: str
    vehicle_id: Optional[str]
    officer_id: str
    cad_incident_id: Optional[str]
    police_incident_id: Optional[str]
    city: str
    state: str
    zip_code: str
    latitude: float
    longitude: float
    violations: List[Dict]  # [{code, description, statute, fine_amount}]
    total_fine_amount: float
    court_name: str
    court_address: str
    court_date: Optional[str]
    disposition: str  # ISSUED, DISMISSED, PAID, CONVERTED_TO_WARRANT, etc.
    status: str       # OPEN, CLOSED, PENDING_COURT
    created_date: str
    # Case linking fields
    related_case_id: Optional[str]       # If this citation led to a follow-up case
    case_number: Optional[str]           # Case number if applicable
    created_by_agency: str
    speed: Optional[int]
    radar_device_id: Optional[str]
    payment_status: str                  # UNPAID, PARTIAL, PAID
    balance_due: float

@dataclass
class Case:
    case_id: str
    case_number: str
    agency: str
    case_status: str
    reported_date_time: str
    start_date_time: str
    end_date_time: str
    offense_start_date_time: str
    zip: str
    offense_code: str
    nibrs_code: str
    approved_by: str  # Real officer name
    approved_date_time: str
    is_reviewed: bool
    reviewed_date_time: Optional[str]
    jacket_id: str  # Person entity that is a suspect
    is_restricted: bool
    bcs_code: str
    bcs_description: str  # Corresponds to BCS code
    synopsis: str  # Brief description of the incident
    closed_date: Optional[str]
    arrest_date_time: Optional[str]
    threat_to_life_indicator: bool
    created_date: str
    # CAD incident linking fields
    cad_incident_id: Optional[str]  # Source CAD incident
    cad_incident_type: Optional[str]  # Original CAD incident type
    cad_incident_description: Optional[str]  # Original CAD incident description
    # Arrest linking fields
    arrest_id: Optional[str]  # Source arrest that led to this case
    arrest_datetime: Optional[str]  # When the arrest occurred

@dataclass
class CADIncident:
    cad_id: str
    agency: str
    cad_source_link: str
    call_date_time: str
    incident_number: str
    address: str
    apartment_number: Optional[str]
    reporting_district: str
    beat: str
    incident_type_code: str
    incident_type_description: str
    incident_priority: str
    incident_status: str
    cad_safety_flag: bool
    reporting_party_name: Optional[str]
    reporting_party_phone: Optional[str]
    is_self_initiated: bool
    call_entry_date_time: str
    created_date: str
    # Person linking fields
    person_id: Optional[str]  # Primary person involved in the incident
    person_name: Optional[str]  # Person's full name
    person_phone: Optional[str]  # Person's phone number

@dataclass
class Offense:
    offense_id: str
    parent_type: str  # 'CASE' | 'CITATION'
    parent_id: str
    person_id: Optional[str]
    nibrs_offense: str
    nibrs_code: str
    nibrs_group_name: str
    nibrs_crime_against: str
    statute: str
    description: str
    severity: str
    offense_start_datetime: str
    offense_end_datetime: str
    created_date: str
    created_by_agency: str
    weapon_involved: bool
    victim_count: int
    loss_value: float
    attempt_flag: bool

@dataclass
class FieldInterview:
    fi_id: str
    source: str  # e.g., "Mark43 RMS", "Spillman"
    agency: str
    fi_number: str  # Field Interview number
    case_number: Optional[str]
    incident_number: Optional[str]
    contact_reason: str  # e.g., "415 LOITERERS"
    contact_type: str  # e.g., "pedestrian stop"
    fi_datetime: str
    fi_location: str  # location description
    beat: str
    reporting_district: str
    full_name: str  # person's full name
    officer: str  # officer name
    is_confidential: bool
    # Legacy fields for backward compatibility
    person_id: str
    officer_id: str
    vehicle_id: Optional[str]
    reason: str
    outcome: str
    notes: str
    address: str
    city: str
    state: str
    zip_code: str
    latitude: float
    longitude: float
    related_cad_id: Optional[str]
    related_incident_id: Optional[str]
    created_date: str
    created_by_agency: str

class EnhancedDataGenerator:
    def __init__(self):
        self.persons = []
        self.vehicles = []
        self.properties = []
        self.police_incidents = []
        self.arrests = []
        self.jail_bookings = []
        self.fire_incidents = []
        self.ems_incidents = []
        self.officers = []
        self.fire_personnel = []
        self.fire_reports = []
        self.calls_for_service = []
        self.citations = []
        self.field_interviews = []
        self.cases = []
        self.cad_incidents = []
        self.offenses = []
        # Fast lookup for persons by id
        self.person_index = {}
        # Jail collections
        self.corrections_facilities = []
        self.jail_sentences = []
        self.jail_incidents = []
        self.bail_bonds = []
        self.jail_programs = []
        self.jail_logs = []
        
        # Tracking sets for unique identifiers
        self.used_ssns = set()
        self.used_license_plates = set()
        self.used_vins = set()
        self.used_incident_numbers = set()
        self.used_booking_numbers = set()
        
        # Cross-reference tracking
        self.person_incident_history = defaultdict(list)
        self.vehicle_owner_map = {}
        
        # Officer pool for consistency
        self.officer_pool = {}
        self.badge_to_officer = {}
        self.address_resident_map = defaultdict(list)
                
    def generate_arrest(self, cad_incident, person):
        """Generate an arrest record linked to a CAD incident and person"""
        # Arrest types and methods
        arrest_types = ['ON_VIEW', 'WARRANT', 'INVESTIGATIVE', 'TRAFFIC', 'DOMESTIC_VIOLENCE']
        arrest_methods = ['HANDCUFFS', 'VERBAL_COMMAND', 'PHYSICAL_RESTRAINT', 'TASER', 'FIREARM_DISPLAY']
        
        arrest_type = random.choice(arrest_types)
        arrest_method = random.choice(arrest_methods)
        
        # Generate arrest timing
        fmt = '%Y-%m-%d %H:%M:%S'
        if cad_incident is not None:
            # If linked to CAD incident, arrest should be after call time
            base = cad_incident.call_date_time
            base_dt = datetime.strptime(base, fmt) if isinstance(base, str) else base
            arrest_dt = base_dt + timedelta(minutes=15)
        else:
            # If no CAD incident, generate random recent time
            arrest_dt = fake.date_time_between(start_date='-30d', end_date='now')
        arrest_datetime = arrest_dt.strftime(fmt)
        
        # Generate booking time (1-3 hours after arrest)
        booking_delay = timedelta(hours=random.randint(1, 3))
        booking_datetime = arrest_dt + booking_delay
        
        # Generate arrest location (near CAD incident location)
        # Use default coordinates since the CADIncident doesn't have lat/lon
        arrest_lat = 47.6062 + random.uniform(-0.01, 0.01)  # Seattle area
        arrest_lon = -122.3321 + random.uniform(-0.01, 0.01)
        
        # Generate realistic arrest location description
        location_types = ['Street', 'Residence', 'Business', 'Parking Lot', 'Vehicle', 'Public Place']
        location_type = random.choice(location_types)
        if cad_incident is not None:
            arrest_location = f"{location_type} near {cad_incident.address}"
        else:
            # Generate a realistic address if no CAD incident
            address, city, state, zip_code = self._generate_king_county_address()
            arrest_location = f"{location_type} near {address}"
        
        # Generate charges based on arrest type
        charges = self.generate_arrest_charges(arrest_type)
        
        # Generate arrest details
        use_of_force = random.random() < 0.15  # 15% chance of use of force
        force_types = ['HANDCUFFS', 'PHYSICAL_RESTRAINT', 'TASER', 'OC_SPRAY', 'BATON']
        force_type = random.choice(force_types) if use_of_force else ''
        
        injuries_sustained = random.random() < 0.08  # 8% chance of injuries
        injury_descriptions = ['Minor abrasions', 'Bruising', 'Sprained wrist', 'Cut on hand']
        injury_description = random.choice(injury_descriptions) if injuries_sustained else ''
        
        arrestee_conditions = ['COOPERATIVE', 'RESISTANT', 'COMBATIVE', 'INTOXICATED', 'MENTAL_HEALTH_CRISIS']
        arrestee_condition = random.choice(arrestee_conditions)
        
        transport_methods = ['PATROL_CAR', 'AMBULANCE', 'WALKING', 'CITIZEN_TRANSPORT']
        transport_method = random.choice(transport_methods)
        
        destinations = ['King County Jail', 'Seattle City Jail', 'Harborview Medical Center']
        destination = random.choice(destinations)
        
        # Generate officers
        # Use consistent officer from pool
        agency = 'KCSO'  # Default agency for arrests
        primary_officer = self.get_or_create_officer(agency)
        arresting_officer = f"{primary_officer.first_name} {primary_officer.last_name} ({primary_officer.badge_number})"
        backup_officers = [f"{self.get_or_create_officer(agency).first_name} {self.get_or_create_officer(agency).last_name} ({self.get_or_create_officer(agency).badge_number})" for _ in range(random.randint(0, 2))]
        
        # Generate evidence and witness information
        evidence_types = ['PHOTOGRAPHS', 'VIDEO_RECORDING', 'PHYSICAL_EVIDENCE', 'BODY_CAMERA', 'DASH_CAMERA']
        evidence_collected = random.sample(evidence_types, random.randint(1, 3)) if random.random() < 0.7 else []
        # Create linked Property records for each piece of evidence collected
        evidence_property_ids = []
        if evidence_collected:
            for ev in evidence_collected:
                incident_id = cad_incident.cad_id if cad_incident is not None else f"AR-{uuid.uuid4()}"
                agency = getattr(cad_incident, 'created_by_agency', 'KCSO') if cad_incident is not None else 'KCSO'
                prop = self.generate_property(
                    incident_id=incident_id,
                    person_id=person.person_id,
                    agency=agency,
                    property_type='EVIDENCE',
                    description_override=f"{ev} collected as evidence."
                )
                self.properties.append(prop)
                evidence_property_ids.append(prop.property_id)
        
        witness_statements = []
        if random.random() < 0.6:  # 60% chance of witness statements
            num_witnesses = random.randint(1, 3)
            for i in range(num_witnesses):
                witness_statements.append(f"Witness {i+1} statement recorded")
        
        # Generate search authorization
        search_types = ['CONSENT', 'SEARCH_WARRANT', 'EXIGENT_CIRCUMSTANCES', 'INVENTORY_SEARCH', 'PROBABLE_CAUSE']
        search_authorization = random.choice(search_types)
        
        pc_narrative = random.choice(['Observed hand-to-hand transaction','Victim identified suspect on scene','Probable cause established via witness statements'])
        intoxication_bac = round(random.uniform(0.08, 0.24), 3) if arrest_type=='TRAFFIC' and random.random()<0.3 else None
        restraint_details = ['HANDCUFFS'] + (['LEG_IRONS'] if random.random()<0.05 else [])
        transport_unit_id = random.choice(['A1','B2','C3','D4']) if random.random()<0.8 else None
        
        return Arrest(
            arrest_id=f"AR-{datetime.now().year}-{random.randint(100000, 999999)}",
            cad_incident_id=cad_incident.cad_id if cad_incident is not None else None,
            person_id=person.person_id,
            arrest_datetime=arrest_datetime,
            arrest_location=arrest_location,
            arrest_latitude=arrest_lat,
            arrest_longitude=arrest_lon,
            arresting_officer=arresting_officer,
            backup_officers=backup_officers,
            arrest_reason=self.generate_arrest_reason(arrest_type, charges),
            charges=charges,
            arrest_type=arrest_type,
            arrest_method=arrest_method,
            use_of_force=use_of_force,
            force_type=force_type,
            injuries_sustained=injuries_sustained,
            injury_description=injury_description,
            arrestee_condition=arrestee_condition,
            transport_method=transport_method,
            destination=destination,
            booking_datetime=booking_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            miranda_read=random.random() < 0.95,  # 95% chance Miranda was read
            miranda_datetime=arrest_datetime,
            search_authorization=search_authorization,
            evidence_collected=evidence_collected,
            evidence_property_ids=evidence_property_ids,
            witness_statements=witness_statements,
            agency='KCSO',
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency='KCSO',
            pc_narrative=pc_narrative,
            intoxication_bac=intoxication_bac,
            restraint_details=restraint_details,
            transport_unit_id=transport_unit_id,
        )
    
    def generate_arrest_charges(self, arrest_type):
        """Generate arrest charges using a weighted distribution of offenses."""
        offenses = [
            # (label, category, severity, weight)
            ('Possession of Controlled Substance', 'DRUG', 'MISDEMEANOR', 100),
            ('Trafficking in Controlled Substance', 'DRUG', 'FELONY', 85),
            ('Having Weapons While Under Disability', 'WEAPONS', 'FELONY', 70),
            ('Theft', 'PROPERTY', 'MISDEMEANOR', 65),
            ('Felonious Assault', 'VIOLENT', 'FELONY', 60),
            ('Receiving Stolen Property', 'PROPERTY', 'MISDEMEANOR', 55),
            ('Failure to Comply with Order', 'COURT_ORDER', 'MISDEMEANOR', 50),
            ('Domestic Violence', 'VIOLENT', 'MISDEMEANOR', 45),
            ('Burglary', 'PROPERTY', 'FELONY', 40),
            ('Tampering with Evidence', 'JUSTICE', 'FELONY', 35),
            ('Breaking and Entering', 'PROPERTY', 'FELONY', 32),
            ('Robbery', 'VIOLENT', 'FELONY', 30),
            ('Rape', 'SEX', 'FELONY', 15),
            ('Gross Sexual Imposition', 'SEX', 'FELONY', 13),
            ('Aggravated Robbery', 'VIOLENT', 'FELONY', 12),
            ('Improperly Handling Firearms in a Motor Vehicle', 'WEAPONS', 'FELONY', 10),
            ('Involuntary Manslaughter', 'VIOLENT', 'FELONY', 8),
            ('Driving While Under the Influence of Alcohol', 'TRAFFIC', 'MISDEMEANOR', 7),
            ('Escape', 'COURT_ORDER', 'FELONY', 5),
        ]

        labels = [o[0] for o in offenses]
        weights = [o[3] for o in offenses]

        # Choose primary offense
        idx = random.choices(range(len(offenses)), weights=weights, k=1)[0]
        primary = offenses[idx]

        selected = [{
            'charge': primary[0],
            'category': primary[1],
            'severity': primary[2],
            'statute': f"RCW {random.randint(9, 69)}.{random.randint(10, 999)}"
        }]

        # Occasionally add a secondary lesser offense
        if random.random() < 0.25:
            # Avoid duplicates; bias toward ancillary charges
            ancillary_pool = [
                ('Failure to Comply with Order', 'COURT_ORDER', 'MISDEMEANOR'),
                ('Tampering with Evidence', 'JUSTICE', 'FELONY'),
                ('Receiving Stolen Property', 'PROPERTY', 'MISDEMEANOR'),
                ('Theft', 'PROPERTY', 'MISDEMEANOR'),
            ]
            anc = random.choice(ancillary_pool)
            if anc[0] != primary[0]:
                selected.append({
                    'charge': anc[0],
                    'category': anc[1],
                    'severity': anc[2],
                    'statute': f"RCW {random.randint(9, 69)}.{random.randint(10, 999)}"
                })

        return selected
    
    def generate_arrest_reason(self, arrest_type, charges):
        """Generate realistic arrest reason based on type and charges"""
        reasons = {
            'ON_VIEW': 'Officer observed criminal activity in progress',
            'WARRANT': 'Active arrest warrant for failure to appear',
            'INVESTIGATIVE': 'Investigation revealed probable cause for arrest',
            'TRAFFIC': 'Traffic stop resulted in arrest for DUI',
            'DOMESTIC_VIOLENCE': 'Domestic violence incident reported and investigated'
        }
        
        return reasons.get(arrest_type, 'Arrest made based on probable cause')
    
    def generate_associate(self, person_id, person_ethnicity):
        """Generate an associate linked to a person"""
        # Different types of associates
        associate_types = ['CODEFENDANT', 'COCONSPIRATOR', 'ACCOMPLICE', 'WITNESS', 'INFORMANT', 'VICTIM']
        associate_type = random.choice(associate_types)
        
        # 60% chance of same ethnicity for criminal associates, 40% chance of different
        if associate_type in ['CODEFENDANT', 'COCONSPIRATOR', 'ACCOMPLICE']:
            same_ethnicity = random.random() < 0.6
        else:
            same_ethnicity = random.random() < 0.3
        
        if same_ethnicity:
            associate_ethnicity = person_ethnicity
        else:
            associate_ethnicity = random.choice(['WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'NATIVE_AMERICAN', 'OTHER'])
        
        # Get appropriate locale
        locale_fake = faker_locales[associate_ethnicity]
        if associate_ethnicity == 'ASIAN':
            locale_fake = random.choice(locale_fake)
        
        # Generate name
        associate_sex = random.choice(['M', 'F'])
        if associate_sex == 'M':
            associate_first_name = locale_fake.first_name_male()
        else:
            associate_first_name = locale_fake.first_name_female()
        
        associate_last_name = locale_fake.last_name()
        
        # Generate address
        street_number = random.randint(100, 9999)
        street_names = ['Main St', 'Oak Ave', 'Pine Rd', 'Cedar Ln', 'Maple Dr', 'Elm St', 'Washington Ave', 'Lake Dr']
        street_name = random.choice(street_names)
        associate_address = f"{street_number} {street_name}"
        
        # Generate phone
        seattle_area_codes = ['206', '425', '360', '509']
        area_code = random.choice(seattle_area_codes)
        associate_phone = f"({area_code}){fake.msisdn()[:3]}-{fake.msisdn()[:4]}"
        
        return {
            'associate_id': f"A-{datetime.now().year}-{random.randint(100000, 999999)}",
            'person_id': person_id,
            'name': f"{associate_first_name} {associate_last_name}",
            'type': associate_type,
            'relationship': self.generate_associate_relationship(associate_type),
            'address': associate_address,
            'city': random.choice(['Seattle', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island']),
            'state': 'WA',
            'phone': associate_phone,
            'ethnicity': associate_ethnicity,
            'criminal_history': self.generate_criminal_history(),
            'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def generate_associate_relationship(self, associate_type):
        """Generate realistic relationship based on associate type"""
        relationships = {
            'CODEFENDANT': ['CODEFENDANT', 'COCONSPIRATOR', 'ACCOMPLICE'],
            'COCONSPIRATOR': ['COCONSPIRATOR', 'CODEFENDANT', 'ACCOMPLICE'],
            'ACCOMPLICE': ['ACCOMPLICE', 'CODEFENDANT', 'COCONSPIRATOR'],
            'WITNESS': ['WITNESS', 'INFORMANT', 'VICTIM'],
            'INFORMANT': ['INFORMANT', 'WITNESS', 'VICTIM'],
            'VICTIM': ['VICTIM', 'WITNESS']
        }
        return random.choice(relationships.get(associate_type, ['UNKNOWN']))
    
    def generate_suspect(self, incident_id, person_id=None):
        """Generate a suspect linked to an incident"""
        # Generate suspect person if not provided
        if not person_id:
            person = self.generate_person()
            person_id = person.person_id
        
        # Suspect details
        suspect_status = random.choice(['ACTIVE', 'ARRESTED', 'WANTED', 'CLEARED'])
        arrest_date = None
        if suspect_status == 'ARRESTED':
            arrest_date = fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            'suspect_id': f"S-{datetime.now().year}-{random.randint(100000, 999999)}",
            'incident_id': incident_id,
            'person_id': person_id,
            'status': suspect_status,
            'arrest_date': arrest_date,
            'charges': (charges := self.generate_charges()),
            'bail_amount': self.calculate_bail_amount([charges]) if suspect_status == 'ARRESTED' else None,
            'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def generate_arrestee(self, incident_id, person_id=None):
        """Generate an arrestee with full arrest details"""
        # Generate arrestee person if not provided
        if not person_id:
            person = self.generate_person()
            person_id = person.person_id
        
        arrest_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        booking_datetime = arrest_datetime + timedelta(hours=random.randint(1, 6))
        
        return {
            'arrestee_id': f"AR-{datetime.now().year}-{random.randint(100000, 999999)}",
            'incident_id': incident_id,
            'person_id': person_id,
            'arrest_datetime': arrest_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'booking_datetime': booking_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'arresting_officer': f"Officer {fake.last_name()}",
            'arrest_location': self.generate_arrest_location(),
            'charges': (charges := self.generate_charges()),
            'bail_amount': self.calculate_bail_amount([charges]),
            'jail_facility': random.choice(['King County Jail', 'Seattle City Jail']),
            'release_date': None,  # Will be set if released
            'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def generate_arrest_location(self):
        """Generate realistic arrest location in Seattle area"""
        locations = [
            'Downtown Seattle', 'Capitol Hill', 'Ballard', 'Fremont', 'Queen Anne',
            'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island'
        ]
        return random.choice(locations)
    
    def calculate_bail_amount(self, charges):
        """Calculate realistic bail amount based on charge severity and type"""
        if not charges:
            return 0
        
        # Base bail amounts by severity and category
        # Note: Traffic infractions typically don't require bail - they're handled as citations/fines
        bail_schedule = {
            'INFRACTION': {
                'TRAFFIC': {'min': 0, 'max': 0},  # Traffic infractions = citations, no bail
                'OTHER': {'min': 50, 'max': 200}
            },
            'MISDEMEANOR': {
                'DRUG': {'min': 1000, 'max': 5000},
                'PROPERTY': {'min': 500, 'max': 2500},
                'VIOLENT': {'min': 2000, 'max': 10000},
                'TRAFFIC': {'min': 500, 'max': 2000},  # Reduced for traffic misdemeanors (DUI, etc.)
                'COURT_ORDER': {'min': 1500, 'max': 5000},
                'OTHER': {'min': 500, 'max': 2000}
            },
            'FELONY': {
                'DRUG': {'min': 10000, 'max': 50000},  # Increased for serious drug crimes
                'PROPERTY': {'min': 5000, 'max': 25000},  # Increased for property felonies
                'VIOLENT': {'min': 25000, 'max': 500000},  # Much higher for violent felonies
                'WEAPONS': {'min': 15000, 'max': 100000},  # Increased for weapon violations
                'SEX': {'min': 50000, 'max': 1000000},  # Highest bail for sex crimes
                'JUSTICE': {'min': 25000, 'max': 200000},  # Increased for justice system violations
                'COURT_ORDER': {'min': 30000, 'max': 200000}  # Increased for court order violations
            }
        }
        
        total_bail = 0
        
        # Calculate bail for each charge
        for charge in charges:
            severity = charge.get('severity', 'MISDEMEANOR')
            category = charge.get('category', 'OTHER')
            
            # Get bail range for this severity/category
            if severity in bail_schedule and category in bail_schedule[severity]:
                bail_range = bail_schedule[severity][category]
                charge_bail = random.randint(bail_range['min'], bail_range['max'])
            else:
                # Default bail amounts if category not found
                if severity == 'INFRACTION':
                    charge_bail = random.randint(100, 500)
                elif severity == 'MISDEMEANOR':
                    charge_bail = random.randint(1000, 5000)
                else:  # FELONY
                    charge_bail = random.randint(5000, 25000)
            
            total_bail += charge_bail
        
        # Apply stacking multipliers for multiple charges based on severity
        if len(charges) > 1:
            # Count charges by severity
            felony_count = sum(1 for c in charges if c.get('severity') == 'FELONY')
            misdemeanor_count = sum(1 for c in charges if c.get('severity') == 'MISDEMEANOR')
            
            # Progressive stacking based on charge severity and count
            if felony_count >= 2:
                total_bail = int(total_bail * 1.8)  # 80% increase for multiple felonies
            elif felony_count == 1 and misdemeanor_count >= 2:
                total_bail = int(total_bail * 1.6)  # 60% increase for felony + multiple misdemeanors
            elif len(charges) == 2:
                total_bail = int(total_bail * 1.3)  # 30% increase for 2 charges
            elif len(charges) >= 3:
                total_bail = int(total_bail * 1.7)  # 70% increase for 3+ charges
        
        # Traffic infractions should not result in bail - they're handled as citations
        if all(charge.get('severity') == 'INFRACTION' and charge.get('category') == 'TRAFFIC' for charge in charges):
            return 0  # No bail for traffic infractions - handled as citations
        
        # Some charges may result in no bail (held without bail)
        serious_violent_crimes = ['murder', 'rape', 'aggravated assault', 'involuntary manslaughter', 'kidnapping', 'armed robbery']
        if any(charge.get('charge', '').lower() in serious_violent_crimes for charge in charges):
            if random.random() < 0.4:  # 40% chance of no bail for serious violent crimes
                return 0  # No bail - held without bail
        
        # Multiple serious felonies increase chance of no bail
        felony_count = sum(1 for c in charges if c.get('severity') == 'FELONY')
        if felony_count >= 3 and random.random() < 0.2:  # 20% chance for 3+ felonies
            return 0  # No bail - held without bail
        
        return total_bail

    def generate_charges(self):
        """Generate realistic criminal charges"""
        charge_categories = {
            'DRUG': ['Possession of Controlled Substance', 'Drug Trafficking', 'Possession of Marijuana'],
            'PROPERTY': ['Theft', 'Burglary', 'Robbery', 'Vandalism'],
            'VIOLENT': ['Assault', 'Domestic Violence', 'Disorderly Conduct'],
            'TRAFFIC': ['DUI', 'Reckless Driving', 'Driving Without License'],
            'OTHER': ['Trespassing', 'Public Intoxication', 'Resisting Arrest']
        }
        
        category = random.choice(list(charge_categories.keys()))
        charge = random.choice(charge_categories[category])
        
        return {
            'charge': charge,
            'category': category,
            'severity': random.choice(['MISDEMEANOR', 'FELONY', 'INFRACTION']),
            'statute': f"RCW {random.randint(9, 69)}.{random.randint(10, 999)}"
        }
    
    def generate_criminal_history(self):
        """Generate realistic criminal history"""
        history = []
        num_convictions = random.randint(0, 5)
        
        for _ in range(num_convictions):
            conviction_date = fake.date_between(start_date='-10y', end_date='-1y')
            history.append({
                'conviction_date': conviction_date.strftime('%Y-%m-%d'),
                'charge': self.generate_charges()['charge'],
                'sentence': random.choice(['PROBATION', 'JAIL_TIME', 'FINE', 'COMMUNITY_SERVICE']),
                'disposition': 'CONVICTED'
            })
        
        return history
    
    def generate_jail_booking(self, person_id, arrest_id, agency='KCSO', charges=None, arrest_datetime=None):
        """Generate comprehensive jail booking record"""
        # Booking should happen shortly after arrest (1-6 hours)
        if arrest_datetime:
            arrest_dt = datetime.strptime(arrest_datetime, '%Y-%m-%d %H:%M:%S')
            # Ensure booking happens after arrest (1-6 hours later)
            booking_datetime = arrest_dt + timedelta(hours=random.randint(1, 6))
        else:
            # Fallback: generate recent booking time
            booking_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        
        # Classification based on charges and person history
        classification_levels = ['MINIMUM', 'MEDIUM', 'MAXIMUM']
        classification_weights = [40, 45, 15]
        classification_level = random.choices(classification_levels, classification_weights)[0]
        
        # Special housing considerations
        special_housing = []
        if random.random() < 0.1:  # 10% need medical housing
            special_housing.append('MEDICAL')
        if random.random() < 0.05:  # 5% need mental health housing
            special_housing.append('MENTAL_HEALTH')
        if random.random() < 0.02:  # 2% need protective custody
            special_housing.append('PROTECTIVE_CUSTODY')
        
        # Suicide risk assessment
        suicide_risk_levels = ['NONE', 'LOW', 'MEDIUM', 'HIGH']
        suicide_weights = [80, 15, 4, 1]
        suicide_risk = random.choices(suicide_risk_levels, suicide_weights)[0]
        
        # Release information
        days_served = random.randint(1, 365)
        release_types = ['BAIL', 'TIME_SERVED', 'DISMISSED', 'TRANSFER']
        release_weights = [40, 30, 20, 10]
        release_type = random.choices(release_types, release_weights)[0]
        
        booking = JailBooking(
            booking_id=str(uuid.uuid4()),
            person_id=person_id,
            arrest_id=arrest_id,
            booking_number=f"BK{booking_datetime.year}{random.randint(100000, 999999)}",
            inmate_number=f"IN{random.randint(100000, 999999)}",
            booking_datetime=booking_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            booking_officer=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            booking_type='NEW_ARREST',
            housing_assignment=f"BLOCK_{random.choice(['A', 'B', 'C', 'D'])}{random.randint(1, 20)}",
            housing_datetime=booking_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            classification_level=classification_level,
            special_housing=special_housing,
            medical_screening_datetime=booking_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            medical_screening_nurse=f"NURSE {fake.last_name().upper()}",
            medical_alerts=['DIABETES', 'HYPERTENSION'] if random.random() < 0.2 else [],
            mental_health_screening=random.random() < 0.3,
            suicide_risk_level=suicide_risk,
            charges_at_booking=[],  # Will be populated from arrest
            personal_property=[],  # Will be populated later
            visitors=[],  # Will be populated later
            phone_calls=[],  # Will be populated later
            disciplinary_actions=[],  # Will be populated later
            programs_enrolled=['EDUCATION', 'SUBSTANCE_ABUSE'] if random.random() < 0.4 else [],
            court_dates=[],  # Will be populated later
            bail_amount=self.calculate_bail_amount(charges) if charges else 0,
            bail_posted=random.random() < 0.6,
            release_datetime=(booking_datetime + timedelta(days=days_served)).strftime('%Y-%m-%d %H:%M:%S'),
            release_type=release_type,
            release_officer=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            days_served=days_served,
            created_date=booking_datetime.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        # Populate inmate-related fields on Person (persisting on in-memory person)
        person = self.person_index.get(person_id)
        if person:
            # incarceration site from curated facilities or default
            fac = random.choice(CORRECTIONS_FACILITY_CATALOG) if CORRECTIONS_FACILITY_CATALOG else None
            person.incarceration_site = (fac['name'] if fac else 'King County Correctional Facility')
            person.inmate_key = booking.inmate_number
            person.inmate_received_date = booking.booking_datetime
            person.incarceration_status = 'IN_CUSTODY'
            person.probation_status = random.choice(['NONE','ACTIVE','INACTIVE'])
            if person.probation_status != 'NONE':
                person.probation_officer_name = f"{fake.first_name()} {fake.last_name()}"
                person.probation_officer_phone_number = self._format_seattle_phone()
            person.parole_status = random.choice(['NONE','CONSIDERATION','ACTIVE','COMPLETED'])
            person.parole_offense = random.choice(['THEFT 3','ASSAULT 4','DUI','BURGLARY 2','POSSESSION']) if person.parole_status in ['CONSIDERATION','ACTIVE'] else None
            person.parole_timing = random.choice(['30_DAYS','60_DAYS','90_DAYS','UNKNOWN']) if person.parole_status in ['CONSIDERATION','ACTIVE'] else None
            person.gang_membership_description = (random.choice(['NONE','KNOWN_ASSOCIATE','DOCUMENTED_MEMBER']) if random.random()<0.15 else 'NONE')
            person.ccis_offender_key = f"CCIS-{random.randint(100000,999999)}"
            person.is_multistate_offender = (random.random()<0.1)

        return booking
    
    def generate_property(self, incident_id=None, person_id=None, agency='KCSO', property_type=None, description_override=None):
        """Generate comprehensive property/evidence record"""
        property_types = ['EVIDENCE', 'FOUND', 'STOLEN', 'SEIZED']
        weights = [40, 30, 20, 10]
        property_type = property_type or random.choices(property_types, weights=weights)[0]
        
        # Property categories (expanded for better variety)
        categories = {
            'WEAPON': ['FIREARM', 'KNIFE', 'BAT', 'TASER', 'PEPPER_SPRAY', 'BRASS_KNUCKLES', 'MACHETE'],
            'AMMUNITION': ['9MM', '45_ACP', '223_REM', '12_GAUGE', '22_LR', '40_S_W', '308_WIN'],
            'DRUG': ['MARIJUANA', 'METHAMPHETAMINE', 'HEROIN', 'COCAINE', 'PRESCRIPTION', 'FENTANYL', 'ECSTASY', 'XANAX'],
            'ELECTRONICS': ['PHONE', 'LAPTOP', 'TABLET', 'CAMERA', 'GAMING_SYSTEM', 'DRONE', 'SMARTWATCH', 'VR_HEADSET', 'E_READER', 'SMART_HOME_HUB'],
            'COMPUTER_ACCESSORY': ['HARD_DRIVE', 'USB_DRIVE', 'SD_CARD', 'MONITOR', 'PRINTER', 'GPU', 'CPU', 'MOTHERBOARD'],
            'MOBILE_ACCESSORY': ['SIM_CARD', 'CHARGER', 'HEADPHONES', 'CASE', 'POWER_BANK', 'BLUETOOTH_SPEAKER', 'CAR_MOUNT'],
            'JEWELRY': ['RING', 'NECKLACE', 'WATCH', 'BRACELET', 'EARRINGS', 'ANKLET', 'BROOCH'],
            'VEHICLE_PART': ['RADIO', 'WHEELS', 'CATALYTIC_CONVERTER', 'AIRBAG', 'GPS_UNIT', 'DASH_CAM', 'CAR_STEREO'],
            'TOOLS': ['POWER_DRILL', 'SAW', 'HAMMER', 'WRENCH_SET', 'AIR_COMPRESSOR', 'NAIL_GUN', 'CHAINSAW'],
            'MUSICAL_INSTRUMENT': ['GUITAR', 'KEYBOARD', 'VIOLIN', 'DRUM_SET', 'SAXOPHONE', 'TRUMPET', 'FLUTE'],
            'SPORTS_EQUIPMENT': ['BICYCLE', 'SKATEBOARD', 'BASKETBALL', 'GOLF_CLUBS', 'TENNIS_RACKET', 'CLIMBING_GEAR', 'SURFBOARD'],
            'HOUSEHOLD': ['TV', 'MICROWAVE', 'VACUUM', 'BLENDER', 'COFFEE_MAKER', 'AIR_PURIFIER', 'SPACE_HEATER', 'AIR_FRYER'],
            'ART': ['PAINTING', 'SCULPTURE', 'PRINT', 'PHOTOGRAPH', 'POSTER', 'LIMITED_EDITION_PRINT'],
            'ANTIQUES': ['CLOCK', 'VASE', 'FURNITURE', 'COIN_COLLECTION', 'STAMP_COLLECTION', 'TYPEWRITER'],
            'CLOTHING': ['JACKET', 'SHOES', 'PURSE', 'BACKPACK', 'HAT', 'BELT', 'GLOVES'],
            'CURRENCY': ['CASH', 'COINS', 'GIFT_CARDS', 'MONEY_ORDER'],
            'DOCUMENT': ['ID', 'CREDIT_CARD', 'CHECKBOOK', 'PASSPORT', 'BIRTH_CERTIFICATE', 'VEHICLE_TITLE', 'SOCIAL_SECURITY_CARD']
        }
        
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        # Value estimation
        value_ranges = {
            'WEAPON': (100, 2000),
            'AMMUNITION': (10, 500),
            'DRUG': (10, 1000),
            'ELECTRONICS': (50, 2500),
            'COMPUTER_ACCESSORY': (10, 800),
            'MOBILE_ACCESSORY': (5, 300),
            'JEWELRY': (100, 10000),
            'VEHICLE_PART': (50, 1500),
            'TOOLS': (20, 1500),
            'MUSICAL_INSTRUMENT': (100, 5000),
            'SPORTS_EQUIPMENT': (20, 1500),
            'HOUSEHOLD': (30, 2000),
            'ART': (100, 20000),
            'ANTIQUES': (50, 15000),
            'CLOTHING': (20, 500),
            'CURRENCY': (1, 1000),
            'DOCUMENT': (5, 100)
        }
        
        value_range = value_ranges[category]
        value_estimated = random.randint(value_range[0], value_range[1])
        
        # lab processing fields (define before use)
        lab_case_number = f"LAB-{random.randint(100000, 999999)}" if random.random() < 0.25 else None
        analysis_results = random.choice(['NEGATIVE','POSITIVE','INCONCLUSIVE']) if lab_case_number else None

        # Compute make/model and serial number probability by category
        brand_pool = ['SAMSUNG','APPLE','SONY','DELL','HP','BOSCH','DEWALT','YAMAHA','FENDER','KITCHENAID','GARMIN','CANON','NIKON','LENOVO','MICROSOFT']
        weapon_brands = ['GLOCK','SMITH_WESSON','SIG_SAUER','RUGER','BERETTA']
        instrument_brands = ['FENDER','GIBSON','YAMAHA','ROLAND','KORG']
        tool_brands = ['DEWALT','MAKITA','BOSCH','MILWAUKEE']
        household_brands = ['SAMSUNG','LG','WHIRLPOOL','KITCHENAID','DYSON']

        serial_prob_by_category = {
            'WEAPON': 0.95,
            'ELECTRONICS': 0.7,
            'COMPUTER_ACCESSORY': 0.5,
            'MOBILE_ACCESSORY': 0.3,
            'TOOLS': 0.5,
            'MUSICAL_INSTRUMENT': 0.5,
            'HOUSEHOLD': 0.4,
            'VEHICLE_PART': 0.6,
            'SPORTS_EQUIPMENT': 0.3,
            'JEWELRY': 0.2,
            'DRUG': 0.0,
            'AMMUNITION': 0.0,
            'CURRENCY': 0.0,
            'DOCUMENT': 0.0,
            'ART': 0.0,
            'ANTIQUES': 0.1,
            'CLOTHING': 0.1,
        }

        # Default make/model falsey
        make_model_val = ''
        if category == 'WEAPON':
            make_model_val = f"{random.choice(weapon_brands)} {random.choice(['19','M&P9','P320','LC9','92FS'])}"
        elif category in ['ELECTRONICS','COMPUTER_ACCESSORY','MOBILE_ACCESSORY']:
            make_model_val = f"{random.choice(brand_pool)} {random.choice(['PRO','PLUS','MAX','X','2000','III','S'])}"
        elif category == 'MUSICAL_INSTRUMENT':
            make_model_val = f"{random.choice(instrument_brands)} {random.choice(['STANDARD','CUSTOM','STAGE','PRO'])}"
        elif category == 'TOOLS':
            make_model_val = f"{random.choice(tool_brands)} {random.choice(['XR','FUEL','BRUSHLESS','PRO'])}"
        elif category == 'HOUSEHOLD':
            make_model_val = f"{random.choice(household_brands)} {random.choice(['DELUXE','ULTRA','PLUS','SERIES_5'])}"

        serial_chance = serial_prob_by_category.get(category, 0.3)
        serial_number_val = f"SN{random.randint(100000, 999999)}" if random.random() < serial_chance else ''

        # Units and quantity selection by category for variety
        unit_of_measure_val = 'EACH'
        quantity_val = random.randint(1, 10)
        currency_amount_val = value_estimated
        if category == 'DRUG':
            if subcategory == 'PRESCRIPTION':
                unit_of_measure_val = 'PILLS'
                quantity_val = random.randint(5, 60)
            else:
                unit_of_measure_val = 'GRAMS'
                quantity_val = random.randint(1, 500)
        elif category == 'AMMUNITION':
            unit_of_measure_val = 'ROUNDS'
            quantity_val = random.randint(10, 200)
        elif category == 'CURRENCY':
            unit_of_measure_val = 'USD'
            quantity_val = 1
        elif category == 'CLOTHING' and subcategory == 'SHOES':
            unit_of_measure_val = 'PAIR'

        # Build a deterministic, readable description without random extra text
        subcat_readable = subcategory.replace('_', ' ').title()
        detail_parts = []
        if make_model_val:
            detail_parts.append(make_model_val)
        if serial_number_val:
            # Show serial in a tidy way
            detail_parts.append(serial_number_val)
        details_suffix = f" ({', '.join(detail_parts)})" if detail_parts else ''
        action_map = {
            'EVIDENCE': 'collected as evidence',
            'FOUND': 'found and logged',
            'STOLEN': 'reported stolen',
            'SEIZED': 'seized by officers',
        }
        action_phrase = action_map.get(property_type, 'logged')
        description_val = description_override or f"{subcat_readable}{details_suffix} {action_phrase}."

        property_record = Property(
            property_id=str(uuid.uuid4()),
            property_type=property_type,
            case_number=f"PROP{random.randint(100000, 999999)}",
            incident_number=incident_id or '',
            description=description_val,
            category=category,
            subcategory=subcategory,
            serial_number=serial_number_val,
            make_model=make_model_val,
            value_estimated=value_estimated,
            currency_amount=currency_amount_val,
            quantity=quantity_val,
            unit_of_measure=unit_of_measure_val,
            found_location=self._generate_king_county_address()[0],  # Use proper Seattle address
            found_date=fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            found_by_officer=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            owner_person_id=person_id or '',
            chain_of_custody=[],
            evidence_locker=f"LOCKER_{random.choice(['A', 'B', 'C'])}{random.randint(1, 100)}",
            destruction_date='' if random.random() < 0.8 else fake.date_time_between(start_date='now', end_date='+1y').strftime('%Y-%m-%d'),
            disposition=random.choice(['HELD', 'RELEASED', 'DESTROYED', 'AUCTION']),
            created_date=fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            agency=agency,
            barcode=f"PROP-{random.randint(100000,999999)}",
            storage_bin=f"SHELF-{random.choice(list('ABCDEFG'))}-{random.randint(1,20)}",
            photo_count=random.randint(0,6),
            lab_case_number=lab_case_number,
            analysis_results=analysis_results,
        )
        
        return property_record

    def generate_person(self, agency='KCSO'):
        """Generate a sample person with consistent name-ethnicity mapping"""
        # Generate unique SSN
        while True:
            ssn = fake.ssn()
            if ssn not in self.used_ssns:
                self.used_ssns.add(ssn)
                break
        
        # Choose ethnicity first, then get appropriate names
        ethnicity = random.choice(['WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'NATIVE_AMERICAN', 'OTHER'])
        locale_fake = faker_locales[ethnicity]
        
        # For ASIAN, randomly select from available locales
        if ethnicity == 'ASIAN':
            locale_fake = random.choice(locale_fake)
        
        # Generate gender-consistent names
        sex = random.choice(['M', 'F'])
        if sex == 'M':
            first_name = locale_fake.first_name_male()
        else:
            first_name = locale_fake.first_name_female()
        
        last_name = locale_fake.last_name()
        
        # Generate realistic demographics
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        
        # Generate realistic Seattle-area address
        seattle_cities = ['Seattle', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island']
        city = random.choice(seattle_cities)
        
        # Generate realistic Seattle-area phone number
        seattle_area_codes = ['206', '425', '360', '509']
        area_code = random.choice(seattle_area_codes)
        phone = f"({area_code}){fake.msisdn()[:3]}-{fake.msisdn()[:4]}"
        
        # Generate realistic email
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"
        
        # Generate proper street address
        street_number = random.randint(100, 9999)
        street_names = ['Main St', 'Oak Ave', 'Pine Rd', 'Cedar Ln', 'Maple Dr', 'Elm St', 'Washington Ave', 'Lake Dr']
        street_name = random.choice(street_names)
        address = f"{street_number} {street_name}"
        
        # Generate enhanced contacts and legal status
        emergency_contact = self.generate_emergency_contact(ethnicity)
        criminal_legal_status = self.generate_criminal_legal_status()
        
        # Generate drivers license info
        drivers_license_number = f"{random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])}{random.randint(100000000, 999999999)}"
        drivers_license_state = random.choice(['WA', 'OR', 'CA', 'ID', 'MT', 'NV', 'UT', 'AZ'])
        
        # Determine if person is transient (15% chance)
        is_transient = random.random() < 0.15
        
        # Generate current timestamp for updates
        current_datetime = datetime.now()
        updated_date = current_datetime.strftime('%Y-%m-%d')
        updated_time = current_datetime.strftime('%H:%M:%S')
        
        address, city, state, zip_code = self._generate_king_county_address()
        phone = self._format_seattle_phone()
        
        # computed
        dl_expiration = (datetime.now() + timedelta(days=random.randint(180, 365*6))).strftime('%Y-%m-%d')
        dl_class = random.choice(['D','M','CDL'])
        aliases = [{'first_name': first_name, 'last_name': last_name, 'source': 'NCIC', 'confidence': round(random.uniform(0.6,0.95),2)}] if random.random()<0.15 else []
        languages = random.sample(['EN','ES','ZH','KO','VI','RU','AR','HI'], k=random.randint(1,2))
        veteran_status = random.random() < 0.08
        homeless_since = (datetime.now() - timedelta(days=random.randint(30, 900))).strftime('%Y-%m-%d') if is_transient else None
        
        # address
        address, city, state, zip_code = self._generate_king_county_address()

        # Compute realistic height and weight using sex-specific height distribution and BMI-derived weight
        if sex == 'M':
            mean_height_inches = 69  # 5'9"
            stddev_height_inches = 3
            min_height_inches = 60   # 5'0"
            max_height_inches = 78   # 6'6"
        else:
            mean_height_inches = 64  # 5'4"
            stddev_height_inches = 3
            min_height_inches = 58   # 4'10"
            max_height_inches = 74   # 6'2"

        sampled_height = int(round(random.gauss(mean_height_inches, stddev_height_inches)))
        height_inches = max(min_height_inches, min(max_height_inches, sampled_height))
        height_feet = height_inches // 12
        height_remainder_inches = height_inches % 12
        height_str = f"{height_feet}'{height_remainder_inches}\""

        # BMI-centered weight (kg = BMI * m^2) then convert to lbs
        bmi = max(18.0, min(40.0, random.gauss(27.0, 5.0)))
        height_meters = height_inches * 0.0254
        weight_kg = bmi * (height_meters ** 2)
        weight_lbs = int(round(weight_kg * 2.20462))
        # Clamp to a reasonable adult range
        weight_lbs = max(90, min(400, weight_lbs))

        # Deterministic person_id derived from SSN (stable across runs)
        stable_hash = uuid.uuid5(uuid.NAMESPACE_DNS, ssn).hex[:10].upper()
        return Person(
            person_id=f"P-{stable_hash}",
            ssn=ssn,
            first_name=first_name,
            last_name=last_name,
            date_of_birth=dob.strftime('%Y-%m-%d'),
            sex=sex,
            race=ethnicity,
            height=height_str,
            weight=weight_lbs,
            hair_color=random.choice(['BLACK', 'BROWN', 'BLONDE', 'RED', 'GRAY']),
            eye_color=random.choice(['BROWN', 'BLUE', 'GREEN', 'HAZEL']),
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            phone=phone,
            email=email,
            emergency_contact=emergency_contact,
            criminal_history=[],
            warrants=[],
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency,
            updated_date=updated_date,
            updated_time=updated_time,
            is_transient=is_transient,
            drivers_license_number=drivers_license_number,
            drivers_license_state=drivers_license_state,
            agency=agency,
            dl_expiration=dl_expiration,
            dl_class=dl_class,
            aliases=aliases,
            languages=languages,
            veteran_status=veteran_status,
            homeless_since=homeless_since,
            incarceration_site=None,
            inmate_key=None,
            parole_status=None,
            gang_membership_description=None,
            inmate_received_date=None,
            earliest_release_date=None,
            release_datetime=None,
            parole_offense=None,
            ccis_offender_key=None,
            parole_timing=None,
            incarceration_status=None,
            probation_status=None,
            probation_officer_name=None,
            probation_officer_phone_number=None,
            is_multistate_offender=None,
        )
        
    def generate_vehicle(self, owner_id=None, agency='KCSO'):
        """Generate a sample vehicle"""
        # Generate unique VIN
        while True:
            vin = ''.join([random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(17)])
            if vin not in self.used_vins:
                self.used_vins.add(vin)
                break
        
        # Generate unique license plate
        while True:
            plate = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(100, 999)}"
            if plate not in self.used_license_plates:
                self.used_license_plates.add(plate)
                break
        
        # Vehicle characteristics
        makes = ['TOYOTA', 'HONDA', 'FORD', 'CHEVROLET', 'NISSAN', 'BMW', 'MERCEDES', 'AUDI', 'VOLKSWAGEN', 'HYUNDAI']
        models = {
            'TOYOTA': ['CAMRY', 'COROLLA', 'RAV4', 'HIGHLANDER', 'TACOMA'],
            'HONDA': ['CIVIC', 'ACCORD', 'CR-V', 'PILOT', 'ODYSSEY'],
            'FORD': ['F-150', 'EXPLORER', 'ESCAPE', 'FOCUS', 'MUSTANG'],
            'CHEVROLET': ['SILVERADO', 'EQUINOX', 'MALIBU', 'CAMARO', 'TAHOE'],
            'NISSAN': ['ALTIMA', 'SENTRA', 'ROGUE', 'PATHFINDER', 'MAXIMA']
        }
        
        make = random.choice(makes)
        model = random.choice(models.get(make, ['UNKNOWN']))
        year = random.randint(1995, 2024)
        
        # Registration and insurance status
        reg_expiry = fake.date_between(start_date='now', end_date='+2y').strftime('%Y-%m-%d')
        insurance_status = random.choice(['ACTIVE', 'EXPIRED', 'SUSPENDED', 'UNKNOWN'])
        stolen_status = 'STOLEN' if random.random() < 0.02 else 'NOT_STOLEN'  # 2% stolen rate
        
        color = random.choice(['BLACK','WHITE','GRAY','SILVER','BLUE','RED','GREEN','BROWN'])
        body_type = random.choice(['SEDAN','SUV','TRUCK','VAN','COUPE','WAGON','MOTORCYCLE'])
        registration_expiration = (datetime.now() + timedelta(days=random.randint(90, 365*2))).strftime('%Y-%m-%d')
        stolen_flag_dt = (datetime.now() - timedelta(days=random.randint(1,120))).strftime('%Y-%m-%d') if stolen_status else None
        lienholder = random.choice(['ALLY','CHASE','WELLS_FARGO','BOA','TOYOTA_FIN','HONDA_FIN']) if random.random()<0.25 else None
        
        vehicle = Vehicle(
            vehicle_id=str(uuid.uuid4()),
            vin=vin,
            license_plate=plate,
            state='WA',
            make=make,
            model=model,
            year=year,
            color=color,
            body_type=body_type,
            owner_person_id=owner_id or '',
            registration_expiration=registration_expiration,
            insurance_status=insurance_status,
            stolen_status=stolen_status,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency,
            stolen_flag_dt=stolen_flag_dt,
            lienholder=lienholder,
        )
        
        return vehicle
    
    def generate_police_incident(self, agency='KCSO'):
        """Generate a sample police incident"""
        incident_types = [
            'TRAFFIC_VIOLATION', 'THEFT', 'SUSPICIOUS_ACTIVITY', 'DOMESTIC_DISTURBANCE',
            'ASSAULT', 'BURGLARY', 'DRUG_POSSESSION', 'OVERDOSE'
        ]
        weights = [28, 23, 12, 10, 8, 7, 7, 5]  # adjust names to match your enums
        incident_type = random.choices(incident_types, weights=weights, k=1)[0]
        
        # Generate timing
        call_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        dispatch_delay = timedelta(seconds=random.randint(30, 180))
        en_route_delay = timedelta(seconds=random.randint(45, 300))
        arrive_delay = timedelta(minutes=random.randint(4, 12))
        clear_delay = timedelta(minutes=random.randint(15, 120))
        
        # Location
        cities = ['SEATTLE', 'KIRKLAND', 'REDMOND', 'SAMMAMISH']
        city = random.choice(cities)
        
        # Generate incident number
        incident_number = f"{agency}{call_datetime.year}{random.randint(100000, 999999)}"
        
        incident = PoliceIncident(
            incident_id=str(uuid.uuid4()),
            incident_type=incident_type,
            incident_date=call_datetime.strftime('%Y-%m-%d'),
            incident_time=call_datetime.strftime('%H:%M:%S'),
            call_datetime=call_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            cad_id=str(uuid.uuid4()),  # Add this missing attribute
            location=self._generate_king_county_address()[0],
            latitude=random.uniform(47.5, 47.8),
            longitude=random.uniform(-122.5, -122.1),
            district=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']),
            beat=f"{random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL'])}{random.randint(1, 9)}",
            reporting_party=f"{fake.first_name()} {fake.last_name()}",
            reporting_party_phone=f"({random.choice(['206', '425', '360', '509'])}){fake.msisdn()[:3]}-{fake.msisdn()[:4]}",
            incident_description=f"{incident_type.lower().replace('_', ' ')} incident reported. {fake.sentence()}",
            primary_officer=f"Officer {fake.last_name()}",
            backup_officers=[f"Officer {fake.last_name()}" for _ in range(random.randint(0, 2))],
            suspect_id=str(uuid.uuid4()),
            victim_id=str(uuid.uuid4()),
            witness_id=str(uuid.uuid4()),
            evidence_collected=random.sample(['PHOTOGRAPHS', 'VIDEO_RECORDING', 'PHYSICAL_EVIDENCE', 'BODY_CAMERA', 'DASH_CAMERA'], random.randint(1, 3)) if random.random() < 0.7 else [],
            case_status=random.choice(['OPEN', 'CLOSED', 'PENDING']),
            caller_person_id=random.choice(self.persons).person_id if self.persons else None,
            created_date=call_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency
        )
        
        return incident

    def generate_fire_incident(self, cad_incident=None):
        """Generate a sample fire incident with realistic distribution"""
        # Focus FireIncident on fire/hazard calls; EMS medicals are handled in EMSIncident
        weights_map = {
            'ALARM_ACTIVATION': 30,
            'FIRE_ALARM': 25,
            'SMOKE_INVESTIGATION': 18,
            'OUTSIDE_FIRE': 8,
            'GAS_LEAK': 6,
            'GAS_ODOR': 5,
            'ELECTRICAL_HAZARD': 5,
            'CO_ALARM': 4,
            'STRUCTURE_FIRE': 3,
            'VEHICLE_FIRE': 3,
            'FUEL_SPILL': 2,
            'FUEL_ODOR': 1,
            'VEGETATION_WILDLAND_FIRE': 1,
            'WILDLAND_FIRE': 1,
            'AIRCRAFT_EMERGENCY': 1,
            'HAZMAT': 2,
        }
        
        # Use CAD incident data if available, otherwise generate new
        if cad_incident:
            alarm_datetime = datetime.strptime(cad_incident.call_date_time, '%Y-%m-%d %H:%M:%S')
            incident_type = cad_incident.incident_type_code
            address = cad_incident.address
            apartment_number = cad_incident.apartment_number
        else:
            incident_types = list(weights_map.keys())
            weights = list(weights_map.values())
            incident_type = random.choices(incident_types, weights=weights, k=1)[0]
            
            alarm_datetime = fake.date_time_between(start_date='-30d', end_date='now')
            address, city, state, zip_code = self._generate_king_county_address()
            apartment_number = f"Apt {random.randint(1, 500)}" if random.random() < 0.3 else None
        dispatch_delay = timedelta(seconds=random.randint(30, 180))
        en_route_delay = timedelta(seconds=random.randint(45, 300))
        arrive_delay = timedelta(minutes=random.randint(4, 12))
        controlled_delay = timedelta(minutes=random.randint(15, 120))
        clear_delay = timedelta(minutes=random.randint(30, 180))
        
        incident = FireIncident(
            incident_id=str(uuid.uuid4()),
            incident_number=f"SFD{alarm_datetime.year}{random.randint(100000, 999999)}",
            call_number=f"F{alarm_datetime.year}{random.randint(1000000, 9999999)}",
            incident_type=incident_type,
            incident_subtype=f"{incident_type}_SUBTYPE",
            nfirs_code=f"{random.randint(100, 999)}",
            alarm_datetime=alarm_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_datetime=(alarm_datetime + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            en_route_datetime=(alarm_datetime + dispatch_delay + en_route_delay).strftime('%Y-%m-%d %H:%M:%S'),
            arrive_datetime=(alarm_datetime + dispatch_delay + en_route_delay + arrive_delay).strftime('%Y-%m-%d %H:%M:%S'),
            controlled_datetime=(alarm_datetime + dispatch_delay + en_route_delay + arrive_delay + controlled_delay).strftime('%Y-%m-%d %H:%M:%S'),
            last_unit_cleared_datetime=(alarm_datetime + dispatch_delay + en_route_delay + arrive_delay + controlled_delay + clear_delay).strftime('%Y-%m-%d %H:%M:%S'),
            response_time_seconds=int((en_route_delay + arrive_delay).total_seconds()),
            suppression_time_seconds=int(controlled_delay.total_seconds()),
            on_scene_time_seconds=int((controlled_delay + clear_delay).total_seconds()),
            total_time_seconds=int((dispatch_delay + en_route_delay + arrive_delay + controlled_delay + clear_delay).total_seconds()),
            address=self._generate_king_county_address()[0],  # Use proper Seattle address
            city='SEATTLE',
            latitude=random.uniform(47.5, 47.8),
            longitude=random.uniform(-122.5, -122.1),
            district=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']),
            first_due_station=random.randint(1, 37),
            units_responding=[f"ENGINE_{random.randint(1, 37)}", f"LADDER_{random.randint(1, 20)}"],
            incident_commander=f"BC{random.randint(1, 7)}, {fake.last_name().upper()}",
            fire_cause=random.choice(['ACCIDENTAL', 'ARSON', 'ELECTRICAL', 'UNKNOWN']),
            fire_origin=random.choice(['KITCHEN', 'BEDROOM', 'LIVING_ROOM', 'GARAGE', 'UNKNOWN']),
            ignition_factor=random.choice(['CIGARETTE', 'ELECTRICAL_FAULT', 'COOKING', 'HEATING', 'UNKNOWN']),
            property_type=random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL', 'VEHICLE', 'OUTDOOR']),
            property_use=random.choice(['SINGLE_FAMILY', 'APARTMENT', 'OFFICE', 'RETAIL', 'WAREHOUSE']),
            occupancy_type=random.choice(['RESIDENTIAL', 'BUSINESS', 'EDUCATIONAL', 'INSTITUTIONAL']),
            construction_type=random.choice(['WOOD_FRAME', 'CONCRETE', 'STEEL', 'MASONRY']),
            stories=random.randint(1, 5) if incident_type == 'STRUCTURE_FIRE' else 0,
            total_floor_area=random.randint(1000, 10000) if incident_type == 'STRUCTURE_FIRE' else 0,
            property_loss=random.randint(5000, 500000) if incident_type in ['STRUCTURE_FIRE', 'VEHICLE_FIRE'] else 0,
            contents_loss=random.randint(1000, 100000) if incident_type in ['STRUCTURE_FIRE', 'VEHICLE_FIRE'] else 0,
            casualties=[],
            fatalities=0,
            injuries=0,
            caller_person_id=random.choice(self.persons).person_id if self.persons else None,
            created_date=alarm_datetime.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        return incident
    
    def generate_ems_incident(self, cad_incident=None):
        # Use CAD incident data if available, otherwise generate new
        if cad_incident:
            incident_datetime = datetime.strptime(cad_incident.call_date_time, '%Y-%m-%d %H:%M:%S')
            incident_type = cad_incident.incident_type_code
            address = cad_incident.address
            apartment_number = cad_incident.apartment_number
        else:
            incident_datetime = fake.date_time_between(start_date='-30d', end_date='now')
            incident_type = self._choose_ems_incident_type(incident_datetime)
            address, city, state, zip_code = self._generate_king_county_address()
            apartment_number = f"Apt {random.randint(1, 500)}" if random.random() < 0.3 else None

        priority_map = {
            'CARDIAC_ARREST': 'HIGH',
            'STROKE': 'HIGH',
            'OVERDOSE': 'HIGH',
            'UNCONSCIOUS_FAINTING': 'HIGH',
            'TRAUMA': 'MEDIUM',
            'CHEST_PAIN': 'MEDIUM',
            'BREATHING_PROBLEMS': 'MEDIUM',
            'HEMORRHAGE': 'MEDIUM',
            'SEIZURE': 'MEDIUM',
            'SICK_PERSON': 'LOW',
            'FALLS': 'LOW',
            'ABDOMINAL_PAIN': 'LOW',
            'DIABETIC_PROBLEM': 'LOW',
            'ALLERGIC_REACTION': 'LOW',
            'PREGNANCY': 'LOW',
            'POISONING': 'LOW',
            'HEAT_EXPOSURE': 'LOW',
            'COLD_EXPOSURE': 'LOW',
        }
        priority = priority_map.get(incident_type, 'LOW')

        address, city, state, zip_code = self._generate_king_county_address()
        # ... keep your existing timing, unit assignment, coords, etc., then set fields:
        # incident_type=incident_type, priority=priority, address=address, city=city, state=state, zip_code=zip_code

        call_dt = incident_datetime
        dispatch_delay = timedelta(seconds=random.randint(30, 180))
        en_route_delay = timedelta(seconds=random.randint(45, 300))
        arrive_delay = timedelta(minutes=random.randint(4, 12))
        transport_delay = timedelta(minutes=random.randint(15, 45))
        hospital_delay = timedelta(minutes=random.randint(10, 30))
        clear_delay = timedelta(minutes=random.randint(30, 90))
        
        # Map complaint to impression, treatments, meds, and bias vitals accordingly
        cc = incident_type
        def clamp(v, lo, hi):
            return max(lo, min(hi, v))
        profile = {
            'CHEST_PAIN': {
                'impression': 'CARDIAC',
                'treat': ['OXYGEN'],
                'meds': ['ASPIRIN','NITROGLYCERIN'],
                'bp': (140, 180, 80, 110), 'hr': (90, 130), 'rr': (16, 24), 'spo2': (92, 98), 'temp': (97.0, 99.0)
            },
            'BREATHING_PROBLEMS': {
                'impression': 'RESPIRATORY',
                'treat': ['OXYGEN'],
                'meds': ['ALBUTEROL'],
                'bp': (110, 150, 70, 95), 'hr': (90, 120), 'rr': (20, 30), 'spo2': (85, 94), 'temp': (97.0, 99.0)
            },
            'OVERDOSE': {
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': ['NARCAN'],
                'bp': (90, 120, 50, 80), 'hr': (50, 100), 'rr': (6, 12), 'spo2': (75, 90), 'temp': (96.0, 99.0)
            },
            'UNCONSCIOUS_FAINTING': {
                'impression': 'MEDICAL',
                'treat': ['OXYGEN','IV_FLUIDS'],
                'meds': [],
                'bp': (80, 110, 50, 70), 'hr': (50, 80), 'rr': (10, 16), 'spo2': (90, 98), 'temp': (96.0, 99.0)
            },
            'TRAUMA': {
                'impression': 'TRAUMA',
                'treat': ['SPLINTING','IV_FLUIDS'],
                'meds': [],
                'bp': (85, 120, 50, 80), 'hr': (90, 130), 'rr': (18, 28), 'spo2': (92, 98), 'temp': (97.0, 99.0)
            },
            'SEIZURE': {
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': [],
                'bp': (110, 160, 70, 100), 'hr': (100, 140), 'rr': (18, 26), 'spo2': (90, 98), 'temp': (97.0, 99.0)
            },
            'ABDOMINAL_PAIN': {
                'impression': 'MEDICAL',
                'treat': ['IV_FLUIDS'],
                'meds': [],
                'bp': (110, 150, 70, 95), 'hr': (80, 110), 'rr': (14, 22), 'spo2': (95, 100), 'temp': (97.0, 100.4)
            },
            'ALLERGIC_REACTION': {
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': ['ALBUTEROL'],
                'bp': (100, 150, 60, 95), 'hr': (100, 130), 'rr': (20, 28), 'spo2': (88, 96), 'temp': (97.0, 99.0)
            },
            'CARDIAC_ARREST': {
                'impression': 'CARDIAC',
                'treat': ['CPR','OXYGEN'],
                'meds': [],
                'bp': (0, 0, 0, 0), 'hr': (0, 0), 'rr': (0, 0), 'spo2': (60, 85), 'temp': (96.0, 99.0)
            },
            'HEMORRHAGE': {
                'impression': 'TRAUMA',
                'treat': ['IV_FLUIDS','SPLINTING'],
                'meds': [],
                'bp': (80, 110, 40, 70), 'hr': (100, 140), 'rr': (18, 28), 'spo2': (90, 98), 'temp': (97.0, 99.0)
            },
            'STROKE': {
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': [],
                'bp': (150, 200, 90, 120), 'hr': (70, 100), 'rr': (14, 22), 'spo2': (95, 100), 'temp': (97.0, 99.0)
            },
        }
        prof = profile.get(incident_type, profile['ABDOMINAL_PAIN'])
        # generate vitals within ranges, with small random noise chance
        sys_lo, sys_hi, dia_lo, dia_hi = prof['bp']
        hr_lo, hr_hi = prof['hr']
        rr_lo, rr_hi = prof['rr']
        spo2_lo, spo2_hi = prof['spo2']
        t_lo, t_hi = prof['temp']
        bp_sys = random.randint(sys_lo, sys_hi) if sys_hi>0 else 0
        bp_dia = random.randint(dia_lo, dia_hi) if dia_hi>0 else 0
        hr = random.randint(hr_lo, hr_hi)
        rr = random.randint(rr_lo, rr_hi)
        spo2 = random.randint(spo2_lo, spo2_hi)
        temp = round(random.uniform(t_lo, t_hi), 1)
        if random.random() < 0.08:
            # atypical variation
            hr = clamp(int(random.gauss(hr, 15)), 30, 180)
            rr = clamp(int(random.gauss(rr, 6)), 6, 40)
            spo2 = clamp(int(random.gauss(spo2, 5)), 50, 100)
        treat_list = list(dict.fromkeys(prof['treat']))  # de-dup
        meds_list = list(dict.fromkeys(prof['meds']))

        incident = EMSIncident(
            incident_id=str(uuid.uuid4()),
            incident_number=f"EMS{incident_datetime.year}{random.randint(100000, 999999)}",
            call_number=f"E{incident_datetime.year}{random.randint(1000000, 9999999)}",
            incident_type=incident_type,
            incident_subtype=f"{incident_type}_SUBTYPE",
            priority=priority,
            call_datetime=call_dt.strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_datetime=(call_dt + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            en_route_datetime=(call_dt + dispatch_delay + en_route_delay).strftime('%Y-%m-%d %H:%M:%S'),
            arrive_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay).strftime('%Y-%m-%d %H:%M:%S'),
            transport_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay).strftime('%Y-%m-%d %H:%M:%S'),
            hospital_arrival_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay).strftime('%Y-%m-%d %H:%M:%S'),
            clear_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + clear_delay).strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_to_enroute_seconds=int(en_route_delay.total_seconds()),
            enroute_to_arrival_seconds=int(arrive_delay.total_seconds()),
            arrival_to_transport_seconds=int(transport_delay.total_seconds()),
            transport_to_hospital_seconds=int(hospital_delay.total_seconds()),
            total_scene_time_seconds=int((arrive_delay + clear_delay).total_seconds()),
            total_incident_time_seconds=int((dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + clear_delay).total_seconds()),
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            district=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']),
            responding_unit=self._assign_ems_unit_by_location(city, zip_code),
            crew_members=[f"PARAMEDIC_{fake.last_name().upper()}", f"EMT_{fake.last_name().upper()}"],
            patient_person_id=random.choice(self.persons).person_id if self.persons else None,
            patient_age=random.randint(5, 85),
            patient_sex=random.choice(['M', 'F']),
            chief_complaint=cc,
            primary_impression=prof['impression'],
            vital_signs={
                'blood_pressure': f"{bp_sys}/{bp_dia}",
                'heart_rate': hr,
                'respiratory_rate': rr,
                'oxygen_saturation': spo2,
                'temperature': temp
            },
            treatment_provided=treat_list,
            medications_given=meds_list,
            transport_destination=random.choice(['HARBORVIEW', 'UW_MEDICAL', 'VIRGINIA_MASON', 'SWEDISH']),
            transport_mode=random.choice(['GROUND_AMBULANCE', 'AIR_AMBULANCE', 'PRIVATE_VEHICLE']),
            created_date=call_dt.strftime('%Y-%m-%d %H:%M:%S'),
            # Enhanced EMS fields based on schema
            complaint_reported_by_dispatch=random.choice(['Caller', 'Bystander', 'Police', 'Fire Department', 'Hospital', 'Family Member']),
            patient_full_name=f"{fake.first_name()} {fake.last_name()}",
            unit_call_sign=f"EMS{random.randint(1, 50)}",
            patient_contact=random.random() < 0.95,  # 95% of calls have patient contact
            patient_disposition=random.choice(['TRANSPORTED', 'REFUSED_CARE', 'TREATED_AT_SCENE', 'DEAD_ON_ARRIVAL', 'NO_PATIENT_FOUND']),
            crew_disposition=random.choice(['CLEARED', 'AVAILABLE', 'OUT_OF_SERVICE', 'EN_ROUTE_TO_STATION']),
            patient_evaluation_care_disposition=random.choice(['EVALUATED_AND_TRANSPORTED', 'EVALUATED_NO_TRANSPORT', 'EVALUATED_REFUSED_CARE', 'NO_EVALUATION_POSSIBLE']),
            transport_disposition=random.choice(['TRANSPORTED_TO_HOSPITAL', 'TRANSPORTED_TO_CLINIC', 'TRANSPORTED_TO_HOME', 'NOT_TRANSPORTED', 'TRANSPORTED_DECLINED']),
            transfer_destination=random.choice(['HARBORVIEW_MEDICAL_CENTER', 'UW_MEDICAL_CENTER', 'VIRGINIA_MASON_MEDICAL_CENTER', 'SWEDISH_MEDICAL_CENTER', 'CHILDRENS_HOSPITAL']),
            destination_type=random.choice(['HOSPITAL', 'CLINIC', 'HOME', 'NURSING_HOME', 'MENTAL_HEALTH_FACILITY']),
            transportation_method=random.choice(['GROUND_AMBULANCE', 'AIR_AMBULANCE', 'PRIVATE_VEHICLE', 'WHEELCHAIR_VAN', 'NOT_TRANSPORTED']),
            unit_level_of_care=random.choice(['BLS', 'ALS', 'CRITICAL_CARE', 'SPECIAL_EVENTS']),
            cad_emd_code=random.choice(['01-A', '02-A', '03-A', '04-A', '05-A', '06-A', '07-A', '08-A', '09-A', '10-A']),
            prearrival_activation=random.choice(['TRAUMA_ALERT', 'STROKE_ALERT', 'CARDIAC_ALERT', 'NONE', 'BURN_ALERT']),
            # Medical Details fields
            complaint_reported_by_dispatch_code=random.choice(['CALLER', 'BYSTANDER', 'POLICE', 'FIRE_DEPT', 'HOSPITAL', 'FAMILY']),
            patient_acuity=random.choice(['CRITICAL', 'EMERGENT', 'URGENT', 'LESS_URGENT', 'NON_URGENT']),
            attempted_procedures=random.sample(['IV_ACCESS', 'INTUBATION', 'CPR', 'DEFIBRILLATION', 'MEDICATION_ADMIN', 'SPLINTING', 'BANDAGING'], k=random.randint(0, 3)),
            successful_procedures=random.sample(['IV_ACCESS', 'INTUBATION', 'CPR', 'DEFIBRILLATION', 'MEDICATION_ADMIN', 'SPLINTING', 'BANDAGING'], k=random.randint(0, 2)),
            cardiac_arrest_datetime=(call_dt + timedelta(minutes=random.randint(5, 15))).strftime('%Y-%m-%d %H:%M:%S') if incident_type == 'CARDIAC_ARREST' else None,
            cardiac_arrest_resuscitation_discontinuation_datetime=(call_dt + timedelta(minutes=random.randint(20, 45))).strftime('%Y-%m-%d %H:%M:%S') if incident_type == 'CARDIAC_ARREST' else None,
            ecg_findings=random.choice(['NORMAL_SINUS_RHYTHM', 'ATRIAL_FIBRILLATION', 'VENTRICULAR_TACHYCARDIA', 'ASYSTOLE', 'PULSELESS_ELECTRICAL_ACTIVITY', 'ST_ELEVATION']),
            incident_emd_performed=random.choice(['YES', 'NO', 'PARTIAL']),
            incident_emd_performed_code=random.choice(['01-A', '02-A', '03-A', '04-A', '05-A', '06-A', '07-A', '08-A', '09-A', '10-A']),
            cad_level_of_care_provided=random.choice(['BLS', 'ALS', 'CRITICAL_CARE']),
            incident_level_of_care_provided=random.choice(['BLS', 'ALS', 'CRITICAL_CARE']),
            provider_primary_impression=prof['impression'],
            situation_patient_acuity=random.choice(['CRITICAL', 'EMERGENT', 'URGENT', 'LESS_URGENT', 'NON_URGENT']),
            # Crew Details fields
            crew_member_name=f"{fake.first_name()} {fake.last_name()}",
            crew_member_level=random.choice(['EMT_BASIC', 'EMT_ADVANCED', 'PARAMEDIC', 'CRITICAL_CARE_PARAMEDIC']),
            crew_badge_number=f"EMS{random.randint(1000, 9999)}",
            # Patient Details fields
            patient_id=str(uuid.uuid4()),
            patient_date_of_birth=(datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            patient_weight=round(random.uniform(100, 300), 1),
            patient_home_address=f"{fake.street_address()}, {city}, {state} {zip_code}",
            is_superuser=False,
            # Unit Details fields
            agency_number=f"KC{random.randint(100, 999)}",
            agency_name="King County EMS",
            agency_affiliation="King County Fire District",
            primary_unit_role=random.choice(['PRIMARY_RESPONSE', 'BACKUP_RESPONSE', 'SPECIAL_EVENTS', 'MUTUAL_AID']),
            # Incident Dates/Times fields
            total_commit_time=random.randint(1800, 7200),  # 30 minutes to 2 hours
            unit_notified_by_dispatch_datetime=(call_dt + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_arrived_at_patient_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S'),
            transfer_of_ems_patient_care_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=random.randint(10, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            arrival_at_destination_landing_area_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_left_scene_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S'),
            patient_arrived_at_destination_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S'),
            unit_back_in_service_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + clear_delay).strftime('%Y-%m-%d %H:%M:%S'),
            last_modified=(call_dt + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'),
            # Ungrouped Properties fields
            incident_status=random.choice(['ACTIVE', 'COMPLETED', 'CANCELLED', 'PENDING_REVIEW']),
            created_by=f"USER_{random.randint(1000, 9999)}",
            patient_pk=str(uuid.uuid4()),
            primary_patient_caregiver_on_scene=f"PARAMEDIC_{fake.last_name().upper()}",
            crew_with_als_pt_contact_response_role=random.choice(['PRIMARY_ALS', 'SECONDARY_ALS', 'ALS_SUPERVISOR'])
        )
        
        return incident

    def generate_fire_rms_incident(self):
        """Generate a detailed Fire RMS incident with rich attributes"""
        # Codes and lookups
        incident_type_codes = ['111', '131', '251', '321', '324', '611', '651']  # NFIRS-like
        # Keep medicals common for RMS/dispatch; alarms/hazards frequent; aircraft low
        initial_dispatch_codes = [
            'SICK_PERSON','BREATHING_DIFFICULTY','CHEST_PAIN','FALLS','ABDOMINAL_PAIN','DIABETIC_PROBLEM','SEIZURE',
            'OVERDOSE_POISONING','UNCONSCIOUS_FAINTING','STROKE','PSYCHIATRIC','MENTAL_HEALTH','PREGNANCY_CHILDBIRTH',
            'ALARM_ACTIVATION','FIRE_ALARM','SMOKE_INVESTIGATION','OUTSIDE_FIRE','GAS_LEAK','GAS_ODOR','ELECTRICAL_HAZARD','CO_ALARM',
            'STRUCTURE_FIRE','VEHICLE_FIRE','VEGETATION_WILDLAND_FIRE','WILDLAND_FIRE','FUEL_SPILL','FUEL_ODOR','EXTRICATION','ENTRAPMENT',
            'TRAFFIC_COLLISION','SERVICE_CALL','UNKNOWN_PROBLEM','MUTUAL_AID_RESPONSE','AIRCRAFT_EMERGENCY','HAZMAT'
        ]
        # Heavier weights for medical/alarms; very low for aircraft
        dispatch_weights = [
            24,22,20,18,16,14,12,
            12,11,10,9,9,8,
            14,14,12,8,6,6,6,5,
            4,4,2,2,2,2,3,3,
            10,6,6,4,1,3
        ]
        shifts = ['A', 'B', 'C']
        districts = ['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']
        district_names = {
            'NORTH': 'North Battalion',
            'SOUTH': 'South Battalion',
            'EAST': 'East Battalion',
            'WEST': 'West Battalion',
            'CENTRAL': 'Central Battalion'
        }
        stations = [f"STATION_{i}" for i in range(1, 38)]
        action_code_map = {
            'EXT': 'Extinguishment',
            'OVH': 'Overhaul',
            'RES': 'Rescue/Extrication',
            'MED': 'Medical Care',
            'INV': 'Investigation',
            'VEN': 'Ventilation',
        }
        location_type_codes = ['111', '121', '122', '123', '124', '130', '140']
        property_use_codes = ['419', '429', '449', '500', '511', '419']
        location_details_opts = ['1 OR 2 FAMILY DWELLING', 'APARTMENT', 'COMMERCIAL', 'OUTSIDE', 'GARAGE']
        aid_codes = ['AUTO_AID', 'MUTUAL_AID_GIVEN', 'MUTUAL_AID_RECEIVED', 'NONE']

        # Timing
        call_dt = fake.date_time_between(start_date='-30d', end_date='now')
        dispatch_notified_dt = call_dt + timedelta(seconds=random.randint(10, 180))
        alarm_dt = dispatch_notified_dt + timedelta(seconds=random.randint(15, 120))
        arrival_dt = alarm_dt + timedelta(minutes=random.randint(4, 12))
        last_unit_cleared_dt = arrival_dt + timedelta(minutes=random.randint(20, 180))
        response_time = (arrival_dt - alarm_dt).total_seconds()
        modified_dt = last_unit_cleared_dt + timedelta(minutes=random.randint(1, 60))

        incident_type_code = random.choice(incident_type_codes)
        initial_dispatch_code = random.choices(initial_dispatch_codes, weights=dispatch_weights, k=1)[0]
        shift = random.choice(shifts)
        district = random.choice(districts)
        district_name = district_names[district]
        station = random.choice(stations)
        action_codes = random.sample(list(action_code_map.keys()), k=random.randint(1, 3))
        action_descriptions = [action_code_map[c] for c in action_codes]
        final_incident_type = random.choice(['FIRE', 'MEDICAL', 'HAZMAT', 'RESCUE'])

        # Resources
        suppression_apparatus_count = random.randint(0, 5)
        suppression_personnel_count = suppression_apparatus_count * random.randint(3, 5)
        ems_apparatus_count = random.randint(0, 3)
        ems_personnel_count = ems_apparatus_count * random.randint(2, 4)

        personnel_deployed = [str(random.randint(4000, 4999)) for _ in range(random.randint(1, 6))]
        apparatus_prefixes = ['E', 'L', 'M', 'B', 'A']  # Engine, Ladder, Medic, Battalion, Aid
        apparatus_deployed = [f"{random.choice(apparatus_prefixes)}{random.randint(1, 37):03d}" for _ in range(random.randint(1, 6))]

        # Aid
        aid_code = random.choice(aid_codes)
        aid_details = 'Mutual aid with neighboring jurisdiction' if 'MUTUAL' in aid_code else 'None'
        resources_include_mutual_aid = 'MUTUAL' in aid_code

        # Flags
        is_locked = random.random() < 0.1
        is_active = random.random() < 0.1
        resource_form_used = random.random() < 0.5

        # Location
        address, city, state, zip_code = self._generate_king_county_address()
        location = f"{address}, {city}, {state} {zip_code}"
        wildland_address = random.random() < 0.05
        location_type_code = random.choice(location_type_codes)
        property_use_code = random.choice(property_use_codes)
        location_details = random.choice(location_details_opts)

        incident = FireRMSIncident(
            incident_id=str(uuid.uuid4()),
            incident_number=f"SFD{call_dt.year}{random.randint(100000, 999999)}",
            incident_type_code=incident_type_code,
            incident_datetime=call_dt.strftime('%Y-%m-%d %H:%M:%S'),
            initial_dispatch_code=initial_dispatch_code,
            shift=shift,
            district=district,
            district_name=district_name,
            modified_datetime=modified_dt.strftime('%Y-%m-%d %H:%M:%S'),
            station=station,
            action_descriptions=action_descriptions,
            action_codes=action_codes,
            final_incident_type=final_incident_type,
            incident_times={
                'call_datetime': call_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'dispatch_notified_datetime': dispatch_notified_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'alarm_datetime': alarm_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'arrival_datetime': arrival_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'last_unit_cleared_datetime': last_unit_cleared_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'response_time_seconds': int(response_time),
            },
            location=location,
            wildland_address=wildland_address,
            location_type_code=location_type_code,
            property_use_code=property_use_code,
            location_details=location_details,
            suppression_apparatus_count=suppression_apparatus_count,
            suppression_personnel_count=suppression_personnel_count,
            ems_apparatus_count=ems_apparatus_count,
            ems_personnel_count=ems_personnel_count,
            personnel_deployed=personnel_deployed,
            apparatus_deployed=apparatus_deployed,
            aid_code=aid_code,
            aid_details=aid_details,
            resources_include_mutual_aid=resources_include_mutual_aid,
            is_locked=is_locked,
            is_active=is_active,
            resource_form_used=resource_form_used,
        )

        return incident

    def generate_cad_incident(self, related_persons=None):
        """Generate a CAD incident with links to persons"""
        # CAD call types and priorities
        call_types = {
            'DISTURBANCE': ['HIGH', 'MEDIUM'],
            'SUSPICIOUS_ACTIVITY': ['MEDIUM', 'LOW'],
            'TRAFFIC_VIOLATION': ['MEDIUM', 'LOW'],
            'DOMESTIC_DISTURBANCE': ['HIGH', 'MEDIUM'],
            'BURGLARY_ALARM': ['HIGH', 'MEDIUM'],
            'VEHICLE_THEFT': ['MEDIUM', 'LOW'],
            'ASSAULT': ['HIGH', 'MEDIUM'],
            'DRUG_ACTIVITY': ['MEDIUM', 'LOW'],
            'NOISE_COMPLAINT': ['LOW'],
            'WELFARE_CHECK': ['MEDIUM', 'LOW'],
            'MISSING_PERSON': ['HIGH', 'MEDIUM'],
            'FOUND_PROPERTY': ['LOW'],
            'LOST_PROPERTY': ['LOW'],
            'FRAUD': ['MEDIUM', 'LOW'],
            'HARASSMENT': ['MEDIUM', 'LOW']
        }
        
        call_type = random.choice(list(call_types.keys()))
        priority = random.choice(call_types[call_type])
        
        # Generate realistic timing
        call_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        dispatch_delay = timedelta(seconds=random.randint(30, 180))
        en_route_delay = timedelta(seconds=random.randint(60, 300))
        on_scene_delay = timedelta(seconds=random.randint(120, 600))
        clear_delay = timedelta(seconds=random.randint(300, 1800))
        
        dispatch_datetime = call_datetime + dispatch_delay
        en_route_datetime = dispatch_datetime + en_route_delay
        on_scene_datetime = en_route_datetime + on_scene_delay
        clear_datetime = on_scene_datetime + clear_delay
        
        # Generate realistic Seattle area location
        districts = ['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL', 'SOUTHEAST', 'SOUTHWEST', 'NORTHEAST', 'NORTHWEST']
        district = random.choice(districts)
        beat = f"{district[0]}{random.randint(1, 9)}"
        
        # Generate coordinates within King County
        if district in ['NORTH', 'NORTHEAST', 'NORTHWEST']:
            lat_range = (47.6, 47.8)
            lon_range = (-122.4, -122.1)
        elif district in ['SOUTH', 'SOUTHEAST', 'SOUTHWEST']:
            lat_range = (47.4, 47.6)
            lon_range = (-122.4, -122.1)
        elif district in ['EAST', 'CENTRAL']:
            lat_range = (47.5, 47.7)
            lon_range = (-122.2, -121.9)
        else:  # WEST
            lat_range = (47.5, 47.7)
            lon_range = (-122.5, -122.2)
        
        latitude = random.uniform(lat_range[0], lat_range[1])
        longitude = random.uniform(lon_range[0], lon_range[1])
        
        # Generate location description
        street_number = random.randint(100, 9999)
        street_names = ['Main St', 'Oak Ave', 'Pine Rd', 'Cedar Ln', 'Maple Dr', 'Elm St', 'Washington Ave', 'Lake Dr']
        street_name = random.choice(street_names)
        location = f"{street_number} {street_name}"
        
        # Generate units and officers
        units = [f"{random.choice(['A', 'B', 'C', 'D'])}{random.randint(1, 9)}" for _ in range(random.randint(1, 3))]
        # Use consistent officer from pool
        agency = 'KCSO'  # Default agency for police incidents
        primary_officer_obj = self.get_or_create_officer(agency)
        primary_officer = f"{primary_officer_obj.first_name} {primary_officer_obj.last_name} ({primary_officer_obj.badge_number})"
        backup_officers = [f"{self.get_or_create_officer(agency).first_name} {self.get_or_create_officer(agency).last_name} ({self.get_or_create_officer(agency).badge_number})" for _ in range(random.randint(0, 2))]
        
        # Generate reporting party info
        reporting_party = f"{fake.first_name()} {fake.last_name()}"
        seattle_area_codes = ['206', '425', '360', '509']
        area_code = random.choice(seattle_area_codes)
        reporting_party_phone = f"({area_code}){fake.msisdn()[:3]}-{fake.msisdn()[:4]}"
        
        # Generate incident description
        incident_descriptions = {
            'DISTURBANCE': [
                'Loud party in progress, multiple people yelling',
                'Neighbor dispute over noise levels',
                'Verbal altercation between two individuals'
            ],
            'SUSPICIOUS_ACTIVITY': [
                'Person loitering in parking lot',
                'Suspicious vehicle parked for extended period',
                'Unknown person checking car doors'
            ],
            'TRAFFIC_VIOLATION': [
                'Vehicle running red light',
                'Speeding vehicle in residential area',
                'Illegal parking blocking driveway'
            ],
            'DOMESTIC_DISTURBANCE': [
                'Verbal argument between family members',
                'Physical altercation reported',
                'Domestic violence call'
            ],
            'BURGLARY_ALARM': [
                'Residential alarm activation',
                'Commercial building alarm',
                'Vehicle alarm sounding'
            ]
        }
        
        description = random.choice(incident_descriptions.get(call_type, [f'{call_type} incident reported']))
        
        # Link to related persons if provided
        related_persons_data = []
        if related_persons:
            num_related = random.randint(1, min(3, len(related_persons)))
            selected_persons = random.sample(related_persons, num_related)
            for person in selected_persons:
                role = random.choice(['REPORTING_PARTY', 'SUSPECT', 'VICTIM', 'WITNESS', 'INVOLVED_PARTY'])
                related_persons_data.append({
                    'person_id': person.person_id,
                    'role': role,
                    'name': f"{person.first_name} {person.last_name}",
                    'involvement_level': random.choice(['PRIMARY', 'SECONDARY', 'WITNESS'])
                })
        
        weather = random.choice(['CLEAR','RAIN','OVERCAST','FOG','SNOW'])
        premise_type = random.choice(['RESIDENTIAL','COMMERCIAL','PUBLIC','VEHICLE'])
        cross_streets = f"{random.choice(['1st','2nd','3rd','Pine','Cedar','Maple'])} & {random.choice(['Union','Madison','Yesler','Denny'])}"
        landmark = random.choice(['Gas Station','Park','Mall','School','Transit Stop'])
        location_quality = random.choice(['GPS','CALLER','UNIT_REPORTED'])
        response_delays = [
          {'stage':'DISPATCH_DELAY_SEC','seconds': int(dispatch_delay.total_seconds())},
          {'stage':'EN_ROUTE_DELAY_SEC','seconds': int(en_route_delay.total_seconds())},
          {'stage':'ON_SCENE_DELAY_SEC','seconds': int(on_scene_delay.total_seconds())},
          {'stage':'CLEAR_DELAY_SEC','seconds': int(clear_delay.total_seconds())}
        ]
        
        return CADIncident(
            cad_id=f"CAD-{datetime.now().year}-{random.randint(100000, 999999)}",
            incident_number=f"24-{random.randint(10000, 99999)}",
            call_type=call_type,
            priority=priority,
            status='CLOSED',
            call_datetime=call_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_datetime=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            en_route_datetime=en_route_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            on_scene_datetime=on_scene_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            clear_datetime=clear_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            location=location,
            latitude=latitude,
            longitude=longitude,
            district=district,
            beat=beat,
            reporting_party=reporting_party,
            reporting_party_phone=reporting_party_phone,
            incident_description=description,
            units_assigned=units,
            primary_officer=primary_officer,
            backup_officers=backup_officers,
            related_persons=related_persons_data,
            related_incidents=[],
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency='KCSO',
            weather=weather,
            premise_type=premise_type,
            cross_streets=cross_streets,
            landmark=landmark,
            location_quality=location_quality,
            response_delays=response_delays
        )

    def generate_emergency_contact(self, person_ethnicity):
        """Generate realistic emergency contact with consistent ethnicity for family relationships"""
        # 70% chance of family relationship (should share ethnicity), 30% chance of other relationship
        is_family = random.random() < 0.7
        
        if is_family:
            # Family relationships - use same ethnicity
            relationship = random.choice(['SPOUSE', 'PARENT', 'CHILD', 'SIBLING'])
            contact_ethnicity = person_ethnicity
        else:
            # Non-family relationships - ethnicity can differ
            relationship = random.choice(['FRIEND', 'COWORKER', 'NEIGHBOR', 'DOMESTIC_PARTNER'])
            contact_ethnicity = random.choice(['WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'NATIVE_AMERICAN', 'OTHER'])
        
        # Get appropriate locale for the contact's ethnicity
        locale_fake = faker_locales[contact_ethnicity]
        if contact_ethnicity == 'ASIAN':
            locale_fake = random.choice(locale_fake)
        
        # Generate gender-consistent name for the contact
        contact_sex = random.choice(['M', 'F'])
        if contact_sex == 'M':
            contact_first_name = locale_fake.first_name_male()
        else:
            contact_first_name = locale_fake.first_name_female()
        
        contact_last_name = locale_fake.last_name()
        
        # Generate Seattle-area phone number
        seattle_area_codes = ['206', '425', '360', '509']
        area_code = random.choice(seattle_area_codes)
        contact_phone = f"({area_code}){fake.msisdn()[:3]}-{fake.msisdn()[:4]}"
        
        # Generate proper street address
        street_number = random.randint(100, 9999)
        street_names = ['Main St', 'Oak Ave', 'Pine Rd', 'Cedar Ln', 'Maple Dr', 'Elm St', 'Washington Ave', 'Lake Dr']
        street_name = random.choice(street_names)
        contact_address = f"{street_number} {street_name}"

        return {
            'name': f"{contact_first_name} {contact_last_name}",
            'relationship': relationship,
            'phone': contact_phone,
            'address': contact_address,
            'city': random.choice(['Seattle', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island']),
            'state': 'WA',
            'zip_code': fake.zipcode_in_state('WA')
        }

    def create_cross_agency_links(self):
        """Create cross-agency relationships"""
        pass  # Placeholder for now

    def save_data(self):
        """Save data to JSON files"""
        print("Saving data to JSON files...")
        
        # Convert dataclass objects to dictionaries
        data_to_save = {
            'persons': [asdict(p) for p in self.persons],
            'vehicles': [asdict(v) for v in self.vehicles],
            'properties': [asdict(p) for p in self.properties],
            'police_incidents': [asdict(i) for i in self.police_incidents],
            'arrests': [asdict(a) for a in self.arrests],
            'cases': [asdict(c) for c in self.cases],
            'cad_incidents': [asdict(c) for c in self.cad_incidents],
            'citations': [asdict(c) for c in getattr(self, 'citations', [])],
            'field_interviews': [asdict(fi) for fi in getattr(self, 'field_interviews', [])],
            'jail_bookings': [asdict(b) for b in self.jail_bookings],
            'jail_sentences': [asdict(s) for s in getattr(self, 'jail_sentences', [])],
            'jail_incidents': [asdict(j) for j in getattr(self, 'jail_incidents', [])],
            'bail_bonds': [asdict(bb) for bb in getattr(self, 'bail_bonds', [])],
            'jail_programs': [asdict(p) for p in getattr(self, 'jail_programs', [])],
            'jail_logs': [asdict(l) for l in getattr(self, 'jail_logs', [])],
            'corrections_facilities': [asdict(f) for f in getattr(self, 'corrections_facilities', [])],
            'fire_incidents': [asdict(f) for f in self.fire_incidents],
            'ems_incidents': [asdict(e) for e in self.ems_incidents],
            'fire_rms_incidents': [asdict(f) for f in getattr(self, 'fire_rms_incidents', [])],
            'fire_shifts': [asdict(s) for s in getattr(self, 'fire_shifts', [])],
            'fire_personnel': [asdict(p) for p in getattr(self, 'fire_personnel', [])],
            'fire_reports': [asdict(r) for r in getattr(self, 'fire_reports', [])]
        }
        
        # Save each entity type to separate JSON files
        for entity_type, data in data_to_save.items():
            filename = f"{entity_type}.json"
            with open(filename, 'w', encoding='utf-8') as f:  # Add UTF-8 encoding
                json_module.dump(data, f, indent=2, ensure_ascii=False)  # Set ensure_ascii=False
            print(f"Saved {len(data)} {entity_type} to {filename}")
        
        # Save combined data
        with open('all_sample_data.json', 'w', encoding='utf-8') as f:  # Add UTF-8 encoding
            json_module.dump(data_to_save, f, indent=2, ensure_ascii=False)  # Set ensure_ascii=False
        print("Saved combined data to all_sample_data.json")

        # CSV export for each dataclass (flat columns)
        print("Saving CSV exports...")
        def write_csv(filename, records):
            if not records:
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    pass
                return
            # Flatten nested dicts/lists to JSON strings to keep columns consistent
            dicts = []
            for r in records:
                row = {}
                for k, v in r.items():
                    if isinstance(v, (dict, list)):
                        row[k] = json_module.dumps(v, ensure_ascii=False)
                    else:
                        row[k] = v
                dicts.append(row)
            fieldnames = sorted({k for d in dicts for k in d.keys()})
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(dicts)

        # Write CSVs
        write_csv('persons.csv', data_to_save['persons'])
        write_csv('vehicles.csv', data_to_save['vehicles'])
        write_csv('properties.csv', data_to_save['properties'])
        write_csv('police_incidents.csv', data_to_save['police_incidents'])
        write_csv('arrests.csv', data_to_save['arrests'])
        write_csv('cases.csv', data_to_save['cases'])
        write_csv('cad_incidents.csv', data_to_save['cad_incidents'])
        write_csv('citations.csv', data_to_save['citations'])
        write_csv('field_interviews.csv', data_to_save['field_interviews'])
        write_csv('jail_bookings.csv', data_to_save['jail_bookings'])
        write_csv('jail_sentences.csv', data_to_save['jail_sentences'])
        write_csv('jail_incidents.csv', data_to_save['jail_incidents'])
        write_csv('bail_bonds.csv', data_to_save['bail_bonds'])
        write_csv('jail_programs.csv', data_to_save['jail_programs'])
        write_csv('jail_logs.csv', data_to_save.get('jail_logs', []))
        write_csv('fire_incidents.csv', data_to_save['fire_incidents'])
        write_csv('ems_incidents.csv', data_to_save['ems_incidents'])
        write_csv('fire_rms_incidents.csv', data_to_save['fire_rms_incidents'])
        write_csv('fire_shifts.csv', data_to_save['fire_shifts'])
        write_csv('fire_personnel.csv', data_to_save['fire_personnel'])
        write_csv('fire_reports.csv', data_to_save['fire_reports'])

        print("CSV export completed!")

    def generate_all_data(self):
        """Generate all comprehensive synthetic data"""
        print("Enhanced Multi-Agency Complex Data Generator Starting...")
        print("Generating comprehensive data for King County Sheriff's Office (KCSO) jurisdiction")
        start_time = time.time()
        
        # Generate persons
        print(f"Generating {CONFIG['num_persons']:,} persons...")
        for i in range(CONFIG['num_persons']):
            agency = 'KCSO'
            person = self.generate_person(agency)
            self.persons.append(person)
            self.person_index[person.person_id] = person
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} persons...")
        
        # Generate vehicles
        print(f"Generating {CONFIG['num_vehicles']:,} vehicles...")
        for i in range(CONFIG['num_vehicles']):
            owner_id = random.choice(self.persons).person_id if random.random() < 0.7 else None
            vehicle = self.generate_vehicle(owner_id)
            self.vehicles.append(vehicle)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} vehicles...")
        
        # Generate CAD incidents FIRST (these are the initial calls for service)
        print(f"Generating CAD incidents...")
        cad_incidents_count = CONFIG['num_ems_incidents'] + int(CONFIG['num_fire_incidents'] * 0.8) + CONFIG['num_arrests'] * 2  # Include police CAD incidents
        for i in range(cad_incidents_count):
            agency = 'KCSO'
            # Link CAD incident to a person (90% chance)
            person = random.choice(self.persons) if random.random() < 0.9 else None
            cad_incident = self.generate_cad_incident(agency, person=person)
            self.cad_incidents.append(cad_incident)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} CAD incidents...")
        
        # Generate police incidents (linked to CAD incidents)
        print(f"Generating {CONFIG['num_police_incidents']:,} police incidents...")
        for i in range(CONFIG['num_police_incidents']):
            agency = 'KCSO'
            # Link to a CAD incident (80% chance)
            cad_incident = random.choice(self.cad_incidents) if random.random() < 0.8 else None
            incident = self.generate_police_incident(agency)
            self.police_incidents.append(incident)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} police incidents...")
        
        # Generate arrests (linked to CAD incidents and police incidents)
        print(f"Generating {CONFIG['num_arrests']:,} arrests...")
        for i in range(CONFIG['num_arrests']):
            person = random.choice(self.persons)
            # Link to a CAD incident (70% chance)
            cad_incident = random.choice(self.cad_incidents) if random.random() < 0.7 else None
            arrest = self.generate_arrest(cad_incident, person)
            self.arrests.append(arrest)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} arrests...")
        
        # Generate cases immediately after arrests (linked to arrests when charges warrant prosecution)
        num_cases = CONFIG['num_arrests'] // 2  # Generate half as many cases as arrests
        print(f"Generating {num_cases:,} cases...")
        for i in range(num_cases):
            agency = 'KCSO'
            # Link case to an arrest (80% chance) and CAD incident (60% chance)
            arrest = random.choice(self.arrests) if random.random() < 0.8 else None
            cad_incident = random.choice(self.cad_incidents) if random.random() < 0.6 and self.cad_incidents else None
            case = self.generate_case(agency, cad_incident, arrest)
            self.cases.append(case)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} cases...")
        
        # Generate jail bookings (linked to arrests)
        print(f"Generating {CONFIG['num_jail_bookings']:,} jail bookings...")
        for i in range(CONFIG['num_jail_bookings']):
            arrest = random.choice(self.arrests)
            booking = self.generate_jail_booking(arrest.person_id, arrest.arrest_id, arrest.agency, arrest.charges, arrest.arrest_datetime)
            self.jail_bookings.append(booking)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} jail bookings...")

        # Generate Corrections Facilities
        print("Loading curated corrections facilities...")
        self.corrections_facilities = [
            CorrectionsFacility(
                facility_id=fac['facility_id'],
                name=fac['name'],
                address=fac['address'],
                city=fac['city'],
                state=fac['state'],
                zip_code=fac['zip_code'],
                capacity=fac['capacity'],
                security_level=fac['security_level'],
                contact_phone=self._format_seattle_phone(),
                contact_email=None,
                warden_name=None,
                created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            )
            for fac in CORRECTIONS_FACILITY_CATALOG
        ]

        # Generate Jail Programs
        print(f"Generating {CONFIG['num_jail_programs']:,} jail programs...")
        self.jail_programs = [self.generate_jail_program() for _ in range(CONFIG['num_jail_programs'])]

        # For each booking, optionally generate sentence, bail bond, and incidents
        print("Linking jail sentences, bail bonds, and incidents to bookings/persons...")
        self.jail_sentences = []
        self.bail_bonds = []
        self.jail_incidents = []
        for booking in self.jail_bookings:
            # Sentence based on charges (55% of bookings)
            if random.random() < CONFIG['pct_bookings_with_sentence']:
                # Try to align offense description with a case/offense when possible
                case_numbers = [c.case_number for c in self.cases if booking.person_id == c.jacket_id]
                court_case_number = case_numbers[0] if case_numbers else None
                sentence = self.generate_jail_sentence(booking.person_id, booking.booking_id, court_case_number)
                # If we have a case, reflect a charge in the sentence description
                if self.offenses:
                    linked_offenses = [o for o in self.offenses if getattr(o, 'person_id', None) == booking.person_id]
                    if linked_offenses:
                        pick = random.choice(linked_offenses)
                        sentence.offense_description = pick.description
                        sentence.statute = pick.statute
                self.jail_sentences.append(sentence)

            # Bail bond for ~45% of bookings if bail was posted
            if booking.bail_posted and random.random() < CONFIG['pct_bookings_with_bail_bond']:
                # person_id comes from booking.person_id, which is generated from an Arrest->Person
                self.bail_bonds.append(self.generate_bail_bond(booking.person_id, booking.booking_id))

            # Incidents: average 0.2 per booking
            incidents_count = 1 if random.random() < CONFIG['avg_jail_incidents_per_booking'] else 0
            for _ in range(incidents_count):
                self.jail_incidents.append(self.generate_jail_incident(booking.booking_id, booking.person_id))
            # Logs per booking
            self.jail_logs.extend(self.generate_jail_logs_for_booking(booking))
        
        # Generate properties/evidence
        print(f"Generating {CONFIG['num_properties']:,} properties/evidence...")
        for i in range(CONFIG['num_properties']):
            incident = random.choice(self.police_incidents) if random.random() < 0.6 else None
            person = random.choice(self.persons) if random.random() < 0.4 else None
            property_record = self.generate_property(
                incident.incident_id if incident else None,
                person.person_id if person else None,
                'KCSO'
            )
            self.properties.append(property_record)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} properties...")
        
        # Generate fire incidents (80% linked to CAD incidents, 20% automatic alarms)
        if CONFIG['generate_fire_data']:
            print(f"Generating {CONFIG['num_fire_incidents']:,} fire incidents...")
            for i in range(CONFIG['num_fire_incidents']):
                # 80% chance of linking to CAD incident, 20% automatic alarm
                if random.random() < 0.8 and self.cad_incidents:
                    cad_incident = random.choice(self.cad_incidents)
                    incident = self.generate_fire_incident(cad_incident)
                else:
                    incident = self.generate_fire_incident()  # Automatic alarm
                self.fire_incidents.append(incident)
                
                if (i + 1) % 5000 == 0:
                    print(f"   Generated {i + 1:,} fire incidents...")
        
        # Generate Fire Personnel prior to shifts
        print(f"Generating {CONFIG['num_fire_personnel']:,} Fire Personnel...")
        self.fire_personnel = []
        for i in range(CONFIG['num_fire_personnel']):
            fp = self.generate_fire_personnel()
            self.fire_personnel.append(fp)
            if (i + 1) % 1000 == 0:
                print(f"   Generated {i + 1:,} Fire Personnel...")

        # CAD incidents already generated above
        
        # Generate citations (after CAD incidents are available)
        num_citations = CONFIG['num_arrests'] * 3  # Generate 3x more citations than arrests
        print(f"Generating {num_citations:,} citations...")
        for i in range(num_citations):
            person = random.choice(self.persons)
            vehicle = random.choice(self.vehicles) if random.random() < 0.8 else None
            cad_incident = random.choice(self.cad_incidents) if random.random() < 0.3 else None
            agency = 'KCSO'
            citation = self.generate_citation(person, cad_incident, vehicle, None, agency)
            self.citations.append(citation)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} citations...")
        
        # Generate field interviews (after CAD incidents are available)
        num_field_interviews = CONFIG['num_arrests'] * 2  # Generate 2x more field interviews than arrests
        print(f"Generating {num_field_interviews:,} field interviews...")
        for i in range(num_field_interviews):
            person = random.choice(self.persons)
            vehicle = random.choice(self.vehicles) if random.random() < 0.6 else None
            cad_incident = random.choice(self.cad_incidents) if random.random() < 0.4 else None
            agency = 'KCSO'
            field_interview = self.generate_field_interview(person, cad_incident, vehicle, None, agency)
            self.field_interviews.append(field_interview)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} field interviews...")
        
        
        # Generate EMS incidents (all linked to CAD incidents)
        if CONFIG['generate_ems_data']:
            print(f"Generating {CONFIG['num_ems_incidents']:,} EMS incidents...")
            for i in range(CONFIG['num_ems_incidents']):
                # Link to a CAD incident
                cad_incident = random.choice(self.cad_incidents)
                incident = self.generate_ems_incident(cad_incident)
                self.ems_incidents.append(incident)
                
                if (i + 1) % 10000 == 0:
                    print(f"   Generated {i + 1:,} EMS incidents...")
        
        # Generate detailed Fire RMS incidents
        print(f"Generating {CONFIG['num_fire_rms_incidents']:,} Fire RMS incidents...")
        self.fire_rms_incidents = []
        self.fire_reports = []
        for i in range(CONFIG['num_fire_rms_incidents']):
            rms_incident = self.generate_fire_rms_incident()
            self.fire_rms_incidents.append(rms_incident)
            # Generate one FireReport per Fire RMS incident
            self.fire_reports.append(self.generate_fire_report(rms_incident.incident_number, datetime.strptime(rms_incident.modified_datetime, '%Y-%m-%d %H:%M:%S')))
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} Fire RMS incidents...")

        # Generate Fire Shifts
        print(f"Generating {CONFIG['num_fire_shifts']:,} Fire Shifts...")
        self.fire_shifts = []
        for i in range(CONFIG['num_fire_shifts']):
            shift = self.generate_fire_shift()
            self.fire_shifts.append(shift)
            if (i + 1) % 1000 == 0:
                print(f"   Generated {i + 1:,} Fire Shifts...")
        
        # Create cross-agency relationships
        if CONFIG['enable_cross_agency_sharing']:
            self.create_cross_agency_links()
        
        total_time = time.time() - start_time
        print(f"\nTotal generation time: {total_time:.1f} seconds")
        print(f"Generation rate: {len(self.persons)/total_time:.0f} persons/second")
        
        self.print_summary()

    def print_summary(self):
        """Print comprehensive data summary"""
        print(f"\n" + "="*80)
        print(f"ENHANCED MULTI-AGENCY SYNTHETIC DATA GENERATION COMPLETE")
        print(f"="*80)
        
        print(f"\nCore Records:")
        print(f"  • Persons: {len(self.persons):,}")
        print(f"  • Vehicles: {len(self.vehicles):,}")
        print(f"  • Properties: {len(self.properties):,}")
        
        print(f"\nLaw Enforcement Records:")
        print(f"  • Police Incidents: {len(self.police_incidents):,}")
        print(f"  • Arrests: {len(self.arrests):,}")
        print(f"  • Citations: {len(self.citations):,}")
        print(f"  • Field Interviews: {len(self.field_interviews):,}")
        print(f"  • Jail Bookings: {len(self.jail_bookings):,}")
        
        print(f"\nFire/EMS Records:")
        print(f"  • Fire Incidents: {len(self.fire_incidents):,}")
        print(f"  • EMS Incidents: {len(self.ems_incidents):,}")
        print(f"  • Fire Personnel: {len(getattr(self, 'fire_personnel', [])):,}")
        print(f"  • Fire Reports: {len(getattr(self, 'fire_reports', [])):,}")
        
        # Agency breakdown
        if self.persons:
            agency_counts = Counter(p.created_by_agency for p in self.persons)
            print(f"\nPersons by Creating Agency:")
            for agency, count in agency_counts.items():
                print(f"  • {agency}: {count:,}")
        
        # Incident type breakdown
        if self.police_incidents:
            incident_counts = Counter(i.incident_type for i in self.police_incidents)
            print(f"\nTop Police Incident Types:")
            for inc_type, count in incident_counts.most_common(5):
                print(f"  • {inc_type}: {count:,}")
        
        if self.fire_incidents:
            fire_counts = Counter(i.incident_type for i in self.fire_incidents)
            print(f"\nTop Fire Incident Types:")
            for inc_type, count in fire_counts.most_common(3):
                print(f"  • {inc_type}: {count:,}")
        
        if self.ems_incidents:
            ems_counts = Counter(i.incident_type for i in self.ems_incidents)
            print(f"\nTop EMS Incident Types:")
            for inc_type, count in ems_counts.most_common(5):
                print(f"  • {inc_type}: {count:,}")
        
        print(f"\n" + "="*80)

    def generate_criminal_legal_status(self):
        """Generate realistic criminal and legal status"""
        # Probation status
        probation_status = random.choice(['NONE', 'ACTIVE', 'INACTIVE', 'TERMINATED'])
        if probation_status == 'ACTIVE':
            probation_officer = f"Officer {fake.last_name()}"
            probation_end_date = fake.date_between(start_date='now', end_date='+2y').strftime('%Y-%m-%d')
        else:
            probation_officer = None
            probation_end_date = None
        
        # Parole status
        parole_status = random.choice(['NONE', 'ACTIVE', 'INACTIVE', 'TERMINATED'])
        if parole_status == 'ACTIVE':
            parole_officer = f"Officer {fake.last_name()}"
            parole_end_date = fake.date_between(start_date='now', end_date='+3y').strftime('%Y-%m-%d')
        else:
            parole_officer = None
            parole_end_date = None
        
        # Restraining orders
        restraining_orders = []
        if random.random() < 0.15:  # 15% chance of having restraining orders
            num_orders = random.randint(1, 2)
            for _ in range(num_orders):
                restraining_orders.append({
                    'order_id': f"RO-{random.randint(100000, 999999)}",
                    'protected_party': f"{fake.first_name()} {fake.last_name()}",
                    'restricted_party': f"{fake.first_name()} {fake.last_name()}",
                    'order_date': fake.date_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d'),
                    'expiration_date': fake.date_between(start_date='now', end_date='+2y').strftime('%Y-%m-%d'),
                    'restrictions': random.choice(['NO_CONTACT', 'NO_APPROACH', 'NO_COMMUNICATION']),
                    'status': random.choice(['ACTIVE', 'EXPIRED', 'VIOLATED'])
                })
        
        # Gang affiliations
        gang_affiliations = []
        if random.random() < 0.08:  # 8% chance of gang affiliation
            gangs = ['Crips', 'Bloods', 'MS-13', '18th Street', 'Sureños', 'Norteños']
            num_gangs = random.randint(1, 2)
            for _ in range(num_gangs):
                gang_affiliations.append({
                    'gang_name': random.choice(gangs),
                    'affiliation_type': random.choice(['MEMBER', 'ASSOCIATE', 'FORMER_MEMBER']),
                    'start_date': fake.date_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d'),
                    'end_date': None if random.random() < 0.7 else fake.date_between(start_date='now', end_date='+1y').strftime('%Y-%m-%d')
                })
        
        # Court cases
        court_cases = []
        if random.random() < 0.25:  # 25% chance of active court cases
            num_cases = random.randint(1, 3)
            for _ in range(num_cases):
                court_cases.append({
                    'case_number': f"CR-{random.randint(100000, 999999)}",
                    'court': random.choice(['King County Superior Court', 'Seattle Municipal Court']),
                    'case_type': random.choice(['CRIMINAL', 'TRAFFIC', 'DOMESTIC_VIOLENCE']),
                    'filing_date': fake.date_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d'),
                    'next_hearing': fake.date_between(start_date='now', end_date='+3m').strftime('%Y-%m-%d'),
                    'status': random.choice(['PENDING', 'SCHEDULED', 'CONTINUED', 'DISPOSED'])
                })
        
        # Bail status
        bail_status = random.choice(['NONE', 'OUT_ON_BAIL', 'HELD_WITHOUT_BAIL', 'NO_BAIL'])
        bail_amount = None
        if bail_status == 'OUT_ON_BAIL':
            # Generate sample charges for bail calculation
            sample_charges = [self.generate_charges()]
            bail_amount = self.calculate_bail_amount(sample_charges)
        
        return {
            'probation_status': probation_status,
            'probation_officer': probation_officer,
            'probation_end_date': probation_end_date,
            'parole_status': parole_status,
            'parole_officer': parole_officer,
            'parole_end_date': parole_end_date,
            'restraining_orders': restraining_orders,
            'gang_affiliations': gang_affiliations,
            'court_cases': court_cases,
            'bail_status': bail_status,
            'bail_amount': bail_amount
        }

    def _format_seattle_phone(self):
        area_code = random.choices(['206','425','360','564'], weights=[60,25,10,5])[0]
        exchange = random.randint(200, 999)
        line = random.randint(1000, 9999)
        parsed = phonenumbers.parse(f"+1{area_code}{exchange}{line}", "US")
        return phonenumbers.format_number(parsed, PhoneNumberFormat.NATIONAL)

    def _determine_agency_by_location(self, city, zip_code):
        """Determine the correct agency based on location - always KCSO"""
        # All locations are now handled by KCSO
        return 'KCSO'

    def _assign_ems_unit_by_location(self, city, zip_code):
        """Assign EMS unit based on location coverage area"""
        # Seattle Fire Department EMS units by district
        if city.upper() == 'SEATTLE':
            # Downtown/Central (98101-98109)
            if zip_code in ['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109']:
                return f"MEDIC_{random.randint(1, 5)}"
            # North (98115, 98117, 98125, 98133, 98177)
            elif zip_code in ['98115', '98117', '98125', '98133', '98177']:
                return f"MEDIC_{random.randint(6, 10)}"
            # South (98118, 98126, 98136, 98144, 98146, 98148)
            elif zip_code in ['98118', '98126', '98136', '98144', '98146', '98148']:
                return f"MEDIC_{random.randint(11, 15)}"
            # East (98112, 98122)
            elif zip_code in ['98112', '98122']:
                return f"MEDIC_{random.randint(16, 20)}"
            else:
                return f"MEDIC_{random.randint(1, 20)}"
        # King County EMS units for all other areas
        # King County EMS units for other areas
        else:
            return f"KC_MEDIC_{random.randint(1, 15)}"

    def _generate_king_county_address(self):
        """Generate realistic addresses using actual Seattle/King County street names and numbers."""
        # Real Seattle addresses with actual street names
        seattle_addresses = {
            '98101': {  # Downtown Seattle
                'streets': ['1st Ave', '2nd Ave', '3rd Ave', '4th Ave', '5th Ave', 'Pike St', 'Pine St', 'Union St', 'University St', 'Spring St', 'Madison St', 'Marion St', 'Columbia St', 'Yesler Way', 'James St', 'Cherry St', 'Jefferson St', 'Seneca St', 'Broad St'],
                'number_range': (100, 2000)
            },
            '98102': {  # Capitol Hill
                'streets': ['Broadway', 'Pine St', 'Pike St', 'E Olive Way', 'E John St', 'E Denny Way', 'E Roy St', 'E Mercer St', 'E Thomas St', 'E Harrison St', 'E Republican St', 'E Galer St', 'E Highland Dr', 'E Aloha St', 'E Howe St', 'E Boston St', 'E Prospect St', 'E Blaine St', 'E Newton St', 'E Lynn St'],
                'number_range': (100, 1500)
            },
            '98103': {  # Wallingford/Green Lake
                'streets': ['N 45th St', 'N 50th St', 'N 55th St', 'N 40th St', 'N 35th St', 'Stone Way N', 'Wallingford Ave N', 'Interlake Ave N', 'Meridian Ave N', 'Latona Ave NE', 'Burke Ave N', 'Greenwood Ave N', 'Phinney Ave N', 'Fremont Ave N', 'Ashworth Ave N', 'Bagley Ave N', 'Dayton Ave N', 'Evanston Ave N', 'Keystone Pl N', 'N 34th St'],
                'number_range': (100, 2000)
            },
            '98104': {  # Pioneer Square
                'streets': ['1st Ave S', '2nd Ave S', '3rd Ave S', 'S Jackson St', 'S King St', 'S Main St', 'S Washington St', 'S Dearborn St', 'S Weller St', 'S Lane St', 'S Charles St', 'S Forest St', 'S Walker St', 'S Horton St', 'S Orcas St', 'S Brandon St', 'S Norman St', 'S Nevada St', 'S Oregon St', 'S Idaho St'],
                'number_range': (100, 1500)
            },
            '98105': {  # University District
                'streets': ['University Way NE', '15th Ave NE', 'Roosevelt Way NE', '12th Ave NE', '11th Ave NE', '10th Ave NE', '8th Ave NE', '7th Ave NE', '5th Ave NE', 'NE 45th St', 'NE 50th St', 'NE 55th St', 'NE 40th St', 'NE 35th St', 'NE 30th St', 'NE 25th St', 'NE 20th St', 'NE 15th St', 'NE 10th St', 'NE Campus Pkwy'],
                'number_range': (100, 2000)
            },
            '98106': {  # West Seattle
                'streets': ['California Ave SW', 'Fauntleroy Way SW', 'SW Alaska St', 'SW Edmunds St', 'SW Genesee St', 'SW Hudson St', 'SW Juneau St', 'SW Kenyon St', 'SW Lander St', 'SW Manning St', 'SW Oregon St', 'SW Othello St', 'SW Raymond St', 'SW Thistle St', 'SW Trenton St', 'SW Webster St', 'SW Yancy St', 'SW Admiral Way', 'SW Avalon Way', 'SW Andover St'],
                'number_range': (100, 3000)
            },
            '98107': {  # Ballard
                'streets': ['NW Market St', 'NW Leary Way', 'NW 56th St', 'NW 57th St', 'NW 58th St', 'NW 59th St', 'NW 60th St', 'NW 61st St', 'NW 62nd St', 'NW 63rd St', 'NW 64th St', 'NW 65th St', 'NW 70th St', 'NW 75th St', 'NW 80th St', 'NW 85th St', 'NW 90th St', 'NW 95th St', '24th Ave NW', '32nd Ave NW'],
                'number_range': (100, 2000)
            },
            '98108': {  # Georgetown/South Park
                'streets': ['S Bailey St', 'S Cloverdale St', 'S Dawson St', 'S Eddy St', 'S Forest St', 'S Garden St', 'S Henderson St', 'S Irving St', 'S Jansen St', 'S Kenyon St', 'S Lucile St', 'S Morgan St', 'S Nevada St', 'S Oregon St', 'S Portland St', 'S Rose St', 'S Sullivan St', 'S Trenton St', 'S Webster St', 'S Willow St'],
                'number_range': (100, 1500)
            },
            '98109': {  # Queen Anne
                'streets': ['Queen Anne Ave N', '1st Ave N', '2nd Ave N', '3rd Ave N', '4th Ave N', '5th Ave N', '6th Ave N', '7th Ave N', '8th Ave N', '9th Ave N', '10th Ave N', 'W Galer St', 'W Highland Dr', 'W Howe St', 'W Boston St', 'W Prospect St', 'W Blaine St', 'W Newton St', 'W Lynn St', 'W McGraw St'],
                'number_range': (100, 2000)
            },
            '98112': {  # Montlake/Madison Park
                'streets': ['E Madison St', 'E Union St', 'E Pike St', 'E Pine St', 'E Olive Way', 'E John St', 'E Denny Way', 'E Roy St', 'E Mercer St', 'E Thomas St', 'E Harrison St', 'E Republican St', 'E Galer St', 'E Highland Dr', 'E Aloha St', 'E Howe St', 'E Boston St', 'E Prospect St', 'E Blaine St', 'E Newton St'],
                'number_range': (100, 1500)
            }
        }
        
        # Use only Seattle addresses
        all_addresses = seattle_addresses
        
        # Select a random zip code and get its data
        zip_code = random.choice(list(all_addresses.keys()))
        address_data = all_addresses[zip_code]
        
        # Generate street number and name
        street_number = random.randint(address_data['number_range'][0], address_data['number_range'][1])
        street_name = random.choice(address_data['streets'])
        
        # All addresses are now in Seattle area
        city = 'Seattle'
        
        address = f"{street_number} {street_name}"
        return address, city, 'WA', zip_code

    def get_or_create_officer(self, agency='KCSO'):
        """Get an existing officer or create a new one for consistency"""
        # Check if we have officers for this agency
        if agency not in self.officer_pool:
            self.officer_pool[agency] = []
        
        # If we have less than 50 officers for this agency, create a new one
        if len(self.officer_pool[agency]) < 50:
            officer = self.generate_officer(agency)
            self.officer_pool[agency].append(officer)
            self.badge_to_officer[officer.badge_number] = officer
            return officer
        
        # Return a random existing officer
        return random.choice(self.officer_pool[agency])
    
    def generate_officer(self, agency='KCSO'):
        badge = str(random.randint(1000, 9999))
        # Ensure badge is unique
        while badge in self.badge_to_officer:
            badge = str(random.randint(1000, 9999))
            
        call_sign = f"{random.choice(['A','B','C','D'])}{random.randint(1,9)}"
        return Officer(
            officer_id=f"OFF-{uuid.uuid4()}",
            badge_number=badge,
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            rank=random.choice(['OFFICER','CORPORAL','SERGEANT','LIEUTENANT','CAPTAIN']),
            unit_id=None,
            unit_name=random.choice(['PATROL','TRAFFIC','NARCOTICS','GANG_UNIT','DETECTIVES']),
            call_sign=call_sign,
            agency=agency,
            email=f"{badge.lower()}@{agency.lower()}.gov",
            phone=self._format_seattle_phone() if hasattr(self, '_format_seattle_phone') else f"(206) {random.randint(200,999)}-{random.randint(1000,9999)}",
            hire_date=fake.date_between(start_date='-15y', end_date='-1y').strftime('%Y-%m-%d'),
            active=True,
            certifications=random.sample(['FTO','EVOC','TASER','AR15','LESS_LETHAL','K9','SWAT'], random.randint(1,3)),
            trainings=random.sample(['ICAT','CIT','DV','NARC','HNT'], random.randint(1,3)),
            specialties=random.sample(['DUI','DV','GANGS','BURGLARY','FRAUD'], random.randint(1,2)),
            current_assignment=random.choice(['Patrol - Night','Patrol - Day','Traffic','Detectives']),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            updated_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            bodycam_id=f"BWC-{random.randint(10000,99999)}" if random.random()<0.9 else None,
            shift_status=random.choice(['ON_DUTY', 'OFF_DUTY', 'ON_CALL']),
            supervisor_officer_id=random.choice([o['officer_id'] for o in self.officers if o['rank'] != 'OFFICER']) if self.officers else None
        )

    def generate_call_for_service(self, related_cad=None, caller_person=None, agency='KCSO'):
        now = fake.date_time_between(start_date='-30d', end_date='now')
        dispatch = now + timedelta(minutes=random.randint(1,5))
        en_route = dispatch + timedelta(minutes=random.randint(1,6))
        on_scene = en_route + timedelta(minutes=random.randint(2,10))
        cleared = on_scene + timedelta(minutes=random.randint(5,45))
        address, city, state, zip_code = (self._generate_king_county_address()
                                          if hasattr(self, '_generate_king_county_address')
                                          else (f"{random.randint(100,9999)} Main St",'Seattle','WA','98101'))
        primary_officer = random.choice(self.officers)['officer_id'] if self.officers else None
        backups = random.sample([o['officer_id'] if isinstance(o, dict) else o.officer_id for o in self.officers], 
                                k=min(len(self.officers), random.randint(0,2))) if self.officers else []
        return CallForService(
            cfs_id=f"CFS-{uuid.uuid4()}",
            call_number=f"CF-{random.randint(100000,999999)}",
            call_type=random.choice(['DISTURBANCE','SUSPICIOUS','TRAFFIC','DV','THEFT','ASSAULT']),
            priority=random.choice(['LOW','MEDIUM','HIGH']),
            status='CLOSED',
            call_received_datetime=now.strftime('%Y-%m-%d %H:%M:%S'),
            call_entered_datetime=(now+timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_datetime=dispatch.strftime('%Y-%m-%d %H:%M:%S'),
            en_route_datetime=en_route.strftime('%Y-%m-%d %H:%M:%S'),
            on_scene_datetime=on_scene.strftime('%Y-%m-%d %H:%M:%S'),
            clear_datetime=cleared.strftime('%Y-%m-%d %H:%M:%S'),
            caller_person_id=caller_person.person_id if caller_person else None,
            caller_phone=self._format_seattle_phone() if hasattr(self,'_format_seattle_phone') else f"(206) {random.randint(200,999)}-{random.randint(1000,9999)}",
            address=address, city=city, state=state, zip_code=zip_code,
            latitude=random.uniform(47.5,47.75), longitude=random.uniform(-122.45,-122.1),
            district=random.choice(['NORTH','SOUTH','EAST','WEST','CENTRAL']),
            beat=f"{random.choice(list('NSEWC'))}{random.randint(1,9)}",
            units_assigned=[random.choice(['A1','B2','C3','D4'])],
            primary_officer_id=primary_officer,
            backup_officer_ids=backups,
            related_cad_id=related_cad.cad_id if related_cad else None,
            related_police_incident_id=None,
            related_fire_incident_id=None,
            related_ems_incident_id=None,
            notes=random.choice(['Caller reports loud argument','Vehicle blocking driveway','Unknown person loitering']),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency
        )

    def _generate_fire_email_username(self, first_name, last_name):
        base = f"{first_name[0]}{last_name}".lower()
        base = ''.join(ch for ch in base if ch.isalnum())
        return base, f"{base}@firedepartment.org"

    def _compose_fire_narrative(self, units: List[str], incident_type: str) -> str:
        """Generate realistic fire incident narratives based on incident type"""
        
        fire_narratives = {
            'STRUCTURE_FIRE': [
                "{units_str} responded to a structure fire at {address}. Fire was contained to {area}. {casualties} reported. Fire investigation determined cause was {cause}. All units returned to quarters.",
                "{units_str} dispatched to working structure fire. Fire extinguished in {time} minutes. {damage} damage reported. No injuries. Cause under investigation. All units cleared.",
                "{units_str} responded to residential fire. Fire quickly extinguished. {evacuations} evacuated safely. Property damage estimated at ${damage}. All units returned to service."
            ],
            'VEHICLE_FIRE': [
                "{units_str} responded to vehicle fire on {location}. Fire extinguished using {method}. No injuries reported. Vehicle towed from scene. All units cleared.",
                "{units_str} dispatched to vehicle fire. Fire quickly controlled. Driver evacuated safely. Vehicle declared total loss. All units returned to quarters."
            ],
            'GRASS_FIRE': [
                "{units_str} responded to grass fire covering {size} acres. Fire contained using {method}. No structures threatened. All units returned to service.",
                "{units_str} dispatched to wildland fire. Fire extinguished after {time} hours. {acres} acres burned. No injuries. All units cleared."
            ],
            'ALARM_ACTIVATION': [
                "{units_str} responded to fire alarm activation. No fire found. Alarm reset. Building cleared. All units returned to quarters.",
                "{units_str} dispatched to automatic alarm. False alarm due to {cause}. System reset. All units cleared without incident."
            ],
            'GAS_LEAK': [
                "{units_str} responded to reported gas leak. Area evacuated. Gas company contacted. Leak isolated. All units cleared.",
                "{units_str} dispatched to gas odor. Building evacuated as precaution. Gas company on scene. Situation resolved. All units returned."
            ],
            'HAZMAT': [
                "{units_str} responded to hazmat incident. Area secured. Hazmat team requested. Situation contained. All units cleared.",
                "{units_str} dispatched to chemical spill. Area evacuated. Cleanup in progress. No injuries reported. All units returned."
            ],
            'RESCUE': [
                "{units_str} responded to technical rescue. Victim extricated safely. {injuries} treated on scene. All units cleared.",
                "{units_str} dispatched to rescue operation. {victims} rescued without injury. All units returned to service."
            ],
            'MEDICAL': [
                "{units_str} responded to medical emergency. Patient treated and transported to {hospital}. All units returned to quarters.",
                "{units_str} dispatched to medical call. Patient assessed and transported. All units cleared."
            ]
        }
        
        # Get appropriate narrative template based on incident type
        if incident_type in fire_narratives:
            template = random.choice(fire_narratives[incident_type])
        else:
            # Default template for unknown incident types
            template = "{units_str} responded to fire incident. Situation resolved. All units returned to quarters."
        
        # Generate realistic details for the narrative
        details = {
            'address': random.choice(['123 Main St', '456 Oak Ave', '789 Pine Rd']),
            'area': random.choice(['kitchen', 'bedroom', 'garage', 'basement', 'attic']),
            'casualties': random.choice(['No casualties', '1 minor injury', '2 minor injuries']),
            'cause': random.choice(['electrical', 'cooking', 'heating equipment', 'unknown']),
            'time': random.choice(['15', '20', '30', '45']),
            'damage': random.choice(['minimal', 'moderate', 'extensive']),
            'evacuations': random.choice(['2 residents', '4 residents', 'No evacuations']),
            'location': random.choice(['Highway 99', 'I-5', 'local street']),
            'method': random.choice(['foam', 'water', 'dry chemical']),
            'size': random.choice(['0.5', '1.2', '2.8', '5.1']),
            'acres': random.choice(['2', '5', '8', '12']),
            'cause': random.choice(['cooking smoke', 'steam', 'dust', 'system malfunction']),
            'hospital': random.choice(['Harborview', 'Swedish', 'Virginia Mason']),
            'injuries': random.choice(['minor', 'no', 'serious']),
            'victims': random.choice(['1', '2', '3'])
        }
        
        # Format the narrative with realistic details
        try:
            return template.format(
                units_str=', '.join(units),
                **{k: v for k, v in details.items() if k in template}
            )
        except KeyError:
            # Fallback if template formatting fails
            return f"{', '.join(units)} responded to {incident_type.lower().replace('_', ' ')} incident. All units returned to quarters."

    def generate_fire_report(self, incident_number: str, created_dt: datetime = None):
        created_dt = created_dt or fake.date_time_between(start_date='-30d', end_date='now')
        if self.fire_personnel:
            writer = random.choice(self.fire_personnel)
            writer_name = writer.full_name
            writer_emp = writer.employee_id
            suffix = f" {writer.last_name.upper()} {writer.role.replace('_','/')} {writer.employee_id}"
        else:
            writer_name = f"{fake.first_name()} {fake.last_name()}"
            writer_emp = str(random.randint(4000,4999))
            suffix = f" {writer_name.split(' ')[1].upper()} FF/PM/ACO {writer_emp}"

        # Try to infer units from fire_rms_incidents with same incident_number
        units = []
        for rms in getattr(self, 'fire_rms_incidents', []):
            if getattr(rms, 'incident_number', None) == incident_number:
                units = rms.apparatus_deployed[:2]
                break
        if not units:
            units = [f"E{random.randint(101, 199)}", f"M{random.randint(101, 199)}"]

        # Determine incident type from incident number or use default
        incident_type = 'STRUCTURE_FIRE'  # Default
        # Try to infer incident type from fire incidents if available
        if hasattr(self, 'fire_incidents') and self.fire_incidents:
            # Look for matching incident number in fire incidents
            for fire_incident in self.fire_incidents:
                if fire_incident.incident_number == incident_number:
                    incident_type = fire_incident.incident_type
                    break
        
        narrative = self._compose_fire_narrative(units, incident_type) + f"\n\n{suffix}"

        return FireReport(
            report_id=f"FR-{uuid.uuid4()}",
            incident_number=incident_number,
            created_datetime=created_dt.strftime('%Y-%m-%d %H:%M:%S'),
            report_writer_employee_id=writer_emp,
            report_writer_name=writer_name,
            narrative=narrative,
        )

    # === Jail generators ===
    def generate_corrections_facility(self):
        address, city, state, zip_code = self._generate_king_county_address()
        name = random.choice([
            'King County Correctional Facility',
            'Maleng Regional Justice Center',
            'Eastside Detention Annex'
        ])
        return CorrectionsFacility(
            facility_id=f"FAC-{uuid.uuid4()}",
            name=name,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            capacity=random.randint(200, 2000),
            security_level=random.choice(['MINIMUM','MEDIUM','MAXIMUM']),
            contact_phone=self._format_seattle_phone(),
            contact_email=f"info@{name.lower().replace(' ','').replace('regional','rg').replace('justice','js')}.gov",
            warden_name=f"{fake.first_name()} {fake.last_name()}",
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

    def generate_jail_sentence(self, person_id: str, booking_id: Optional[str], court_case_number: Optional[str] = None):
        sentence_date = fake.date_between(start_date='-3y', end_date='-7d')

        # Look for a linked offense to weight sentence types by severity
        linked_offenses = [o for o in getattr(self, 'offenses', []) if getattr(o, 'person_id', None) == person_id]
        offense = linked_offenses[-1] if linked_offenses else None
        severity = getattr(offense, 'severity', None) or random.choice(['MISDEMEANOR','FELONY','INFRACTION'])

        # Choose sentence type with weights based on severity
        if severity == 'FELONY':
            sentence_options = ['JAIL_TIME','PRISON','WORK_RELEASE','HOUSE_ARREST','ELECTRONIC_MONITORING','PROBATION','SUSPENDED']
            weights = [40, 10, 10, 5, 5, 20, 10]
        elif severity == 'MISDEMEANOR':
            sentence_options = ['PROBATION','COMMUNITY_SERVICE','TIME_SERVED','JAIL_TIME','SUSPENDED','FINE_ONLY','ELECTRONIC_MONITORING','DEFERRED_SENTENCE','DIVERSION']
            weights = [30, 20, 10, 15, 10, 10, 5, 5, 5]
        else:  # INFRACTION/OTHER
            sentence_options = ['FINE_ONLY','TIME_SERVED','DEFERRED_SENTENCE','DIVERSION','COMMUNITY_SERVICE']
            weights = [45, 20, 15, 10, 10]

        sentence_type = random.choices(sentence_options, weights=weights, k=1)[0]

        # Determine duration and notes based on sentence type
        notes = None
        good_time_eligible = False

        if sentence_type in ['JAIL_TIME']:
            total_days = random.choice([15, 30, 60, 90, 120, 180, 365])
            good_time_eligible = True
        elif sentence_type == 'PRISON':
            total_days = random.choice([365, 730, 1095, 1460, 1825])  # 1-5 years
            good_time_eligible = True
        elif sentence_type == 'WORK_RELEASE':
            total_days = random.choice([30, 60, 90, 120, 180, 365])
            notes = 'Eligible for work release'
            good_time_eligible = True
        elif sentence_type in ['HOUSE_ARREST','ELECTRONIC_MONITORING']:
            total_days = random.choice([14, 30, 60, 90, 120, 180])
            notes = 'Electronic monitoring' if sentence_type == 'ELECTRONIC_MONITORING' else 'House arrest'
        elif sentence_type == 'PROBATION':
            total_days = random.choice([180, 365, 730, 1095])  # 6-36 months
            notes = random.choice(['Supervised probation','Unsupervised probation'])
        elif sentence_type == 'COMMUNITY_SERVICE':
            total_days = random.choice([30, 60, 90, 120])
            hours = random.choice([20, 40, 80, 120])
            notes = f'{hours} hours community service'
        elif sentence_type == 'TIME_SERVED':
            pretrial_days = random.randint(1, 30)
            total_days = pretrial_days
            notes = 'Time served'
        elif sentence_type == 'FINE_ONLY':
            total_days = 0
            fine_amt = random.choice([250, 500, 1000, 2000])
            notes = f'Fine only: ${fine_amt}'
        elif sentence_type in ['DEFERRED_SENTENCE','DIVERSION']:
            total_days = random.choice([90, 180, 365])
            notes = 'Deferred sentence' if sentence_type == 'DEFERRED_SENTENCE' else 'Diversion program'
        elif sentence_type == 'SUSPENDED':
            total_days = random.choice([30, 60, 90, 120, 180])
            notes = 'Sentence suspended'
        else:
            total_days = random.choice([30, 60, 90, 120, 180, 365])

        time_served = random.randint(0, min(45, total_days)) if total_days > 0 else 0
        start_date = sentence_date + timedelta(days=random.randint(0, 14))
        end_date = (start_date + timedelta(days=total_days)) if total_days > 0 else None

        # Additional sentencing attributes
        crime_date = fake.date_between(start_date='-2y', end_date='now').strftime('%Y-%m-%d')
        def yrs(lo, hi):
            return round(random.uniform(lo, hi), 1)
        orc_code = f"{random.randint(2901, 2999)}.{random.randint(1,99):02d}" if random.random()<0.6 else None
        definite_years = yrs(0.5, 10.0) if sentence_type in ['PRISON','JAIL_TIME'] and random.random()<0.5 else None
        min_years = yrs(0.5, 5.0) if random.random()<0.4 else None
        max_years = (min_years + yrs(0.5, 10.0)) if min_years and random.random()<0.7 else None
        rvo_years = yrs(0.5, 3.0) if random.random()<0.15 else None
        mdo_years = yrs(1.0, 5.0) if 'DRUG' in (getattr(offense,'nibrs_group_name','') or '') and random.random()<0.2 else None
        gun_years = yrs(1.0, 3.0) if random.random()<0.1 else None
        commit_cty_years = yrs(0.5, 2.0) if random.random()<0.1 else None

        # Inmate identity snapshot
        p = self.person_index.get(person_id)
        inmate_first = p.first_name if p else None
        inmate_middle = None
        inmate_last = p.last_name if p else None
        inmate_dob = p.date_of_birth if p else None
        inmate_key_snap = getattr(p, 'inmate_key', None)

        sentence = JailSentence(
            sentence_id=f"SEN-{uuid.uuid4()}",
            person_id=person_id,
            booking_id=booking_id,
            court_case_number=court_case_number or f"CR-{random.randint(100000,999999)}",
            sentence_date=sentence_date.strftime('%Y-%m-%d'),
            offense_description=(getattr(offense, 'description', None) or random.choice(['THEFT 3', 'ASSAULT 4', 'DUI', 'DV VIOLATION', 'BURGLARY 2'])),
            statute=(getattr(offense, 'statute', None) or f"RCW {random.randint(9,69)}.{random.randint(10,999)}"),
            orc_code=orc_code,
            sentence_type=sentence_type,
            total_days=total_days,
            time_served_days=time_served,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=(end_date.strftime('%Y-%m-%d') if end_date else None),
            concurrent_with_case_numbers=[f"CR-{random.randint(100000,999999)}" for _ in range(random.randint(0,2))],
            good_time_eligible=good_time_eligible,
            notes=notes,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            crime_date=crime_date,
            definite_stated_term_years=definite_years,
            minimum_years=min_years,
            maximum_years=max_years,
            return_violator_offender_years=rvo_years,
            major_drug_offender_years=mdo_years,
            gun_years=gun_years,
            commitment_county_years=commit_cty_years,
            inmate_first_name=inmate_first,
            inmate_middle_name=inmate_middle,
            inmate_last_name=inmate_last,
            inmate_date_of_birth=inmate_dob,
            inmate_key=inmate_key_snap,
        )

        # Update person based on sentencing outcome
        person = self.person_index.get(person_id)
        if person:
            person.earliest_release_date = sentence.end_date or None
            if sentence.sentence_type in ['JAIL_TIME','PRISON','WORK_RELEASE','HOUSE_ARREST','ELECTRONIC_MONITORING']:
                person.incarceration_status = 'SENTENCED'
            elif sentence.sentence_type in ['PROBATION','DEFERRED_SENTENCE','DIVERSION']:
                person.probation_status = 'ACTIVE'
                person.probation_officer_name = person.probation_officer_name or f"{fake.first_name()} {fake.last_name()}"
                person.probation_officer_phone_number = person.probation_officer_phone_number or self._format_seattle_phone()

        return sentence

    def generate_jail_incident(self, booking_id: Optional[str] = None, person_id: Optional[str] = None):
        when = fake.date_time_between(start_date='-180d', end_date='now')
        inc_type = random.choice(['ASSAULT','CONTRABAND','SELF_HARM','MEDICAL','PROPERTY_DAMAGE'])
        location = random.choice(['POD A','POD B','DAYROOM','YARD','CELL 2','MEDICAL'])
        return JailIncident(
            incident_id=f"JINC-{uuid.uuid4()}",
            booking_id=booking_id,
            person_id=person_id,
            incident_datetime=when.strftime('%Y-%m-%d %H:%M:%S'),
            incident_type=inc_type,
            location=location,
            involved_inmates=[str(uuid.uuid4()) for _ in range(random.randint(0,3))],
            involved_staff=[f"{fake.first_name()} {fake.last_name()}" for _ in range(random.randint(1,3))],
            description=random.choice([
                'Altercation between inmates; separated by staff.',
                'Contraband discovered during cell search.',
                'Inmate referred for medical evaluation.',
                'Property damage to dayroom table recorded.'
            ]),
            actions_taken=random.sample(['MEDICAL_EVAL','DISCIPLINARY','ISOLATION','REPORT_FILED','EVIDENCE_SEIZED'], k=random.randint(1,3)),
            referral_to_outside_agency=random.choice([None,'KCSO','PROSECUTOR']),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

    def generate_jail_logs_for_booking(self, booking: JailBooking):
        logs = []
        base_dt = datetime.strptime(booking.booking_datetime, '%Y-%m-%d %H:%M:%S')
        # Initial entry
        logs.append(JailLog(
            log_id=f"JLOG-{uuid.uuid4()}",
            booking_id=booking.booking_id,
            person_id=booking.person_id,
            log_datetime=booking.booking_datetime,
            action_type='BOOKED',
            details={'booking_number': booking.booking_number, 'housing': booking.housing_assignment},
            actor=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            created_date=booking.booking_datetime,
        ))
        # Property intake at booking
        intake_items = random.randint(1, 8)
        dt_prop_in = (base_dt + timedelta(minutes=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')
        logs.append(JailLog(
            log_id=f"JLOG-{uuid.uuid4()}",
            booking_id=booking.booking_id,
            person_id=booking.person_id,
            log_datetime=dt_prop_in,
            action_type='PROPERTY_INTAKE',
            details={'items_logged': intake_items},
            actor=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            created_date=dt_prop_in,
        ))
        # Medical intake shortly after
        if random.random() < 0.9:
            dt = (base_dt + timedelta(minutes=random.randint(5, 45))).strftime('%Y-%m-%d %H:%M:%S')
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=dt,
                action_type='MEDICAL_INTAKE',
                details={'nurse': f"NURSE {fake.last_name().upper()}", 'alerts': booking.medical_alerts},
                actor=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
                created_date=dt,
            ))
        # Possible housing change
        if random.random() < 0.3:
            dt = (base_dt + timedelta(hours=random.randint(6, 72))).strftime('%Y-%m-%d %H:%M:%S')
            old = booking.housing_assignment
            new = f"BLOCK_{random.choice(['A','B','C','D'])}{random.randint(1,20)}"
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=dt,
                action_type='HOUSING_CHANGE',
                details={'from': old, 'to': new},
                actor=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
                created_date=dt,
            ))
        # Court appearance
        if random.random() < 0.4:
            dt = (base_dt + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=dt,
                action_type='COURT_APPEARANCE',
                details={'court': random.choice(['KCSC','SMC','BMC']), 'result': random.choice(['CONTINUED','HELD','RELEASED_ON_BOND'])},
                actor='SYSTEM',
                created_date=dt,
            ))
        # Visitors and phone calls over the stay
        try:
            release_dt = datetime.strptime(booking.release_datetime, '%Y-%m-%d %H:%M:%S') if booking.release_datetime else (base_dt + timedelta(days=booking.days_served))
        except Exception:
            release_dt = base_dt + timedelta(days=7)
        stay_days = max(0, (release_dt - base_dt).days)
        # Visitors ~0-1 per 2 weeks
        num_visitors = random.randint(0, min(3, max(0, stay_days // 14)))
        for _ in range(num_visitors):
            v_dt = base_dt + timedelta(days=random.randint(1, max(1, stay_days or 1)), hours=random.randint(9, 19), minutes=random.randint(0, 59))
            v_dt_str = v_dt.strftime('%Y-%m-%d %H:%M:%S')
            v_name = f"{fake.first_name()} {fake.last_name()}"
            v_relation = random.choice(['FAMILY','FRIEND','ATTORNEY','CLERGY'])
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=v_dt_str,
                action_type='VISITOR_CHECKIN',
                details={'visitor_name': v_name, 'relation': v_relation, 'duration_min': random.randint(15, 60)},
                actor='VISITATION_DESK',
                created_date=v_dt_str,
            ))
            booking.visitors.append({'name': v_name, 'relation': v_relation, 'datetime': v_dt_str})
        # Phone calls: a few across the stay
        num_calls = random.randint(0, min(6, 1 + stay_days // 7))
        for _ in range(num_calls):
            c_dt = base_dt + timedelta(days=random.randint(0, max(0, stay_days)), hours=random.randint(8, 21), minutes=random.randint(0, 59))
            c_dt_str = c_dt.strftime('%Y-%m-%d %H:%M:%S')
            c_to = f"{fake.first_name()} {fake.last_name()}"
            c_rel = random.choice(['FAMILY','FRIEND','ATTORNEY'])
            dur = random.randint(3, 30)
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=c_dt_str,
                action_type='PHONE_CALL',
                details={'to': c_to, 'relation': c_rel, 'duration_min': dur},
                actor='INMATE_PHONE',
                created_date=c_dt_str,
            ))
            booking.phone_calls.append({'to': c_to, 'relation': c_rel, 'duration_min': dur, 'datetime': c_dt_str})
        # Occasional disciplinary action
        if random.random() < 0.1:
            d_dt = base_dt + timedelta(days=random.randint(1, max(1, stay_days)), hours=random.randint(9, 18))
            d_dt_str = d_dt.strftime('%Y-%m-%d %H:%M:%S')
            action = random.choice(['WARNING','LOSS_OF_PRIVILEGES','SEGREGATION'])
            reason = random.choice(['CONTRABAND','UNAUTHORIZED_COMMUNICATION','ALTERCATION'])
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=d_dt_str,
                action_type='DISCIPLINARY',
                details={'action': action, 'reason': reason},
                actor=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
                created_date=d_dt_str,
            ))
            booking.disciplinary_actions.append({'action': action, 'reason': reason, 'datetime': d_dt_str})
        # Program enroll/complete if enrolled
        if booking.programs_enrolled:
            dt_en = (base_dt + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d %H:%M:%S')
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=dt_en,
                action_type='PROGRAM_ENROLL',
                details={'programs': booking.programs_enrolled},
                actor='PROGRAMS_DESK',
                created_date=dt_en,
            ))
            if random.random() < 0.5:
                dt_co = (base_dt + timedelta(days=random.randint(15, 120))).strftime('%Y-%m-%d %H:%M:%S')
                logs.append(JailLog(
                    log_id=f"JLOG-{uuid.uuid4()}",
                    booking_id=booking.booking_id,
                    person_id=booking.person_id,
                    log_datetime=dt_co,
                    action_type='PROGRAM_COMPLETE',
                    details={'programs': booking.programs_enrolled},
                    actor='PROGRAMS_DESK',
                    created_date=dt_co,
                ))
        # Release log
        if booking.release_datetime:
            logs.append(JailLog(
                log_id=f"JLOG-{uuid.uuid4()}",
                booking_id=booking.booking_id,
                person_id=booking.person_id,
                log_datetime=booking.release_datetime,
                action_type='RELEASED',
                details={'release_type': booking.release_type},
                actor=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
                created_date=booking.release_datetime,
            ))
            # Update person release attributes
            person = self.person_index.get(booking.person_id)
            if person:
                person.release_datetime = booking.release_datetime
                person.incarceration_status = 'RELEASED'
        return logs

    def generate_bail_bond(self, person_id: str, booking_id: Optional[str]):
        # Ensure person_id maps to a generated person
        if not any(p.person_id == person_id for p in self.persons):
            person_id = random.choice(self.persons).person_id if self.persons else person_id
        bond_type = random.choice(['CASH','SURETY','PROPERTY','PERSONAL_RECOGNIZANCE'])
        amount = random.choice([500.0, 1000.0, 2500.0, 5000.0, 10000.0])
        posted = random.random() < 0.5
        posted_dt = (datetime.now() - timedelta(days=random.randint(1,90))).strftime('%Y-%m-%d %H:%M:%S') if posted else None
        return BailBond(
            bond_id=f"BOND-{uuid.uuid4()}",
            person_id=person_id,
            booking_id=booking_id,
            bond_type=bond_type,
            amount=amount,
            posted=posted,
            posted_datetime=posted_dt,
            poster_name=(f"{fake.first_name()} {fake.last_name()}" if posted else None),
            bondsman_company=(random.choice(['AAA Bail Bonds','Quick Release Bail','Liberty Bonding']) if bond_type=='SURETY' and posted else None),
            receipt_number=(f"RCPT-{random.randint(100000,999999)}" if posted else None),
            conditions=random.sample(['NO_CONTACT','PRETRIAL_SUPERVISION','TRAVEL_RESTRICTIONS','SURRENDER_PASSPORT'], k=random.randint(0,2)),
            exonerated=random.random() < 0.2,
            exonerated_datetime=(datetime.now() - timedelta(days=random.randint(1,60))).strftime('%Y-%m-%d %H:%M:%S') if posted and random.random()<0.2 else None,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

    def generate_jail_program(self):
        name = random.choice(['GED Prep','Substance Abuse Counseling','Vocational Training','Anger Management','Cognitive Behavioral Therapy'])
        category_map = {
            'GED Prep': 'EDUCATION',
            'Substance Abuse Counseling': 'SUBSTANCE_ABUSE',
            'Vocational Training': 'VOCATIONAL',
            'Anger Management': 'MENTAL_HEALTH',
            'Cognitive Behavioral Therapy': 'MENTAL_HEALTH',
        }
        location = random.choice(['CLASSROOM 1','CLASSROOM 2','PROGRAM ROOM','MULTIPURPOSE'])
        start = fake.date_between(start_date='-90d', end_date='+30d')
        end = start + timedelta(days=random.choice([30, 60, 90]))
        # Build pool of known inmates from bookings
        inmate_pool = list({b.person_id for b in getattr(self, 'jail_bookings', [])})
        if not inmate_pool:
            inmate_pool = [p.person_id for p in self.persons]
        return JailProgram(
            program_id=f"PROG-{uuid.uuid4()}",
            name=name,
            category=category_map[name],
            provider=random.choice(['County Dept. of Corrections','Community College','Nonprofit Partner']),
            location=location,
            schedule=random.choice(['Mon/Wed/Fri 10:00-12:00','Tue/Thu 13:00-15:00','Sat 09:00-12:00']),
            capacity=(capacity := random.randint(10, 30)),
            enrolled_person_ids=(lambda pool: (random.sample(pool, k=min(len(pool), random.randint(5, 20), capacity))))(inmate_pool),
            waitlist_person_ids=(lambda pool, enrolled: random.sample(
                [pid for pid in pool if pid not in enrolled], k=min(len(pool), random.randint(0, 5))
            ))(inmate_pool, (lambda pool: (random.sample(pool, k=min(len(pool), random.randint(5, 20), capacity))))(inmate_pool)),
            start_date=start.strftime('%Y-%m-%d'),
            end_date=end.strftime('%Y-%m-%d'),
            active=random.random() < 0.7,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )
    def generate_fire_personnel(self):
        first_name = fake.first_name()
        last_name = fake.last_name()
        full_name = f"{first_name} {last_name}"
        username, email = self._generate_fire_email_username(first_name, last_name)
        employee_id = f"FD{random.randint(1000,9999)}"
        # Assign realistic station numbers based on Seattle Fire Department districts
        # Seattle has stations 1-37, with specific districts
        station_num = random.randint(1, 37)
        station = f"STATION_{station_num:02d}"
        
        # Assign unit types and numbers based on station
        if station_num <= 10:  # Downtown/Central district
            unit_type = random.choice(['ENGINE', 'LADDER', 'MEDIC', 'AID'])
            unit_num = station_num
        elif station_num <= 20:  # North district
            unit_type = random.choice(['ENGINE', 'MEDIC', 'AID'])
            unit_num = station_num
        elif station_num <= 30:  # South district
            unit_type = random.choice(['ENGINE', 'LADDER', 'MEDIC'])
            unit_num = station_num
        else:  # East district
            unit_type = random.choice(['ENGINE', 'MEDIC', 'AID'])
            unit_num = station_num
        
        unit = f"{unit_type} {unit_num:03d}"
        group = random.choice(['OPERATIONS', 'TRAINING', 'PREVENTION', 'ADMIN'])
        role = random.choice(['FIREFIGHTER','PARAMEDIC','LIEUTENANT','CAPTAIN','BATTALION_CHIEF'])
        hire_date = fake.date_between(start_date='-25y', end_date='-1y').strftime('%Y-%m-%d')
        phone_mobile = self._format_seattle_phone()
        phone_work = self._format_seattle_phone()
        shift = random.choice(['A','B','C'])
        certs_pool = ['EMT','PARAMEDIC','HAZMAT','TECH_REScue','FIRE_INSTRUCTOR','FIRE_INVESTIGATOR']
        certifications = sorted(set(random.choices(certs_pool, k=random.randint(1,3))))
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return FirePersonnel(
            personnel_id=f"FP-{uuid.uuid4()}",
            employee_id=employee_id,
            full_name=full_name,
            first_name=first_name,
            last_name=last_name,
            group=group,
            role=role,
            phone_mobile=phone_mobile,
            phone_work=phone_work,
            email=email,
            username=username,
            user_id=username,
            date_of_hire=hire_date,
            station=station,
            unit=unit,
            shift=shift,
            active=True,
            certifications=certifications,
            created_date=now,
            updated_date=now,
        )

    def generate_fire_shift(self):
        """Generate a Fire shift with staffing and scheduling details"""
        unit_type = random.choice(['ENGINE', 'LADDER', 'MEDIC', 'AID', 'BATTALION'])
        unit_num = random.randint(1, 37)
        unit_name = f"{unit_type} {unit_num:03d}"
        code_map = {'ENGINE': 'E', 'LADDER': 'L', 'MEDIC': 'M', 'AID': 'A', 'BATTALION': 'B'}
        unit_code = f"{code_map[unit_type]}{unit_num:03d}"

        address, city, state, zip_code = self._generate_king_county_address()
        location = f"{address}, {city}, {state} {zip_code}"
        station = f"STATION_{random.randint(1,37)}"

        num_staff = random.randint(3, 6) if unit_type in ['ENGINE','LADDER'] else random.randint(2, 4)
        if getattr(self, 'fire_personnel', None):
            staff = random.sample(self.fire_personnel, k=min(len(self.fire_personnel), num_staff))
            employee_ids = [p.employee_id for p in staff]
            employee_names = [p.full_name for p in staff]
        else:
            employee_ids = [str(random.randint(4000, 4999)) for _ in range(num_staff)]
            employee_names = [f"{fake.first_name()} {fake.last_name()}" for _ in range(num_staff)]

        qualifiers_pool = ['PARAMEDIC','FIREFIGHTER','COMPANY_OFFICER']
        qualifiers = sorted(set(random.choices(qualifiers_pool, k=random.randint(1, 3))))
        work_type = random.choice(['REGULAR', 'OVERTIME'])
        scheduled_by = [f"{fake.first_name()} {fake.last_name()}" for _ in range(random.randint(1, 2))]

        start_dt = fake.date_time_between(start_date='-14d', end_date='+14d')
        shift_hours = random.choice([8, 10, 12, 24])
        end_dt = start_dt + timedelta(hours=shift_hours)
        length_minutes = int((end_dt - start_dt).total_seconds() // 60)

        return FireShift(
            shift_id=str(uuid.uuid4()),
            unit_name=unit_name,
            unit_code=unit_code,
            station=station,
            location=location,
            employee_ids=employee_ids,
            employee_names=employee_names,
            qualifiers=qualifiers,
            work_type=work_type,
            scheduled_by=scheduled_by,
            start_datetime=start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            end_datetime=end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            length_minutes=length_minutes,
        )

    def _nibrs_pick(self):
        table = {
            'ROBBERY': ('120','Robbery','Property','Armed robbery'),
            'BURGLARY': ('220','Burglary/Breaking and Entering','Property','Residential burglary'),
            'ASSAULT': ('13A','Aggravated Assault','Person','Aggravated assault'),
            'THEFT': ('23A','Larceny/Theft','Property','Theft of property'),
            'DRUG_VIOLATION': ('35A','Drug/Narcotic Violations','Society','Possession of controlled substance'),
            'VEHICLE_THEFT': ('240','Motor Vehicle Theft','Property','Vehicle theft')
        }
        k = random.choice(list(table.keys()))
        code, group, against, summary = table[k]
        return k, code, group, against, summary

    def generate_offense(
        self,
        parent_type,
        parent_id,
        person_id=None,
        agency='KCSO',
        nibrs=None,                # tuple: (offense, code, group, against)
        start=None, end=None,      # datetimes
        description=None,
        severity=None,
        statute=None
    ):
        if nibrs is None:
            k, code, group, against, summary = self._nibrs_pick()
        else:
            k, code, group, against = nibrs
            summary = description or self._nibrs_pick()[4]

        start = start or fake.date_time_between(start_date='-30d', end_date='-1d')
        end = end or (start + timedelta(hours=random.randint(1,8)))
        return Offense(
            offense_id=f"OFF-{uuid.uuid4()}",
            parent_type=parent_type,
            parent_id=parent_id,
            person_id=person_id,
            nibrs_offense=k,
            nibrs_code=code,
            nibrs_group_name=group,
            nibrs_crime_against=against,
            statute=statute or f"RCW {random.randint(9,69)}.{random.randint(10,999)}",
            description=summary,
            severity=severity or random.choice(['INFRACTION','MISDEMEANOR','FELONY']),
            offense_start_datetime=start.strftime('%Y-%m-%d %H:%M:%S'),
            offense_end_datetime=end.strftime('%Y-%m-%d %H:%M:%S'),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency,
            weapon_involved=random.random() < 0.2,
            victim_count=random.randint(0, 3),
            loss_value=round(random.uniform(0, 5000), 2),
            attempt_flag=random.random() < 0.1
        )

    def generate_citation(self, person, cad_incident=None, vehicle=None, officer_id=None, agency='KCSO'):
        address, city, state, zip_code = (self._generate_king_county_address()
                                          if hasattr(self,'_generate_king_county_address')
                                          else (f"{random.randint(100,9999)} Main St",'Seattle','WA','98101'))
        lat, lon = random.uniform(47.5,47.75), random.uniform(-122.45,-122.1)
        n1 = self.generate_offense('CITATION', 'PENDING', person.person_id, agency)  # temp parent_id, fix after creating
        speed_limit = random.choice([25,30,35,40,45,50,60])
        speed = speed_limit + random.randint(1, 35) if random.random()<0.6 else None
        radar_device_id = f"RAD-{random.randint(1000,9999)}" if speed else None
        
        # Generate citation-specific data
        citation_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        date_entered = citation_datetime + timedelta(hours=random.randint(1, 24))
        
        # Generate officer name
        officer_name = f"{fake.first_name()} {fake.last_name()}"
        
        # Generate person details
        first_name = person.first_name
        middle_name = fake.first_name() if random.random() < 0.3 else None
        last_name = person.last_name
        person_cited_name = f"{first_name} {middle_name} {last_name}" if middle_name else f"{first_name} {last_name}"
        
        # Generate vehicle details if vehicle is provided
        if vehicle:
            license_plate = vehicle.license_plate
            vehicle_year = vehicle.year
            vehicle_color = vehicle.color
            vehicle_make = vehicle.make
            vehicle_model = vehicle.model
        else:
            license_plate = f"{random.choice(['ABC', 'DEF', 'GHI', 'JKL', 'MNO'])}{random.randint(1000, 9999)}"
            vehicle_year = random.randint(2010, 2024)
            vehicle_color = random.choice(['White', 'Black', 'Silver', 'Red', 'Blue', 'Gray', 'Green'])
            vehicle_make = random.choice(['Toyota', 'Honda', 'Ford', 'Chevrolet', 'Nissan', 'BMW', 'Mercedes'])
            vehicle_model = random.choice(['Camry', 'Accord', 'F-150', 'Silverado', 'Altima', '3 Series', 'C-Class'])
        
        # Generate citation type and corresponding violation with realistic fine amounts
        citation_type_fine_mapping = {
            'TRAFFIC': {
                'violations': [
                    # Speeding violations (realistic Washington state amounts)
                    ("RCW 46.61.400 - Speed 1-10 mph over limit", 150.0),
                    ("RCW 46.61.400 - Speed 11-20 mph over limit", 300.0),
                    ("RCW 46.61.400 - Speed 21+ mph over limit", 450.0),
                    ("RCW 46.61.400 - Speed in school zone", 350.0),
                    ("RCW 46.61.400 - Speed in construction zone", 400.0),
                    
                    # Traffic control violations
                    ("RCW 46.61.055 - Red light violation", 200.0),
                    ("RCW 46.61.190 - Stop sign violation", 150.0),
                    ("RCW 46.61.305 - Yield sign violation", 100.0),
                    ("RCW 46.61.050 - Disobey traffic control device", 220.0),
                    
                    # Lane and turning violations
                    ("RCW 46.61.140 - Unsafe lane change", 150.0),
                    ("RCW 46.61.290 - Improper turn", 160.0),
                    ("RCW 46.61.310 - Turn signal violation", 100.0),
                    ("RCW 46.61.100 - Following too closely", 180.0),
                    
                    # Vehicle equipment violations
                    ("RCW 46.37.030 - No seatbelt", 75.0),
                    ("RCW 46.61.667 - Cell phone while driving", 150.0),
                    ("RCW 46.61.668 - Texting while driving", 200.0),
                    
                    # Right of way violations
                    ("RCW 46.61.235 - Pedestrian right of way", 250.0),
                    ("RCW 46.61.190 - Emergency vehicle right of way", 300.0),
                    
                    # Registration and insurance
                    ("RCW 46.16A.030 - Expired registration", 125.0),
                    ("RCW 46.30.020 - No insurance", 250.0),
                    
                    # Serious violations
                    ("RCW 46.61.502 - DUI", 1000.0),
                    ("RCW 46.61.500 - Reckless driving", 600.0),
                    ("RCW 46.61.024 - Negligent driving", 400.0)
                ]
            },
            'PARKING': {
                'violations': [
                    # Parking violations (realistic King County amounts)
                    ("RCW 46.61.570 - Meter expired", 50.0),
                    ("RCW 46.61.570 - No parking zone", 75.0),
                    ("RCW 46.61.570 - Time limit exceeded", 50.0),
                    ("RCW 46.61.570 - Handicap parking violation", 500.0),
                    ("RCW 46.61.570 - Fire hydrant violation", 150.0),
                    ("RCW 46.61.570 - Bus zone parking", 100.0),
                    ("RCW 46.61.570 - Loading zone violation", 75.0),
                    ("RCW 46.61.570 - Street sweeping violation", 75.0),
                    ("RCW 46.61.570 - Blocking driveway", 100.0),
                    ("RCW 46.61.570 - Double parking", 100.0),
                    ("VC 22528 - Loading zone", 85.0),
                    ("VC 22529 - Blocking driveway", 95.0),
                    ("VC 22530 - Too close to intersection", 110.0)
                ]
            },
            'CRIMINAL': {
                'violations': [
                    ("PC 148 - Resisting arrest", 1000.0),
                    ("PC 415 - Disturbing the peace", 500.0),
                    ("PC 602 - Trespassing", 400.0),
                    ("PC 647 - Disorderly conduct", 600.0),
                    ("PC 484 - Petty theft", 800.0),
                    ("PC 459 - Burglary", 2000.0),
                    ("PC 245 - Assault with deadly weapon", 3000.0),
                    ("PC 273.5 - Domestic violence", 1500.0),
                    ("PC 11350 - Drug possession", 1000.0),
                    ("PC 11351 - Drug sales", 5000.0),
                    ("PC 12020 - Weapon violation", 1200.0),
                    ("PC 647F - Public intoxication", 300.0)
                ]
            },
            'MUNICIPAL': {
                'violations': [
                    ("MC 8.12.010 - Noise violation", 200.0),
                    ("MC 8.12.020 - Littering", 150.0),
                    ("MC 8.12.030 - Graffiti", 300.0),
                    ("MC 8.12.040 - Public urination", 250.0),
                    ("MC 8.12.050 - Loitering", 100.0),
                    ("MC 8.12.060 - Panhandling", 150.0),
                    ("MC 8.12.070 - Public drinking", 200.0),
                    ("MC 8.12.080 - Curfew violation", 100.0),
                    ("MC 8.12.090 - Skateboarding", 75.0),
                    ("MC 8.12.100 - Dog off leash", 125.0)
                ]
            }
        }
        
        # Select citation type and corresponding violation
        citation_type = random.choice(list(citation_type_fine_mapping.keys()))
        violation_summary, fine = random.choice(citation_type_fine_mapping[citation_type]['violations'])
        
        # Calculate payment status and balance
        payment_status = random.choice(['UNPAID','PARTIAL','PAID'])
        balance_due = 0.0 if payment_status=='PAID' else (fine if payment_status=='UNPAID' else round(fine*random.uniform(0.2,0.8),2))
        
        citation = Citation(
            citation_id=f"CITE-{uuid.uuid4()}",
            citation_number=f"{random.randint(100000,999999)}",
            source=random.choice(["Mark43 RMS", "Spillman", "New World Systems", "Tyler Technologies"]),
            agency=agency,
            citation_type=citation_type,
            is_commercial_driver=random.random() < 0.15,
            violation_summary=violation_summary,
            citation_datetime=citation_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            date_entered=date_entered.strftime('%Y-%m-%d %H:%M:%S'),
            address=address,
            beat=f"Beat {random.randint(1, 20)}",
            reporting_district=f"RD{random.randint(100, 999)}",
            officer=officer_name,
            person_cited_name=person_cited_name,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            date_of_birth=person.date_of_birth,
            sex=person.sex,
            race=person.race,
            drivers_license_number=person.drivers_license_number,
            dl_state=person.drivers_license_state,
            residential_address=person.address,
            license_plate_number=license_plate,
            vehicle_year=vehicle_year,
            vehicle_color=vehicle_color,
            vehicle_make=vehicle_make,
            vehicle_model=vehicle_model,
            approximate_speed=speed,
            speed_limit=speed_limit,
            is_confidential=random.random() < 0.05,
            # Legacy fields for backward compatibility
            person_id=person.person_id,
            vehicle_id=vehicle.vehicle_id if vehicle else None,
            officer_id=officer_id,
            cad_incident_id=cad_incident.cad_id if cad_incident else None,
            police_incident_id=None,
            city=city, 
            state=state, 
            zip_code=zip_code,
            latitude=lat, 
            longitude=lon,
            violations=[{
                'code': violation_summary.split(' - ')[0],  # Extract code from violation summary
                'description': violation_summary.split(' - ')[1],  # Extract description from violation summary
                'statute': violation_summary.split(' - ')[0],  # Use the same code as statute
                'fine_amount': fine
            }],
            total_fine_amount=fine,
            court_name='Seattle Municipal Court',
            court_address='600 5th Ave, Seattle, WA 98104',
            court_date=fake.date_between(start_date='+7d', end_date='+60d').strftime('%Y-%m-%d'),
            disposition=random.choice(['ISSUED','PAID','DISMISSED']),
            status=random.choice(['OPEN','PENDING_COURT','CLOSED']),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency,
            speed=speed,
            radar_device_id=radar_device_id,
            payment_status=payment_status,
            balance_due=balance_due,
            # Case linking fields
            related_case_id=None,  # Most citations don't lead to cases
            case_number=None
        )
        # finalize offense parent_id to this citation
        n1.parent_id = citation.citation_id
        self.offenses.append(n1)
        return citation

    def generate_field_interview(self, person, cad_incident=None, vehicle=None, officer_id=None, agency='KCSO'):
        address, city, state, zip_code = (
            self._generate_king_county_address()
            if hasattr(self,'_generate_king_county_address')
            else (f"{random.randint(100,9999)} Main St",'Seattle','WA','98101')
        )
        
        # Generate FI-specific data
        fi_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        
        # Generate officer name
        officer_name = f"{fake.first_name()} {fake.last_name()}"
        
        # Generate person's full name
        full_name = f"{person.first_name} {person.last_name}"
        
        # Generate contact reasons and types
        contact_reasons = [
            "415 LOITERERS",
            "647F DISTURBING PEACE",
            "602 TRESPASSING",
            "647A DISORDERLY CONDUCT",
            "647B PROSTITUTION",
            "647C VAGRANCY",
            "647D PUBLIC INTOXICATION",
            "647E PEEPING TOM",
            "647G ANNOYING PHONE CALLS",
            "647H HARASSMENT",
            "647I LEWD CONDUCT",
            "647J PROSTITUTION SOLICITATION",
            "647K LOITERING FOR PROSTITUTION",
            "647L PROSTITUTION IN VEHICLE",
            "647M PROSTITUTION NEAR SCHOOL",
            "647N PROSTITUTION NEAR CHURCH",
            "647O PROSTITUTION NEAR PARK",
            "647P PROSTITUTION NEAR RESIDENCE",
            "647Q PROSTITUTION NEAR BUSINESS",
            "647R PROSTITUTION NEAR PUBLIC BUILDING"
        ]
        
        contact_types = [
            "pedestrian stop",
            "vehicle stop",
            "consensual encounter",
            "welfare check",
            "suspicious activity",
            "trespassing investigation",
            "loitering investigation",
            "public intoxication",
            "disturbance call",
            "gang activity",
            "drug activity",
            "prostitution investigation",
            "vandalism investigation",
            "theft investigation",
            "assault investigation"
        ]
        
        # Generate case and incident numbers if available
        case_number = None
        incident_number = None
        if cad_incident:
            incident_number = cad_incident.cad_id
            # Sometimes link to a case
            if random.random() < 0.3 and hasattr(self, 'cases') and self.cases:
                case = random.choice(self.cases)
                case_number = case.case_number
        
        return FieldInterview(
            fi_id=f"FI-{uuid.uuid4()}",
            source=random.choice(["Mark43 RMS", "Spillman", "New World Systems", "Tyler Technologies"]),
            agency=agency,
            fi_number=f"FI{random.randint(100000, 999999)}",
            case_number=case_number,
            incident_number=incident_number,
            contact_reason=random.choice(contact_reasons),
            contact_type=random.choice(contact_types),
            fi_datetime=fi_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            fi_location=address,
            beat=f"Beat {random.randint(1, 20)}",
            reporting_district=f"RD{random.randint(100, 999)}",
            full_name=full_name,
            officer=officer_name,
            is_confidential=random.random() < 0.05,
            # Legacy fields for backward compatibility
            person_id=person.person_id,
            officer_id=officer_id,
            vehicle_id=(vehicle.vehicle_id if vehicle else None),
            reason=random.choice(['SUSPICIOUS_ACTIVITY','CONSENT_CONTACT','GANG_AFFILIATION','CURFEW']),
            outcome=random.choice(['WARNING','INFO_ONLY','CONSENT_SEARCH','NO_ACTION']),
            notes=random.choice(['No contraband found','Subject cooperative','Photo taken','Released at scene']),
            address=address, 
            city=city, 
            state=state, 
            zip_code=zip_code,
            latitude=random.uniform(47.5,47.75), 
            longitude=random.uniform(-122.45,-122.1),
            related_cad_id=(cad_incident.cad_id if cad_incident else None),
            related_incident_id=None,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency
        )

    def generate_case(self, agency='KCSO', cad_incident=None, arrest=None):
        """Generate a new Case with all required parameters"""
        # Generate timestamps
        reported_dt = fake.date_time_between(start_date='-30d', end_date='now')
        start_dt = reported_dt - timedelta(hours=random.randint(1, 12))
        end_dt = reported_dt + timedelta(hours=random.randint(1, 8))
        offense_start_dt = start_dt - timedelta(minutes=random.randint(0, 60))
        
        # Enhanced CAD incident to case type mapping for logical consistency
        cad_to_case_mapping = {
            # Police incidents with detailed offense descriptions
            'ASSAULT': {
                'case_types': ['Aggravated Assault', 'Simple Assault', 'Assault - Other/Unknown Weapon'],
                'narratives': ['Subject assaulted victim with hands/fists', 'Subject struck victim with weapon', 'Subject threatened victim with bodily harm']
            },
            'BURGLARY': {
                'case_types': ['Burglary', 'Burglary - Forcible Entry', 'Burglary - Unlawful Entry (No Force)', 'Burglary - Residential', 'Burglary - Non-Residential'],
                'narratives': ['Subject unlawfully entered residence', 'Subject forced entry into business', 'Subject stole property from vehicle']
            },
            'THEFT': {
                'case_types': ['Larceny-Theft', 'Larceny - From Motor Vehicle', 'Larceny - Shoplifting', 'Larceny - All Other'],
                'narratives': ['Subject stole merchandise from store', 'Subject took property from vehicle', 'Subject removed items without permission']
            },
            'ROBBERY': {
                'case_types': ['Robbery'],
                'narratives': ['Subject used force/threat to take property', 'Subject robbed victim at gunpoint', 'Subject demanded money under threat']
            },
            'DOMESTIC_VIOLENCE': {
                'case_types': ['Aggravated Assault', 'Simple Assault', 'Domestic Violence'],
                'narratives': ['Domestic violence incident between partners', 'Subject assaulted family member', 'Subject threatened intimate partner']
            },
            'TRAFFIC_ACCIDENT': {
                'case_types': ['Traffic Violation', 'Reckless Driving', 'DUI'],
                'narratives': ['Subject caused traffic collision', 'Subject drove under influence', 'Subject operated vehicle recklessly']
            },
            'SUSPICIOUS_ACTIVITY': {
                'case_types': ['Suspicious Activity', 'Trespassing', 'Loitering'],
                'narratives': ['Subject observed in suspicious circumstances', 'Subject trespassed on private property', 'Subject loitering in prohibited area']
            },
            'DISTURBANCE': {
                'case_types': ['Disorderly Conduct', 'Disturbing the Peace', 'Public Intoxication'],
                'narratives': ['Subject caused public disturbance', 'Subject engaged in disorderly conduct', 'Subject intoxicated in public']
            },
            
            # Fire incidents
            'STRUCTURE_FIRE': {
                'case_types': ['Arson', 'Arson - Structure'],
                'narratives': ['Subject intentionally set fire to structure', 'Arson investigation - structure fire']
            },
            'VEHICLE_FIRE': {
                'case_types': ['Arson', 'Arson - Mobile Property'],
                'narratives': ['Subject set fire to vehicle', 'Arson investigation - vehicle fire']
            },
            'ALARM_ACTIVATION': {
                'case_types': ['False Alarm', 'Burglary - Attempted'],
                'narratives': ['False alarm activation', 'Attempted burglary - alarm activated']
            },
            'GAS_LEAK': {
                'case_types': ['Hazardous Materials', 'Public Safety'],
                'narratives': ['Gas leak reported - public safety response', 'Hazardous materials incident']
            },
            
            # EMS incidents (typically don't generate cases, but some might)
            'MEDICAL_EMERGENCY': {
                'case_types': ['Public Safety', 'Welfare Check'],
                'narratives': ['Medical emergency response', 'Welfare check conducted']
            },
            'OVERDOSE': {
                'case_types': ['Drug/Narcotic Violations - Possession', 'Public Safety'],
                'narratives': ['Drug overdose - medical response', 'Subject found unconscious - possible overdose']
            },
            'TRAUMA': {
                'case_types': ['Assault', 'Traffic Violation', 'Public Safety'],
                'narratives': ['Trauma incident - medical response', 'Subject injured - trauma response']
            }
        }
        
        # Determine case details based on CAD incident if provided
        if cad_incident:
            cad_type = cad_incident.incident_type_code
            # Get appropriate case types and narratives for this CAD incident
            if cad_type in cad_to_case_mapping:
                possible_case_types = cad_to_case_mapping[cad_type]['case_types']
                bcs_description = random.choice(possible_case_types)
                synopsis = random.choice(cad_to_case_mapping[cad_type]['narratives'])
            else:
                bcs_description = 'All Other Offenses'
                synopsis = f"Case generated from CAD incident type: {cad_type}"
            
            # Get corresponding BCS code
            bcs_code = None
            for code, desc in BCS_CODES.items():
                if desc == bcs_description:
                    bcs_code = code
                    break
            
            # If no exact match, use a related code
            if not bcs_code:
                if 'Assault' in bcs_description:
                    bcs_code = random.choice(['04', '13'])  # Aggravated or Simple Assault
                elif 'Burglary' in bcs_description:
                    bcs_code = '05'
                elif 'Theft' in bcs_description or 'Larceny' in bcs_description:
                    bcs_code = '06'
                elif 'Robbery' in bcs_description:
                    bcs_code = '03'
                elif 'Arson' in bcs_description:
                    bcs_code = '08'
                else:
                    bcs_code = random.choice(list(BCS_CODES.keys()))
        else:
            # Generate random case details if no CAD incident
            bcs_code = random.choice(list(BCS_CODES.keys()))
            bcs_description = BCS_CODES[bcs_code]
            synopsis = f"Case generated without specific CAD incident - {bcs_description}"
        
        # Generate case status and related dates
        case_status = random.choice(['OPEN', 'INVESTIGATING', 'PENDING_APPROVAL', 'APPROVED', 'CLOSED'])
        is_reviewed = random.random() < 0.7
        reviewed_dt = reported_dt + timedelta(days=random.randint(1, 14)) if is_reviewed else None
        
        # Generate approval info
        approved_dt = reported_dt + timedelta(hours=random.randint(2, 48))
        approved_by = f"{fake.first_name()} {fake.last_name()}"
        
        # Generate NIBRS code
        nibrs_code = f"{random.randint(10, 99)}.{random.randint(1, 99):02d}"
        
        # Generate offense code
        offense_code = f"OC{random.randint(1000, 9999)}"
        
        # Generate zip code
        zip_code = random.choice(['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109', '98112', '98115', '98116', '98117', '98118', '98119', '98121', '98122', '98125', '98126', '98133', '98134', '98136', '98144', '98146', '98154', '98155', '98158', '98164', '98166', '98168', '98174', '98177', '98178', '98188', '98195', '98199'])
        
        # Select a suspect person for jacket_id - use arrest person if available
        if arrest:
            jacket_id = arrest.person_id
        else:
            jacket_id = random.choice(self.persons).person_id if self.persons else str(uuid.uuid4())
        
        # Generate synopsis based on BCS description
        synopsis_templates = {
            # Part I Offenses
            'Criminal Homicide': 'Victim found deceased with apparent gunshot wounds. Investigation ongoing.',
            'Forcible Rape': 'Victim reported sexual assault. Suspect identified and investigation proceeding.',
            'Robbery': 'Victim robbed at gunpoint. Suspect fled scene, investigation active.',
            'Aggravated Assault': 'Victim assaulted with weapon. Suspect arrested at scene.',
            'Burglary': 'Residence broken into, property stolen. Investigation ongoing.',
            'Larceny-Theft': 'Property stolen from victim. Suspect identified through surveillance.',
            'Motor Vehicle Theft': 'Vehicle reported stolen. Investigation in progress.',
            'Arson': 'Structure fire determined to be arson. Investigation active.',
            
            # Part II Offenses
            'Simple Assault': 'Physical altercation between parties. Case under investigation.',
            'Forgery and Counterfeiting': 'Forged documents discovered. Investigation ongoing.',
            'Fraud': 'Victim defrauded of money. Investigation ongoing.',
            'Embezzlement': 'Funds misappropriated by employee. Investigation active.',
            'Stolen Property; Buying, Receiving, Possessing': 'Suspect in possession of stolen property. Investigation ongoing.',
            'Vandalism': 'Property damaged by unknown suspect. Investigation active.',
            'Weapons; Carrying, Possessing, etc.': 'Suspect arrested for illegal weapon possession.',
            'Prostitution and Commercialized Vice': 'Prostitution activity reported. Investigation ongoing.',
            'Sex Offenses (except forcible rape and prostitution)': 'Sexual offense reported. Investigation active.',
            'Drug Abuse Violations': 'Suspect arrested for drug possession. Evidence collected.',
            'Gambling': 'Illegal gambling operation discovered. Investigation ongoing.',
            'Offenses Against Family and Children': 'Family violence incident reported. Investigation active.',
            'Driving Under the Influence': 'Driver arrested for DUI. Blood alcohol test pending.',
            'Liquor Laws': 'Liquor law violation reported. Investigation ongoing.',
            'Drunkenness': 'Public intoxication incident. Suspect cited.',
            'Disorderly Conduct': 'Suspect arrested for disorderly conduct and public disturbance.',
            'Vagrancy': 'Vagrancy violation reported. Suspect cited.',
            'All Other Offenses': 'Miscellaneous offense reported. Investigation ongoing.',
            'Suspicion': 'Suspicious activity reported. Investigation ongoing.',
            'Curfew and Loitering Law Violations': 'Curfew violation reported. Suspect cited.',
            'Runaway': 'Juvenile runaway reported. Investigation ongoing.',
            
            # Additional BCS Codes
            'Bad Checks': 'Bad check passed to business. Investigation ongoing.',
            'Bribery': 'Bribery attempt reported. Investigation active.',
            'Extortion': 'Extortion attempt reported. Investigation ongoing.',
            'Kidnapping/Abduction': 'Person reported missing/abducted. Investigation active.',
            'Negligent Manslaughter': 'Negligent death reported. Investigation ongoing.',
            'Non-Negligent Manslaughter': 'Manslaughter incident reported. Investigation active.',
            'Rape by Force': 'Forcible rape reported. Investigation ongoing.',
            'Statutory Rape': 'Statutory rape reported. Investigation active.',
            'Assault with Deadly Weapon': 'Assault with weapon reported. Investigation ongoing.',
            'Assault with Intent to Kill': 'Attempted murder reported. Investigation active.',
            'Assault with Intent to Rape': 'Attempted rape reported. Investigation ongoing.',
            'Assault with Intent to Rob': 'Attempted robbery reported. Investigation active.',
            'Assault with Intent to Commit Other Felony': 'Assault with intent reported. Investigation ongoing.',
            'Assault on Police Officer': 'Officer assaulted in line of duty. Investigation active.',
            'Assault on Firefighter': 'Firefighter assaulted. Investigation ongoing.',
            'Assault on Emergency Medical Personnel': 'EMS personnel assaulted. Investigation active.',
            'Assault on School Employee': 'School employee assaulted. Investigation ongoing.',
            'Assault on Transit Employee': 'Transit employee assaulted. Investigation active.',
            'Assault on Other Public Employee': 'Public employee assaulted. Investigation ongoing.',
            'Assault on Family Member': 'Domestic violence incident. Investigation active.',
            'Assault on Child': 'Child abuse reported. Investigation ongoing.',
            'Assault on Elderly': 'Elder abuse reported. Investigation active.',
            'Assault on Disabled Person': 'Assault on disabled person reported. Investigation ongoing.',
            'Assault with Firearm': 'Shooting incident reported. Investigation active.',
            'Assault with Knife/Cutting Instrument': 'Stabbing incident reported. Investigation ongoing.',
            'Assault with Other Dangerous Weapon': 'Assault with weapon reported. Investigation active.',
            'Assault with Hands/Fists/Feet': 'Physical assault reported. Investigation ongoing.',
            'Assault with Other Object': 'Assault with object reported. Investigation active.',
            'Assault - Other/Unknown Weapon': 'Assault reported. Investigation ongoing.',
            'Burglary - Forcible Entry': 'Forced entry burglary reported. Investigation active.',
            'Burglary - Unlawful Entry (No Force)': 'Unlawful entry burglary reported. Investigation ongoing.',
            'Burglary - Attempted Forcible Entry': 'Attempted burglary reported. Investigation active.',
            'Burglary - Residential': 'Home burglary reported. Investigation ongoing.',
            'Burglary - Non-Residential': 'Business burglary reported. Investigation active.',
            'Burglary - Attempted': 'Attempted burglary reported. Investigation ongoing.',
            'Larceny - From Motor Vehicle': 'Vehicle break-in reported. Investigation ongoing.',
            'Larceny - Motor Vehicle Parts/Accessories': 'Vehicle parts stolen. Investigation active.',
            'Larceny - Bicycles': 'Bicycle theft reported. Investigation ongoing.',
            'Larceny - From Building': 'Building theft reported. Investigation active.',
            'Larceny - From Coin-Operated Machine': 'Vending machine theft reported. Investigation ongoing.',
            'Larceny - Pocket Picking': 'Pickpocketing reported. Investigation active.',
            'Larceny - Purse Snatching': 'Purse snatching reported. Investigation ongoing.',
            'Larceny - Shoplifting': 'Shoplifting incident reported. Investigation active.',
            'Larceny - All Other': 'Theft reported. Investigation ongoing.',
            'Motor Vehicle Theft - Auto': 'Auto theft reported. Investigation active.',
            'Motor Vehicle Theft - Truck/Bus': 'Truck/bus theft reported. Investigation ongoing.',
            'Motor Vehicle Theft - Other Vehicle': 'Vehicle theft reported. Investigation active.',
            'Motor Vehicle Theft - Attempted': 'Attempted vehicle theft reported. Investigation ongoing.',
            'Arson - Structure': 'Structure arson reported. Investigation active.',
            'Arson - Mobile Property': 'Vehicle arson reported. Investigation ongoing.',
            'Arson - Other Property': 'Property arson reported. Investigation active.',
            'Arson - Attempted': 'Attempted arson reported. Investigation ongoing.',
            'Drug/Narcotic Violations - Sale/Manufacturing': 'Drug dealing operation discovered. Investigation active.',
            'Drug/Narcotic Violations - Possession': 'Drug possession arrest. Investigation ongoing.',
            'Drug/Narcotic Violations - Other': 'Drug violation reported. Investigation active.',
            'Weapons Violations - Carrying/Possessing': 'Illegal weapon possession. Investigation ongoing.',
            'Weapons Violations - Sale/Manufacturing': 'Illegal weapon sales. Investigation active.',
            'Weapons Violations - Other': 'Weapon violation reported. Investigation ongoing.',
            'Prostitution - Assisting/Promoting': 'Prostitution promotion reported. Investigation ongoing.',
            'Prostitution - Purchasing Prostitution': 'Prostitution solicitation reported. Investigation active.',
            'Prostitution - Other': 'Prostitution activity reported. Investigation ongoing.',
            'Sex Offenses - Forcible': 'Forcible sex offense reported. Investigation active.',
            'Sex Offenses - Non-Forcible': 'Non-forcible sex offense reported. Investigation ongoing.',
            'Sex Offenses - Other': 'Sex offense reported. Investigation active.',
            'Gambling - Bookmaking': 'Illegal bookmaking operation. Investigation active.',
            'Gambling - Numbers/Lottery': 'Illegal lottery operation. Investigation ongoing.',
            'Gambling - Other': 'Illegal gambling reported. Investigation active.',
            'All Other Offenses': 'Miscellaneous offense reported. Investigation ongoing.'
        }
        
        synopsis = synopsis_templates.get(bcs_description, f"Case involving {bcs_description.lower()}. Investigation ongoing.")
        
        # Generate arrest date if case is closed
        arrest_dt = None
        closed_date = None
        if case_status == 'CLOSED' and random.random() < 0.8:
            arrest_dt = reported_dt + timedelta(hours=random.randint(1, 72))
            closed_date = arrest_dt + timedelta(days=random.randint(1, 30))
        
        # Generate threat to life indicator
        threat_to_life = bcs_code in ['01', '02', '03', '04', '08'] or random.random() < 0.1
        
        # Generate unique case number with agency prefix
        case_number = f"KC-24-{random.randint(10000, 99999)}"
        
        case = Case(
            case_id=f"CASE-{uuid.uuid4()}",
            case_number=case_number,
            agency=agency,
            case_status=case_status,
            reported_date_time=reported_dt.strftime('%Y-%m-%d %H:%M:%S'),
            start_date_time=start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            end_date_time=end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            offense_start_date_time=offense_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            zip=zip_code,
            offense_code=offense_code,
            nibrs_code=nibrs_code,
            approved_by=approved_by,
            approved_date_time=approved_dt.strftime('%Y-%m-%d %H:%M:%S'),
            is_reviewed=is_reviewed,
            reviewed_date_time=reviewed_dt.strftime('%Y-%m-%d %H:%M:%S') if reviewed_dt else None,
            jacket_id=jacket_id,
            is_restricted=random.random() < 0.15,  # 15% of cases are restricted
            bcs_code=bcs_code,
            bcs_description=bcs_description,
            synopsis=synopsis,
            closed_date=closed_date.strftime('%Y-%m-%d') if closed_date else None,
            arrest_date_time=arrest_dt.strftime('%Y-%m-%d %H:%M:%S') if arrest_dt else None,
            threat_to_life_indicator=threat_to_life,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            # CAD incident linking fields
            cad_incident_id=cad_incident.cad_id if cad_incident else None,
            cad_incident_type=cad_incident.incident_type_code if cad_incident else None,
            cad_incident_description=cad_incident.incident_type_description if cad_incident else None,
            # Arrest linking fields
            arrest_id=arrest.arrest_id if arrest else None,
            arrest_datetime=arrest.arrest_datetime if arrest else None
        )
        
        return case

    def generate_cad_incident(self, agency='KCSO', incident_type=None, is_self_initiated=None, person=None):
        """Generate a CAD incident with all required parameters"""
        # Generate timestamps
        call_dt = fake.date_time_between(start_date='-30d', end_date='now')
        call_entry_dt = call_dt - timedelta(minutes=random.randint(1, 10))
        
        # Generate address
        address, city, state, zip_code = self._generate_king_county_address()
        
        # Determine correct agency based on location
        correct_agency = self._determine_agency_by_location(city, zip_code)
        
        # Generate apartment number (30% chance)
        apartment_number = f"Apt {random.randint(1, 500)}" if random.random() < 0.3 else None
        
        # Generate reporting district and beat
        reporting_district = random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL', 'NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST'])
        beat = f"{random.choice(list('NSEWC'))}{random.randint(1, 9)}"
        
        # Generate incident type if not provided
        if incident_type is None:
            incident_types = {
                'EMS': ['MEDICAL_EMERGENCY', 'CARDIAC_ARREST', 'TRAUMA', 'FALLS', 'BREATHING_PROBLEMS', 'CHEST_PAIN', 'OVERDOSE', 'STROKE', 'SEIZURE', 'DIABETIC_EMERGENCY'],
                'FIRE': ['STRUCTURE_FIRE', 'VEHICLE_FIRE', 'GRASS_FIRE', 'ALARM_ACTIVATION', 'GAS_LEAK', 'HAZMAT', 'RESCUE', 'MUTUAL_AID'],
                'POLICE': ['ASSAULT', 'BURGLARY', 'THEFT', 'ROBBERY', 'DOMESTIC_VIOLENCE', 'TRAFFIC_ACCIDENT', 'SUSPICIOUS_ACTIVITY', 'DISTURBANCE']
            }
            
            # Weight towards EMS and Fire for CAD incidents
            incident_category = random.choices(['EMS', 'FIRE', 'POLICE'], weights=[50, 35, 15])[0]
            incident_type = random.choice(incident_types[incident_category])
        
        # Generate incident type description
        incident_descriptions = {
            'MEDICAL_EMERGENCY': 'Medical emergency response',
            'CARDIAC_ARREST': 'Cardiac arrest response',
            'TRAUMA': 'Trauma response',
            'FALLS': 'Fall victim response',
            'BREATHING_PROBLEMS': 'Respiratory emergency',
            'CHEST_PAIN': 'Chest pain response',
            'OVERDOSE': 'Drug overdose response',
            'STROKE': 'Stroke response',
            'SEIZURE': 'Seizure response',
            'DIABETIC_EMERGENCY': 'Diabetic emergency',
            'STRUCTURE_FIRE': 'Structure fire response',
            'VEHICLE_FIRE': 'Vehicle fire response',
            'GRASS_FIRE': 'Grass fire response',
            'ALARM_ACTIVATION': 'Fire alarm activation',
            'GAS_LEAK': 'Gas leak response',
            'HAZMAT': 'Hazardous materials response',
            'RESCUE': 'Rescue operation',
            'MUTUAL_AID': 'Mutual aid response',
            'ASSAULT': 'Assault response',
            'BURGLARY': 'Burglary response',
            'THEFT': 'Theft response',
            'ROBBERY': 'Robbery response',
            'DOMESTIC_VIOLENCE': 'Domestic violence response',
            'TRAFFIC_ACCIDENT': 'Traffic accident response',
            'SUSPICIOUS_ACTIVITY': 'Suspicious activity response',
            'DISTURBANCE': 'Disturbance response'
        }
        
        incident_type_description = incident_descriptions.get(incident_type, f'{incident_type} response')
        
        # Generate priority based on incident type
        high_priority_types = ['CARDIAC_ARREST', 'STRUCTURE_FIRE', 'ROBBERY', 'ASSAULT', 'DOMESTIC_VIOLENCE', 'TRAFFIC_ACCIDENT']
        medium_priority_types = ['MEDICAL_EMERGENCY', 'TRAUMA', 'BREATHING_PROBLEMS', 'CHEST_PAIN', 'BURGLARY', 'THEFT', 'VEHICLE_FIRE']
        
        if incident_type in high_priority_types:
            incident_priority = random.choices(['HIGH', 'MEDIUM'], weights=[70, 30])[0]
        elif incident_type in medium_priority_types:
            incident_priority = random.choices(['HIGH', 'MEDIUM', 'LOW'], weights=[20, 60, 20])[0]
        else:
            incident_priority = random.choices(['MEDIUM', 'LOW'], weights=[40, 60])[0]
        
        # Generate incident status
        incident_status = random.choices(['DISPATCHED', 'EN_ROUTE', 'ON_SCENE', 'COMPLETED', 'CANCELLED'], weights=[10, 15, 25, 45, 5])[0]
        
        # Generate CAD safety flag (higher for certain incident types)
        safety_flag_types = ['ASSAULT', 'ROBBERY', 'DOMESTIC_VIOLENCE', 'SUSPICIOUS_ACTIVITY', 'DISTURBANCE']
        cad_safety_flag = incident_type in safety_flag_types or random.random() < 0.15
        
        # Generate reporting party info (80% chance of having a reporting party)
        reporting_party_name = None
        reporting_party_phone = None
        if random.random() < 0.8:
            reporting_party_name = f"{fake.first_name()} {fake.last_name()}"
            reporting_party_phone = self._format_seattle_phone()
        
        # Generate self-initiated flag
        if is_self_initiated is None:
            # Higher chance for police incidents, lower for EMS/Fire
            if incident_type in ['ASSAULT', 'BURGLARY', 'THEFT', 'ROBBERY', 'DOMESTIC_VIOLENCE', 'SUSPICIOUS_ACTIVITY', 'DISTURBANCE']:
                is_self_initiated = random.random() < 0.3
            else:
                is_self_initiated = random.random() < 0.1
        
        # Generate CAD source link
        cad_source_link = f"CAD-{random.randint(100000, 999999)}"
        
        # Generate incident number
        incident_number = f"{agency}-{call_dt.year}{random.randint(100000, 999999)}"
        
        # Generate person information if person is provided
        person_id = person.person_id if person else None
        person_name = f"{person.first_name} {person.last_name}" if person else None
        person_phone = person.phone if person else None
        
        cad_incident = CADIncident(
            cad_id=f"CAD-{uuid.uuid4()}",
            agency=correct_agency,  # Use the correct agency based on location
            cad_source_link=cad_source_link,
            call_date_time=call_dt.strftime('%Y-%m-%d %H:%M:%S'),
            incident_number=incident_number,
            address=address,
            apartment_number=apartment_number,
            reporting_district=reporting_district,
            beat=beat,
            incident_type_code=incident_type,
            incident_type_description=incident_type_description,
            incident_priority=incident_priority,
            incident_status=incident_status,
            cad_safety_flag=cad_safety_flag,
            reporting_party_name=reporting_party_name,
            reporting_party_phone=reporting_party_phone,
            is_self_initiated=is_self_initiated,
            call_entry_date_time=call_entry_dt.strftime('%Y-%m-%d %H:%M:%S'),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            # Person linking fields
            person_id=person_id,
            person_name=person_name,
            person_phone=person_phone
        )
        
        return cad_incident

    def _choose_ems_incident_type(self, dt):
        base = {
            'SICK_PERSON': 14,
            'BREATHING_PROBLEMS': 12,
            'CHEST_PAIN': 11,
            'FALLS': 13,
            'ABDOMINAL_PAIN': 7,
            'SEIZURE': 6,
            'PSYCHIATRIC': 6,
            'OVERDOSE': 5,
            'TRAUMA': 6,
            'STROKE': 4,
            'UNCONSCIOUS_FAINTING': 5,
            'DIABETIC_PROBLEM': 4,
            'ALLERGIC_REACTION': 3,
            'PREGNANCY': 2,
            'CARDIAC_ARREST': 2,
            'POISONING': 2,
            'HEMORRHAGE': 2,
            'HEAT_EXPOSURE': 1,
            'COLD_EXPOSURE': 1,
        }
        hour = dt.hour
        wknd = dt.weekday() >= 5

        mult = {k: 1.0 for k in base}
        if 22 <= hour or hour < 5:
            for k in ['OVERDOSE','PSYCHIATRIC','UNCONSCIOUS_FAINTING','TRAUMA']:
                mult[k] *= 1.4
        if 7 <= hour <= 19:
            for k in ['FALLS','CHEST_PAIN','BREATHING_PROBLEMS','ABDOMINAL_PAIN']:
                mult[k] *= 1.25
        if wknd:
            for k in ['OVERDOSE','PSYCHIATRIC','TRAUMA','FALLS']:
                mult[k] *= 1.2

        types = list(base.keys())
        weights = [base[t] * mult[t] for t in types]
        return random.choices(types, weights=weights, k=1)[0]

# put near top-level
CITY_ZIPS = {
    'Seattle': ['98101','98102','98103','98104','98105','98106','98107','98108','98109','98112','98115','98116','98117','98118','98119','98121','98122','98125','98126','98133','98134','98136','98144','98146','98154','98164','98177','98178','98195'],
    'Redmond': ['98052','98053'],
    'Kirkland': ['98033','98034'],
    'Sammamish': ['98074','98075'],
    'Issaquah': ['98027','98029'],
    'Mercer Island': ['98040'],
    'Renton': ['98055','98056','98057','98058'],
    'Shoreline': ['98155','98177'],
    'Bothell': ['98011','98012','98021'],
    'Kenmore': ['98028'],
    'Newcastle': ['98056'],
    'SeaTac': ['98158','98188'],
    'Tukwila': ['98168'],
    'Woodinville': ['98072'],
    'Burien': ['98146'],
}

def main():
    """Main execution function"""
    print("Enhanced Multi-Agency Complex Synthetic Data Generator")
    print("Generating comprehensive data for King County Sheriff's Office (KCSO) jurisdiction")
    print(f"Configuration: {CONFIG}")
    
    generator = EnhancedDataGenerator()
    
    try:
        # Generate all data
        generator.generate_all_data()
        
        # Save in requested formats
        generator.save_data()
        
        print("\nEnhanced multi-agency synthetic data generation completed successfully!")
        print("Files created:")
        
        if 'json' in CONFIG['output_formats']:
            print("   • *.json files for each data type")
        if 'csv' in CONFIG['output_formats']:
            print("   • *.csv files for each data type")
        if 'sqlite' in CONFIG['output_formats']:
            print("   • multi_agency_data.db (SQLite database)")
        
        total = sum([
            len(generator.persons),
            len(generator.vehicles), 
            len(generator.properties),
            len(generator.police_incidents),
            len(generator.arrests),
            len(generator.jail_bookings),
            len(generator.fire_incidents),
            len(generator.ems_incidents)
        ])

        print(f"\nTotal records: {total:,}")
        
    except Exception as e:
        print(f"Error during generation: {str(e)}")
        raise

if __name__ == "__main__":
    main()