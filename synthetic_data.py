import json
import csv
import random
import sqlite3
from datetime import datetime, timedelta
from faker import Faker
import uuid
from collections import defaultdict, Counter
import time
from enum import Enum
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Set
import math

# Initialize Faker with multiple providers
fake = Faker('en_US')
Faker.seed(42)
random.seed(42)

# Add additional providers
fake.add_provider('faker.providers.automotive')
fake.add_provider('faker.providers.bank')

# Create locale-specific faker instances for consistent ethnicity-name mapping
faker_locales = {
    'WHITE': Faker('en_US'),  
    'BLACK': Faker('en_US'),  
    'HISPANIC': Faker('es_ES'), 
    'ASIAN': [Faker('ja_JP'), Faker('zh_CN'), Faker('ko_KR'), Faker('vi_VN'), Faker('en_IN')],
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
    'include_bellevue_districts': True,
    'include_king_county_unincorporated': True,
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

# Bellevue districts and neighborhoods
BELLEVUE_DISTRICTS = {
    'DOWNTOWN_BELLEVUE': {'population': 25000, 'crime_rate': 'MEDIUM', 'fire_stations': [1, 2]},
    'CROSSROADS': {'population': 20000, 'crime_rate': 'LOW', 'fire_stations': [3]},
    'LAKE_HILLS': {'population': 15000, 'crime_rate': 'LOW', 'fire_stations': [4]},
    'WILBURTON': {'population': 12000, 'crime_rate': 'LOW', 'fire_stations': [5]},
    'EASTGATE': {'population': 10000, 'crime_rate': 'LOW', 'fire_stations': [6]},
    'NEWPORT': {'population': 18000, 'crime_rate': 'LOW', 'fire_stations': [7]},
    'SOMERSET': {'population': 8000, 'crime_rate': 'LOW', 'fire_stations': [8]},
    'ENATAI': {'population': 6000, 'crime_rate': 'LOW', 'fire_stations': [9]},
    'BRIDLE_TRAILS': {'population': 5000, 'crime_rate': 'LOW', 'fire_stations': [10]},
}

# Enhanced incident types with Seattle/King County specific patterns
ENHANCED_INCIDENT_TYPES = {
    # Law Enforcement
    'TRAFFIC_VIOLATION': {'weight': 35, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'citation_likely': 0.8},
    'DOMESTIC_VIOLENCE': {'weight': 15, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.6},
    'THEFT': {'weight': 20, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'report_only': 0.7},
    'ASSAULT': {'weight': 10, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.5},
    'DUI': {'weight': 8, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.95},
    'DRUG_POSSESSION': {'weight': 12, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.4},
    'BURGLARY': {'weight': 5, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'report_investigation': 0.9},
    'ROBBERY': {'weight': 3, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.3},
    'HOMELESS_ENCAMPMENT': {'weight': 8, 'agencies': ['KCSO'], 'citation_likely': 0.2},
    'MENTAL_HEALTH_CRISIS': {'weight': 10, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'crisis_team': 0.4},
    'OVERDOSE': {'weight': 6, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'medical_response': 1.0},
    'GANG_ACTIVITY': {'weight': 4, 'agencies': ['KCSO'], 'investigation': 1.0},
    'HATE_CRIME': {'weight': 2, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'investigation': 1.0},
    'SEXUAL_ASSAULT': {'weight': 3, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'special_investigation': 1.0},
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

# Bellevue hospitals
BELLEVUE_HOSPITALS = {
    'OVERLAKE_MEDICAL_CENTER': {'type': 'TRAUMA_2', 'location': 'DOWNTOWN_BELLEVUE', 'capacity': 349},
    'EVERGREEN_HEALTH_MEDICAL_CENTER': {'type': 'ACUTE_CARE', 'location': 'KIRKLAND', 'capacity': 318},
    'SWEDISH_ISSAQUAH': {'type': 'ACUTE_CARE', 'location': 'ISSAQUAH', 'capacity': 175},
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
    warrants: List[Dict]
    created_date: str
    created_by_agency: str

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
    registration_expiry: str
    insurance_status: str
    stolen_status: str
    created_date: str
    created_by_agency: str

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
    witness_statements: List[str]
    agency: str  # Add this missing attribute
    created_date: str
    created_by_agency: str

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
    created_date: str

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
    address: str
    city: str
    latitude: float
    longitude: float
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

@dataclass
class CADIncident:
    cad_id: str
    incident_number: str
    call_type: str
    priority: str
    status: str
    call_datetime: str
    dispatch_datetime: str
    en_route_datetime: str
    on_scene_datetime: str
    clear_datetime: str
    location: str
    latitude: float
    longitude: float
    district: str
    beat: str
    reporting_party: str
    reporting_party_phone: str
    incident_description: str
    units_assigned: List[str]
    primary_officer: str
    backup_officers: List[str]
    related_persons: List[Dict]  # Links to persons involved
    related_incidents: List[str]  # Links to other incident types
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
        
        # Tracking sets for unique identifiers
        self.used_ssns = set()
        self.used_license_plates = set()
        self.used_vins = set()
        self.used_incident_numbers = set()
        self.used_booking_numbers = set()
        
        # Cross-reference tracking
        self.person_incident_history = defaultdict(list)
        self.vehicle_owner_map = {}
        self.address_resident_map = defaultdict(list)
        
    def generate_arrest(self, cad_incident, person):
        """Generate an arrest record linked to a CAD incident and person"""
        # Arrest types and methods
        arrest_types = ['ON_VIEW', 'WARRANT', 'INVESTIGATIVE', 'TRAFFIC', 'DOMESTIC_VIOLENCE']
        arrest_methods = ['HANDCUFFS', 'VERBAL_COMMAND', 'PHYSICAL_RESTRAINT', 'TASER', 'FIREARM_DISPLAY']
        
        arrest_type = random.choice(arrest_types)
        arrest_method = random.choice(arrest_methods)
        
        # Generate arrest timing (should be after CAD incident call time)
        cad_call_time = datetime.strptime(cad_incident.call_datetime, '%Y-%m-%d %H:%M:%S')
        arrest_delay = timedelta(minutes=random.randint(5, 45))
        arrest_datetime = cad_call_time + arrest_delay
        
        # Generate booking time (1-3 hours after arrest)
        booking_delay = timedelta(hours=random.randint(1, 3))
        booking_datetime = arrest_datetime + booking_delay
        
        # Generate arrest location (near CAD incident location)
        arrest_lat = cad_incident.latitude + random.uniform(-0.01, 0.01)
        arrest_lon = cad_incident.longitude + random.uniform(-0.01, 0.01)
        
        # Generate realistic arrest location description
        location_types = ['Street', 'Residence', 'Business', 'Parking Lot', 'Vehicle', 'Public Place']
        location_type = random.choice(location_types)
        arrest_location = f"{location_type} near {cad_incident.location}"
        
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
        
        destinations = ['King County Jail', 'Bellevue City Jail', 'Seattle City Jail', 'Harborview Medical Center']
        destination = random.choice(destinations)
        
        # Generate officers
        arresting_officer = cad_incident.primary_officer
        backup_officers = cad_incident.backup_officers if cad_incident.backup_officers else []
        
        # Generate evidence and witness information
        evidence_types = ['PHOTOGRAPHS', 'VIDEO_RECORDING', 'PHYSICAL_EVIDENCE', 'BODY_CAMERA', 'DASH_CAMERA']
        evidence_collected = random.sample(evidence_types, random.randint(1, 3)) if random.random() < 0.7 else []
        
        witness_statements = []
        if random.random() < 0.6:  # 60% chance of witness statements
            num_witnesses = random.randint(1, 3)
            for i in range(num_witnesses):
                witness_statements.append(f"Witness {i+1} statement recorded")
        
        # Generate search authorization
        search_types = ['CONSENT', 'SEARCH_WARRANT', 'EXIGENT_CIRCUMSTANCES', 'INVENTORY_SEARCH', 'PROBABLE_CAUSE']
        search_authorization = random.choice(search_types)
        
        return Arrest(
            arrest_id=f"AR-{datetime.now().year}-{random.randint(100000, 999999)}",
            cad_incident_id=cad_incident.cad_id,
            person_id=person.person_id,
            arrest_datetime=arrest_datetime.strftime('%Y-%m-%d %H:%M:%S'),
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
            miranda_datetime=arrest_datetime.strftime('%Y-%m-%d %H:%M:%S') if random.random() < 0.9 else '',
            search_authorization=search_authorization,
            evidence_collected=evidence_collected,
            witness_statements=witness_statements,
            agency=random.choice(['KCSO', 'BELLEVUE_PD']),
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=random.choice(['KCSO', 'BELLEVUE_PD'])
        )
    
    def generate_arrest_charges(self, arrest_type):
        """Generate realistic charges based on arrest type"""
        charge_mappings = {
            'ON_VIEW': [
                {'charge': 'Disorderly Conduct', 'category': 'PUBLIC_ORDER', 'severity': 'MISDEMEANOR'},
                {'charge': 'Public Intoxication', 'category': 'PUBLIC_ORDER', 'severity': 'MISDEMEANOR'},
                {'charge': 'Trespassing', 'category': 'PROPERTY', 'severity': 'MISDEMEANOR'},
                {'charge': 'Assault', 'category': 'VIOLENT', 'severity': 'MISDEMEANOR'}
            ],
            'WARRANT': [
                {'charge': 'Failure to Appear', 'category': 'COURT_ORDER', 'severity': 'MISDEMEANOR'},
                {'charge': 'Probation Violation', 'category': 'COURT_ORDER', 'severity': 'MISDEMEANOR'},
                {'charge': 'Bail Jumping', 'category': 'COURT_ORDER', 'severity': 'FELONY'}
            ],
            'INVESTIGATIVE': [
                {'charge': 'Possession of Controlled Substance', 'category': 'DRUG', 'severity': 'MISDEMEANOR'},
                {'charge': 'Theft', 'category': 'PROPERTY', 'severity': 'MISDEMEANOR'},
                {'charge': 'Burglary', 'category': 'PROPERTY', 'severity': 'FELONY'},
                {'charge': 'Fraud', 'category': 'PROPERTY', 'severity': 'FELONY'}
            ],
            'TRAFFIC': [
                {'charge': 'DUI', 'category': 'TRAFFIC', 'severity': 'MISDEMEANOR'},
                {'charge': 'Reckless Driving', 'category': 'TRAFFIC', 'severity': 'MISDEMEANOR'},
                {'charge': 'Driving Without License', 'category': 'TRAFFIC', 'severity': 'MISDEMEANOR'},
                {'charge': 'Hit and Run', 'category': 'TRAFFIC', 'severity': 'FELONY'}
            ],
            'DOMESTIC_VIOLENCE': [
                {'charge': 'Domestic Violence Assault', 'category': 'VIOLENT', 'severity': 'MISDEMEANOR'},
                {'charge': 'Domestic Violence Harassment', 'category': 'VIOLENT', 'severity': 'MISDEMEANOR'},
                {'charge': 'Violation of Protection Order', 'category': 'COURT_ORDER', 'severity': 'MISDEMEANOR'}
            ]
        }
        
        base_charges = charge_mappings.get(arrest_type, [{'charge': 'Disorderly Conduct', 'category': 'PUBLIC_ORDER', 'severity': 'MISDEMEANOR'}])
        
        # Add statute numbers
        for charge in base_charges:
            charge['statute'] = f"RCW {random.randint(9, 69)}.{random.randint(10, 999)}"
        
        return base_charges
    
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
            'city': random.choice(['Seattle', 'Bellevue', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island']),
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
            'charges': self.generate_charges(),
            'bail_amount': random.randint(1000, 50000) if suspect_status == 'ARRESTED' else None,
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
            'charges': self.generate_charges(),
            'bail_amount': random.randint(1000, 50000),
            'jail_facility': random.choice(['King County Jail', 'Bellevue City Jail', 'Seattle City Jail']),
            'release_date': None,  # Will be set if released
            'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def generate_arrest_location(self):
        """Generate realistic arrest location in Seattle area"""
        locations = [
            'Downtown Seattle', 'Capitol Hill', 'Ballard', 'Fremont', 'Queen Anne',
            'Bellevue Downtown', 'Crossroads', 'Factoria', 'Redmond', 'Kirkland'
        ]
        return random.choice(locations)
    
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
    
    def generate_jail_booking(self, person_id, arrest_id, agency='KCSO'):
        """Generate comprehensive jail booking record"""
        booking_datetime = fake.date_time_between(start_date='-2y', end_date='now')
        
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
            bail_amount=random.randint(1000, 50000),
            bail_posted=random.random() < 0.6,
            release_datetime=(booking_datetime + timedelta(days=days_served)).strftime('%Y-%m-%d %H:%M:%S'),
            release_type=release_type,
            release_officer=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            days_served=days_served,
            created_date=booking_datetime.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        return booking
    
    def generate_property(self, incident_id=None, person_id=None, agency='KCSO'):
        """Generate comprehensive property/evidence record"""
        property_types = ['EVIDENCE', 'FOUND', 'STOLEN', 'SEIZED']
        weights = [40, 30, 20, 10]
        property_type = random.choices(property_types, weights=weights)[0]
        
        # Property categories
        categories = {
            'WEAPON': ['FIREARM', 'KNIFE', 'BAT', 'TASER'],
            'DRUG': ['MARIJUANA', 'METHAMPHETAMINE', 'HEROIN', 'COCAINE', 'PRESCRIPTION'],
            'ELECTRONICS': ['PHONE', 'LAPTOP', 'TABLET', 'CAMERA', 'GAMING_SYSTEM'],
            'JEWELRY': ['RING', 'NECKLACE', 'WATCH', 'BRACELET', 'EARRINGS'],
            'VEHICLE_PART': ['RADIO', 'WHEELS', 'CATALYTIC_CONVERTER', 'AIRBAG'],
            'CLOTHING': ['JACKET', 'SHOES', 'PURSE', 'BACKPACK'],
            'CURRENCY': ['CASH', 'COINS', 'GIFT_CARDS'],
            'DOCUMENT': ['ID', 'CREDIT_CARD', 'CHECKBOOK', 'PASSPORT']
        }
        
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        # Value estimation
        value_ranges = {
            'WEAPON': (100, 2000),
            'DRUG': (10, 500),
            'ELECTRONICS': (50, 2000),
            'JEWELRY': (100, 10000),
            'VEHICLE_PART': (50, 1000),
            'CLOTHING': (20, 500),
            'CURRENCY': (1, 1000),
            'DOCUMENT': (5, 100)
        }
        
        value_range = value_ranges[category]
        value_estimated = random.randint(value_range[0], value_range[1])
        
        property_record = Property(
            property_id=str(uuid.uuid4()),
            property_type=property_type,
            case_number=f"PROP{random.randint(100000, 999999)}",
            incident_number=incident_id or '',
            description=f"{subcategory} - {fake.sentence()}",
            category=category,
            subcategory=subcategory,
            serial_number=f"SN{random.randint(100000, 999999)}" if random.random() < 0.3 else '',
            make_model=f"{fake.company()} {fake.word().upper()}" if random.random() < 0.4 else '',
            value_estimated=value_estimated,
            currency_amount=value_estimated,
            quantity=random.randint(1, 10),
            unit_of_measure='EACH',
            found_location=fake.street_address(),  # This should generate a proper address
            found_date=fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            found_by_officer=f"{random.randint(1000, 9999)}, {fake.last_name().upper()}",
            owner_person_id=person_id or '',
            chain_of_custody=[],
            evidence_locker=f"LOCKER_{random.choice(['A', 'B', 'C'])}{random.randint(1, 100)}",
            destruction_date='' if random.random() < 0.8 else fake.date_time_between(start_date='now', end_date='+1y').strftime('%Y-%m-%d'),
            disposition=random.choice(['HELD', 'RELEASED', 'DESTROYED', 'AUCTION']),
            created_date=fake.date_time_between(start_date='-30d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            agency=agency
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
        seattle_cities = ['Seattle', 'Bellevue', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island']
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
        
        return Person(
            person_id=f"P-{datetime.now().year}-{random.randint(100000, 999999)}",
            ssn=ssn,
            first_name=first_name,
            last_name=last_name,
            date_of_birth=dob.strftime('%Y-%m-%d'),
            sex=sex,
            race=ethnicity,
            height=f"{random.randint(5, 6)}'{random.randint(0, 11)}\"",
            weight=random.randint(100, 250),
            hair_color=random.choice(['BLACK', 'BROWN', 'BLONDE', 'RED', 'GRAY']),
            eye_color=random.choice(['BROWN', 'BLUE', 'GREEN', 'HAZEL']),
            address=address,
            city=city,
            state='WA',
            zip_code=fake.zipcode_in_state('WA'),
            phone=phone,
            email=email,
            emergency_contact=self.generate_emergency_contact(ethnicity),
            criminal_history=[],
            warrants=[],
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency
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
        
        vehicle = Vehicle(
            vehicle_id=str(uuid.uuid4()),
            vin=vin,
            license_plate=plate,
            state='WA',
            make=make,
            model=model,
            year=year,
            color=random.choice(['WHITE', 'BLACK', 'SILVER', 'GRAY', 'RED', 'BLUE', 'GREEN']),
            body_type=random.choice(['SEDAN', 'SUV', 'TRUCK', 'COUPE', 'WAGON', 'VAN']),
            owner_person_id=owner_id or '',
            registration_expiry=reg_expiry,
            insurance_status=insurance_status,
            stolen_status=stolen_status,
            created_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency
        )
        
        return vehicle
    
    def generate_police_incident(self, agency='KCSO'):
        """Generate a sample police incident"""
        incident_types = [
            'TRAFFIC_VIOLATION', 'DOMESTIC_VIOLENCE', 'THEFT', 'ASSAULT', 
            'DUI', 'DRUG_POSSESSION', 'BURGLARY', 'ROBBERY', 'HOMELESS_ENCAMPMENT',
            'MENTAL_HEALTH_CRISIS', 'OVERDOSE', 'GANG_ACTIVITY'
        ]
        
        incident_type = random.choice(incident_types)
        
        # Generate timing
        call_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        dispatch_delay = timedelta(seconds=random.randint(30, 180))
        en_route_delay = timedelta(seconds=random.randint(45, 300))
        arrive_delay = timedelta(minutes=random.randint(4, 12))
        clear_delay = timedelta(minutes=random.randint(15, 120))
        
        # Location
        cities = ['SEATTLE', 'BELLEVUE', 'KIRKLAND', 'REDMOND', 'SAMMAMISH']
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
            location=fake.street_address(),
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
            created_date=call_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            created_by_agency=agency
        )
        
        return incident

    def generate_fire_incident(self):
        """Generate a sample fire incident"""
        incident_types = ['STRUCTURE_FIRE', 'VEHICLE_FIRE', 'BRUSH_FIRE', 'ALARM_ACTIVATION', 'MEDICAL_EMERGENCY']
        incident_type = random.choice(incident_types)
        
        alarm_datetime = fake.date_time_between(start_date='-30d', end_date='now')
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
            address=fake.street_address(),  # Make sure this is called properly
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
            created_date=alarm_datetime.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        return incident
    
    def generate_ems_incident(self):
        """Generate a sample EMS incident"""
        incident_types = ['MEDICAL_EMERGENCY', 'TRAUMA', 'CARDIAC_ARREST', 'OVERDOSE', 'STROKE', 'DIABETIC_EMERGENCY']
        incident_type = random.choice(incident_types)
        
        call_datetime = fake.date_time_between(start_date='-30d', end_date='now')
        dispatch_delay = timedelta(seconds=random.randint(30, 180))
        en_route_delay = timedelta(seconds=random.randint(45, 300))
        arrive_delay = timedelta(minutes=random.randint(4, 12))
        transport_delay = timedelta(minutes=random.randint(15, 45))
        hospital_delay = timedelta(minutes=random.randint(10, 30))
        clear_delay = timedelta(minutes=random.randint(30, 90))
        
        incident = EMSIncident(
            incident_id=str(uuid.uuid4()),
            incident_number=f"EMS{call_datetime.year}{random.randint(100000, 999999)}",
            call_number=f"E{call_datetime.year}{random.randint(1000000, 9999999)}",
            incident_type=incident_type,
            incident_subtype=f"{incident_type}_SUBTYPE",
            priority=random.choice(['LOW', 'MEDIUM', 'HIGH', 'EMERGENCY']),
            call_datetime=call_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_datetime=(call_datetime + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            en_route_datetime=(call_datetime + dispatch_delay + en_route_delay).strftime('%Y-%m-%d %H:%M:%S'),
            arrive_datetime=(call_datetime + dispatch_delay + en_route_delay + arrive_delay).strftime('%Y-%m-%d %H:%M:%S'),
            transport_datetime=(call_datetime + dispatch_delay + en_route_delay + arrive_delay + transport_delay).strftime('%Y-%m-%d %H:%M:%S'),
            hospital_arrival_datetime=(call_datetime + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay).strftime('%Y-%m-%d %H:%M:%S'),
            clear_datetime=(call_datetime + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + clear_delay).strftime('%Y-%m-%d %H:%M:%S'),
            address=fake.street_address(),  # Make sure this is called properly
            city='SEATTLE',
            latitude=random.uniform(47.5, 47.8),
            longitude=random.uniform(-122.5, -122.1),
            district=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']),
            responding_unit=f"MEDIC_{random.randint(1, 20)}",
            crew_members=[f"PARAMEDIC_{fake.last_name().upper()}", f"EMT_{fake.last_name().upper()}"],
            patient_person_id=str(uuid.uuid4()),
            patient_age=random.randint(5, 85),
            patient_sex=random.choice(['M', 'F']),
            chief_complaint=random.choice(['CHEST_PAIN', 'DIFFICULTY_BREATHING', 'UNCONSCIOUS', 'INJURY', 'OVERDOSE']),
            primary_impression=random.choice(['CARDIAC', 'RESPIRATORY', 'TRAUMA', 'MEDICAL', 'PSYCHIATRIC']),
            vital_signs={
                'blood_pressure': f"{random.randint(90, 180)}/{random.randint(60, 100)}",
                'heart_rate': random.randint(60, 120),
                'respiratory_rate': random.randint(12, 20),
                'oxygen_saturation': random.randint(85, 100),
                'temperature': round(random.uniform(96.0, 102.0), 1)
            },
            treatment_provided=random.sample(['OXYGEN', 'IV_FLUIDS', 'MEDICATION', 'SPLINTING', 'CPR'], random.randint(1, 3)),
            medications_given=random.sample(['ASPIRIN', 'NITROGLYCERIN', 'ALBUTEROL', 'NARCAN'], random.randint(0, 2)),
            transport_destination=random.choice(['HARBORVIEW', 'UW_MEDICAL', 'VIRGINIA_MASON', 'SWEDISH']),
            transport_mode=random.choice(['GROUND_AMBULANCE', 'AIR_AMBULANCE', 'PRIVATE_VEHICLE']),
            created_date=call_datetime.strftime('%Y-%m-%d %H:%M:%S')
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
        primary_officer = f"Officer {fake.last_name()}"
        backup_officers = [f"Officer {fake.last_name()}" for _ in range(random.randint(0, 2))]
        
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
            created_by_agency='KCSO'
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
            'city': random.choice(['Seattle', 'Bellevue', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Mercer Island']),
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
            'jail_bookings': [asdict(b) for b in self.jail_bookings],
            'fire_incidents': [asdict(f) for f in self.fire_incidents],
            'ems_incidents': [asdict(e) for e in self.ems_incidents]
        }
        
        # Save each entity type to separate JSON files
        for entity_type, data in data_to_save.items():
            filename = f"{entity_type}.json"
            with open(filename, 'w', encoding='utf-8') as f:  # Add UTF-8 encoding
                json.dump(data, f, indent=2, ensure_ascii=False)  # Set ensure_ascii=False
            print(f"Saved {len(data)} {entity_type} to {filename}")
        
        # Save combined data
        with open('all_sample_data.json', 'w', encoding='utf-8') as f:  # Add UTF-8 encoding
            json.dump(data_to_save, f, indent=2, ensure_ascii=False)  # Set ensure_ascii=False
        print("Saved combined data to all_sample_data.json")
        
        print("JSON export completed!")

    def generate_all_data(self):
        """Generate all comprehensive synthetic data"""
        print("Enhanced Multi-Agency Complex Data Generator Starting...")
        print("Generating comprehensive data for Seattle, King County, Bellevue, and EMS scenarios")
        start_time = time.time()
        
        # Generate persons
        print(f"Generating {CONFIG['num_persons']:,} persons...")
        for i in range(CONFIG['num_persons']):
            agency = random.choices(['KCSO', 'BELLEVUE_PD'], weights=[70, 30])[0]
            person = self.generate_person(agency)
            self.persons.append(person)
            
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
        
        # Generate police incidents
        print(f"Generating {CONFIG['num_police_incidents']:,} police incidents...")
        for i in range(CONFIG['num_police_incidents']):
            agency = random.choices(['KCSO', 'BELLEVUE_PD'], weights=[75, 25])[0]
            incident = self.generate_police_incident(agency)
            self.police_incidents.append(incident)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} police incidents...")
        
        # Generate arrests
        print(f"Generating {CONFIG['num_arrests']:,} arrests...")
        for i in range(CONFIG['num_arrests']):
            person = random.choice(self.persons)
            incident = random.choice(self.police_incidents)
            arrest = self.generate_arrest(incident, person)
            self.arrests.append(arrest)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} arrests...")
        
        # Generate jail bookings
        print(f"Generating {CONFIG['num_jail_bookings']:,} jail bookings...")
        for i in range(CONFIG['num_jail_bookings']):
            arrest = random.choice(self.arrests)
            booking = self.generate_jail_booking(arrest.person_id, arrest.arrest_id, arrest.agency)
            self.jail_bookings.append(booking)
            
            if (i + 1) % 5000 == 0:
                print(f"   Generated {i + 1:,} jail bookings...")
        
        # Generate properties/evidence
        print(f"Generating {CONFIG['num_properties']:,} properties/evidence...")
        for i in range(CONFIG['num_properties']):
            incident = random.choice(self.police_incidents) if random.random() < 0.6 else None
            person = random.choice(self.persons) if random.random() < 0.4 else None
            property_record = self.generate_property(
                incident.incident_id if incident else None,
                person.person_id if person else None,
                random.choice(['KCSO', 'BELLEVUE_PD'])
            )
            self.properties.append(property_record)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} properties...")
        
        # Generate fire incidents
        if CONFIG['generate_fire_data']:
            print(f"Generating {CONFIG['num_fire_incidents']:,} fire incidents...")
            for i in range(CONFIG['num_fire_incidents']):
                incident = self.generate_fire_incident()
                self.fire_incidents.append(incident)
                
                if (i + 1) % 5000 == 0:
                    print(f"   Generated {i + 1:,} fire incidents...")
        
        # Generate EMS incidents
        if CONFIG['generate_ems_data']:
            print(f"Generating {CONFIG['num_ems_incidents']:,} EMS incidents...")
            for i in range(CONFIG['num_ems_incidents']):
                incident = self.generate_ems_incident()
                self.ems_incidents.append(incident)
                
                if (i + 1) % 10000 == 0:
                    print(f"   Generated {i + 1:,} EMS incidents...")
        
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
        print(f"  • Jail Bookings: {len(self.jail_bookings):,}")
        
        print(f"\nFire/EMS Records:")
        print(f"  • Fire Incidents: {len(self.fire_incidents):,}")
        print(f"  • EMS Incidents: {len(self.ems_incidents):,}")
        
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

def main():
    """Main execution function"""
    print("Enhanced Multi-Agency Complex Synthetic Data Generator")
    print("Generating comprehensive data for Seattle, King County, Bellevue, and EMS scenarios")
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

