"""
EMS Data

This module contains all EMS-specific data generation functionality including:
- EMS incident codes and mappings
- EMS dataclasses (EMSIncident, EMSMedication, EMSPatient)
- EMS data generation methods
- EMS-specific utility functions
"""

import random
import uuid
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
from faker import Faker
from collections import deque
import threading
import multiprocessing as mp

# SDV imports removed for performance

# EMS Incident Code Mapping - Comprehensive list of synthetic EMS codes and descriptions
EMS_INCIDENT_CODES = {
    '2301001': 'Abdominal Pain/Problems',
    '2301003': 'Allergic Reaction/Stings',
    '2301005': 'Animal Bite',
    '2301007': 'Assault',
    '2301009': 'Automated Crash Notification',
    '2301011': 'Back Pain (Non-Traumatic)',
    '2301013': 'Breathing Problem',
    '2301015': 'Burns/Explosion',
    '2301017': 'Carbon Monoxide/Hazmat/Inhalation/CBRN',
    '2301019': 'Cardiac Arrest/Death',
    '2301021': 'Chest Pain (Non-Traumatic)',
    '2301023': 'Choking',
    '2301025': 'Convulsions/Seizure',
    '2301027': 'Diabetic Problem',
    '2301029': 'Electrocution/Lightning',
    '2301031': 'Eye Problem/Injury',
    '2301033': 'Falls',
    '2301035': 'Fire',
    '2301037': 'Headache',
    '2301039': 'Healthcare Professional/Admission',
    '2301041': 'Heart Problems/AICD',
    '2301043': 'Heat/Cold Exposure',
    '2301045': 'Hemorrhage/Laceration',
    '2301047': 'Industrial Accident/Inaccessible Incident/Other Entrapments (Non-Vehicle)',
    '2301049': 'Medical Alarm',
    '2301051': 'No Other Appropriate Choice',
    '2301053': 'Overdose/Poisoning/Ingestion',
    '2301055': 'Pandemic/Epidemic/Outbreak',
    '2301057': 'Pregnancy/Childbirth/Miscarriage',
    '2301059': 'Psychiatric Problem/Abnormal Behavior/Suicide Attempt',
    '2301061': 'Sick Person',
    '2301063': 'Stab/Gunshot Wound/Penetrating Trauma',
    '2301065': 'Standby',
    '2301067': 'Stroke/CVA',
    '2301069': 'Traffic/Transportation Incident',
    '2301071': 'Transfer/Interfacility/Palliative Care',
    '2301073': 'Traumatic Injury',
    '2301075': 'Well Person Check',
    '2301077': 'Unconscious/Fainting/Near-Fainting',
    '2301079': 'Unknown Problem/Person Down',
    '2301081': 'Drowning/Diving/SCUBA Accident',
    '2301083': 'Airmedical Transport'
}

@dataclass
class EMSIncident:
    incident_id: str
    incident_number: str
    call_number: str
    incident_type: str
    incident_type_code: str
    incident_type_description: str
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
    patient_race: str
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
    procedure_complications: List[Dict]
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
    patient_medical_history: List[str]
    patient_chronic_conditions: List[str]
    patient_current_medications: List[str]
    is_superuser: bool
    # Unit Details fields
    agency_number: str
    agency_name: str
    agency_affiliation: str
    primary_unit_role: str
    # Incident Dates/Times fields
    total_commit_time: int
    unit_notified_by_dispatch_datetime: str = ""
    unit_enroute_datetime: str = ""
    unit_arrived_at_patient_datetime: str = ""
    unit_arrive_on_scene_datetime: str = ""
    unit_clear_datetime: str = ""
    transfer_of_ems_patient_care_datetime: str = ""
    arrival_at_destination_landing_area_datetime: str = ""
    unit_left_scene_datetime: str = ""
    patient_arrived_at_destination_datetime: str = ""
    unit_back_in_service_datetime: str = ""
    last_modified: str = ""
    # Ungrouped Properties fields
    incident_status: str = ""
    created_by: str = ""
    patient_pk: str = ""
    primary_patient_caregiver_on_scene: str = ""
    crew_with_als_pt_contact_response_role: str = ""
    
    # GPS/Geocoding fields (missing from current implementation)
    incident_location_latitude: float = 0.0
    incident_location_longitude: float = 0.0
    incident_location_accuracy: str = ""
    patient_home_latitude: float = 0.0
    patient_home_longitude: float = 0.0
    
    # Additional EMS-specific fields
    incident_dispatch_priority: str = ""
    incident_response_priority: str = ""
    scene_safety_assessment: str = ""
    environmental_conditions: str = ""
    incident_narrative: str = ""
    patient_refusal_reason: Optional[str] = None
    transport_refusal_reason: Optional[str] = None
    destination_facility_name: str = ""
    destination_facility_type: str = ""
    
    # Crew and unit tracking
    responding_unit_type: str = ""
    responding_unit_capability: str = ""
    backup_unit_assigned: Optional[str] = None
    mutual_aid_requested: bool = False
    
    # Patient assessment details
    initial_patient_assessment: str = ""
    final_patient_assessment: str = ""
    patient_condition_on_arrival: str = ""
    patient_condition_on_transfer: str = ""
    pain_scale_score: Optional[int] = None
    glasgow_coma_scale: Optional[int] = None

@dataclass
class EMSMedication:
    medication_id: str
    administered_prior_to_ems_care: str
    medication_rxcui_code: str
    medication_name: str
    medication_administration_route: str
    medication_site: str
    dosage: float
    dosage_unit: str
    patient_response: str
    complications: str
    crew_member_name: str
    crew_member_level: str
    crew_badge_number: str
    medication_authorization: str
    last_modified: str
    incident_id: str
    created_date: str
    administered_datetime: str
    broken_seal: str

@dataclass
class EMSReport:
    """Comprehensive EMS report that links to persons, medications, and incidents"""
    # Report Identification
    report_id: str
    report_number: str
    report_date: str
    created_date: str
    last_modified: str
    created_by: str
    
    # Incident Linkage
    incident_id: str
    incident_number: str
    call_number: str
    
    # Patient Linkage and Demographics
    patient_id: str
    patient_pk: str
    patient_full_name: str
    patient_gender: str
    patient_age: int
    patient_date_of_birth: str
    patient_weight: float
    patient_home_address: str
    patient_race: str
    patient_ethnicity: str
    
    # Incident Overview
    incident_datetime: str
    complaint_reported_by_dispatch: str
    complaint_reported_by_dispatch_code: str
    unit_call_sign: str
    location: str  # Geo coordinates
    
    # Disposition Details
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
    
    # Medical Details
    patient_acuity: str
    situation_patient_acuity: str
    medications_given: str  # Summary of all medications
    attempted_procedures: str  # Summary of all procedures
    successful_procedures: str  # Summary of successful procedures
    cardiac_arrest_datetime: Optional[str]
    cardiac_arrest_resuscitation_discontinuation_datetime: Optional[str]
    ecg_findings: str
    incident_emd_performed: str
    incident_emd_performed_code: str
    cad_level_of_care_provided: str
    incident_level_of_care_provided: str
    provider_primary_impression: str
    
    # Crew Details
    crew_member_name: str
    crew_member_level: str
    crew_badge_number: str
    primary_patient_caregiver_on_scene: str
    crew_with_als_pt_contact_response_role: str
    
    # Unit Details
    agency_number: str
    agency_name: str
    agency_affiliation: str
    primary_unit_role: str
    
    # Incident Timeline
    total_commit_time: int
    unit_notified_by_dispatch_datetime: str
    unit_en_route_datetime: str
    unit_arrived_on_scene_datetime: str
    unit_arrived_at_patient_datetime: str
    transfer_of_ems_patient_care_datetime: str
    arrival_at_destination_landing_area_datetime: str
    unit_left_scene_datetime: str
    patient_arrived_at_destination_datetime: str
    unit_back_in_service_datetime: str
    
    # Status and Classification
    incident_status: str
    incident_type: str
    incident_type_code: str
    priority: str
    
    # Linked Entity References
    linked_medications: List[str]  # List of medication IDs
    linked_patients: List[str]     # List of patient IDs (for multi-patient incidents)
    linked_incidents: List[str]    # List of related incident IDs
    
    # Additional Report Fields
    report_narrative: str
    quality_assurance_review: Optional[str]
    supervisor_approval: Optional[str]
    billing_status: str
    insurance_verified: bool
    patient_signature_obtained: bool
@dataclass
class EMSPatient:
    # Basic Demographics
    patient_id: str
    patient_full_name: str
    patient_date_of_birth: str
    patient_age: int
    patient_gender: str
    patient_race: str
    patient_ethnicity: str
    patient_marital_status: str
    
    # Physical Characteristics
    patient_weight: float
    patient_height: int  # in inches
    patient_bmi: float
    
    # Contact Information
    patient_home_address: str
    patient_home_city: str
    patient_home_state: str
    patient_home_zip: str
    patient_home_address_geo: str
    patient_home_latitude: float
    patient_home_longitude: float
    patient_phone: str
    patient_email: str
    
    # Emergency Contacts
    emergency_contact_name: str
    emergency_contact_relationship: str
    emergency_contact_phone: str
    
    # Insurance & Financial
    insurance_provider: str
    insurance_policy_number: str
    insurance_group_number: str
    
    # Employment & Education
    occupation: str
    employer: str
    education_level: str
    
    # Medical History & Risk Factors
    primary_care_physician: str
    primary_care_physician_phone: str
    known_allergies: List[str]
    current_medications: List[str]
    chronic_conditions: List[str]
    medical_history: List[str]
    family_medical_history: List[str]
    social_history: Dict  # smoking, alcohol, drugs, etc.
    
    # Functional Status
    mobility_status: str
    cognitive_status: str
    living_situation: str
    caregiver_status: str
    
    # Legal & Administrative
    patient_pk: str
    incident_id: str
    created_date: str
    last_modified: str
    is_superuser: bool
    incidents_past_month: int
    incidents_past_year: int
    
    # Additional Demographics
    preferred_language: str
    interpreter_needed: bool
    veteran_status: bool
    disability_status: str


class EMSDataGenerator:
    """EMS-specific data generation functionality"""
    
    def __init__(self, fake_instance=None):
        """Initialize with optional faker instance"""
        self.fake = fake_instance or Faker()
        
        # Address caching system (DISABLED for speed
        self._address_cache = deque(maxlen=2000)  # Cache up to 2000 addresses
        self._address_cache_lock = threading.Lock()
        self._cache_initialized = False
        self._bypass_address_caching = True  # Skip address cache, use optimized geocoding
        self._geocoding_cache = {}  # Cache successful geocoding results
        self._geocoding_rate_limit = 0.5  # Minimum seconds between geocoding calls
        self._last_geocoding_time = 0
        self._real_address_pool = []  # Pool of pre-geocoded real addresses
        self._pool_initialized = False
        self._address_lock = threading.Lock()  # Thread-safe address loading
        
        # Copula modeling removed for performance
        
        # Patient pool for 1:Many relationships
        self._patient_pool = []  # Store generated patients for reuse
        self._patient_incident_history = {}  # Track incidents per patient
        
        # Initialize cache in background (SKIPPED for speed)
        if not self._bypass_address_caching:
            self._initialize_address_cache()
        else:
            print("Address caching bypassed for faster generation")
    
    def _initialize_address_cache(self):
        """Initialize address cache with real geocoded addresses (DISABLED for speed)"""
        if self._bypass_address_caching:
            print("Address cache initialization bypassed")
            return
            
        print("Initializing address cache with real King County addresses...")
        
        # Generate real geocoded addresses in batches
        batch_size = 50  # Smaller batches for geocoding
        total_addresses = 500  # Pre-generate 500 real addresses
        
        for i in range(0, total_addresses, batch_size):
            batch = []
            for _ in range(min(batch_size, total_addresses - i)):
                address = self._generate_real_geocoded_address()
                batch.append(address)
            
            with self._address_cache_lock:
                self._address_cache.extend(batch)
            
            print(f"  Cached {len(self._address_cache)}/{total_addresses} addresses...")
        
        self._cache_initialized = True
        print(f"Address cache initialized with {len(self._address_cache)} real King County addresses")
    
    def _generate_real_geocoded_address(self):
        """Generate a real geocoded address within King County"""
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
        
        # King County geographic bounds
        seattle_bounds = {
            'seattle': {
                'lat_range': (47.5000, 47.7200),  # Excludes extreme north/south water areas
                'lon_range': (-122.4200, -122.2500),  # Excludes Puget Sound (-122.46+) and Lake Washington (-122.22-)
                'zip_codes': ['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109', '98112', '98115', '98116', '98117', '98118', '98119', '98121', '98122', '98125', '98126', '98133', '98134', '98136', '98144', '98154', '98164', '98174', '98177', '98178', '98195', '98199']
            },
            'bellevue': {
                'lat_range': (47.5500, 47.6500),
                'lon_range': (-122.2500, -122.1000),
                'zip_codes': ['98004', '98005', '98006', '98007', '98008', '98009']
            },
            'tacoma': {
                'lat_range': (47.2000, 47.3000),
                'lon_range': (-122.5000, -122.3500),
                'zip_codes': ['98401', '98402', '98403', '98404', '98405', '98406', '98407', '98408', '98409', '98411', '98412', '98413', '98415', '98416', '98418', '98421', '98422', '98424', '98433', '98443', '98444', '98445', '98446', '98447', '98465', '98466', '98467', '98471', '98481', '98493']
            },
            'everett': {
                'lat_range': (47.9500, 48.0500),
                'lon_range': (-122.2500, -122.1500),
                'zip_codes': ['98201', '98203', '98204', '98205', '98206', '98207', '98208', '98213']
            },
            'kent': {
                'lat_range': (47.3500, 47.4500),
                'lon_range': (-122.2500, -122.1000),
                'zip_codes': ['98030', '98031', '98032', '98042', '98089']
            },
            'renton': {
                'lat_range': (47.4500, 47.5500),
                'lon_range': (-122.2500, -122.1500),
                'zip_codes': ['98055', '98056', '98057', '98058', '98059']
            }
        }
        
        # Select a random city/area
        city_name = random.choice(list(seattle_bounds.keys()))
        city_data = seattle_bounds[city_name]
        
        # Generate realistic coordinates within the city bounds
        lat = random.uniform(city_data['lat_range'][0], city_data['lat_range'][1])
        lon = random.uniform(city_data['lon_range'][0], city_data['lon_range'][1])
        
        # Select a random zip code for this city
        zip_code = random.choice(city_data['zip_codes'])
        
        # Try to reverse geocode to get a real address
        try:
            geolocator = Nominatim(user_agent="seattle_data_generator")
            location = geolocator.reverse(f"{lat}, {lon}", timeout=10)
            
            if location and location.address:
                # Parse the address components
                address_parts = location.address.split(', ')
                if len(address_parts) >= 2:
                    street_address = address_parts[0]
                    city_state_zip = address_parts[-2] if len(address_parts) >= 3 else f"{city_name.title()}, WA"
                    
                    # Extract city name
                    if ',' in city_state_zip:
                        city = city_state_zip.split(',')[0].strip()
                    else:
                        city = city_name.title()
                    
                    # Add apartment number occasionally
                    full_address = street_address
                    if random.random() < 0.3:
                        apartment_number = f"Apt {random.randint(1, 500)}"
                        full_address = f"{street_address}, {apartment_number}"
                    
                    return (full_address, city, 'WA', zip_code)
        except (GeocoderTimedOut, GeocoderUnavailable, Exception) as e:
            # Fallback to coordinate-based address if geocoding fails
            pass
        
        # Fallback: Generate realistic-looking address based on coordinates
        street_numbers = [str(random.randint(100, 9999))]
        street_names = ['Main St', 'First Ave', 'Second St', 'Park Ave', 'Oak St', 'Pine St', 
                       'Cedar Ave', 'Elm St', 'Maple Dr', 'Washington St', 'Broadway', 'Center St',
                       'Highland Ave', 'Sunset Blvd', 'Riverside Dr', 'Hill St', 'Valley Rd']
        street_suffixes = ['St', 'Ave', 'Blvd', 'Rd', 'Dr', 'Ln', 'Way', 'Pl', 'Ct']
        
        street_number = random.choice(street_numbers)
        street_name = random.choice(street_names)
        street_suffix = random.choice(street_suffixes)
        
        address = f'{street_number} {street_name} {street_suffix}'
        
        # Add apartment number occasionally
        if random.random() < 0.3:
            apartment_number = f"Apt {random.randint(1, 500)}"
            address = f'{address}, {apartment_number}'
        
        return (address, city_name.title(), 'WA', zip_code)
    
    def _get_cached_address(self):
        """Get a random address from the pre-generated address library"""
        if not self._pool_initialized:
            self._load_address_library()
        return random.choice(self._real_address_pool)
    
    def _generate_cad_level_and_provider_type(self):
        """Generate CAD level of care and dispatched provider type"""
        # CAD Level of Care distribution (synthetic)
        cad_levels = {
            'BLS': 0.70,    # Most common
            'AEMT': 0.27,   # Second most common
            'P': 0.012,     # Third most common
            'I/P': 0.011,   # Fourth most common
            'AP': 0.006     # Least common
        }
        
        # Select CAD level based on synthetic distribution
        cad_names = list(cad_levels.keys())
        cad_weights = list(cad_levels.values())
        selected_cad_level = random.choices(cad_names, weights=cad_weights, k=1)[0]
        
        # Provider type distribution based on selected CAD level (synthetic)
        provider_distributions = {
            'BLS': {
                'ALS': 0.20, 'BLS': 0.17, 'BLS-EMT': 0.11, 'PARAMEDIC': 0.15,
                'INTERMEDIATE_1': 0.03, 'AEMT/EMT': 0.017, 'INTERMEDIATE_2': 0.006,
                'EMT': 0.0007, 'BLS-EMR': 0.0003, 'NAN': 0.003, 'No_value': 0.002,
                'SPECIALTY_Other': 0.0002, 'NURSE_INTERMEDIATE': 0.000006, 'PHYSICIAN': 0.00004
            },
            'AEMT': {
                'ALS': 0.23, 'PARAMEDIC': 0.12, 'BLS-EMT': 0.07, 'BLS': 0.06,
                'INTERMEDIATE_1': 0.025, 'AEMT/EMT': 0.014, 'INTERMEDIATE_2': 0.006,
                'EMT': 0.0006, 'BLS-EMR': 0.0003, 'No_value': 0.001, 'NAN': 0.0002,
                'SPECIALTY_Other': 0.0002, 'NURSE_INTERMEDIATE': 0.000015, 'PHYSICIAN': 0.000035
            },
            'P': {
                'ALS': 0.51, 'PARAMEDIC': 0.30, 'BLS-EMT': 0.11, 'INTERMEDIATE_1': 0.039,
                'AEMT/EMT': 0.015, 'INTERMEDIATE_2': 0.004, 'BLS': 0.013, 'EMT': 0.0009,
                'BLS-EMR': 0.0004, 'No_value': 0.001, 'SPECIALTY_Other': 0.0015, 'PHYSICIAN': 0.0005,
                'NAN': 0.0, 'NURSE_INTERMEDIATE': 0.0
            },
            'I/P': {
                'ALS': 0.50, 'PARAMEDIC': 0.25, 'BLS-EMT': 0.13, 'INTERMEDIATE_1': 0.049,
                'AEMT/EMT': 0.017, 'INTERMEDIATE_2': 0.007, 'BLS': 0.044, 'EMT': 0.0005,
                'BLS-EMR': 0.0003, 'No_value': 0.001, 'NAN': 0.0003, 'SPECIALTY_Other': 0.0008,
                'PHYSICIAN': 0.0003, 'NURSE_INTERMEDIATE': 0.0
            },
            'AP': {
                'ALS': 0.57, 'PARAMEDIC': 0.24, 'BLS-EMT': 0.09, 'INTERMEDIATE_1': 0.027,
                'AEMT/EMT': 0.023, 'INTERMEDIATE_2': 0.014, 'BLS': 0.017, 'EMT': 0.001,
                'BLS-EMR': 0.0005, 'No_value': 0.007, 'NAN': 0.004, 'SPECIALTY_Other': 0.008,
                'PHYSICIAN': 0.0007, 'NURSE_INTERMEDIATE': 0.0
            }
        }
        
        # Select provider type based on CAD level distribution
        provider_dist = provider_distributions[selected_cad_level]
        provider_names = list(provider_dist.keys())
        provider_weights = list(provider_dist.values())
        selected_provider_type = random.choices(provider_names, weights=provider_weights, k=1)[0]
        
        return selected_cad_level, selected_provider_type
    
    def _generate_patient_and_situation_acuity(self):
        """Generate realistic patient acuity and situation acuity"""
        # Patient Acuity distribution (synthetic)
        patient_acuity_levels = {
            'EMERGENT': 0.999,
            'CRITICAL': 0.0003,
            'LOWER ACUITY': 0.0009,
            'SCHEDULED TRANSFER OR STANDBY': 0.0002
        }
        
        # Select patient acuity based on synthetic distribution
        acuity_names = list(patient_acuity_levels.keys())
        acuity_weights = list(patient_acuity_levels.values())
        # Normalize to probabilities
        acuity_total = sum(acuity_weights)
        if acuity_total > 0:
            acuity_probs = [w / acuity_total for w in acuity_weights]
        else:
            acuity_probs = [1.0 / len(acuity_weights)] * len(acuity_weights)
        selected_patient_acuity = random.choices(acuity_names, weights=acuity_probs, k=1)[0]
        
        # Situation acuity distribution based on selected patient acuity (synthetic)
        situation_distributions = {
            'EMERGENT': {
                'EMERGENT': 0.79, 'CRITICAL (R)': 0.018, 'LOWER ACU': 0.12, 'No value': 0.065,
                'DEAD WITH': 0.002, 'DECEASED': 0.002, 'NOT APPLIC': 0.004, 'NOT RECOR': 0.0009
            },
            'CRITICAL': {
                'EMERGENT': 0.50, 'CRITICAL (R)': 0.46, 'LOWER ACU': 0.016, 'DEAD WITH': 0.016,
                'DECEASED': 0.0, 'NOT APPLIC': 0.0, 'NOT RECOR': 0.0, 'No value': 0.0
            },
            'LOWER ACUITY': {
                'LOWER ACU': 0.36, 'EMERGENT': 0.63, 'CRITICAL (R)': 0.003, 'No value': 0.003,
                'DEAD WITH': 0.003, 'DECEASED': 0.0, 'NOT APPLIC': 0.0, 'NOT RECOR': 0.0
            },
            'SCHEDULED TRANSFER OR STANDBY': {
                'EMERGENT': 0.81, 'LOWER ACU': 0.16, 'NOT APPLIC': 0.027, 'CRITICAL (R)': 0.0,
                'DEAD WITH': 0.0, 'DECEASED': 0.0, 'NOT RECOR': 0.0, 'No value': 0.0
            }
        }
        
        # Select situation acuity based on patient acuity distribution
        situation_dist = situation_distributions[selected_patient_acuity]
        situation_names = list(situation_dist.keys())
        situation_values = list(situation_dist.values())
        # Normalize to probabilities
        situation_total = sum(situation_values)
        if situation_total > 0:
            situation_probs = [v / situation_total for v in situation_values]
        else:
            situation_probs = [1.0 / len(situation_values)] * len(situation_values)
        selected_situation_acuity = random.choices(situation_names, weights=situation_probs, k=1)[0]
        
        return selected_patient_acuity, selected_situation_acuity
    def _generate_patient_disposition(self):
        """Generate realistic patient disposition"""
        disposition_levels = {
            'TREATED, TRANSPORTED BY THIS EMS UNIT': 0.377,  # Most common
            'UNIT ASSIST (MANPOWER ONLY)': 0.175,            # Very common
            'CANCELED (PRIOR TO ARRIVAL AT SCENE)': 0.098,   # Common
            'PATIENT REFUSAL': 0.091,                        # Common
            'TREATED, TRANSFERRED CARE': 0.066,              # Common
            'CANCELED ON SCENE (NO PATIENT CONTACT)': 0.063, # Common
            'STANDBY (FIRE, EMS OPS, OR PUBLIC SAFTEY EVENT)': 0.021,  # Uncommon
            'CANCELED (NO PATIENT FOUND)': 0.029,            # Uncommon
            'TREATED, TRANSPORTED WITH THIS EMS PROVIDER IN ANOTHER VEHICLE': 0.020,  # Uncommon
            'COMMAND / SUPERVISION ONLY': 0.013,             # Uncommon
            'CARDIAC ARREST - RESUSCITATION ATTEMPTED (NOT TRANSPORTED)': 0.007,     # Rare
            'DEAD ON ARRIVAL': 0.005,                        # Rare
            'PERSON EVALUATED - NO EMS REQUIRED': 0.005,     # Rare
            'TRANSPORTED TO LANDING ZONE, CARE TRANSFERRED': 0.0002,  # Very rare
            'MUTUAL AID TX & TRANSPORT': 0.000003            # Extremely rare
        }
        
        # Select disposition based on synthetic distribution
        disposition_names = list(disposition_levels.keys())
        disposition_weights = list(disposition_levels.values())
        selected_disposition = random.choices(disposition_names, weights=disposition_weights, k=1)[0]
        
        return selected_disposition
    
    def _generate_complaint_reported_by_dispatch(self):
        # Complaint distribution (synthetic)
        complaint_levels = {
            'TRAFFIC/TRANSPORTATION INCIDENT': 0.123,      # Most common
            'SICK PERSON': 0.136,                          # Most common
            'FALLS': 0.097,                               # Very common
            'UNCONSCIOUS/FAINTING/NEAR-FAINTING': 0.072,  # Very common
            'BREATHING PROBLEM': 0.077,                   # Very common
            'CARDIAC ARREST/DEATH': 0.054,               # Common
            'CHEST PAIN (NON-TRAUMATIC)': 0.048,         # Common
            'STROKE/CVA': 0.017,                          # Common
            'SEIZURE': 0.025,                             # Common
            'HEMORRHAGE/LACERATION': 0.023,               # Common
            'FIRE': 0.023,                                # Common
            'ASSAULT': 0.012,                             # Common
            'OVERDOSE/POISONING/INGESTION': 0.011,        # Common
            'PANDEMIC - COVID19': 0.014,                  # Common
            'PSYCHIATRIC PROBLEM/ABNORMAL E': 0.013,      # Common
            'PUBLIC SERVICE': 0.014,                      # Common
            'MEDICAL ALARM': 0.016,                       # Common
            'HEART PROBLEMS': 0.018,                      # Common
            'ABDOMINAL PAIN/PROBLEMS': 0.018,             # Common
            'ALLERGIC REACTION/STINGS': 0.012,           # Common
            'DIABETIC PROBLEM': 0.010,                    # Common
            'HAZARD INVESTIGATION': 0.010,                # Common
            'BACK PAIN (NON-TRAUMATIC)': 0.008,          # Uncommon
            'UNKNOWN PROBLEM/PERSON DOWN': 0.008,        # Uncommon
            'CHOKING': 0.005,                             # Uncommon
            'PREGANCY/CHILDBIRTH/MISCARRIA': 0.003,      # Uncommon
            'HEADACHE': 0.004,                            # Uncommon
            'TECHNICAL RESCUE': 0.004,                    # Uncommon
            'INVESTIGATION': 0.004,                       # Uncommon
            'HEAT/COLD EXPOSURE': 0.002,                   # Uncommon
            'WATER RESCUE': 0.001,                         # Uncommon
            'HAZARDOUS MATERIALS INCIDENT': 0.001,        # Uncommon
            'TRAUMATIC INJURY': 0.021,                    # Common
            'GUNSHOT': 0.001,                              # Uncommon
            'STAB/GUNSHOT WOUND/PENETRATIN': 0.001,       # Uncommon
            'ASSAULT - SEXUAL': 0.0007,                     # Uncommon
            'EYE PROBLEM/INJURY': 0.0007,                   # Uncommon
            'INDUSTRIAL ACCIDENT (INCLUDES EN': 0.0007,    # Uncommon
            'HAZARDOUS CONDITION': 0.0006,                  # Uncommon
            'DROWNING': 0.0005,                             # Uncommon
            'COMMERCIAL FIRE ALARM': 0.0005,               # Uncommon
            'STANDBY': 0.0004,                              # Uncommon
            'CARBON MONOXIDE/HAZMAT/INHAL': 0.0004,        # Uncommon
            'BURNS/EXPLOSION': 0.001,                      # Uncommon
            'RESIDENTIAL FIRE ALARM': 0.0003,              # Uncommon
            'AIRCRAFT DOWN': 0.0003,                        # Uncommon
            'TASER SHOCK': 0.0003,                          # Uncommon
            'WELL PERSON CHECK': 0.0003,                    # Uncommon
            'BARIATRIC PATIENT': 0.0001,                     # Uncommon
            'HANGING': 0.0001,                               # Uncommon
            'AIRMEDICAL TRANSPORT': 0.0001,                  # Uncommon
            'PAIN': 0.0001,                                  # Uncommon
            'RESPIRATORY ARREST': 0.0001,                    # Uncommon
            'AUTO VS. PEDESTRIAN': 0.00008,                   # Uncommon
            'AUTOMATED CRASH NOTIFICATION': 0.00008,         # Uncommon
            'MOTORCYCLE COLLISION': 0.00002,                   # Uncommon
            'EPISTAXIS (NOSEBLEED)': 0.00002,                  # Uncommon
            'PENETRATING WOUNDS': 0.00001,                     # Uncommon
            'SEARCH AND RESCUE': 0.00001,                      # Uncommon
            'STRUCTURE FIRE': 0.00001,                         # Uncommon
            'MCI (MULTIPLE CASUALTY INCIDENT)': 0.000008,      # Uncommon
            'FRACTURE': 0.000005,                               # Uncommon
            'MEDICAL TRANSPORT': 0.000005,                      # Uncommon
            'TRANSFER/INTERFACILITY': 0.0003,               # Uncommon
            'TRANSFER/INTERFACILITY/PALLIATIVE': 0.0008,   # Uncommon
            'MUTUAL AID-MEDICAL': 0.002,                   # Uncommon
            'NO OTHER APPROPRIATE CHOICE': 0.002,         # Uncommon
            'OTHER': 0.0005,                                # Uncommon
            'CARDIAC ARREST - POSSIBLE DOA': 0.0007,       # Uncommon
            'ALTERED MENTAL STATUS': 0.00009,                 # Uncommon
            'FIRE STANDBY': 0.00007,                          # Uncommon
            'ASSIST OTHER AGENCY': 0.0005,                  # Uncommon
            'ALCOHOL INTOXICATION': 0.00003,                  # Uncommon
            'HAZMAT STANDBY': 0.000003,                         # Extremely rare
            'CONFINED SPACE / STRUCTURE COLLA': 0.000003,      # Extremely rare
            'VEHICLE FIRE': 0.000003,                           # Extremely rare
            'ELECTROCUTION/LIGHTNING': 0.0004,              # Uncommon
            'STABBING': 0.0008,                             # Uncommon
            'ANIMAL BITE': 0.001                           # Uncommon
        }
        
        # Select complaint based on synthetic distribution
        complaint_names = list(complaint_levels.keys())
        complaint_weights = list(complaint_levels.values())
        selected_complaint = random.choices(complaint_names, weights=complaint_weights, k=1)[0]
        
        return selected_complaint
    
    def _generate_incident_emd_performed(self):
        # EMD performed distribution
        emd_performed_levels = {
            'YES, UNKNOWN': 274258,        # Most common (89.8%)
            'YES, WITH FEEDBACK': 28135,   # Common (9.2%)
            'NOT RECORDED': 1725,          # Uncommon (0.6%)
            'NO': 285,                     # Rare (0.1%)
            'YES, WITHOUT FEEDBACK': 4     # Extremely rare (0.001%)
        }
        
        # Select EMD performed based on synthetic distribution
        emd_names = list(emd_performed_levels.keys())
        emd_weights = list(emd_performed_levels.values())
        selected_emd_performed = random.choices(emd_names, weights=emd_weights, k=1)[0]
        
        return selected_emd_performed
    
    def _generate_incident_status(self):
        """Generate realistic incident status"""
        # Incident status distribution
        status_levels = {
            'COMPLETED': 224927,        # Most common (61.1%)
            'BILLED': 144933,           # Very common (39.4%)
            'IN PROGRESS': 759,         # Uncommon (0.2%)
            'PENDING': 306,             # Uncommon (0.08%)
            'READY TO BILL': 114,       # Uncommon (0.03%)
            'REQUIRES EDIT': 59,        # Uncommon (0.02%)
            'DOUBLE EP': 12,            # Rare (0.003%)
            'FINISHED': 6,              # Rare (0.002%)
            'IMPORTED': 5               # Rare (0.001%)
        }
        
        # Select incident status based on synthetic distribution
        status_names = list(status_levels.keys())
        status_weights = list(status_levels.values())
        selected_status = random.choices(status_names, weights=status_weights, k=1)[0]
        
        return selected_status
    
    def _generate_dispatched_vs_prearrival_activation(self, dispatched_provider_type):
        """Generate realistic prearrival activation based on dispatched provider type"""
        activation_distributions = {
            'ALS': {
                'No value': 0.963, 'ADULT TRA': 0.008, 'CARDIAC AI': 0.004, 'STROKE AC': 0.009,
                'GENERAL TI': 0.001, 'STEMI ACTI': 0.003, 'PEDIATRIC': 0.0004, 'OBSTETRIC': 0.00004
            },
            'PARAMEDIC': {
                'No value': 0.965, 'ADULT TRA': 0.006, 'CARDIAC AI': 0.003, 'STROKE AC': 0.009,
                'GENERAL TI': 0.001, 'STEMI ACTI': 0.003, 'PEDIATRIC': 0.0003, 'OBSTETRIC': 0.0
            },
            'BLS': {
                'No value': 0.987, 'ADULT TRA': 0.003, 'CARDIAC AI': 0.0002, 'STROKE AC': 0.00006,
                'GENERAL TI': 0.004, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0001, 'OBSTETRIC': 0.0
            },
            'BLS-EMT': {
                'No value': 0.989, 'ADULT TRA': 0.008, 'CARDIAC AI': 0.0004, 'STROKE AC': 0.00006,
                'GENERAL TI': 0.002, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0002, 'OBSTETRIC': 0.0
            },
            'EMT': {
                'No value': 0.988, 'ADULT TRA': 0.010, 'CARDIAC AI': 0.0005, 'STROKE AC': 0.0001,
                'GENERAL TI': 0.002, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0002, 'OBSTETRIC': 0.0
            },
            'AEMT': {
                'No value': 0.989, 'ADULT TRA': 0.011, 'CARDIAC AI': 0.0006, 'STROKE AC': 0.0001,
                'GENERAL TI': 0.002, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0001, 'OBSTETRIC': 0.0
            },
            'INTERMEDIATE': {
                'No value': 0.984, 'ADULT TRA': 0.013, 'CARDIAC AI': 0.0006, 'STROKE AC': 0.0001,
                'GENERAL TI': 0.003, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0001, 'OBSTETRIC': 0.0
            },
            'BLS-EMR': {
                'No value': 0.988, 'ADULT TRA': 0.009, 'CARDIAC AI': 0.0005, 'STROKE AC': 0.0,
                'GENERAL TI': 0.002, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0, 'OBSTETRIC': 0.0
            },
            'AEMT/EMT': {
                'No value': 0.987, 'ADULT TRA': 0.013, 'CARDIAC AI': 0.0007, 'STROKE AC': 0.0,
                'GENERAL TI': 0.003, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0, 'OBSTETRIC': 0.0
            },
            'NAN': {
                'No value': 0.988, 'ADULT TRA': 0.012, 'CARDIAC AI': 0.0, 'STROKE AC': 0.0,
                'GENERAL TI': 0.004, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0, 'OBSTETRIC': 0.0
            },
            'SPECIALTY': {
                'No value': 0.988, 'ADULT TRA': 0.011, 'CARDIAC AI': 0.0, 'STROKE AC': 0.0,
                'GENERAL TI': 0.004, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0, 'OBSTETRIC': 0.0
            },
            'NURSE (IE: FLIGHT NURSE)': {
                'No value': 0.929, 'ADULT TRA': 0.071, 'CARDIAC AI': 0.0, 'STROKE AC': 0.0,
                'GENERAL TI': 0.0, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0, 'OBSTETRIC': 0.0
            },
            'PHYSICIAN (OMD)': {
                'No value': 1.0, 'ADULT TRA': 0.0, 'CARDIAC AI': 0.0, 'STROKE AC': 0.0,
                'GENERAL TI': 0.0, 'STEMI ACTI': 0.0, 'PEDIATRIC': 0.0, 'OBSTETRIC': 0.0
            }
        }
        
        # Get distribution for the dispatched provider type, default to ALS if not found
        distribution = activation_distributions.get(dispatched_provider_type, activation_distributions['ALS'])
        
        # Select prearrival activation based on dispatched provider distribution
        activation_names = list(distribution.keys())
        activation_weights = list(distribution.values())
        selected_activation = random.choices(activation_names, weights=activation_weights, k=1)[0]
        
        return selected_activation
    
    def _generate_attempted_procedures(self):
        """Generate realistic attempted procedures"""
        # Use default procedure distribution (copula removed for performance)
        
        # Default procedure generation
        procedure_raw_counts = {
            'CATHETERIZATION OF VEIN': 18079,
            'ECG, MONITORING': 15320,
            'ECG, 12 LEAD': 20760,
            'BLOOD GLUCOSE CHECK': 6351,
            'BLOOD DRAW': 1004,
            'MONITORING': 3631,
            'CERVICAL COLLAR': 756,
            'AIRWAY SUCTION': 202,
            'ASSIST VENTILATIONS VIA BVM': 211,
            'BAG VALVE MASK VENTILATION': 67,
            'CAPNOGRAPHY': 1584,
            'APPLICATION OF CERVICAL COLLAR': 172,
            'APPLICATION OF SPLINT': 47,
            'APPLICATION OF DRESSING, PRESSURE': 16,
            'CPAP': 136,
            'DEFIBRILLATION, MANUAL': 776,
            'ENDOTRACHEAL INTUBATION - VIDEO': 263,
            'OROTRACHEAL INTUBATION': 166,
            'WOUND CARE': 176,
            'IO CANNULATION': 313,
            'RAPID SEQUENCE INTUBATION (RSI)': 36,
            'ETCO2 CAPNOGRAPHY': 329,
            'VEIN, BLOOD DRAW': 2910,
            'APPLICATION OF TOURNIQUET': 4,
            'NASAL AIRWAY INSERTION (NPA)': 63,
            'ORAL AIRWAY INSERTION (OPA)': 97,
            'INSERTION OF OROPHARYNGEAL AIRWAY': 31,
            'CATHETERIZATION OF EXTERNAL JUGULAR': 14,
            'CERVICAL SPINE IMMOBILIZATION': 50,
            'CHEST COMPRESSIONS, MECHANICAL': 78,
            'CPR, MANUAL': 49,
            'DEFIBRILLATION': 266,
            'CARDIAC PACING': 10,
            'CARDIOVERSION': 3,
            'INSERTION OF NASOGASTRIC TUBE': 7,
            'INSERTION OF OROGASTRIC TUBE': 11,
            'INSERTION OF TRACHEOSTOMY TUBE': 3,
            'INTRAOSSEOUS CANNULATION': 81,
            'MECHANICAL VENTILATION (VENTILATOR)': 55,
            'MECHANICALLY ASSISTED CHEST COMPRESSIONS': 35,
            'NASOPHARYNGEAL AIRWAY INSERTION': 21,
            'NEEDLE DECOMPRESSION': 9,
            'OROGASTRIC TUBE INSERTION (OGT)': 22,
            'ORTHOSTATIC VITAL SIGNS': 1,
            'PROCEDURE_A001': 5,
            'PROCEDURE_A002': 93,
            'PACING': 20,
            'PELVIC BINDER': 4,
            'PELVIC SLING/BINDER': 14,
            'PHYSICAL RESTRAINT': 5,
            'RAPID SEQUENCE INDUCTION (RSI)': 14,
            'RECTAL TEMPERATURE': 3,
            'REMOVAL OF FOREIGN BODY FROM AIRWAY': 3,
            'RESPIRED CARBON DIOXIDE MONITORING (REGIME/THERAPY)': 26,
            'SPINAL STABILIZATION': 24,
            'SPLINTING, GENERAL': 184,
            'SPLINTING, TRACTION': 7,
            'STABILIZATION OF SPINE': 4,
            'TOURNIQUET': 15,
            'ULTRASOUND': 100,
            'ULTRASOUND PROCEDURE': 14,
            'VITAL SIGNS, ORTHOSTATIC': 20,
            'WOUND CARE (APPLICATION OF CELOX)': 1,
            'WOUND CARE (GENERAL)': 32,
            '12 LEAD ECG': 4711,
            'ACTIVE EXTERNAL COOLING': 4,
            'ACTIVE EXTERNAL WARMING': 2,
            'AIRWAY - KING (SUPRAGLOTTIC)': 5,
            'APPLICATION OF CHEMICAL HEMOSTATIC AGENTS': 2,
            'APPLICATION OF TRACTION USING A TRACTION DEVICE': 2,
            'BLOOD ADMINISTRATION': 40,
            'BURN CARE': 4,
            'CHEMICAL RESTRAINT': 2,
            'CORE TEMPERATURE MONITORING': 16,
            'CRICOTHYROIDOTOMY': 1,
            'DECONTAMINATION': 1,
            'DELAYED SEQUENCE INTUBATION (DSI)': 10,
            'DRESSING, OCCLUSIVE': 6,
            'DRESSING, PRESSURE': 43,
            'DUAL SEQUENTIAL DEFIBRILLATION': 16,
            'ENDOTRACHEAL INTUBATION - DIRECT': 5,
            'EXTERNAL JUGULAR - CATHETERIZATION': 36,
            'INTUBATION, RAPID SEQUENCE INTUBATION (RSI)': 1,
            'IRRIGATION OF EYE': 1,
            'IRRIGATION OF WOUND': 21,
            'IV, INSERTION': 3,
            'MECHANICAL ASSISTED CPR - LUCAS': 20,
            'MECHANICAL VENTILATION': 5,
            'MECHANICAL VENTILATION TITRATION': 1,
            'PROCEDURE_A003': 2,
            'OBTURATOR AIRWAY INSERTION': 1,
            'PACKED BLOOD CELL TRANSFUSION': 4,
            'PATIENT COOLING': 8,
            'PATIENT WARMING': 4,
            'PEEP': 1,
            'SUPRAGLOTTIC (EG. KING AIRWAY)': 2,
            'TOURNIQUET (JUNCTIONAL)': 1,
            'ULTRASONOGRAPHY': 4
        }
        
        # Calculate total count and convert to proportions
        total_count = sum(procedure_raw_counts.values())
        procedure_proportions = {proc: count/total_count for proc, count in procedure_raw_counts.items()}
        
        num_procedures = random.randint(1, 4)
        procedure_names = list(procedure_proportions.keys())
        procedure_weights = list(procedure_proportions.values())
        selected_procedures = random.choices(procedure_names, weights=procedure_weights, k=num_procedures)
        
        unique_procedures = list(dict.fromkeys(selected_procedures))
        
        return unique_procedures
    def _generate_successful_procedures(self, attempted_procedures):
        success_rates = {
            '12 LEAD ECG': 4616/4711,  # 98.0%
            'ACTIVE EXTERNAL COOLING': 4/4,  # 100%
            'ACTIVE EXTERNAL WARMING': 2/2,  # 100%
            'AIRWAY - KING (SUPRAGLOTTIC)': 5/5,  # 100%
            'AIRWAY SUCTION': 200/202,  # 99.0%
            'APPLICATION OF CERVICAL COLLAR': 169/172,  # 98.3%
            'APPLICATION OF CHEMICAL HEMOSTATIC AGENTS': 2/2,  # 100%
            'APPLICATION OF DRESSING, PRESSURE': 15/16,  # 93.8%
            'APPLICATION OF SPLINT': 46/47,  # 97.9%
            'APPLICATION OF TOURNIQUET': 4/4,  # 100%
            'APPLICATION OF TRACTION USING A TRACTION DEVICE': 2/2,  # 100%
            'ASSIST VENTILATIONS VIA BVM': 207/211,  # 98.1%
            'BAG VALVE MASK VENTILATION': 67/67,  # 100%
            'BLOOD ADMINISTRATION': 40/40,  # 100%
            'BLOOD DRAW': 994/1004,  # 99.0%
            'BLOOD GLUCOSE CHECK': 6293/6351,  # 99.1%
            'BURN CARE': 4/4,  # 100%
            'CAPNOGRAPHY': 1576/1584,  # 99.5%
            'CARDIAC PACING': 6/10,  # 60.0%
            'CARDIOPULMONARY RESUSCITATION': 6/7,  # 85.7%
            'CARDIOVERSION': 3/3,  # 100%
            'CATHETERIZATION OF EXTERNAL JUGULAR': 11/14,  # 78.6%
            'CATHETERIZATION OF VEIN': 13646/18079,  # 75.5%
            'CERVICAL COLLAR': 736/756,  # 97.4%
            'CERVICAL SPINE IMMOBILIZATION': 50/50,  # 100%
            'CHEMICAL RESTRAINT': 2/2,  # 100%
            'CHEST COMPRESSIONS, MECHANICAL': 69/78,  # 88.5%
            'CORE TEMPERATURE MONITORING': 14/16,  # 87.5%
            'CPAP': 134/136,  # 98.5%
            'CPR, MANUAL': 46/49,  # 93.9%
            'CRICOTHYROIDOTOMY': 1/1,  # 100%
            'DECONTAMINATION': 1/1,  # 100%
            'DEFIBRILLATION': 172/266,  # 64.7%
            'DEFIBRILLATION, MANUAL': 730/776,  # 94.1%
            'DELAYED SEQUENCE INTUBATION (DSI)': 10/10,  # 100%
            'DRESSING, OCCLUSIVE': 6/6,  # 100%
            'DRESSING, PRESSURE': 43/43,  # 100%
            'DUAL SEQUENTIAL DEFIBRILLATION': 14/16,  # 87.5%
            'ECG, 12 LEAD': 20416/20760,  # 98.3%
            'ECG, MONITORING': 15089/15320,  # 98.5%
            'ENDOTRACHEAL INTUBATION - DIRECT': 4/5,  # 80.0%
            'ENDOTRACHEAL INTUBATION - VIDEO': 242/263,  # 92.0%
            'ETCO2 CAPNOGRAPHY': 326/329,  # 99.1%
            'EXTERNAL JUGULAR - CATHETERIZATION': 25/36,  # 69.4%
            'INSERTION OF NASOGASTRIC TUBE': 7/7,  # 100%
            'INSERTION OF OROGASTRIC TUBE': 11/11,  # 100%
            'INSERTION OF OROPHARYNGEAL AIRWAY': 28/31,  # 90.3%
            'INSERTION OF TRACHEOSTOMY TUBE': 3/3,  # 100%
            'INTRAOSSEOUS CANNULATION': 79/81,  # 97.5%
            'INTUBATION, RAPID SEQUENCE INTUBATION (RSI)': 1/1,  # 100%
            'IO CANNULATION': 300/313,  # 95.8%
            'IRRIGATION OF EYE': 1/1,  # 100%
            'IRRIGATION OF WOUND': 21/21,  # 100%
            'IV, INSERTION': 2/3,  # 66.7%
            'MECHANICAL ASSISTED CPR - LUCAS': 20/20,  # 100%
            'MECHANICAL VENTILATION': 5/5,  # 100%
            'MECHANICAL VENTILATION (VENTILATOR)': 52/55,  # 94.5%
            'MECHANICAL VENTILATION TITRATION': 1/1,  # 100%
            'MECHANICALLY ASSISTED CHEST COMPRESSIONS': 34/35,  # 97.1%
            'MONITORING': 3599/3631,  # 99.1%
            'NASAL AIRWAY INSERTION (NPA)': 54/63,  # 85.7%
            'NASOPHARYNGEAL AIRWAY INSERTION': 18/21,  # 85.7%
            'NEEDLE DECOMPRESSION': 9/9,  # 100%
            'PROCEDURE_A003': 2/2,  # 100%
            'OBTURATOR AIRWAY INSERTION': 1/1,  # 100%
            'ORAL AIRWAY INSERTION (OPA)': 92/97,  # 94.8%
            'OROGASTRIC TUBE INSERTION (OGT)': 20/22,  # 90.9%
            'OROTRACHEAL INTUBATION': 157/166,  # 94.6%
            'ORTHOSTATIC VITAL SIGNS': 1/1,  # 100%
            'PROCEDURE_A001': 5/5,  # 100%
            'PROCEDURE_A002': 93/93,  # 100%
            'PACING': 17/20,  # 85.0%
            'PACKED BLOOD CELL TRANSFUSION': 4/4,  # 100%
            'PATIENT COOLING': 8/8,  # 100%
            'PATIENT WARMING': 4/4,  # 100%
            'PEEP': 1/1,  # 100%
            'PELVIC BINDER': 3/4,  # 75.0%
            'PELVIC SLING/BINDER': 14/14,  # 100%
            'PHYSICAL RESTRAINT': 5/5,  # 100%
            'RAPID SEQUENCE INDUCTION (RSI)': 14/14,  # 100%
            'RAPID SEQUENCE INTUBATION (RSI)': 35/36,  # 97.2%
            'RECTAL TEMPERATURE': 3/3,  # 100%
            'REMOVAL OF FOREIGN BODY FROM AIRWAY': 3/3,  # 100%
            'RESPIRED CARBON DIOXIDE MONITORING (REGIME/THERAPY)': 26/26,  # 100%
            'SPINAL STABILIZATION': 23/24,  # 95.8%
            'SPLINTING, GENERAL': 183/184,  # 99.5%
            'SPLINTING, TRACTION': 7/7,  # 100%
            'STABILIZATION OF SPINE': 4/4,  # 100%
            'SUPRAGLOTTIC (EG. KING AIRWAY)': 1/2,  # 50.0%
            'TOURNIQUET': 15/15,  # 100%
            'TOURNIQUET (JUNCTIONAL)': 1/1,  # 100%
            'ULTRASONOGRAPHY': 4/4,  # 100%
            'ULTRASOUND': 99/100,  # 99.0%
            'ULTRASOUND PROCEDURE': 14/14,  # 100%
            'VEIN, BLOOD DRAW': 2872/2910,  # 98.7%
            'VITAL SIGNS, ORTHOSTATIC': 20/20,  # 100%
            'WOUND CARE': 175/176,  # 99.4%
            'WOUND CARE (APPLICATION OF CELOX)': 1/1,  # 100%
            'WOUND CARE (GENERAL)': 32/32  # 100%
        }
        
        successful_procedures = []
        
        for attempted_proc in attempted_procedures:
            if attempted_proc in success_rates:
                success_rate = success_rates[attempted_proc]
                if random.random() < success_rate:
                    successful_procedures.append(attempted_proc)
            else:
                # Default success rate for unmapped procedures
                if random.random() < 0.80:
                    successful_procedures.append(attempted_proc)
        
        return successful_procedures
    
    def _generate_procedure_complications(self, attempted_procedures):
        """Generate synthetic procedure complications based on distributions"""
        procedure_complications = {
            '12 LEAD ECG': {'NONE': 0.95, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.05},
            'ACTIVE EXTERNAL COOLING': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'ACTIVE EXTERNAL WARMING': {'NONE': 0.7, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.3},
            'AIRWAY - KING (SUPRAGLOTTIC)': {'NONE': 0.6, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.4},
            'AIRWAY SUCTION': {'NONE': 0.85, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.15},
            'APPLICATION OF CERVICAL COLLAR': {'NONE': 0.9, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.1},
            'APPLICATION OF CHEMICAL HEMOSTATIC AGENTS': {'NONE': 0.5, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.5},
            'APPLICATION OF DRESSING, PRESSURE': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'APPLICATION OF SPLINT': {'NONE': 0.85, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.15},
            'APPLICATION OF TOURNIQUET': {'NONE': 0.6, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.4},
            'APPLICATION OF TRACTION USING A TRACTION DEVICE': {'NONE': 0.4, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.6},
            'ASSIST VENTILATIONS VIA BVM': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'BAG VALVE MASK VENTILATION': {'NONE': 0.75, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.25},
            'BLOOD ADMINISTRATION': {'NONE': 0.7, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.3},
            'BLOOD DRAW': {'NONE': 0.9, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.1},
            'BLOOD GLUCOSE CHECK': {'NOT_APPLICABLE': 0.99, 'OTHER': 0.0, 'RESPIRATORY': 0.0, 'VOMITING': 0.0, 'NO_VALUE': 0.01},
            'BURN CARE': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'CAPNOGRAPHY': {'NONE': 0.9, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.1},
            'CARDIAC PACING': {'NONE': 0.5, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.5},
            'CARDIOPULMONARY RESUSCITATION': {'NONE': 0.3, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.7},
            'CARDIOVERSION': {'NONE': 0.4, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.6},
            'CATHETERIZATION OF EXTERNAL JUGULAR': {'HYPOTENSION': 0.0, 'HYPOXIA': 0.0, 'INJURY': 0.0, 'NAUSEA': 0.0, 'NOT_APPLICABLE': 0.99, 'OTHER': 0.0, 'RESPIRATORY': 0.0, 'VOMITING': 0.0, 'NO_VALUE': 0.01},
            'CATHETERIZATION OF VEIN': {'HYPOTENSION': 0.0, 'HYPOXIA': 0.0, 'INJURY': 0.0, 'NAUSEA': 0.0, 'NOT_APPLICABLE': 0.99, 'OTHER': 0.0, 'RESPIRATORY': 0.0, 'VOMITING': 0.0, 'NO_VALUE': 0.01},
            'CERVICAL COLLAR': {'NONE': 0.9, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.1},
            'CERVICAL SPINE IMMOBILIZATION': {'NONE': 0.9, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.1},
            'CHEMICAL RESTRAINT': {'NONE': 0.5, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.5},
            'CHEST COMPRESSIONS, MECHANICAL': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'CORE TEMPERATURE MONITORING': {'NONE': 0.7, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.3},
            'CPAP': {'HYPOXIA': 0.0, 'INJURY': 0.0, 'RESPIRATORY': 0.0, 'VOMITING': 0.0, 'NO_VALUE': 1.0},
            'CPR, MANUAL': {'NONE': 0.6, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.4},
            'CRICOTHYROIDOTOMY': {'NONE': 0.3, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.7},
            'DECONTAMINATION': {'NONE': 0.4, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.6},
            'DEFIBRILLATION': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'DEFIBRILLATION, MANUAL': {'NONE': 0.85, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.15},
            'DELAYED SEQUENCE INTUBATION (DSI)': {'NONE': 0.5, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.5},
            'DRESSING, OCCLUSIVE': {'NONE': 0.7, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.3},
            'DRESSING, PRESSURE': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'DUAL SEQUENTIAL DEFIBRILLATION': {'NONE': 0.6, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.4},
            'ECG, 12 LEAD': {'NONE': 0.95, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.05},
            'ECG, MONITORING': {'NONE': 0.9, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.1},
            'ENDOTRACHEAL INTUBATION - DIRECT': {'INJURY': 0.0, 'NOT_APPLICABLE': 0.0, 'RESPIRATORY': 0.0, 'NO_VALUE': 1.0},
            'ENDOTRACHEAL INTUBATION - VIDEO': {'INJURY': 0.0, 'NOT_APPLICABLE': 0.0, 'RESPIRATORY': 0.0, 'NO_VALUE': 1.0},
            'ETCO2 CAPNOGRAPHY': {'INJURY': 0.0, 'NAUSEA': 0.0, 'NOT_APPLICABLE': 0.0, 'RESPIRATORY': 0.0, 'VOMITING': 0.0, 'NO_VALUE': 1.0},
            'EXTERNAL JUGULAR - CATHETERIZATION': {'HYPOTENSION': 0.0, 'HYPOXIA': 0.0, 'INJURY': 0.0, 'NAUSEA': 0.0, 'NOT_APPLICABLE': 0.99, 'OTHER': 0.0, 'RESPIRATORY': 0.0, 'VOMITING': 0.0, 'NO_VALUE': 0.01},
            'INSERTION OF NASOGASTRIC TUBE': {'NONE': 0.6, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.4},
            'INSERTION OF OROGASTRIC TUBE': {'NONE': 0.7, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.3},
            'INSERTION OF OROPHARYNGEAL AIRWAY': {'NONE': 0.8, 'NOT_APPLICABLE': 0.0, 'NOT_RECORDED': 0.0, 'OTHER': 0.0, 'NO_VALUE': 0.2},
            'INSERTION OF TRACHEOSTOMY TUBE': {'NONE': 3, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'INTRAOSSEOUS CANNULATION': {'NONE': 81, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'INTUBATION, RAPID SEQUENCE INTUBATION (RSI)': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'IO CANNULATION': {'NONE': 313, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'IRRIGATION OF EYE': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'IRRIGATION OF WOUND': {'NONE': 21, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'IV, INSERTION': {'NONE': 3, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'MECHANICAL ASSISTED CPR - LUCAS': {'NONE': 20, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'MECHANICAL VENTILATION': {'NONE': 5, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'MECHANICAL VENTILATION (VENTILATOR)': {'NONE': 55, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'MECHANICAL VENTILATION TITRATION': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'MECHANICALLY ASSISTED CHEST COMPRESSIONS': {'NONE': 35, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'MONITORING': {'NOT_APPLICABLE': 3631, 'NO_VALUE': 0},
            'NASAL AIRWAY INSERTION (NPA)': {'NONE': 63, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'NASOPHARYNGEAL AIRWAY INSERTION': {'NONE': 21, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'NEEDLE DECOMPRESSION': {'NONE': 9, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PROCEDURE_A003': {'NONE': 2, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'OBTURATOR AIRWAY INSERTION': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'ORAL AIRWAY INSERTION (OPA)': {'NONE': 97, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'OROGASTRIC TUBE INSERTION (OGT)': {'NONE': 22, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'OROTRACHEAL INTUBATION': {'NONE': 166, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'ORTHOSTATIC VITAL SIGNS': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PROCEDURE_A001': {'NONE': 5, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PROCEDURE_A002': {'NONE': 93, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PACING': {'NONE': 20, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PACKED BLOOD CELL TRANSFUSION': {'NONE': 4, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PATIENT COOLING': {'NONE': 8, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PATIENT WARMING': {'NONE': 4, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PEEP': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PELVIC BINDER': {'NONE': 4, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PELVIC SLING/BINDER': {'NONE': 14, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'PHYSICAL RESTRAINT': {'NONE': 5, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'RAPID SEQUENCE INDUCTION (RSI)': {'NONE': 14, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'RAPID SEQUENCE INTUBATION (RSI)': {'NONE': 36, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'RECTAL TEMPERATURE': {'INJURY': 1, 'NOT_APPLICABLE': 35, 'NO_VALUE': 0},
            'REMOVAL OF FOREIGN BODY FROM AIRWAY': {'NONE': 3, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'RESPIRED CARBON DIOXIDE MONITORING (REGIME/THERAPY)': {'NONE': 26, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'SPINAL STABILIZATION': {'NONE': 24, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'SPLINTING, GENERAL': {'NONE': 184, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'SPLINTING, TRACTION': {'NONE': 7, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'STABILIZATION OF SPINE': {'NONE': 4, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'SUPRAGLOTTIC (EG. KING AIRWAY)': {'NONE': 2, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'TOURNIQUET': {'NONE': 15, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'TOURNIQUET (JUNCTIONAL)': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'ULTRASONOGRAPHY': {'NONE': 4, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'ULTRASOUND': {'NONE': 100, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'ULTRASOUND PROCEDURE': {'NONE': 14, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'VEIN, BLOOD DRAW': {'NONE': 2910, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'VITAL SIGNS, ORTHOSTATIC': {'NONE': 20, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'WOUND CARE': {'NONE': 176, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'WOUND CARE (APPLICATION OF CELOX)': {'NONE': 1, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0},
            'WOUND CARE (GENERAL)': {'NONE': 32, 'NOT_APPLICABLE': 0, 'NOT_RECORDED': 0, 'OTHER': 0, 'NO_VALUE': 0}
        }
        
        procedure_complications_list = []
        
        for procedure in attempted_procedures:
            if procedure in procedure_complications:
                complications_dist = procedure_complications[procedure]
                complication_names = list(complications_dist.keys())
                complication_weights = list(complications_dist.values())
                
                # Select one complication based on the distribution
                selected_complication = random.choices(complication_names, weights=complication_weights, k=1)[0]
                procedure_complications_list.append({
                    'procedure': procedure,
                    'complication': selected_complication
                })
            else:
                # Default: no complication
                procedure_complications_list.append({
                    'procedure': procedure,
                    'complication': 'NONE'
                })
        
        return procedure_complications_list
    
    def _generate_primary_unit_role(self):
        """Generate realistic primary unit role"""
        # Primary unit role distribution
        unit_role_levels = {
            'GROUND TRANSPORT': 246947,
            'FIRE APPARATUS, NON-TRANSPORT ASSISTANCE': 53303,
            'FIRE APPARATUS, BLS (NON-TRANSPORT)': 20610,
            'FIRE APPARATUS': 19076,
            'NON-TRANSPORT ADMINISTRATIVE (SUPERVISOR)': 20000,
            'COMMAND/EMS SUPERVISOR': 3848,
            'NON-TRANSPORT RESCUE': 4185,
            'FIRE APPARATUS, ALS (NON-TRANSPORT)': 845,
            'RESCUE': 614,
            'ALS CHASE': 203
        }
        
        # Select primary unit role based on synthetic distribution
        role_names = list(unit_role_levels.keys())
        role_weights = list(unit_role_levels.values())
        selected_unit_role = random.choices(role_names, weights=role_weights, k=1)[0]
        
        return selected_unit_role
    
    def generate_incidents_batch(self, num_incidents: int, num_processes: int = None):
        """Generate multiple incidents using parallel processing"""
        if num_processes is None:
            num_processes = min(mp.cpu_count(), 4)  # Limit to 4 processes max
        
        if num_incidents < num_processes * 10:
            # For small batches, use sequential processing
            return self._generate_incidents_sequential(num_incidents)
        
        # Split work among processes
        incidents_per_process = num_incidents // num_processes
        remainder = num_incidents % num_processes
        
        # Create process arguments
        process_args = []
        for i in range(num_processes):
            count = incidents_per_process + (1 if i < remainder else 0)
            if count > 0:
                process_args.append(count)
        
        # Generate incidents in parallel
        with mp.Pool(processes=num_processes) as pool:
            results = pool.map(self._generate_incidents_worker, process_args)
        
        # Flatten results
        all_incidents = []
        for incident_batch in results:
            all_incidents.extend(incident_batch)
        
        return all_incidents
    
    @staticmethod
    def _generate_incidents_worker(num_incidents: int):
        """Worker function for parallel incident generation"""
        # Create a new generator instance for each process
        generator = EMSDataGenerator()
        incidents = []
        
        for _ in range(num_incidents):
            incident = generator.generate_ems_incident()
            incidents.append(incident)
        
        return incidents
    
    def _generate_incidents_sequential(self, num_incidents: int):
        """Generate incidents sequentially (for small batches)"""
        incidents = []
        for _ in range(num_incidents):
            incident = self.generate_ems_incident()
            incidents.append(incident)
        return incidents
    def _choose_ems_incident_type(self, dt):
        """Choose EMS incident type based on synthetic frequency and time patterns"""
        # Synthetic EMS codes with realistic frequency weights based on distributions
        base = {
            '2301061': 14,  # Sick Person - most common
            '2301013': 12,  # Breathing Problem
            '2301021': 11,  # Chest Pain (Non-Traumatic)
            '2301033': 13,  # Falls
            '2301001': 7,   # Abdominal Pain/Problems
            '2301025': 6,   # Convulsions/Seizure
            '2301059': 6,   # Psychiatric Problem/Abnormal Behavior/Suicide Attempt
            '2301053': 5,   # Overdose/Poisoning/Ingestion
            '2301073': 6,   # Traumatic Injury
            '2301067': 4,   # Stroke/CVA
            '2301077': 5,   # Unconscious/Fainting/Near-Fainting
            '2301027': 4,   # Diabetic Problem
            '2301003': 3,   # Allergic Reaction/Stings
            '2301057': 2,   # Pregnancy/Childbirth/Miscarriage
            '2301019': 2,   # Cardiac Arrest/Death
            '2301063': 2,   # Stab/Gunshot Wound/Penetrating Trauma
            '2301045': 2,   # Hemorrhage/Laceration
            '2301043': 1,   # Heat/Cold Exposure
            '2301069': 3,   # Traffic/Transportation Incident
            '2301007': 2,   # Assault
            '2301035': 1,   # Fire
            '2301005': 1,   # Animal Bite
            '2301023': 1,   # Choking
            '2301037': 2,   # Headache
            '2301011': 2,   # Back Pain (Non-Traumatic)
        }
        hour = dt.hour
        wknd = dt.weekday() >= 5

        mult = {k: 1.0 for k in base}
        
        # Enhanced time of day patterns
        if 22 <= hour or hour < 5:  # Night time (10 PM - 5 AM)
            mult['2301019'] *= 2.5  # Much more cardiac arrests at night
            mult['2301053'] *= 2.0  # More overdoses at night
            mult['2301077'] *= 1.8  # More unconscious/fainting at night
            mult['2301007'] *= 2.2  # More assaults at night
            mult['2301059'] *= 1.6  # More psychiatric problems at night
            mult['2301069'] *= 1.4  # More traffic incidents at night
        elif 6 <= hour <= 10:  # Morning rush (6 AM - 10 AM)
            mult['2301021'] *= 1.6  # More chest pain in morning
            mult['2301069'] *= 2.0  # Much more traffic incidents in morning
            mult['2301033'] *= 1.4  # More falls in morning
            mult['2301013'] *= 1.3  # More breathing problems in morning
        elif 17 <= hour <= 20:  # Evening rush (5 PM - 8 PM)
            mult['2301069'] *= 2.5  # Much more traffic incidents in evening
            mult['2301073'] *= 1.5  # More traumatic injuries in evening
            mult['2301007'] *= 1.7  # More assaults in evening
        elif 7 <= hour <= 19:  # Day time (7 AM - 7 PM)
            mult['2301033'] *= 1.3  # More falls during day
            mult['2301021'] *= 1.2  # More chest pain during day
            mult['2301013'] *= 1.2  # More breathing problems during day
            mult['2301001'] *= 1.25  # More abdominal pain during day
        
        # Enhanced weekend patterns
        if wknd:
            mult['2301053'] *= 1.5  # More overdoses on weekends
            mult['2301059'] *= 1.4  # More psychiatric problems on weekends
            mult['2301073'] *= 1.6  # More traumatic injuries on weekends
            mult['2301033'] *= 1.3  # More falls on weekends (leisure activities)
            mult['2301069'] *= 1.8  # More traffic incidents on weekends
            mult['2301007'] *= 1.5  # More assaults on weekends
        else:  # Weekdays
            mult['2301021'] *= 1.3  # More chest pain on weekdays
            mult['2301013'] *= 1.2  # More breathing problems on weekdays
            mult['2301061'] *= 1.1  # More sick person calls on weekdays
        
        # Seasonal patterns (based on month)
        month = dt.month
        if month in [12, 1, 2]:  # Winter
            mult['2301043'] *= 4.0  # Much more cold exposure in winter
            mult['2301033'] *= 1.4  # More falls in winter (ice/snow)
            mult['2301073'] *= 1.3  # More traumatic injuries in winter
            mult['2301021'] *= 1.2  # More chest pain in winter
        elif month in [6, 7, 8]:  # Summer
            mult['2301043'] *= 3.0  # Much more heat exposure in summer
            mult['2301003'] *= 1.6  # More allergic reactions in summer
            mult['2301057'] *= 1.3  # More pregnancy/childbirth in summer
            mult['2301073'] *= 1.2  # More traumatic injuries in summer (outdoor activities)
        elif month in [3, 4, 5]:  # Spring
            mult['2301003'] *= 1.8  # More allergic reactions in spring
            mult['2301025'] *= 1.3  # More seizures in spring (pollen/allergies)
        elif month in [9, 10, 11]:  # Fall
            mult['2301003'] *= 1.4  # Some allergic reactions in fall
            mult['2301033'] *= 1.2  # Slightly more falls in fall

        codes = list(base.keys())
        weights = [base[c] * mult[c] for c in codes]
        selected_code = random.choices(codes, weights=weights, k=1)[0]
        return selected_code, EMS_INCIDENT_CODES[selected_code]
    
    def _generate_medical_history(self, age, ethnicity, gender):
        """Generate realistic medical history based on demographics"""
        medical_conditions = []
        
        # Age-based conditions
        if age > 65:
            if random.random() < 0.4: medical_conditions.append("Hypertension")
            if random.random() < 0.3: medical_conditions.append("Diabetes")
            if random.random() < 0.25: medical_conditions.append("Heart Disease")
            if random.random() < 0.2: medical_conditions.append("COPD")
        
        elif age > 45:
            if random.random() < 0.25: medical_conditions.append("Hypertension")
            if random.random() < 0.15: medical_conditions.append("Diabetes")
            if random.random() < 0.1: medical_conditions.append("Heart Disease")
        
        # Gender-specific conditions
        if gender == 'F':
            if age > 35 and random.random() < 0.1: medical_conditions.append("Pregnancy")
            if age > 40 and random.random() < 0.05: medical_conditions.append("Breast Cancer")
        
        # Ethnicity-based conditions (some conditions are more common in certain ethnicities)
        if ethnicity in ['Black', 'African American']:
            if random.random() < 0.15: medical_conditions.append("Sickle Cell Disease")
        
        if ethnicity in ['Hispanic', 'Latino']:
            if random.random() < 0.1: medical_conditions.append("Diabetes")  # Higher diabetes risk
        
        return medical_conditions

    def _influence_incident_by_medical_history(self, medical_conditions, incident_type_code):
        """Influence incident type based on patient's medical history"""
        if not medical_conditions:
            return incident_type_code
        
        # Medical history can influence incident types
        if "Heart Disease" in medical_conditions and random.random() < 0.3:
            return '2301021'  # Chest Pain (Non-Traumatic)
        elif "Diabetes" in medical_conditions and random.random() < 0.2:
            return '2301027'  # Diabetic Problem
        elif "COPD" in medical_conditions and random.random() < 0.25:
            return '2301013'  # Breathing Problem
        elif "Pregnancy" in medical_conditions and random.random() < 0.15:
            return '2301057'  # Pregnancy/Childbirth/Miscarriage
        elif "Sickle Cell Disease" in medical_conditions and random.random() < 0.2:
            return '2301001'  # Abdominal Pain/Problems (sickle cell crisis)
        
        return incident_type_code

    def _generate_realistic_weight(self, age, gender, race):
        """Generate realistic weight based on age and gender with proper BMI considerations"""
        
        # First generate a target BMI based on synthetic distributions
        if age < 2:
            # Infants - use weight-for-age percentiles
            if gender == 'M':
                target_weight = random.uniform(18, 32)  # 18-32 lbs for infants
            else:
                target_weight = random.uniform(16, 30)  # 16-30 lbs for infant girls
        elif age < 5:
            # Toddlers
            if gender == 'M':
                target_weight = random.uniform(25, 45)  # 25-45 lbs for toddlers
            else:
                target_weight = random.uniform(24, 44)  # 24-44 lbs for toddler girls
        elif age < 12:
            # Children
            if gender == 'M':
                target_weight = random.uniform(35, 85)  # 35-85 lbs for boys
            else:
                target_weight = random.uniform(32, 80)  # 32-80 lbs for girls
        elif age < 18:
            # Teenagers - more variation
            if gender == 'M':
                target_weight = random.uniform(90, 180)  # 90-180 lbs for teen boys
            else:
                target_weight = random.uniform(80, 160)  # 80-160 lbs for teen girls
        else:
            # Adults - use BMI-based approach with race considerations
            
            # Generate realistic BMI first (18.5-40+ range)
            # Most adults are in 20-30 BMI range, but we need to include extremes
            bmi_categories = ['UNDERWEIGHT', 'NORMAL', 'OVERWEIGHT', 'OBESE_CLASS_1', 'OBESE_CLASS_2', 'OBESE_CLASS_3']
            bmi_weights = [0.03, 0.35, 0.35, 0.20, 0.05, 0.02]  # Synthetic distribution
            selected_category = random.choices(bmi_categories, weights=bmi_weights, k=1)[0]
            
            if selected_category == 'UNDERWEIGHT':
                target_bmi = random.uniform(16.0, 18.4)
            elif selected_category == 'NORMAL':
                target_bmi = random.uniform(18.5, 24.9)
            elif selected_category == 'OVERWEIGHT':
                target_bmi = random.uniform(25.0, 29.9)
            elif selected_category == 'OBESE_CLASS_1':
                target_bmi = random.uniform(30.0, 34.9)
            elif selected_category == 'OBESE_CLASS_2':
                target_bmi = random.uniform(35.0, 39.9)
            else:  # OBESE_CLASS_3
                target_bmi = random.uniform(40.0, 55.0)
            
            # Generate realistic height for adults (used for BMI calculation)
            if gender == 'M':
                height_inches = random.uniform(64, 75)  # 5'4" to 6'3" for adult males
            else:
                height_inches = random.uniform(58, 69)  # 4'10" to 5'9" for adult females
            
            # Calculate weight from BMI: Weight = BMI × (Height in inches)² / 703
            target_weight = target_bmi * (height_inches ** 2) / 703
            
            # Add some natural variation (±3 lbs)
            target_weight += random.uniform(-3, 3)
        
        # Ensure weight is within reasonable bounds and round to 1 decimal
        if age < 18:
            min_weight = max(15, target_weight * 0.8)  # Allow some variation
            max_weight = target_weight * 1.2
        else:
            min_weight = max(90, target_weight * 0.9)  # Adults should be at least 90 lbs, allow 10% variation down
            max_weight = target_weight * 1.1  # Allow 10% variation up
        
        final_weight = max(min_weight, min(max_weight, target_weight))
        return round(final_weight, 1)

    def _generate_demographic_appropriate_name(self, ethnicity, gender):
        """Generate names that match ethnicity and gender"""
        # Define name pools by ethnicity and gender
        names_by_ethnicity_gender = {
            'WHITE': {
                'M': {
                    'first': ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Christopher', 'Charles', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Kenneth'],
                    'last': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
                },
                'F': {
                    'first': ['Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Nancy', 'Lisa', 'Betty', 'Helen', 'Sandra', 'Donna', 'Carol', 'Ruth', 'Sharon', 'Michelle'],
                    'last': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
                }
            },
            'BLACK': {
                'M': {
                    'first': ['James', 'Michael', 'David', 'Robert', 'William', 'Richard', 'Joseph', 'Christopher', 'Charles', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Kenneth', 'Joshua', 'Kevin'],
                    'last': ['Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'Martin', 'Thompson', 'White', 'Harris', 'Clark', 'Lewis', 'Robinson', 'Walker']
                },
                'F': {
                    'first': ['Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Nancy', 'Lisa', 'Betty', 'Helen', 'Sandra', 'Donna', 'Carol', 'Ruth', 'Sharon', 'Michelle'],
                    'last': ['Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'Martin', 'Thompson', 'White', 'Harris', 'Clark', 'Lewis', 'Robinson', 'Walker']
                }
            },
            'HISPANIC': {
                'M': {
                    'first': ['Jose', 'Luis', 'Carlos', 'Miguel', 'Rafael', 'Antonio', 'Francisco', 'Manuel', 'Jesus', 'Pedro', 'Angel', 'Alejandro', 'Roberto', 'Fernando', 'Sergio', 'Daniel', 'Ricardo', 'Eduardo', 'Andres', 'Gabriel'],
                    'last': ['Garcia', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Perez', 'Sanchez', 'Ramirez', 'Cruz', 'Flores', 'Rivera', 'Gomez', 'Diaz', 'Reyes', 'Morales', 'Jimenez', 'Alvarez', 'Ruiz', 'Mendoza']
                },
                'F': {
                    'first': ['Maria', 'Ana', 'Carmen', 'Rosa', 'Isabel', 'Elena', 'Patricia', 'Monica', 'Alejandra', 'Sofia', 'Valentina', 'Camila', 'Natalia', 'Andrea', 'Paula', 'Fernanda', 'Gabriela', 'Daniela', 'Valeria', 'Ximena'],
                    'last': ['Garcia', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Perez', 'Sanchez', 'Ramirez', 'Cruz', 'Flores', 'Rivera', 'Gomez', 'Diaz', 'Reyes', 'Morales', 'Jimenez', 'Alvarez', 'Ruiz', 'Mendoza']
                }
            },
            'ASIAN': {
                'M': {
                    'first': ['Wei', 'Ming', 'Jian', 'Hao', 'Chen', 'David', 'Michael', 'Kevin', 'Jason', 'Eric', 'Daniel', 'Andrew', 'Steven', 'Ryan', 'Brian', 'John', 'James', 'Robert', 'William', 'Richard'],
                    'last': ['Wang', 'Li', 'Zhang', 'Liu', 'Chen', 'Yang', 'Huang', 'Zhao', 'Wu', 'Zhou', 'Xu', 'Sun', 'Ma', 'Zhu', 'Hu', 'Guo', 'He', 'Gao', 'Lin', 'Luo', 'Kim', 'Lee', 'Park', 'Choi', 'Jung', 'Kang', 'Yoon', 'Jang', 'Lim', 'Han']
                },
                'F': {
                    'first': ['Mei', 'Ling', 'Xia', 'Hui', 'Jennifer', 'Michelle', 'Amy', 'Lisa', 'Sarah', 'Grace', 'Linda', 'Karen', 'Nancy', 'Helen', 'Mary', 'Patricia', 'Elizabeth', 'Barbara', 'Susan', 'Jessica'],
                    'last': ['Wang', 'Li', 'Zhang', 'Liu', 'Chen', 'Yang', 'Huang', 'Zhao', 'Wu', 'Zhou', 'Xu', 'Sun', 'Ma', 'Zhu', 'Hu', 'Guo', 'He', 'Gao', 'Lin', 'Luo', 'Kim', 'Lee', 'Park', 'Choi', 'Jung', 'Kang', 'Yoon', 'Jang', 'Lim', 'Han']
                }
            },
            'OTHER': {
                'M': {
                    'first': ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Christopher', 'Charles', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Steven', 'Paul', 'Andrew', 'Kenneth'],
                    'last': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
                },
                'F': {
                    'first': ['Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty', 'Helen', 'Sandra', 'Donna', 'Carol', 'Ruth', 'Sharon', 'Michelle'],
                    'last': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
                }
            }
        }
        
        # Get name pools for the ethnicity and gender
        if ethnicity in names_by_ethnicity_gender and gender in names_by_ethnicity_gender[ethnicity]:
            name_pools = names_by_ethnicity_gender[ethnicity][gender]
            first_name = random.choice(name_pools['first'])
            last_name = random.choice(name_pools['last'])
        else:
            # Fallback to generic names
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
        
        return f"{first_name} {last_name}"

    def _assign_ems_unit_by_location(self, city, zip_code):
        """Assign EMS unit based on location"""
        # Simple unit assignment based on city/zip patterns
        if city == 'Seattle':
            return f"EMS-{random.randint(1, 25)}"
        elif city in ['Redmond', 'Kirkland', 'Sammamish']:
            return f"EMS-{random.randint(26, 35)}"
        else:
            return f"EMS-{random.randint(36, 50)}"
    
    def _generate_king_county_address(self):
        """Generate synthetic addresses using synthetic Seattle/King County coordinates."""
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
        import time
        # Synthetic Seattle/King County coordinate bounds (LAND-ONLY to avoid water bodies)
        seattle_bounds = {
            'seattle': {
                'lat_range': (47.5000, 47.7200),  # excludes extreme north/south water areas
                'lon_range': (-122.4200, -122.2500),  # excludes Puget Sound (-122.46+) and Lake Washington (-122.22-)
                'zip_codes': ['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109', '98112', '98115', '98116', '98117', '98118', '98119', '98121', '98122', '98125', '98126', '98133', '98134', '98136', '98144', '98146', '98154', '98164', '98177', '98178', '98195']
            },
            'redmond': {
                'lat_range': (47.6698, 47.7001),
                'lon_range': (-122.1616, -122.1016),
                'zip_codes': ['98052', '98053']
            },
            'kirkland': {
                'lat_range': (47.6604, 47.7197),
                'lon_range': (-122.2449, -122.1539),
                'zip_codes': ['98033', '98034']
            },
            'sammamish': {
                'lat_range': (47.6009, 47.6549),
                'lon_range': (-122.0806, -122.0206),
                'zip_codes': ['98074', '98075']
            },
            'issaquah': {
                'lat_range': (47.5301, 47.5701),
                'lon_range': (-122.1206, -122.0606),
                'zip_codes': ['98027', '98029']
            },
            'mercer_island': {
                'lat_range': (47.5604, 47.6004),
                'lon_range': (-122.2249, -122.2049),
                'zip_codes': ['98040']
            },
            'renton': {
                'lat_range': (47.4801, 47.5201),
                'lon_range': (-122.2406, -122.1806),
                'zip_codes': ['98055', '98056', '98057', '98058']
            },
            'shoreline': {
                'lat_range': (47.7504, 47.7904),
                'lon_range': (-122.3606, -122.3006),
                'zip_codes': ['98155', '98177']
            },
            'bothell': {
                'lat_range': (47.7604, 47.8004),
                'lon_range': (-122.2206, -122.1606),
                'zip_codes': ['98011', '98012', '98021']
            },
            'kenmore': {
                'lat_range': (47.7504, 47.7904),
                'lon_range': (-122.2606, -122.2006),
                'zip_codes': ['98028']
            },
            'newcastle': {
                'lat_range': (47.5301, 47.5701),
                'lon_range': (-122.1606, -122.1206),
                'zip_codes': ['98056']
            },
            'seatac': {
                'lat_range': (47.4401, 47.4801),
                'lon_range': (-122.3206, -122.2606),
                'zip_codes': ['98158', '98188']
            },
            'tukwila': {
                'lat_range': (47.4601, 47.5001),
                'lon_range': (-122.2806, -122.2206),
                'zip_codes': ['98168']
            },
            'woodinville': {
                'lat_range': (47.7504, 47.7904),
                'lon_range': (-122.1606, -122.1006),
                'zip_codes': ['98072']
            },
            'burien': {
                'lat_range': (47.4601, 47.5001),
                'lon_range': (-122.3606, -122.3006),
                'zip_codes': ['98146']
            }
        }
        
        # Select a random city/area
        city_name = random.choice(list(seattle_bounds.keys()))
        city_data = seattle_bounds[city_name]
        
        # Generate realistic coordinates within the city bounds
        lat = random.uniform(city_data['lat_range'][0], city_data['lat_range'][1])
        lon = random.uniform(city_data['lon_range'][0], city_data['lon_range'][1])
        
        # Select a random zip code for this city
        zip_code = random.choice(city_data['zip_codes'])
        

        # ---- check to see whether we want to geocode all addresses, possible we just want to geocode a portion of the addresses ---- #
        # Try to reverse geocode to get a real address (only 10% of the time for speed)
        if random.random() < 0.1:  # Only geocode 10% of addresses for speed
            try:
                geolocator = Nominatim(user_agent="seattle_data_generator")
                location = geolocator.reverse(f"{lat}, {lon}", timeout=5)
                
                if location and location.address:
                    # Parse the address components
                    address_parts = location.address.split(', ')
                    if len(address_parts) >= 2:
                        street_address = address_parts[0]
                        city_state_zip = address_parts[-2] if len(address_parts) >= 3 else "Seattle, WA"
                        
                        # Extract city and state
                        if ',' in city_state_zip:
                            city_part = city_state_zip.split(',')[0]
                            state = "WA"
                        else:
                            city_part = city_name.replace('_', ' ').title()
                            state = "WA"
                        
                        return street_address, city_part, state, zip_code
            except (GeocoderTimedOut, GeocoderUnavailable, Exception):
                # Fallback to coordinate-based fake address if geocoding fails
                pass
        
        # Fallback: Generate a realistic-looking address based on coordinates
        street_number = random.randint(100, 9999)
        street_suffixes = ['Ave', 'St', 'Blvd', 'Way', 'Dr', 'Pl', 'Ln']
        
        # Choose street name based on coordinate patterns
        if lat > 47.6:  # North Seattle
            street_name = f"{random.choice(['Aurora', 'Greenwood', 'Broadway', 'Roosevelt', 'Stone'])} {random.choice(street_suffixes)}"
        elif lat < 47.5:  # South Seattle
            street_name = f"{random.choice(['Rainier', 'MLK', 'Beacon', 'Delridge', 'California'])} {random.choice(street_suffixes)}"
        else:  # Central Seattle
            street_name = f"{random.choice(['Pike', 'Pine', 'Madison', 'Union', 'Broadway'])} {random.choice(street_suffixes)}"
        
        # Add directional prefix based on longitude
        if lon > -122.3:  # East side
            if random.random() < 0.3:
                street_name = f"E {street_name}"
        elif lon < -122.35:  # West side
            if random.random() < 0.3:
                street_name = f"W {street_name}"
        
        full_address = f"{street_number} {street_name}"
        city_name_clean = city_name.replace('_', ' ').title()
        
        return full_address, city_name_clean, "WA", zip_code
    
    def _generate_fast_address(self):
        """Generate realistic addresses instantly without any geocoding"""
        # Pre-defined realistic Seattle/King County street names and cities
        seattle_streets = [
            'Aurora Ave N', 'Broadway', 'Pike St', 'Pine St', 'Madison St', 'Union St',
            'Rainier Ave S', 'MLK Jr Way', 'Beacon Ave S', 'Delridge Way SW', 'California Ave SW',
            'Greenwood Ave N', 'Roosevelt Way NE', 'Stone Way N', '15th Ave NW', '24th Ave NW',
            'University Way NE', 'The Ave', 'Capitol Hill Blvd', 'Queen Anne Ave N',
            'Westlake Ave N', 'Eastlake Ave E', 'Lake City Way NE', 'Sand Point Way NE',
            'Montlake Blvd E', 'Boylston Ave E', 'Interlaken Blvd E', '10th Ave E',
            '23rd Ave E', '31st Ave E', 'Madrona Dr', 'Leschi Blvd', 'Yesler Way',
            'Jackson St', 'Cherry St', 'Spring St', 'Seneca St', 'University St',
            'Stewart St', 'Denny Way', 'Republican St', 'Roy St', 'Aloha St',
            'Prospect St', 'Galer St', 'Boylston Ave E', 'Interlaken Blvd E'
        ]
        
        king_county_cities = [
            'Seattle', 'Redmond', 'Kirkland', 'Sammamish', 'Issaquah', 'Bellevue',
            'Bothell', 'Kenmore', 'Newcastle', 'Seatac', 'Tukwila', 'Woodinville',
            'Burien', 'Renton', 'Kent', 'Auburn', 'Federal Way', 'Des Moines',
            'Shoreline', 'Edmonds', 'Lynnwood', 'Mountlake Terrace', 'Brier',
            'Mukilteo', 'Everett', 'Marysville', 'Arlington', 'Granite Falls'
        ]
        
        king_county_zips = [
            '98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109',
            '98112', '98115', '98116', '98117', '98118', '98119', '98121', '98122', '98125',
            '98126', '98133', '98134', '98136', '98144', '98146', '98154', '98164', '98177',
            '98178', '98195', '98052', '98053', '98033', '98034', '98074', '98075', '98027',
            '98029', '98004', '98005', '98006', '98007', '98008', '98009', '98011', '98012',
            '98021', '98028', '98056', '98158', '98188', '98168', '98072', '98032', '98040',
            '98042', '98057', '98058', '98059', '98092', '98148', '98166', '98188', '98198'
        ]
        
        # Generate address components
        street_number = random.randint(100, 9999)
        street_name = random.choice(seattle_streets)
        city = random.choice(king_county_cities)
        zip_code = random.choice(king_county_zips)
        
        # Add apartment number occasionally
        if random.random() < 0.15:  # 15% chance of apartment
            apartment_number = f"Apt {random.randint(1, 500)}"
            full_address = f"{street_number} {street_name}, {apartment_number}"
        else:
            full_address = f"{street_number} {street_name}"
        
        return full_address, city, "WA", zip_code
    
    def _generate_optimized_geocoded_address(self):
        """Generate real geocoded addresses with optimizations for speed"""
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
        import time
        
        # Synthetic Seattle/King County coordinate bounds (LAND-ONLY to avoid water bodies)
        seattle_bounds = {
            'seattle': {
                'lat_range': (47.5000, 47.7200),
                'lon_range': (-122.4200, -122.2500),
                'zip_codes': ['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109', '98112', '98115', '98116', '98117', '98118', '98119', '98121', '98122', '98125', '98126', '98133', '98134', '98136', '98144', '98146', '98154', '98164', '98177', '98178', '98195']
            },
            'bellevue': {
                'lat_range': (47.5800, 47.6300),
                'lon_range': (-122.2000, -122.1400),
                'zip_codes': ['98004', '98005', '98006', '98007', '98008', '98009']
            },
            'redmond': {
                'lat_range': (47.6698, 47.7001),
                'lon_range': (-122.1616, -122.1016),
                'zip_codes': ['98052', '98053']
            },
            'kirkland': {
                'lat_range': (47.6604, 47.7197),
                'lon_range': (-122.2449, -122.1539),
                'zip_codes': ['98033', '98034']
            },
            'renton': {
                'lat_range': (47.4700, 47.5100),
                'lon_range': (-122.2200, -122.1600),
                'zip_codes': ['98055', '98056', '98057', '98058', '98059']
            }
        }
        
        # Select a random city/area
        city_name = random.choice(list(seattle_bounds.keys()))
        city_data = seattle_bounds[city_name]
        
        # Generate realistic coordinates within the city bounds
        lat = random.uniform(city_data['lat_range'][0], city_data['lat_range'][1])
        lon = random.uniform(city_data['lon_range'][0], city_data['lon_range'][1])
        
        # Select a random zip code for this city
        zip_code = random.choice(city_data['zip_codes'])
        
        # Check cache first for this coordinate
        coord_key = f"{lat:.4f},{lon:.4f}"
        if coord_key in self._geocoding_cache:
            return self._geocoding_cache[coord_key]
        
        # Rate limiting to avoid overwhelming the geocoding service
        import time
        current_time = time.time()
        time_since_last = current_time - self._last_geocoding_time
        if time_since_last < self._geocoding_rate_limit:
            time.sleep(self._geocoding_rate_limit - time_since_last)
        
        # Try to reverse geocode with optimized settings
        try:
            self._last_geocoding_time = time.time()
            # Use a more efficient geocoder setup
            geolocator = Nominatim(
                user_agent="seattle_ems_generator",
                timeout=3,  # Reduced timeout
                domain="nominatim.openstreetmap.org"
            )
            
            # Try reverse geocoding
            location = geolocator.reverse(f"{lat}, {lon}", timeout=3, exactly_one=True)
            
            if location and location.address:
                # Parse the address components
                address_parts = location.address.split(', ')
                if len(address_parts) >= 2:
                    street_address = address_parts[0]
                    city_state_zip = address_parts[-2] if len(address_parts) >= 3 else "Seattle, WA"
                    
                    # Extract city and state
                    if ',' in city_state_zip:
                        city_part = city_state_zip.split(',')[0]
                        state = "WA"
                    else:
                        city_part = city_name.replace('_', ' ').title()
                        state = "WA"
                    
                    result = (street_address, city_part, state, zip_code)
                    # Cache the successful result
                    self._geocoding_cache[coord_key] = result
                    return result
                    
        except (GeocoderTimedOut, GeocoderUnavailable, Exception) as e:
            # If geocoding fails, fall back to coordinate-based realistic address
            pass
        
        # Fallback: Generate a realistic-looking address based on coordinates
        street_number = random.randint(100, 9999)
        street_suffixes = ['Ave', 'St', 'Blvd', 'Way', 'Dr', 'Pl', 'Ln']
        
        # Choose street name based on coordinate patterns
        if lat > 47.6:  # North Seattle
            street_name = f"{random.choice(['Aurora', 'Greenwood', 'Broadway', 'Roosevelt', 'Stone'])} {random.choice(street_suffixes)}"
        elif lat < 47.5:  # South Seattle
            street_name = f"{random.choice(['Rainier', 'MLK', 'Beacon', 'Delridge', 'California'])} {random.choice(street_suffixes)}"
        else:  # Central Seattle
            street_name = f"{random.choice(['Pike', 'Pine', 'Madison', 'Union', 'Broadway'])} {random.choice(street_suffixes)}"
        
        # Add directional prefix based on longitude
        if lon > -122.3:  # East side
            if random.random() < 0.3:
                street_name = f"E {street_name}"
        elif lon < -122.35:  # West side
            if random.random() < 0.3:
                street_name = f"W {street_name}"
        
        full_address = f"{street_number} {street_name}"
        city_name_clean = city_name.replace('_', ' ').title()
        
        return full_address, city_name_clean, "WA", zip_code
    
    def _extract_addresses_from_ems_data(self):
        """Generate synthetic addresses instead of extracting from real data"""
        addresses = set()
        
        # Generate 500 synthetic addresses instead of loading from real data
        for i in range(500):
            address = self._generate_synthetic_address()
            # Convert to tuple format expected by the calling code
            if len(address) >= 4:
                addr_tuple = (address[0], address[1], address[2], address[3])
                addresses.add(addr_tuple)
        
        return list(addresses)
    
    def _load_address_library(self):
        """Generate synthetic addresses for the address library (thread-safe)"""
        if self._pool_initialized:
            return  # Already loaded
            
        with self._address_lock:
            if self._pool_initialized:
                return  # Double-check after acquiring lock
                
            # Generate synthetic addresses instead of loading from files
            self._real_address_pool = []
            
            # Generate 1000 synthetic Seattle-area addresses
            for i in range(1000):
                address = self._generate_synthetic_address()
                self._real_address_pool.append(address)
            
            self._pool_initialized = True
            print(f"Generated {len(self._real_address_pool)} synthetic addresses")
    
    def _initialize_real_address_pool(self):
        """Initialize a pool of real geocoded addresses for fast reuse"""
        print("Initializing pool of real geocoded addresses (this may take a moment)...")
        
        # Generate a pool of 1000 real geocoded addresses
        pool_size = 1000
        successful_addresses = 0
        attempts = 0
        max_attempts = pool_size * 3  # Allow 3x attempts to get enough successful geocodes
        
        while successful_addresses < pool_size and attempts < max_attempts:
            attempts += 1
            try:
                address = self._generate_optimized_geocoded_address()
                self._real_address_pool.append(address)
                successful_addresses += 1
                if successful_addresses % 10 == 0:
                    print(f"Generated {successful_addresses}/{pool_size} real addresses...")
            except Exception as e:
                continue
        
        self._pool_initialized = True
        print(f"Address pool initialized with {len(self._real_address_pool)} real geocoded addresses")
    def generate_ems_incident(self, cad_incident=None, persons_list=None):
        """Generate a comprehensive EMS incident with realistic medical data and 1:Many patient relationships"""
        # Use CAD incident data if available, otherwise generate new
        if cad_incident:
            incident_datetime = datetime.strptime(cad_incident.call_date_time, '%Y-%m-%d %H:%M:%S')
            incident_type = cad_incident.incident_type_code
            address = cad_incident.address
            apartment_number = cad_incident.apartment_number
        else:
            incident_datetime = self.fake.date_time_between(start_date='-2y', end_date='now')
            
            # Check if we should reuse an existing patient
            existing_patient, should_reuse = self._should_reuse_existing_patient(incident_datetime)
            
            if should_reuse and existing_patient:
                # Generate incident for existing patient
                return self._generate_incident_for_existing_patient(existing_patient, incident_datetime)
            
            # Generate new incident and patient
            incident_type_code, incident_type_description = self._choose_ems_incident_type(incident_datetime)
            incident_type = incident_type_description  # Use description for legacy compatibility
            address_tuple = self._get_cached_address()
            if len(address_tuple) >= 6:
                address, city, state, zip_code, lat, lon = address_tuple[:6]
            else:
                address, city, state, zip_code = address_tuple[:4]
                lat, lon = None, None
            apartment_number = f"Apt {random.randint(1, 500)}" if random.random() < 0.3 else None

        # Priority mapping based on EMS codes
        priority_map = {
            '2301019': 'HIGH',  # Cardiac Arrest/Death
            '2301067': 'HIGH',  # Stroke/CVA
            '2301053': 'HIGH',  # Overdose/Poisoning/Ingestion
            '2301077': 'HIGH',  # Unconscious/Fainting/Near-Fainting
            '2301073': 'HIGH',  # Traumatic Injury (upgraded - trauma is serious)
            '2301021': 'HIGH',  # Chest Pain (upgraded - could be cardiac)
            '2301013': 'HIGH',  # Breathing Problem (upgraded - respiratory distress)
            '2301045': 'HIGH',  # Hemorrhage/Laceration (upgraded - bleeding is serious)
            '2301025': 'HIGH',  # Convulsions/Seizure (upgraded - neurological emergency)
            '2301061': 'MEDIUM',  # Sick Person (upgraded)
            '2301033': 'MEDIUM',  # Falls (upgraded - could be trauma)
            '2301001': 'MEDIUM',  # Abdominal Pain/Problems (upgraded)
            '2301027': 'MEDIUM',  # Diabetic Problem (upgraded)
            '2301003': 'HIGH',  # Allergic Reaction/Stings (upgraded - could be anaphylaxis)
            '2301057': 'HIGH',  # Pregnancy/Childbirth/Miscarriage (upgraded - obstetric emergency)
            '2301043': 'HIGH',  # Heat/Cold Exposure (upgraded - environmental emergency)
            '2301063': 'HIGH',  # Stab/Gunshot Wound/Penetrating Trauma
            '2301059': 'HIGH',  # Psychiatric Problem/Abnormal Behavior/Suicide Attempt (upgraded)
            '2301069': 'HIGH',  # Traffic/Transportation Incident (upgraded - MVA is serious)
            '2301007': 'HIGH',  # Assault (upgraded)
            '2301035': 'HIGH',  # Fire
            '2301005': 'MEDIUM',  # Animal Bite (upgraded)
            '2301023': 'HIGH',  # Choking
            '2301037': 'MEDIUM',  # Headache (upgraded - could be stroke)
            '2301011': 'LOW',  # Back Pain (Non-Traumatic) - only truly LOW priority
        }
        priority = priority_map.get(incident_type_code if 'incident_type_code' in locals() else '2301001', 'LOW')

        address_tuple = self._get_cached_address()
        if len(address_tuple) >= 6:
            address, city, state, zip_code, lat, lon = address_tuple[:6]
        else:
            address, city, state, zip_code = address_tuple[:4]
            lat, lon = None, None

        # Set patient demographics with age-weight consistency
        patient_age = random.randint(5, 85)
        patient_sex = random.choice(['M', 'F'])
        patient_race = random.choice(['WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'OTHER'])
        
        # Generate medical history and potentially influence incident type
        medical_history = self._generate_medical_history(patient_age, patient_race, patient_sex)
        chronic_conditions = self._generate_chronic_conditions(patient_age, patient_sex, patient_race)
        current_medications = self._generate_current_medications(patient_age, patient_sex)
        incident_type_code = self._influence_incident_by_medical_history(medical_history, incident_type_code)
        incident_type_description = EMS_INCIDENT_CODES[incident_type_code]
        
        # Generate realistic weight based on age, gender, and race
        patient_weight = self._generate_realistic_weight(patient_age, patient_sex, patient_race)
        
        # Generate demographic-appropriate name
        patient_full_name = self._generate_demographic_appropriate_name(patient_race, patient_sex)

        # Generate GPS coordinates for incident location
        lat, lon, accuracy = self._generate_gps_coordinates_from_address(address, city, state, zip_code)
        
        # Generate patient home address (different from incident location)
        patient_home_tuple = self._get_cached_address()
        if len(patient_home_tuple) >= 6:
            patient_home_address, patient_home_city, patient_home_state, patient_home_zip, home_lat, home_lon = patient_home_tuple[:6]
            home_lat, home_lon = float(home_lat), float(home_lon)
        else:
            patient_home_address, patient_home_city, patient_home_state, patient_home_zip = patient_home_tuple[:4]
            home_lat, home_lon, _ = self._generate_gps_coordinates_from_address(patient_home_address, patient_home_city, patient_home_state, patient_home_zip)
        
        # Generate destination facility
        destination_name, destination_type = self._generate_destination_facility(priority)
        
        # Generate patient assessment scores
        pain_score, gcs_score = self._generate_patient_assessment_scores(incident_type_code, patient_age)

        call_dt = incident_datetime
        
        # Response times based on priority (more realistic)
        if priority == 'HIGH':
            dispatch_delay = timedelta(seconds=random.randint(15, 60))  # Faster dispatch
            en_route_delay = timedelta(seconds=random.randint(30, 120))  # Faster response
            arrive_delay = timedelta(minutes=random.randint(3, 8))  # Faster arrival
        elif priority == 'MEDIUM':
            dispatch_delay = timedelta(seconds=random.randint(30, 120))
            en_route_delay = timedelta(seconds=random.randint(45, 180))
            arrive_delay = timedelta(minutes=random.randint(5, 12))
        else:  # LOW
            dispatch_delay = timedelta(seconds=random.randint(60, 180))
            en_route_delay = timedelta(seconds=random.randint(90, 300))
            arrive_delay = timedelta(minutes=random.randint(8, 15))
        transport_delay = timedelta(minutes=random.randint(15, 45))
        hospital_delay = timedelta(minutes=random.randint(10, 30))
        clear_delay = timedelta(minutes=random.randint(30, 90))
        
        # Map complaint to impression, treatments, meds, and bias vitals accordingly
        cc = incident_type
        def clamp(v, lo, hi):
            return max(lo, min(hi, v))
        profile = {
            '2301021': {  # Chest Pain (Non-Traumatic)
                'impression': 'CARDIAC',
                'treat': ['OXYGEN','IV_FLUIDS'],
                'meds': ['ASPIRIN','NITROGLYCERIN'],
                'bp': (140, 180, 80, 110), 'hr': (90, 130), 'rr': (16, 24), 'spo2': (92, 98), 'temp': (97.0, 99.0)
            },
            '2301013': {  # Breathing Problem
                'impression': 'RESPIRATORY',
                'treat': ['OXYGEN'],
                'meds': ['ALBUTEROL'],
                'bp': (110, 150, 70, 95), 'hr': (90, 120), 'rr': (20, 30), 'spo2': (85, 94), 'temp': (97.0, 99.0)
            },
            '2301053': {  # Overdose/Poisoning/Ingestion
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': ['NARCAN'],
                'bp': (90, 120, 50, 80), 'hr': (50, 100), 'rr': (6, 12), 'spo2': (75, 90), 'temp': (96.0, 99.0)
            },
            '2301077': {  # Unconscious/Fainting/Near-Fainting
                'impression': 'MEDICAL',
                'treat': ['OXYGEN','IV_FLUIDS'],
                'meds': [],
                'bp': (80, 110, 50, 70), 'hr': (50, 80), 'rr': (10, 16), 'spo2': (90, 98), 'temp': (96.0, 99.0)
            },
            '2301073': {  # Traumatic Injury
                'impression': 'TRAUMA',
                'treat': ['SPLINTING','IV_FLUIDS','BANDAGING','OXYGEN'],
                'meds': ['MORPHINE'],
                'bp': (85, 120, 50, 80), 'hr': (90, 130), 'rr': (18, 28), 'spo2': (92, 98), 'temp': (97.0, 99.0)
            },
            '2301025': {  # Convulsions/Seizure
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': [],
                'bp': (110, 160, 70, 100), 'hr': (100, 140), 'rr': (18, 26), 'spo2': (90, 98), 'temp': (97.0, 99.0)
            },
            '2301001': {  # Abdominal Pain/Problems
                'impression': 'MEDICAL',
                'treat': ['IV_FLUIDS'],
                'meds': [],
                'bp': (110, 150, 70, 95), 'hr': (80, 110), 'rr': (14, 22), 'spo2': (95, 100), 'temp': (97.0, 100.4)
            },
            '2301003': {  # Allergic Reaction/Stings
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': ['ALBUTEROL'],
                'bp': (100, 150, 60, 95), 'hr': (100, 130), 'rr': (20, 28), 'spo2': (88, 96), 'temp': (97.0, 99.0)
            },
            '2301019': {  # Cardiac Arrest/Death
                'impression': 'CARDIAC',
                'treat': ['CPR','OXYGEN'],
                'meds': [],
                'bp': (0, 0, 0, 0), 'hr': (0, 0), 'rr': (0, 0), 'spo2': (60, 85), 'temp': (96.0, 99.0)
            },
            '2301045': {  # Hemorrhage/Laceration
                'impression': 'TRAUMA',
                'treat': ['IV_FLUIDS','BANDAGING','OXYGEN'],
                'meds': ['MORPHINE'],
                'bp': (80, 110, 40, 70), 'hr': (100, 140), 'rr': (18, 28), 'spo2': (90, 98), 'temp': (97.0, 99.0)
            },
            '2301067': {  # Stroke/CVA
                'impression': 'MEDICAL',
                'treat': ['OXYGEN'],
                'meds': [],
                'bp': (150, 200, 90, 120), 'hr': (70, 100), 'rr': (14, 22), 'spo2': (95, 100), 'temp': (97.0, 99.0)
            },
        }
        prof = profile.get(incident_type_code if 'incident_type_code' in locals() else '2301001', profile['2301001'])  # Default to Abdominal Pain
        # generate vitals within ranges, with small random noise chance
        sys_lo, sys_hi, dia_lo, dia_hi = prof['bp']
        hr_lo, hr_hi = prof['hr']
        rr_lo, rr_hi = prof['rr']
        spo2_lo, spo2_hi = prof['spo2']
        t_lo, t_hi = prof['temp']
        # Handle special cases (like cardiac arrest)
        if sys_hi == 0:  # Cardiac arrest or similar
            bp_sys = 0
            bp_dia = 0
            hr = 0
            rr = 0
        else:
            bp_sys = random.randint(sys_lo, sys_hi)
            bp_dia = random.randint(dia_lo, dia_hi)
            hr = random.randint(hr_lo, hr_hi)
            rr = random.randint(rr_lo, rr_hi)
        
        spo2 = random.randint(spo2_lo, spo2_hi)
        temp = round(random.uniform(t_lo, t_hi), 1)
        if random.random() < 0.08 and sys_hi > 0:  # Skip atypical variation for cardiac arrest
            # atypical variation
            hr = clamp(int(random.gauss(hr, 15)), 30, 180)
            rr = clamp(int(random.gauss(rr, 6)), 6, 40)
            spo2 = clamp(int(random.gauss(spo2, 5)), 50, 100)
        treat_list = list(dict.fromkeys(prof['treat']))  # de-dup
        meds_list = list(dict.fromkeys(prof['meds']))

        # Select patient from persons list if available
        patient_person = random.choice(persons_list) if persons_list else None
        
        # Generate realistic CAD level and provider type
        cad_level, provider_type = self._generate_cad_level_and_provider_type()
        
        # Generate realistic patient acuity and situation acuity
        patient_acuity, situation_acuity = self._generate_patient_and_situation_acuity()
        
        # Generate realistic patient disposition
        patient_disposition = self._generate_patient_disposition()
        
        # Generate realistic complaint reported by dispatch
        complaint_reported_by_dispatch = self._generate_complaint_reported_by_dispatch()
        
        # Generate realistic incident EMD performed
        incident_emd_performed = self._generate_incident_emd_performed()
        
        # Generate realistic incident status
        incident_status = self._generate_incident_status()
        
        # Generate realistic prearrival activation based on dispatched provider type
        prearrival_activation = self._generate_dispatched_vs_prearrival_activation(provider_type)
        
        # Generate realistic attempted procedures
        attempted_procedures = self._generate_attempted_procedures()
        
        # Generate realistic successful procedures based on attempted procedures
        successful_procedures = self._generate_successful_procedures(attempted_procedures)
        
        # Generate realistic procedure complications
        procedure_complications = self._generate_procedure_complications(attempted_procedures)
        
        # Generate realistic primary unit role
        primary_unit_role = self._generate_primary_unit_role()
        
        incident = EMSIncident(
            incident_id=str(uuid.uuid4()),
            incident_number=f"EMS{incident_datetime.year}{random.randint(100000, 999999)}",
            call_number=f"E{incident_datetime.year}{random.randint(1000000, 9999999)}",
            incident_type=incident_type,
            incident_type_code=incident_type_code if 'incident_type_code' in locals() else '2301051',  # Default to "No Other Appropriate Choice"
            incident_type_description=incident_type_description if 'incident_type_description' in locals() else 'No Other Appropriate Choice',
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
            crew_members=[f"PARAMEDIC_{self.fake.last_name().upper()}", f"EMT_{self.fake.last_name().upper()}"],
            patient_person_id=patient_person.person_id if patient_person else None,
            patient_age=patient_age,
            patient_sex=patient_sex,
            patient_race=patient_race,
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
            transport_destination=random.choice(['Harborview Medical Center', 'Swedish Medical Center', 'Virginia Mason Medical Center', 'University of Washington Medical Center']),
            transport_mode='AIR' if priority in ['HIGH', 'CRITICAL'] and random.random() < 0.15 else 'GROUND',
            created_date=call_dt.strftime('%Y-%m-%d %H:%M:%S'),
            # Enhanced EMS fields
            complaint_reported_by_dispatch=complaint_reported_by_dispatch,
            patient_full_name=patient_full_name,
            unit_call_sign=f"EMS-{random.randint(1, 50)}",
            patient_contact=True,
            patient_disposition=patient_disposition,
            crew_disposition='COMPLETED',
            patient_evaluation_care_disposition=random.choice(['EVALUATED', 'TREATED', 'TRANSPORTED']),
            transport_disposition=random.choice(['COMPLETED', 'IN_PROGRESS']),
            transfer_destination=random.choice(['HOSPITAL', 'NURSING_HOME', 'HOME']),
            destination_type=random.choice(['HOSPITAL', 'CLINIC', 'HOME']),
            transportation_method=random.choice(['AMBULANCE', 'HELICOPTER']),
            unit_level_of_care=random.choice(['BLS', 'ALS']),
            cad_emd_code=incident_type_code if 'incident_type_code' in locals() else '2301051',
            prearrival_activation=prearrival_activation,
            # Medical Details
            complaint_reported_by_dispatch_code=incident_type_code if 'incident_type_code' in locals() else '2301051',
            attempted_procedures=attempted_procedures,
            successful_procedures=successful_procedures,
            procedure_complications=procedure_complications,
            cardiac_arrest_datetime=(call_dt + timedelta(minutes=random.randint(5, 15))).strftime('%Y-%m-%d %H:%M:%S') if incident_type == 'CARDIAC_ARREST' else None,
            cardiac_arrest_resuscitation_discontinuation_datetime=(call_dt + timedelta(minutes=random.randint(20, 45))).strftime('%Y-%m-%d %H:%M:%S') if incident_type == 'CARDIAC_ARREST' else None,
            ecg_findings=random.choice(['NORMAL', 'ABNORMAL', 'UNKNOWN']),
            incident_emd_performed=incident_emd_performed,
            incident_emd_performed_code=incident_type_code if 'incident_type_code' in locals() else '2301051',
            cad_level_of_care_provided=cad_level,
            incident_level_of_care_provided=cad_level,
            provider_primary_impression=prof['impression'],
            patient_acuity=patient_acuity,
            situation_patient_acuity=situation_acuity,
            # Crew Details
            crew_member_name=f"{self.fake.first_name()} {self.fake.last_name()}",
            crew_member_level=provider_type,
            crew_badge_number=f"EMS{random.randint(1000, 9999)}",
            # Patient Details
            patient_id=patient_person.person_id if patient_person else str(uuid.uuid4()),
            patient_date_of_birth=self.fake.date_of_birth(minimum_age=5, maximum_age=85).strftime('%Y-%m-%d'),
            patient_weight=patient_weight,
            patient_home_address=address,
            patient_medical_history=medical_history,
            patient_chronic_conditions=chronic_conditions,
            patient_current_medications=current_medications,
            is_superuser=False,
            # Unit Details
            agency_number='230',  # King County EMS
            agency_name='King County Emergency Medical Services',
            agency_affiliation='COUNTY',
            primary_unit_role=primary_unit_role,
            # Incident Dates/Times
            total_commit_time=int((dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + clear_delay).total_seconds()),
            unit_notified_by_dispatch_datetime=(call_dt + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_arrived_at_patient_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay).strftime('%Y-%m-%d %H:%M:%S'),
            transfer_of_ems_patient_care_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=random.randint(10, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            arrival_at_destination_landing_area_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_left_scene_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay).strftime('%Y-%m-%d %H:%M:%S'),
            patient_arrived_at_destination_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_back_in_service_datetime=(call_dt + dispatch_delay + en_route_delay + arrive_delay + transport_delay + hospital_delay + clear_delay).strftime('%Y-%m-%d %H:%M:%S'),
            last_modified=call_dt.strftime('%Y-%m-%d %H:%M:%S'),
            # Ungrouped Properties
            incident_status=incident_status,
            created_by='SYSTEM',
            patient_pk=str(uuid.uuid4()),
            primary_patient_caregiver_on_scene=random.choice(['FAMILY_MEMBER', 'BYSTANDER', 'NONE']),
            crew_with_als_pt_contact_response_role=random.choice(['PRIMARY', 'SECONDARY']),
            
            # GPS/Geocoding fields
            incident_location_latitude=lat,
            incident_location_longitude=lon,
            incident_location_accuracy=accuracy,
            patient_home_latitude=home_lat,
            patient_home_longitude=home_lon,
            
            # Additional EMS-specific fields
            incident_dispatch_priority=priority,
            incident_response_priority=priority,
            scene_safety_assessment=self._generate_scene_safety_assessment(incident_type_code),
            environmental_conditions=self._generate_environmental_conditions(),
            incident_narrative=self._generate_ems_narrative(incident_type_code, patient_age, patient_sex, priority),
            patient_refusal_reason=random.choice(['PATIENT_DECLINED', 'FAMILY_DECLINED', None]) if random.random() < 0.05 else None,
            transport_refusal_reason=random.choice(['PATIENT_DECLINED', 'FAMILY_DECLINED', None]) if random.random() < 0.08 else None,
            destination_facility_name=destination_name,
            destination_facility_type=destination_type,
            
            # Crew and unit tracking
            responding_unit_type=random.choice(['AMBULANCE', 'PARAMEDIC_UNIT', 'FIRE_ENGINE', 'RESCUE_UNIT']),
            responding_unit_capability=random.choice(['BLS', 'ALS', 'CRITICAL_CARE']),
            backup_unit_assigned=random.choice(['UNIT_2', 'SUPERVISOR', None]) if random.random() < 0.15 else None,
            mutual_aid_requested=random.random() < 0.05,
            
            # Patient assessment details
            initial_patient_assessment=f"Initial assessment: {incident_type_description}",
            final_patient_assessment=f"Final assessment: {random.choice(['STABLE', 'IMPROVING', 'CRITICAL', 'UNCHANGED'])}",
            patient_condition_on_arrival=random.choice(['STABLE', 'UNSTABLE', 'CRITICAL', 'CONSCIOUS', 'UNCONSCIOUS']),
            patient_condition_on_transfer=random.choice(['STABLE', 'IMPROVED', 'CRITICAL', 'UNCHANGED']),
            pain_scale_score=pain_score,
            glasgow_coma_scale=gcs_score
        )
        
        # Add new patient to pool for potential reuse
        new_patient_data = {
            'patient_id': incident.patient_person_id,
            'patient_full_name': incident.patient_full_name,
            'patient_date_of_birth': incident.patient_date_of_birth,
            'patient_age': incident.patient_age,
            'patient_sex': incident.patient_sex,
            'patient_race': incident.patient_race,
            'patient_weight': incident.patient_weight,
            'patient_home_address': incident.patient_home_address,
            'chronic_conditions': incident.patient_chronic_conditions,
            'medical_history': incident.patient_medical_history,
            'current_medications': incident.patient_current_medications
        }
        self._patient_pool.append(new_patient_data)
        self._update_patient_incident_history(incident.patient_person_id, incident_datetime)
        
        return incident

    def generate_ems_medication(self, ems_incident=None):
        """Generate EMS medication administration record consistent with incident type"""
        if not ems_incident:
            return None
        
        # Get incident type code for medication selection
        if isinstance(ems_incident, dict):
            incident_type_code = ems_incident.get('incident_type_code', '2301051')  # Default to "No Other Appropriate Choice"
        else:
            incident_type_code = getattr(ems_incident, 'incident_type_code', '2301051')
        
        # Get medically appropriate medications for this incident type
        appropriate_medications = self._generate_medications_for_incident(incident_type_code)
        
        # Select one medication from the appropriate list
        selected_medication_name = random.choice(appropriate_medications)
        
        # Medication distribution with real medication names
        medications = [
            {'name': 'NORMAL SALINE', 'code': '7820', 'routes': {'INTRAVENC': 0.957, 'IV_DRIP': 0.001, 'INTRAMUSI': 0.005, 'INTRANASA': 0.0002, 'INTRAOSSE': 0.020, 'ORAL': 0.0003, 'RECTAL': 0.0003, 'PORTACATH': 0.0001, 'ENDOTRAC': 0.002, 'NASAL_CAN': 0.009, 'NON_REBRE': 0.0001, 'BLOW_BY': 0.002}, 'dosage': (500, 1000), 'unit': 'ML', 'weight': 13747},
            {'name': 'ONDANSETRON', 'code': '7941', 'routes': {'INTRAVENC': 0.929, 'INTRAMUSI': 0.069, 'INTRANASA': 0.001, 'INTRAOSSE': 0.0002, 'ORAL': 0.0001, 'RECTAL': 0.0003, 'BUCCAL': 0.0001}, 'dosage': (4, 8), 'unit': 'MG', 'weight': 10205},
            {'name': 'OXYGEN', 'code': '7781', 'routes': {'IV_PIGGYBA': 0.453, 'INHALATIO': 0.082, 'INTRANASA': 0.036, 'NASAL_CAN': 0.157, 'ORAL': 0.020, 'BUCCAL': 0.001, 'ENDOTRAC': 0.006, 'TRACHEOST': 0.001, 'INTRAMUSI': 0.0002, 'INTRAOSSE': 0.0001, 'INTRAVENC': 0.002, 'BLOW_BY': 0.014, 'OPHTHALM': 0.002}, 'dosage': (2, 15), 'unit': 'L/MIN', 'weight': 7550},
            {'name': 'FENTANYL', 'code': '4337', 'routes': {'INTRAVENC': 0.741, 'INTRANASA': 0.179, 'INTRAMUSI': 0.032, 'INTRAOSSE': 0.001, 'INHALATIO': 0.001, 'IV_DRIP': 0.0001, 'BUCCAL': 0.0001, 'SUBLINGUA': 0.0001}, 'dosage': (25, 100), 'unit': 'MCG', 'weight': 6615},
            {'name': 'ASPIRIN', 'code': '1191', 'routes': {'ORAL': 1.0}, 'dosage': (81, 325), 'unit': 'MG', 'weight': 5223},
            {'name': 'NITROGLYCERIN', 'code': '7417', 'routes': {'SUBLINGUA': 0.972, 'ORAL': 0.028}, 'dosage': (0.3, 0.4), 'unit': 'MG', 'weight': 3647},
            {'name': 'ALBUTEROL', 'code': '435', 'route': 'INHALED', 'dosage': (90, 180), 'unit': 'MCG', 'weight': 3640},
            {'name': 'IPRATROPIUM', 'code': '5737', 'route': 'INHALED', 'dosage': (250, 500), 'unit': 'MCG', 'weight': 3565},
            {'name': 'METHYLPREDNISOLONE', 'code': '7021', 'route': 'INTRAVENOUS', 'dosage': (125, 250), 'unit': 'MG', 'weight': 1999},
            {'name': 'EPI 1:10,000', 'code': '3292', 'route': 'INTRAVENOUS', 'dosage': (0.1, 1.0), 'unit': 'MG', 'weight': 1788},
            {'name': 'NALOXONE', 'code': '7517', 'route': 'INTRAMUSCULAR', 'dosage': (0.4, 2.0), 'unit': 'MG', 'weight': 1378},
            {'name': 'DIPHENHYDRAMINE', 'code': '3498', 'route': 'INTRAVENOUS', 'dosage': (25, 50), 'unit': 'MG', 'weight': 1040},
            {'name': 'GLUCOSE', 'code': '3143', 'route': 'INTRAVENOUS', 'dosage': (25, 50), 'unit': 'ML', 'weight': 821},
            {'name': 'ACETAMINOPHEN', 'code': '161', 'route': 'ORAL', 'dosage': (325, 650), 'unit': 'MG', 'weight': 701},
            {'name': 'EPI 1:1,000', 'code': '3292', 'route': 'INTRAMUSCULAR', 'dosage': (0.3, 0.5), 'unit': 'MG', 'weight': 693},
            {'name': 'DEXTROSE 50%', 'code': '3143', 'route': 'INTRAVENOUS', 'dosage': (25, 50), 'unit': 'ML', 'weight': 664},
            {'name': 'SODIUM BICARBONATE', 'code': '1778', 'route': 'INTRAVENOUS', 'dosage': (25, 50), 'unit': 'MEQ', 'weight': 618},
            {'name': 'METOPROLOL', 'code': '6918', 'route': 'INTRAVENOUS', 'dosage': (5, 15), 'unit': 'MG', 'weight': 594},
            {'name': 'MORPHINE', 'code': '7054', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 591},
            {'name': 'DEXTROSE 10%', 'code': '3143', 'route': 'INTRAVENOUS', 'dosage': (250, 500), 'unit': 'ML', 'weight': 484},
            {'name': 'MIDAZOLAM 5 MG/ML [VERSED]', 'code': '7018', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 466},
            {'name': 'KETAMINE', 'code': '5901', 'route': 'INTRAVENOUS', 'dosage': (0.5, 2.0), 'unit': 'MG/KG', 'weight': 461},
            {'name': 'MIDAZOLAM', 'code': '7018', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 384},
            {'name': 'GLUCAGON', 'code': '4761', 'route': 'INTRAMUSCULAR', 'dosage': (0.5, 1.0), 'unit': 'MG', 'weight': 348},
            {'name': 'ADENOSINE', 'code': '197', 'route': 'INTRAVENOUS', 'dosage': (6, 12), 'unit': 'MG', 'weight': 299},
            {'name': 'ROCURONIUM', 'code': '7949', 'route': 'INTRAVENOUS', 'dosage': (0.6, 1.2), 'unit': 'MG/KG', 'weight': 293},
            {'name': 'AMIODARONE', 'code': '177', 'route': 'INTRAVENOUS', 'dosage': (150, 300), 'unit': 'MG', 'weight': 261},
            {'name': 'LABETALOL', 'code': '3827', 'route': 'INTRAVENOUS', 'dosage': (10, 20), 'unit': 'MG', 'weight': 260},
            {'name': 'ATROPINE', 'code': '174', 'route': 'INTRAVENOUS', 'dosage': (0.5, 1.0), 'unit': 'MG', 'weight': 227},
            {'name': 'CALCIUM CHLORIDE', 'code': '1754', 'route': 'INTRAVENOUS', 'dosage': (500, 1000), 'unit': 'MG', 'weight': 134},
            {'name': 'MIDAZOLAM 1 MG/ML [VERSED]', 'code': '7018', 'route': 'INTRAVENOUS', 'dosage': (1, 5), 'unit': 'MG', 'weight': 130},
            {'name': 'EPINEPHRINE 0.1 MG/ML', 'code': '3292', 'route': 'INTRAVENOUS', 'dosage': (0.1, 0.5), 'unit': 'MG', 'weight': 87},
            {'name': 'DOPAMINE', 'code': '3842', 'route': 'INTRAVENOUS', 'dosage': (2, 20), 'unit': 'MCG/KG/MIN', 'weight': 81},
            {'name': 'MAGNESIUM SULFATE', 'code': '6349', 'route': 'INTRAVENOUS', 'dosage': (1, 4), 'unit': 'GM', 'weight': 70},
            {'name': 'LIDOCAINE', 'code': '6183', 'route': 'INTRAVENOUS', 'dosage': (50, 100), 'unit': 'MG', 'weight': 67},
            {'name': 'MIDAZOLAM 10MG/2ML', 'code': '7018', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 59},
            {'name': 'EPI 1:100,000 PDP', 'code': '3292', 'route': 'INTRAMUSCULAR', 'dosage': (0.2, 0.5), 'unit': 'MG', 'weight': 54},
            {'name': 'EPINEPHRINE', 'code': '3292', 'route': 'INTRAMUSCULAR', 'dosage': (0.3, 0.5), 'unit': 'MG', 'weight': 46},
            {'name': 'FUROSEMIDE', 'code': '4603', 'route': 'INTRAVENOUS', 'dosage': (20, 80), 'unit': 'MG', 'weight': 45},
            {'name': 'HALOPERIDOL', 'code': '5259', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 36},
            {'name': 'LEVOPHED', 'code': '7419', 'route': 'INTRAVENOUS', 'dosage': (2, 20), 'unit': 'MCG/MIN', 'weight': 35},
            {'name': 'TETRACAINE', 'code': '10370', 'route': 'TOPICAL', 'dosage': (0.5, 1.0), 'unit': '%', 'weight': 32},
            {'name': 'EPI 1:100,000 (PDP)', 'code': '3292', 'route': 'INTRAMUSCULAR', 'dosage': (0.2, 0.5), 'unit': 'MG', 'weight': 29},
            {'name': 'SODIUM CHLORIDE', 'code': '7820', 'route': 'INTRAVENOUS', 'dosage': (500, 1000), 'unit': 'ML', 'weight': 19},
            {'name': 'GLUCOSE 100 MG/ML', 'code': '3143', 'route': 'INTRAVENOUS', 'dosage': (25, 50), 'unit': 'ML', 'weight': 16},
            {'name': 'MIDAZOLAM 5MG/5ML', 'code': '7018', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 15},
            {'name': 'SUCCINYLCHOLINE', 'code': '10305', 'route': 'INTRAVENOUS', 'dosage': (1, 2), 'unit': 'MG/KG', 'weight': 9},
            {'name': 'ETOMIDATE', 'code': '3873', 'route': 'INTRAVENOUS', 'dosage': (0.2, 0.3), 'unit': 'MG/KG', 'weight': 7},
            {'name': 'IBUPROFEN', 'code': '5640', 'route': 'ORAL', 'dosage': (400, 800), 'unit': 'MG', 'weight': 5},
            {'name': 'VECURONIUM', 'code': '11170', 'route': 'INTRAVENOUS', 'dosage': (0.08, 0.1), 'unit': 'MG/KG', 'weight': 5},
            {'name': 'STERILE WATER', 'code': '11324', 'route': 'INTRAVENOUS', 'dosage': (5, 10), 'unit': 'ML', 'weight': 4},
            {'name': 'ACTIVATED CHARCOAL', 'code': '435', 'route': 'ORAL', 'dosage': (25, 50), 'unit': 'GM', 'weight': 3},
            {'name': 'NOREPINEPHRINE', 'code': '7419', 'route': 'INTRAVENOUS', 'dosage': (2, 20), 'unit': 'MCG/MIN', 'weight': 3},
            {'name': 'HYDROXOCOBALAMIN', 'code': '6178', 'route': 'INTRAVENOUS', 'dosage': (2.5, 5), 'unit': 'GM', 'weight': 2},
            {'name': 'POTASSIUM CHLORIDE', 'code': '8584', 'route': 'INTRAVENOUS', 'dosage': (10, 20), 'unit': 'MEQ', 'weight': 2},
            {'name': 'CALCIUM GLUCONATE', 'code': '1754', 'route': 'INTRAVENOUS', 'dosage': (500, 1000), 'unit': 'MG', 'weight': 1},
            {'name': 'EPINEPHRINE 0.01 MG/ML', 'code': '3292', 'route': 'INTRAVENOUS', 'dosage': (0.01, 0.1), 'unit': 'MG', 'weight': 1},
            {'name': 'GLUCOSE 500 MG/ML', 'code': '3143', 'route': 'INTRAVENOUS', 'dosage': (25, 50), 'unit': 'ML', 'weight': 1},
            {'name': 'HYDROXOCOBALAMIN INJECTION [CYANOKIT]', 'code': '6178', 'route': 'INTRAVENOUS', 'dosage': (2.5, 5), 'unit': 'GM', 'weight': 1},
            {'name': 'DIAZEPAM', 'code': '3610', 'route': 'INTRAVENOUS', 'dosage': (2, 10), 'unit': 'MG', 'weight': 200}
        ]
        
        # Find the selected medication in the medication database
        selected_med_name = selected_medication_name
        
        # Find the selected medication
        med = next(med for med in medications if med['name'] == selected_med_name)
        
        # Select route based on realistic distribution for this medication
        if 'routes' in med:
            route_names = list(med['routes'].keys())
            route_weights = list(med['routes'].values())
            selected_route = random.choices(route_names, weights=route_weights, k=1)[0]
        else:
            # Fallback to original route
            selected_route = med['route']
        
        # Determine dosage based on realistic medication dosage data
        medication_dosages = {
            'ALBUTEROL': {'2.5': 10, '5': 1},
            'DEXTROSE 10%': {'250': 1, '500': 1},
            'DIAZEPAM': {'2': 1, '5': 1, '10': 1},
            'DIPHENHYDRAMINE': {'25': 1, '50': 1},
            'EPI 1:1,000': {'0.3': 1, '0.5': 1},
            'EPI 1:10,000': {'0.1': 1, '1.0': 1},
            'EPI 1:100,000 (PDP)': {'0.2': 1, '0.5': 1},
            'EPI 1:100,000 PDP': {'0.2': 1, '0.5': 1},
            'FENTANYL': {'25': 1, '50': 1, '75': 1, '100': 1},
            'GLUCOSE': {'25': 1, '50': 1},
            'IPRATROPIUM': {'250': 1, '500': 1},
            'KETAMINE': {'50': 1, '100': 1},
            'LABETALOL': {'10': 1, '20': 1},
            'LEVOPHED': {'2': 1, '20': 1},
            'METHYLPREDNISOLONE': {'125': 1, '250': 1},
            'METOPROLOL': {'5': 1, '15': 1},
            'MIDAZOLAM 1 MG/ML [VERSED]': {'1': 1, '5': 1},
            'MIDAZOLAM 5 MG/ML [VERSED]': {'2': 1, '10': 1},
            'MIDAZOLAM 5MG/5ML': {'2': 1, '10': 1},
            'MIDAZOLAM': {'2': 1, '10': 1},
            'MORPHINE': {'2': 1, '4': 1, '10': 1},
            'MED_J001': {'0.4': 1, '2': 1},
            'MED_E002': {'0.3': 1, '0.4': 1},
            'MED_K001': {'250': 1, '500': 1, '1000': 1},
            'MED_H001': {'4': 1, '8': 1},
            'MED_D001': {'2': 1, '4': 1, '6': 1, '10': 1, '15': 1},
            'MED_O001': {'0.6': 1, '1.2': 1},
            'MED_U001': {'1': 1, '2': 1},
            'MED_I001': {'25': 1, '50': 1},
            'MED_B005': {'0.5': 1, '1': 1},
            'MED_N001': {'6': 1, '12': 1},
            'MED_E005': {'150': 1, '300': 1},
            'MED_E003': {'0.5': 1, '1': 1},
            'MED_I002': {'500': 1, '1000': 1},
            'MED_P001': {'2': 1, '20': 1},
            'MED_Q001': {'1': 1, '4': 1},
            'MED_R001': {'50': 1, '100': 1},
            'MED_S001': {'20': 1, '80': 1},
            'MED_F006': {'2': 1, '10': 1},
            'MED_L001': {'0.5': 1, '1': 1},
            'MED_K002': {'500': 1, '1000': 1},
            'MED_B006': {'25': 1, '50': 1},
            'MED_V001': {'0.2': 1, '0.3': 1},
            'MED_W001': {'400': 1, '800': 1},
            'MED_X001': {'0.08': 1, '0.1': 1},
            'MED_Y001': {'5': 1, '10': 1},
            'MED_Z001': {'25': 1, '50': 1},
            'MED_T002': {'2': 1, '20': 1},
            'MED_AA001': {'2.5': 1, '5': 1},
            'MED_BB001': {'10': 1, '20': 1},
            'MED_CC001': {'500': 1, '1000': 1},
            'MED_E010': {'0.01': 1, '0.1': 1},
            'MED_B007': {'25': 1, '50': 1},
            'MED_DD001': {'2.5': 1, '5': 1},
            'MED_E001': {'81': 1, '325': 1},
            'MED_E006': {'0.1': 1, '0.5': 1},
            'MED_B003': {'25': 1, '50': 1}
        }
        
        # Get dosage distribution for this medication
        if med['name'] in medication_dosages:
            dosage_dist = medication_dosages[med['name']]
            dosage_values = list(dosage_dist.keys())
            dosage_weights = list(dosage_dist.values())
            selected_dosage = random.choices(dosage_values, weights=dosage_weights, k=1)[0]
            dosage = float(selected_dosage)
        else:
            # Default dosage based on original range for medications not in the data
            dosage_range = med['dosage']
            dosage = round(random.uniform(dosage_range[0], dosage_range[1]), 1)
        
        # Determine dosage unit based on realistic medication dosage unit data
        medication_units = {
            'ACETAMINOPHEN': {'MG': 631, 'ML': 65},
            'ACTIVATED CHARCOAL': {'DROPS': 2, 'G': 3},
            'ADENOSINE': {'MG': 465},
            'ALBUTEROL': {'MG': 4422, 'PUFFS': 1},
            'AMIODARONE': {'MG': 345},
            'MED_E001': {'MG': 5255, 'UNITS_PER_I': 1},
            'ATROPINE': {'MG': 277},
            'CALCIUM CHLORIDE': {'G': 102, 'MG': 22, 'ML': 1},
            'CALCIUM GLUCONATE': {'MCG': 1, 'MG': 23, 'ML': 9},
            'DEXTROSE 10%': {'G': 172, 'KEEP_VEIN': 2, 'L': 1, 'MG': 297, 'UNITS': 1},
            'DIAZEPAM': {'MG': 200},
            'DEXTROSE 50%': {'G': 696, 'MG': 6},
            'DIPHENHYDRAMINE': {'MG': 1037, 'ML': 7},
            'DOPAMINE': {'DROPS': 2, 'MCG': 78, 'MEQ': 2, 'MG': 3, 'ML': 4},
            'EPI 1:1,000': {'MCG': 5, 'MEQ': 2, 'MG': 1178, 'ML': 4},
            'EPI 1:10,000': {'MCG': 109, 'MG': 6809, 'ML': 2, 'UNITS_PER_I': 1},
            'EPI 1:100,000 (PDP)': {'MCG': 68, 'MEQ': 1, 'MG': 1, 'ML': 2},
            'EPI 1:100,000 PDP': {'MCG': 124, 'MEQ': 1, 'MG': 4, 'ML': 9},
            'MED_E008': {'MCG': 8, 'MG': 41, 'ML': 1},
            'MED_E010': {'MG': 1},
            'MED_E006': {'MCG': 3, 'MG': 302, 'ML': 12, 'UNITS_PER_I': 2},
            'ETOMIDATE': {'MG': 7},
            'FENTANYL': {'MCG': 8446, 'MG': 1, 'ML': 5},
            'FUROSEMIDE': {'MG': 45},
            'GLUCAGON': {'MG': 353},
            'GLUCOSE': {'G': 996, 'MG': 1, 'ML': 1, 'UNITS': 1, 'UNITS_PER_I': 2},
            'GLUCOSE 100 MG/ML': {'G': 18, 'MG': 1},
            'GLUCOSE 500 MG/ML': {'G': 1, 'MG': 1},
            'HALOPERIDOL': {'MG': 36},
            'HYDROXOCOBALAMIN': {'G': 2, 'MG': 2},
            'HYDROXOCOBALAMIN INJECTION [CYANOKIT]': {'G': 1},
            'IBUPROFEN': {'MCG': 2, 'MG': 4304, 'ML': 1},
            'IPRATROPIUM': {'MCG': 2, 'MG': 721, 'ML': 4},
            'KETAMINE': {'MG': 276},
            'LABETALOL': {'MCG': 40, 'MG': 1, 'ML': 1},
            'LEVOPHED': {'MG': 78},
            'LIDOCAINE': {'MG': 1},
            'MAGNESIUM SULFATE': {'G': 71, 'MG': 1865, 'MEQ': 131, 'ML': 8},
            'METHYLPREDNISOLONE': {'MG': 605},
            'METOPROLOL': {'MG': 447},
            'MIDAZOLAM': {'MCG': 1, 'MG': 140, 'ML': 3},
            'MIDAZOLAM 1 MG/ML [VERSED]': {'MCG': 1, 'MG': 71, 'ML': 6, 'UNITS_PER_I': 1},
            'MIDAZOLAM 10MG/2ML': {'MCG': 1, 'MG': 511, 'ML': 26},
            'MIDAZOLAM 5 MG/ML [VERSED]': {'MCG': 4, 'MG': 13},
            'MIDAZOLAM 5MG/5ML': {'MCG': 6, 'MG': 748, 'ML': 3},
            'MED_G002': {'MCG': 1, 'MG': 1730},
            'MED_J001': {'MCG': 1, 'MG': 5129, 'UNITS': 1},
            'NITROGLYCERIN': {'MCG': 3},
            'MED_T002': {'MCG': 4},
            'NORMAL SALINE': {'DROPS': 29, 'G': 22, 'KEEP_VEIN': 696, 'L': 442, 'LITERS_BO': 29, 'LITERS_PER': 47, 'LOCK_FLUSH': 4, 'MEQ': 11, 'MG': 88, 'ML': 13216, 'NOT_APPLIC': 9, 'NOT_RECORDED': 1, 'OTHER': 22, 'PUFFS': 9, 'UNITS': 54, 'UNITS_PER_I': 9},
            'ONDANSETRON': {'MG': 10241},
            'MED_D001': {'DROPS': 5, 'G': 1, 'KEEP_VEIN': 1, 'L': 1374, 'LITERS_BO': 869, 'LITERS_PER': 5718, 'LOCK_FLUSH': 3, 'METERED_D': 9, 'MEQ': 2, 'MG': 18, 'ML': 85, 'NOT_APPLIC': 4, 'OTHER': 26, 'UNITS_PER_I': 13},
            'POTASSIUM CHLORIDE': {'MG': 2},
            'ROCURONIUM': {'MG': 287, 'ML': 10},
            'SODIUM BICARBONATE': {'MCG': 653, 'MG': 2, 'ML': 19, 'UNITS_PER_I': 1},
            'SODIUM CHLORIDE': {'G': 1, 'MG': 2},
            'STERILE WATER': {'G': 1, 'MG': 1},
            'SUCCINYLCHOLINE': {'G': 1, 'MG': 8, 'ML': 1},
            'TETRACAINE': {'G': 39},
            'VECURONIUM': {'MG': 5}
        }
        
        # Get unit distribution for this medication
        if med['name'] in medication_units:
            unit_dist = medication_units[med['name']]
            unit_names = list(unit_dist.keys())
            unit_weights = list(unit_dist.values())
            selected_unit = random.choices(unit_names, weights=unit_weights, k=1)[0]
            # Map to standard unit abbreviations
            unit_mapping = {
                'MG': 'MG', 'ML': 'ML', 'MCG': 'MCG', 'G': 'G', 'L': 'L', 
                'MEQ': 'MEQ', 'UNITS': 'UNITS', 'PUFFS': 'PUFFS', 'DROPS': 'DROPS',
                'KEEP_VEIN': 'ML', 'LITERS_BO': 'L', 'LITERS_PER': 'L/MIN', 
                'LOCK_FLUSH': 'ML', 'METERED_D': 'PUFFS', 'UNITS_PER_I': 'UNITS',
                'NOT_APPLIC': 'UNITS', 'NOT_RECORDED': 'UNITS', 'OTHER': 'UNITS'
            }
            dosage_unit = unit_mapping.get(selected_unit, selected_unit)
        else:
            # Default unit from medication definition
            dosage_unit = med['unit']
        # Determine patient response based on realistic medication patient response data
        medication_response_probs = {
            'ACETAMINOPHEN': {'IMPROVED': 0.286, 'NOT_RECORDED': 0.269, 'UNCHANGED': 0.445, 'WORSE': 0.0},
            'ACTIVATED CHARCOAL': {'IMPROVED': 0.0, 'NOT_RECORDED': 1.0, 'UNCHANGED': 0.0, 'WORSE': 0.0},
            'ADENOSINE': {'IMPROVED': 0.406, 'NOT_RECORDED': 0.164, 'UNCHANGED': 0.429, 'WORSE': 0.002},
            'ALBUTEROL': {'IMPROVED': 0.641, 'NOT_RECORDED': 0.241, 'UNCHANGED': 0.116, 'WORSE': 0.002},
            'AMIODARONE': {'IMPROVED': 0.145, 'NOT_RECORDED': 0.298, 'UNCHANGED': 0.551, 'WORSE': 0.006},
            'MED_E001': {'IMPROVED': 0.186, 'NOT_RECORDED': 0.255, 'UNCHANGED': 0.558, 'WORSE': 0.002},
            'ATROPINE': {'IMPROVED': 0.537, 'NOT_RECORDED': 0.169, 'UNCHANGED': 0.290, 'WORSE': 0.004},
            'CALCIUM CHLORIDE': {'IMPROVED': 0.082, 'NOT_RECORDED': 0.321, 'UNCHANGED': 0.597, 'WORSE': 0.0},
            'DEXTROSE 10%': {'IMPROVED': 0.800, 'NOT_RECORDED': 0.086, 'UNCHANGED': 0.112, 'WORSE': 0.004},
            'DEXTROSE 50%': {'IMPROVED': 0.504, 'NOT_RECORDED': 0.419, 'UNCHANGED': 0.075, 'WORSE': 0.0},
            'DIPHENHYDRAMINE': {'IMPROVED': 0.584, 'NOT_RECORDED': 0.214, 'UNCHANGED': 0.202, 'WORSE': 0.001},
            'DOPAMINE': {'IMPROVED': 0.458, 'NOT_RECORDED': 0.289, 'UNCHANGED': 0.241, 'WORSE': 0.012},
            'EPI 1:1,000': {'IMPROVED': 0.384, 'NOT_RECORDED': 0.316, 'UNCHANGED': 0.300, 'WORSE': 0.001},
            'EPI 1:10,000': {'IMPROVED': 0.082, 'NOT_RECORDED': 0.299, 'UNCHANGED': 0.613, 'WORSE': 0.001},
            'EPI 1:100,000 (PDP)': {'IMPROVED': 0.620, 'NOT_RECORDED': 0.239, 'UNCHANGED': 0.127, 'WORSE': 0.014},
            'EPI 1:100,000 PDP': {'IMPROVED': 0.787, 'NOT_RECORDED': 0.0, 'UNCHANGED': 0.202, 'WORSE': 0.007},
            'MED_E008': {'IMPROVED': 0.673, 'NOT_RECORDED': 0.102, 'UNCHANGED': 0.224, 'WORSE': 0.0},
            'MED_E010': {'IMPROVED': 0.0, 'NOT_RECORDED': 0.0, 'UNCHANGED': 1.0, 'WORSE': 0.0},
            'MED_E006': {'IMPROVED': 0.102, 'NOT_RECORDED': 0.255, 'UNCHANGED': 0.643, 'WORSE': 0.0},
            'ETOMIDATE': {'IMPROVED': 0.0, 'NOT_RECORDED': 0.8, 'UNCHANGED': 0.2, 'WORSE': 0.0},
            'FENTANYL': {'IMPROVED': 0.673, 'NOT_RECORDED': 0.170, 'UNCHANGED': 0.154, 'WORSE': 0.002},
            'FUROSEMIDE': {'IMPROVED': 0.444, 'NOT_RECORDED': 0.244, 'UNCHANGED': 0.311, 'WORSE': 0.0},
            'GLUCAGON': {'IMPROVED': 0.531, 'NOT_RECORDED': 0.272, 'UNCHANGED': 0.196, 'WORSE': 0.006},
            'GLUCOSE': {'IMPROVED': 0.491, 'NOT_RECORDED': 0.301, 'UNCHANGED': 0.202, 'WORSE': 0.006},
            'GLUCOSE 100 MG/ML': {'IMPROVED': 0.105, 'NOT_RECORDED': 0.316, 'UNCHANGED': 0.579, 'WORSE': 0.0},
            'GLUCOSE 500 MG/ML': {'IMPROVED': 1.0, 'NOT_RECORDED': 0.0, 'UNCHANGED': 0.0, 'WORSE': 0.0},
            'HALOPERIDOL': {'IMPROVED': 0.611, 'NOT_RECORDED': 0.222, 'UNCHANGED': 0.167, 'WORSE': 0.0},
            'HYDROXOCOBALAMIN': {'IMPROVED': 0.0, 'NOT_RECORDED': 0.0, 'UNCHANGED': 1.0, 'WORSE': 0.0},
            'HYDROXOCOBALAMIN INJECTION [CYANOKIT]': {'IMPROVED': 1.0, 'NOT_RECORDED': 0.0, 'UNCHANGED': 0.0, 'WORSE': 0.0},
            'IBUPROFEN': {'IMPROVED': 0.4, 'NOT_RECORDED': 0.6, 'UNCHANGED': 0.0, 'WORSE': 0.0},
            'IPRATROPIUM': {'IMPROVED': 0.629, 'NOT_RECORDED': 0.260, 'UNCHANGED': 0.110, 'WORSE': 0.003},
            'KETAMINE': {'IMPROVED': 0.569, 'NOT_RECORDED': 0.150, 'UNCHANGED': 0.276, 'WORSE': 0.003},
            'LABETALOL': {'IMPROVED': 0.681, 'NOT_RECORDED': 0.220, 'UNCHANGED': 0.097, 'WORSE': 0.0},
            'LEVOPHED': {'IMPROVED': 0.707, 'NOT_RECORDED': 0.049, 'UNCHANGED': 0.244, 'WORSE': 0.0},
            'LIDOCAINE': {'IMPROVED': 0.346, 'NOT_RECORDED': 0.244, 'UNCHANGED': 0.410, 'WORSE': 0.0},
            'MAGNESIUM SULFATE': {'IMPROVED': 0.208, 'NOT_RECORDED': 0.278, 'UNCHANGED': 0.514, 'WORSE': 0.0},
            'METHYLPREDNISOLONE': {'IMPROVED': 0.398, 'NOT_RECORDED': 0.266, 'UNCHANGED': 0.336, 'WORSE': 0.001},
            'METOPROLOL': {'IMPROVED': 0.669, 'NOT_RECORDED': 0.170, 'UNCHANGED': 0.158, 'WORSE': 0.0},
            'MIDAZOLAM': {'IMPROVED': 0.513, 'NOT_RECORDED': 0.319, 'UNCHANGED': 0.166, 'WORSE': 0.0},
            'MIDAZOLAM 1 MG/ML [VERSED]': {'IMPROVED': 0.748, 'NOT_RECORDED': 0.082, 'UNCHANGED': 0.163, 'WORSE': 0.007},
            'MIDAZOLAM 10MG/2ML': {'IMPROVED': 0.671, 'NOT_RECORDED': 0.0, 'UNCHANGED': 0.329, 'WORSE': 0.0},
            'MIDAZOLAM 5 MG/ML [VERSED]': {'IMPROVED': 0.740, 'NOT_RECORDED': 0.076, 'UNCHANGED': 0.183, 'WORSE': 0.002},
            'MIDAZOLAM 5MG/5ML': {'IMPROVED': 0.938, 'NOT_RECORDED': 0.0, 'UNCHANGED': 0.062, 'WORSE': 0.0},
            'MED_G002': {'IMPROVED': 0.559, 'NOT_RECORDED': 0.231, 'UNCHANGED': 0.204, 'WORSE': 0.003},
            'MED_J001': {'IMPROVED': 0.373, 'NOT_RECORDED': 0.219, 'UNCHANGED': 0.405, 'WORSE': 0.001},
            'NITROGLYCERIN': {'IMPROVED': 0.540, 'NOT_RECORDED': 0.185, 'UNCHANGED': 0.266, 'WORSE': 0.010},
            'MED_T002': {'IMPROVED': 0.25, 'NOT_RECORDED': 0.25, 'UNCHANGED': 0.5, 'WORSE': 0.0},
            'NORMAL SALINE': {'IMPROVED': 0.297, 'NOT_RECORDED': 0.212, 'UNCHANGED': 0.492, 'WORSE': 0.003},
            'ONDANSETRON': {'IMPROVED': 0.484, 'NOT_RECORDED': 0.234, 'UNCHANGED': 0.305, 'WORSE': 0.001},
            'MED_D001': {'IMPROVED': 0.617, 'NOT_RECORDED': 0.211, 'UNCHANGED': 0.172, 'WORSE': 0.003},
            'ROCURONIUM': {'IMPROVED': 0.509, 'NOT_RECORDED': 0.263, 'UNCHANGED': 0.229, 'WORSE': 0.0},
            'SODIUM BICARBONATE': {'IMPROVED': 0.082, 'NOT_RECORDED': 0.271, 'UNCHANGED': 0.643, 'WORSE': 0.0},
            'SODIUM CHLORIDE': {'IMPROVED': 0.1, 'NOT_RECORDED': 0.05, 'UNCHANGED': 0.85, 'WORSE': 0.0},
            'STERILE WATER': {'IMPROVED': 0.25, 'NOT_RECORDED': 0.5, 'UNCHANGED': 0.25, 'WORSE': 0.0},
            'SUCCINYLCHOLINE': {'IMPROVED': 0.25, 'NOT_RECORDED': 0.5, 'UNCHANGED': 0.25, 'WORSE': 0.0},
            'TETRACAINE': {'IMPROVED': 0.718, 'NOT_RECORDED': 0.154, 'UNCHANGED': 0.103, 'WORSE': 0.026},
            'VECURONIUM': {'IMPROVED': 0.25, 'NOT_RECORDED': 0.5, 'UNCHANGED': 0.25, 'WORSE': 0.0}
        }
        
        # Get patient response distribution for this medication
        if med['name'] in medication_response_probs:
            response_dist = medication_response_probs[med['name']]
            response_names = list(response_dist.keys())
            response_weights = list(response_dist.values())  # Pre-calculated probabilities
            patient_response = random.choices(response_names, weights=response_weights, k=1)[0]
        else:
            # Default patient response
            patient_response = random.choice(['IMPROVED', 'NOT_RECORDED', 'UNCHANGED', 'WORSE'])
        
        # Determine appropriate site based on realistic medication administration site data
        medication_sites = {
            'ALBUTEROL': {'ANTECUBIT ARM-LEFT': 1, 'MOUTH': 10},
            'DEXTROSE 10%': {'ANTECUBIT ARM-LEFT': 1, 'HAND-LEFT': 1},
            'DIPHENHYDRAMINE': {'ANTECUBITAL-LEFT': 2, 'ARM-RIGHT': 2, 'LOWER EXT': 1},
            'EPI 1:1,000': {'HUMERAL I': 1, 'OTHER': 1},
            'EPI 1:10,000': {'ANTECUBIT ARM-LEFT': 1},
            'EPI 1:100,000 (PDP)': {'HUMERAL': 2},
            'EPI 1:100,000 PDP': {'ANTECUBITAL-LEFT': 2},
            'FENTANYL': {'ANTECUBITAL-LEFT': 11, 'ANTECUBIT ARM-LEFT': 7, 'ARM-RIGHT': 4, 'HUMERAL I': 1, 'NOSE': 3, 'OTHER': 1},
            'GLUCOSE': {'MOUTH': 7},
            'IPRATROPIUM': {'MOUTH': 11},
            'KETAMINE': {'ANTECUBITAL-LEFT': 2, 'ANTECUBIT ARM-LEFT': 4, 'ARM-RIGHT': 1, 'HAND-LEFT': 1, 'HUMERAL I': 1},
            'LABETALOL': {'ANTECUBITAL-LEFT': 1},
            'LEVOPHED': {'ARM-RIGHT': 1, 'HUMERAL I': 1},
            'METHYLPREDNISOLONE': {'ANTECUBITAL-LEFT': 2, 'ANTECUBIT ARM-LEFT': 1},
            'METOPROLOL': {'ARM-RIGHT': 1},
            'MIDAZOLAM 1 MG/ML [VERSED]': {'LOWER EXT': 1},
            'MIDAZOLAM 5 MG/ML [VERSED]': {'ANTECUBITAL-LEFT': 1, 'ARM-RIGHT': 1},
            'MIDAZOLAM 5MG/5ML': {'LOWER EXT': 1},
            'MED_G002': {'ANTECUBITAL-LEFT': 1},
            'MED_J001': {'ANTECUBITAL-LEFT': 1, 'HUMERAL I': 1, 'NOSE': 2},
            'NITROGLYCERIN': {'MOUTH': 13},
            'NORMAL SALINE': {'ANTECUBITAL-LEFT': 6, 'ANTECUBIT ARM-LEFT': 7, 'ARM-RIGHT': 2, 'HAND-LEFT': 1},
            'ONDANSETRON': {'ANTECUBITAL-LEFT': 9, 'ANTECUBIT ARM-LEFT': 9, 'ARM-RIGHT': 3, 'HAND-LEFT': 1, 'HUMERAL I': 1},
            'MED_D001': {'MOUTH': 2, 'OTHER': 9},
            'ROCURONIUM': {'ANTECUBIT ARM-LEFT': 1, 'HUMERAL I': 1, 'OTHER': 1},
            'SUCCINYLCHOLINE': {'ANTECUBITAL-LEFT': 1}
        }
        
        # Get site distribution for this medication
        if med['name'] in medication_sites:
            site_dist = medication_sites[med['name']]
            site_names = list(site_dist.keys())
            site_weights = list(site_dist.values())
            medication_site = random.choices(site_names, weights=site_weights, k=1)[0]
        else:
            # Default site assignment based on selected route for medications not in the data
            if selected_route == 'ORAL':
                medication_site = 'MOUTH'
            elif selected_route == 'SUBLINGUAL':
                medication_site = 'MOUTH'
            elif selected_route == 'INHALED':
                medication_site = 'MOUTH'
            elif selected_route in ['INTRAMUSCULAR', 'INTRAVENOUS', 'INTRAVENC']:
                medication_site = random.choice(['ANTECUBITAL-LEFT', 'ANTECUBIT ARM-LEFT', 'ARM-RIGHT', 'HAND-LEFT', 'HUMERAL I'])
            else:
                medication_site = random.choice(['ANTECUBITAL-LEFT', 'ANTECUBIT ARM-LEFT', 'ARM-RIGHT', 'HAND-LEFT', 'HUMERAL I', 'MOUTH'])
        
        medication = EMSMedication(
            medication_id=str(uuid.uuid4()),
            administered_prior_to_ems_care=random.choice(['YES', 'NO', 'UNKNOWN']),
            medication_rxcui_code=med['code'],
            medication_name=med['name'],
            medication_administration_route=selected_route,
            medication_site=medication_site,
            dosage=dosage,
            dosage_unit=dosage_unit,
            patient_response=patient_response,
            complications=random.choice(['NONE', 'ALLERGIC_REACTION', 'OVERDOSE', 'INEFFECTIVE']),
            crew_member_name=f"{self.fake.first_name()} {self.fake.last_name()}",
            crew_member_level=ems_incident.get('crew_member_level', 'PARAMEDIC') if isinstance(ems_incident, dict) else ems_incident.crew_member_level,
            crew_badge_number=f"EMS{random.randint(1000, 9999)}",
            medication_authorization=random.choice(['PROTOCOL', 'ONLINE_MEDICAL_CONTROL', 'STANDING_ORDER']),                                                                                                           
            last_modified=ems_incident.get('call_datetime', self.fake.date_time_between(start_date='-30d', end_date='now')) if isinstance(ems_incident, dict) else ems_incident.call_datetime,
            incident_id=ems_incident.get('incident_id', str(uuid.uuid4())) if isinstance(ems_incident, dict) else ems_incident.incident_id,
            created_date=ems_incident.get('call_datetime', self.fake.date_time_between(start_date='-30d', end_date='now')) if isinstance(ems_incident, dict) else ems_incident.call_datetime,
            administered_datetime=ems_incident.get('arrive_datetime', self.fake.date_time_between(start_date='-30d', end_date='now')) if isinstance(ems_incident, dict) else ems_incident.arrive_datetime,
            broken_seal=random.choice(['YES', 'NO'])
        )
        
        return medication

    def generate_ems_patient(self, ems_incident=None):
        """Generate comprehensive EMS patient record with realistic demographics"""
        if not ems_incident:
            return None
        
        # Generate comprehensive demographic information
        # Handle both dict and object formats
        if isinstance(ems_incident, dict):
            patient_age = ems_incident.get('patient_age', 45)
            patient_gender = ems_incident.get('patient_sex', 'M')
            patient_race = ems_incident.get('patient_race', 'WHITE')
            patient_weight = ems_incident.get('patient_weight', 70.0)
            patient_full_name = ems_incident.get('patient_full_name', 'Unknown Patient')
            patient_date_of_birth = ems_incident.get('patient_date_of_birth', '1970-01-01')
        else:
            patient_age = ems_incident.patient_age
            patient_gender = ems_incident.patient_sex
            patient_race = ems_incident.patient_race
            patient_weight = ems_incident.patient_weight
            patient_full_name = ems_incident.patient_full_name
            patient_date_of_birth = ems_incident.patient_date_of_birth
        
        # Generate ethnicity based on race
        ethnicity_map = {
            'WHITE': ['NON_HISPANIC', 'HISPANIC'],
            'BLACK': ['NON_HISPANIC', 'HISPANIC'],
            'ASIAN': ['NON_HISPANIC', 'HISPANIC'],
            'HISPANIC': ['HISPANIC'],
            'OTHER': ['NON_HISPANIC', 'HISPANIC', 'UNKNOWN']
        }
        patient_ethnicity = random.choice(ethnicity_map.get(patient_race, ['UNKNOWN']))
        
        # Generate marital status based on age
        if patient_age < 18:
            patient_marital_status = 'SINGLE'
        elif patient_age < 25:
            patient_marital_status = random.choices(['SINGLE', 'MARRIED'], weights=[0.8, 0.2], k=1)[0]
        elif patient_age < 65:
            patient_marital_status = random.choices(['MARRIED', 'SINGLE', 'DIVORCED', 'WIDOWED'], weights=[0.6, 0.25, 0.1, 0.05], k=1)[0]
        else:
            patient_marital_status = random.choices(['MARRIED', 'WIDOWED', 'SINGLE', 'DIVORCED'], weights=[0.4, 0.35, 0.15, 0.1], k=1)[0]
        
        # Generate realistic height based on age and gender
        if patient_age < 18:
            # Pediatric heights
            if patient_gender == 'M':
                patient_height = random.randint(36, 70)  # 3'0" to 5'10"
            else:
                patient_height = random.randint(36, 65)  # 3'0" to 5'5"
        else:
            # Adult heights based on gender only
            if patient_gender == 'M':
                patient_height = random.randint(64, 75)  # 5'4" to 6'3" for adult males
            else:
                patient_height = random.randint(58, 69)  # 4'10" to 5'9" for adult females
        
        # Calculate BMI
        patient_bmi = round((patient_weight / (patient_height ** 2)) * 703, 1)
        
        # Generate home address (different from incident location)
        patient_home_tuple = self._get_cached_address()
        if len(patient_home_tuple) >= 6:
            patient_home_address, patient_home_city, patient_home_state, patient_home_zip, home_lat, home_lon = patient_home_tuple[:6]
        else:
            patient_home_address, patient_home_city, patient_home_state, patient_home_zip = patient_home_tuple[:4]
            home_lat, home_lon = None, None
        patient_home_address_geo = f"{patient_home_address}, {patient_home_city}, {patient_home_state} {patient_home_zip}"
        
        # Generate contact information
        patient_phone = self.fake.phone_number()
        patient_email = self.fake.email()
        
        # Generate emergency contact
        emergency_contact_name = self._generate_demographic_appropriate_name(patient_race, patient_gender)
        emergency_contact_relationship = random.choice(['SPOUSE', 'PARENT', 'CHILD', 'SIBLING', 'FRIEND', 'OTHER'])
        emergency_contact_phone = self.fake.phone_number()
        
        # Generate insurance information
        insurance_providers = ['AETNA', 'ANTHEM', 'BLUE_CROSS_BLUE_SHIELD', 'CIGNA', 'HUMANA', 'KAISER_PERMANENTE', 'MEDICARE', 'MEDICAID', 'UNITED_HEALTHCARE', 'SELF_PAY']
        insurance_provider = random.choices(insurance_providers, weights=[0.15, 0.15, 0.20, 0.10, 0.10, 0.05, 0.10, 0.05, 0.08, 0.02], k=1)[0]
        insurance_policy_number = f"{random.randint(100000000, 999999999)}"
        insurance_group_number = f"{random.randint(100000, 999999)}"
        
        # Generate employment and education
        occupation = self._generate_realistic_occupation(patient_age, patient_gender)
        employer = self.fake.company() if occupation != 'UNEMPLOYED' and occupation != 'RETIRED' else 'N/A'
        
        education_levels = ['LESS_THAN_HIGH_SCHOOL', 'HIGH_SCHOOL', 'SOME_COLLEGE', 'BACHELORS_DEGREE', 'GRADUATE_DEGREE']
        if patient_age < 18:
            education_level = random.choice(['LESS_THAN_HIGH_SCHOOL', 'HIGH_SCHOOL'])
        elif patient_age < 25:
            education_level = random.choices(['HIGH_SCHOOL', 'SOME_COLLEGE'], weights=[0.6, 0.4], k=1)[0]
        else:
            education_level = random.choices(education_levels, weights=[0.15, 0.35, 0.25, 0.15, 0.10], k=1)[0]
        
        # Generate medical information
        primary_care_physician = f"Dr. {self.fake.last_name()}"
        primary_care_physician_phone = self.fake.phone_number()
        
        # Use medical information from incident for consistency, or generate if not available
        if hasattr(ems_incident, 'patient_medical_history'):
            medical_history = ems_incident.patient_medical_history
        else:
            medical_history = self._generate_medical_history(patient_age, patient_race, patient_gender)
            
        if hasattr(ems_incident, 'patient_chronic_conditions'):
            chronic_conditions = ems_incident.patient_chronic_conditions
        else:
            chronic_conditions = self._generate_chronic_conditions(patient_age, patient_gender, patient_race)
            
        if hasattr(ems_incident, 'patient_current_medications'):
            current_medications = ems_incident.patient_current_medications
        else:
            current_medications = self._generate_current_medications(patient_age, patient_gender)
            
        # Generate allergies and family history (these are less incident-specific)
        known_allergies = self._generate_realistic_allergies(patient_age)
        family_medical_history = self._generate_family_medical_history(patient_race, patient_gender)
        
        # Generate social history
        social_history = {
            'smoking_status': random.choices(['NEVER', 'FORMER', 'CURRENT'], weights=[0.6, 0.25, 0.15], k=1)[0],
            'alcohol_use': random.choices(['NONE', 'OCCASIONAL', 'MODERATE', 'HEAVY'], weights=[0.4, 0.35, 0.2, 0.05], k=1)[0],
            'drug_use': random.choices(['NONE', 'RECREATIONAL', 'PRESCRIPTION_MISUSE'], weights=[0.85, 0.1, 0.05], k=1)[0],
            'exercise_frequency': random.choices(['NONE', 'OCCASIONAL', 'REGULAR', 'DAILY'], weights=[0.3, 0.4, 0.25, 0.05], k=1)[0]
        }
        
        # Generate functional status
        mobility_status = random.choices(['INDEPENDENT', 'ASSISTED', 'WHEELCHAIR', 'BEDBOUND'], weights=[0.8, 0.15, 0.04, 0.01], k=1)[0]
        cognitive_status = random.choices(['NORMAL', 'MILD_IMPAIRMENT', 'MODERATE_IMPAIRMENT', 'SEVERE_IMPAIRMENT'], weights=[0.85, 0.1, 0.04, 0.01], k=1)[0]
        living_situation = random.choices(['ALONE', 'WITH_FAMILY', 'WITH_SPOUSE', 'ASSISTED_LIVING', 'NURSING_HOME'], weights=[0.3, 0.35, 0.25, 0.08, 0.02], k=1)[0]
        caregiver_status = random.choices(['NONE', 'FAMILY', 'PAID_CAREGIVER', 'HEALTHCARE_FACILITY'], weights=[0.6, 0.3, 0.08, 0.02], k=1)[0]
        
        # Generate additional demographics
        preferred_language = random.choices(['ENGLISH', 'SPANISH', 'CHINESE', 'OTHER'], weights=[0.85, 0.1, 0.02, 0.03], k=1)[0]
        interpreter_needed = preferred_language != 'ENGLISH' and random.random() < 0.8
        veteran_status = patient_age > 18 and random.random() < 0.1
        disability_status = random.choices(['NONE', 'PHYSICAL', 'COGNITIVE', 'BOTH'], weights=[0.9, 0.06, 0.03, 0.01], k=1)[0]
        
        patient = EMSPatient(
            # Basic Demographics
            patient_id=ems_incident.patient_id if hasattr(ems_incident, 'patient_id') else ems_incident.get('patient_id', str(uuid.uuid4())),
            patient_full_name=patient_full_name,
            patient_date_of_birth=patient_date_of_birth,
            patient_age=patient_age,
            patient_gender=patient_gender,
            patient_race=patient_race,
            patient_ethnicity=patient_ethnicity,
            patient_marital_status=patient_marital_status,
            
            # Physical Characteristics
            patient_weight=patient_weight,
            patient_height=patient_height,
            patient_bmi=patient_bmi,
            
            # Contact Information
            patient_home_address=patient_home_address,
            patient_home_city=patient_home_city,
            patient_home_state=patient_home_state,
            patient_home_zip=patient_home_zip,
            patient_home_address_geo=patient_home_address_geo,
            patient_phone=patient_phone,
            patient_email=patient_email,
            
            # Emergency Contacts
            emergency_contact_name=emergency_contact_name,
            emergency_contact_relationship=emergency_contact_relationship,
            emergency_contact_phone=emergency_contact_phone,
            
            # Insurance & Financial
            insurance_provider=insurance_provider,
            insurance_policy_number=insurance_policy_number,
            insurance_group_number=insurance_group_number,
            
            # Employment & Education
            occupation=occupation,
            employer=employer,
            education_level=education_level,
            
            # Medical History & Risk Factors
            primary_care_physician=primary_care_physician,
            primary_care_physician_phone=primary_care_physician_phone,
            known_allergies=known_allergies,
            current_medications=current_medications,
            chronic_conditions=chronic_conditions,
            medical_history=medical_history,
            family_medical_history=family_medical_history,
            social_history=social_history,
            
            # Functional Status
            mobility_status=mobility_status,
            cognitive_status=cognitive_status,
            living_situation=living_situation,
            caregiver_status=caregiver_status,
            
            # Legal & Administrative
            patient_pk=ems_incident.patient_pk,
            incident_id=ems_incident.incident_id,
            created_date=ems_incident.call_datetime,
            last_modified=ems_incident.call_datetime,
            is_superuser=False,
            incidents_past_month=random.randint(0, 5),
            incidents_past_year=random.randint(0, 20),
            
            # Additional Demographics
            preferred_language=preferred_language,
            interpreter_needed=interpreter_needed,
            veteran_status=veteran_status,
            disability_status=disability_status,
            
            # Geographic coordinates
            patient_home_latitude=float(home_lat) if home_lat is not None else 0.0,
            patient_home_longitude=float(home_lon) if home_lon is not None else 0.0
        )
        
        return patient
    def _generate_realistic_occupation(self, age, gender):
        """Generate realistic occupation based on age and gender"""
        if age < 16:
            return 'STUDENT'
        elif age < 18:
            return random.choices(['STUDENT', 'PART_TIME_WORKER', 'UNEMPLOYED'], weights=[0.8, 0.15, 0.05], k=1)[0]
        elif age < 65:
            occupations = [
                'OFFICE_WORKER', 'HEALTHCARE_WORKER', 'EDUCATOR', 'RETAIL_WORKER', 'CONSTRUCTION_WORKER',
                'TECHNICAL_WORKER', 'SERVICE_WORKER', 'MANAGER', 'SELF_EMPLOYED', 'UNEMPLOYED'
            ]
            weights = [0.25, 0.15, 0.10, 0.15, 0.10, 0.10, 0.08, 0.05, 0.02, 0.05]
            return random.choices(occupations, weights=weights, k=1)[0]
        else:
            return random.choices(['RETIRED', 'PART_TIME_WORKER', 'UNEMPLOYED'], weights=[0.85, 0.10, 0.05], k=1)[0]

    def _generate_realistic_allergies(self, age):
        """Generate realistic allergies based on age"""
        allergies = []
        common_allergies = ['PENICILLIN', 'SULFONAMIDES', 'ASPIRIN', 'NUTS', 'SHELLFISH', 'POLLEN', 'DUST', 'NONE']
        
        if random.random() < 0.3:  # 30% have allergies
            num_allergies = random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.10, 0.05], k=1)[0]
            selected_allergies = random.sample(common_allergies[:-1], num_allergies)  # Exclude 'NONE'
            allergies.extend(selected_allergies)
        
        return allergies if allergies else ['NONE']

    def _generate_current_medications(self, age, gender):
        """Generate realistic current medications based on age and gender"""
        medications = []
        
        # Age-based medication likelihood
        if age > 50:
            medication_chance = 0.7  # 70% of older adults take medications
        elif age > 30:
            medication_chance = 0.4  # 40% of middle-aged adults
        else:
            medication_chance = 0.2  # 20% of younger adults
        
        if random.random() < medication_chance:
            common_medications = [
                'METFORMIN', 'LISINOPRIL', 'ATORVASTATIN', 'OMEPRAZOLE', 'METOPROLOL',
                'AMLODIPINE', 'SERTRALINE', 'LEVOTHYROXINE', 'ALBUTEROL', 'PREDNISONE',
                'WARFARIN', 'FUROSEMIDE', 'GABAPENTIN', 'TRAMADOL', 'CYCLOBENZAPRINE'
            ]
            num_meds = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.10, 0.05], k=1)[0]
            selected_meds = random.sample(common_medications, num_meds)
            medications.extend(selected_meds)
        
        return medications if medications else ['NONE']

    def _generate_chronic_conditions(self, age, gender, race):
        """Generate realistic chronic conditions based on age and gender"""
        conditions = []
        
        # Age-based condition likelihood
        if age > 65:
            condition_chance = 0.8  # 80% of seniors have chronic conditions
        elif age > 50:
            condition_chance = 0.6  # 60% of middle-aged adults
        elif age > 30:
            condition_chance = 0.3  # 30% of younger adults
        else:
            condition_chance = 0.1  # 10% of young adults
        
        if random.random() < condition_chance:
            common_conditions = [
                'HYPERTENSION', 'DIABETES_TYPE_2', 'HIGH_CHOLESTEROL', 'ASTHMA', 'COPD',
                'ARTHRITIS', 'DEPRESSION', 'ANXIETY', 'HEART_DISEASE', 'KIDNEY_DISEASE',
                'THYROID_DISORDER', 'MIGRAINE', 'BACK_PAIN', 'OSTEOPOROSIS'
            ]
            num_conditions = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05], k=1)[0]
            selected_conditions = random.sample(common_conditions, num_conditions)
            conditions.extend(selected_conditions)
        
        return conditions if conditions else ['NONE']

    def _generate_family_medical_history(self, race, gender):
        """Generate realistic family medical history based on general population patterns"""
        family_history = []
        
        # Common family medical conditions
        family_conditions = ['HEART_DISEASE', 'CANCER', 'DIABETES', 'HYPERTENSION', 'STROKE', 'ALZHEIMERS', 'DEPRESSION', 'ASTHMA']
        
        if random.random() < 0.6:  # 60% have family history
            num_conditions = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1], k=1)[0]
            selected_conditions = random.sample(family_conditions, num_conditions)
            family_history.extend(selected_conditions)
        
        return family_history if family_history else ['NONE']

    def _adjust_medication_weights_for_conditions(self, med_names, med_weights, patient_conditions, ems_incident):
        """Adjust medication selection weights based on patient conditions and incident type for medical consistency"""
        adjusted_weights = list(med_weights)  # Start with original weights
        
        # Define condition-specific medication preferences
        condition_medications = {
            'HYPERTENSION': ['LISINOPRIL', 'METOPROLOL', 'AMLODIPINE', 'LABETALOL'],
            'DIABETES_TYPE_2': ['METFORMIN', 'GLUCOSE', 'DEXTROSE_50%', 'DEXTROSE_10%'],
            'ASTHMA': ['ALBUTEROL', 'IPRATROPIUM', 'EPINEPHRINE', 'METHYLPREDNISOLONE'],
            'COPD': ['ALBUTEROL', 'IPRATROPIUM', 'METHYLPREDNISOLONE', 'OXYGEN'],
            'HEART_DISEASE': ['ASPIRIN', 'NITROGLYCERIN', 'ATROPINE', 'EPI_1:1,000', 'AMIODARONE'],
            'DEPRESSION': ['SERTRALINE', 'MIDAZOLAM'],
            'ANXIETY': ['MIDAZOLAM', 'LORAZEPAM'],
            'PAIN': ['FENTANYL', 'MORPHINE', 'KETAMINE'],
            'NAUSEA': ['ONDANSETRON', 'DIPHENHYDRAMINE'],
            'ALLERGIC_REACTION': ['EPI_1:1,000', 'DIPHENHYDRAMINE', 'METHYLPREDNISOLONE'],
            'CARDIAC_ARREST': ['EPI_1:1,000', 'ATROPINE', 'AMIODARONE', 'SODIUM_BICARBONATE'],
            'STROKE': ['ASPIRIN', 'GLUCOSE'],
            'SEIZURE': ['MIDAZOLAM', 'DIAZEPAM']
        }
        
        # Define incident type specific medication preferences
        incident_medications = {
            '2301019': ['EPI_1:1,000', 'ATROPINE', 'AMIODARONE', 'SODIUM_BICARBONATE'],  # Cardiac Arrest
            '2301021': ['ASPIRIN', 'NITROGLYCERIN', 'OXYGEN', 'MORPHINE'],  # Chest Pain
            '2301013': ['ALBUTEROL', 'IPRATROPIUM', 'OXYGEN', 'METHYLPREDNISOLONE'],  # Breathing Problem
            '2301053': ['NALOXONE', 'NORMAL_SALINE', 'OXYGEN'],  # Overdose
            '2301025': ['MIDAZOLAM', 'DIAZEPAM', 'LORAZEPAM'],  # Seizure
            '2301003': ['EPI_1:1,000', 'DIPHENHYDRAMINE', 'METHYLPREDNISOLONE'],  # Allergic Reaction
            '2301077': ['GLUCOSE', 'DEXTROSE_50%', 'OXYGEN', 'NORMAL_SALINE'],  # Unconscious
            '2301027': ['GLUCOSE', 'DEXTROSE_50%', 'GLUCAGON'],  # Diabetic Problem
            '2301073': ['FENTANYL', 'MORPHINE', 'KETAMINE', 'NORMAL_SALINE'],  # Traumatic Injury
            '2301045': ['NORMAL_SALINE', 'MORPHINE', 'OXYGEN']  # Hemorrhage
        }
        
        # Boost weights for condition-appropriate medications
        for i, med_name in enumerate(med_names):
            weight_multiplier = 1.0
            
            # Check if medication is appropriate for patient conditions
            for condition in patient_conditions:
                if condition in condition_medications and med_name in condition_medications[condition]:
                    weight_multiplier *= 2.0  # Double the weight for condition-appropriate meds
            
            # Check if medication is appropriate for incident type
            if hasattr(ems_incident, 'incident_type_code'):
                incident_code = ems_incident.incident_type_code
                if incident_code in incident_medications and med_name in incident_medications[incident_code]:
                    weight_multiplier *= 1.5  # 50% boost for incident-appropriate meds
            
            # Reduce weight for contraindicated medications
            contraindications = {
                'ASPIRIN': ['BLEEDING_DISORDER', 'PEPTIC_ULCER'],
                'NITROGLYCERIN': ['HYPOTENSION', 'HEAD_INJURY'],
                'MORPHINE': ['RESPIRATORY_DEPRESSION', 'HEAD_INJURY'],
                'FENTANYL': ['RESPIRATORY_DEPRESSION', 'HEAD_INJURY'],
                'EPI_1:1,000': ['HYPERTENSION', 'HEART_DISEASE']
            }
            
            for contraindication in contraindications.get(med_name, []):
                if contraindication in patient_conditions:
                    weight_multiplier *= 0.1  # Reduce to 10% for contraindicated meds
            
            adjusted_weights[i] = int(adjusted_weights[i] * weight_multiplier)
        
        return adjusted_weights

    def _generate_gps_coordinates_from_address(self, address, city, state, zip_code):
        """Generate GPS coordinates for an address using existing coordinates from address pool"""
        # First, try to find the exact address in our pool with coordinates
        if not self._pool_initialized:
            self._load_address_library()
            
        for addr_tuple in self._real_address_pool:
            if len(addr_tuple) >= 6:  # Has coordinates
                addr, addr_city, addr_state, addr_zip, lat, lon = addr_tuple[:6]
                if (addr == address and addr_city.upper() == city.upper() and 
                    addr_state.upper() == state.upper() and addr_zip == zip_code):
                    return float(lat), float(lon), "EXACT_MATCH"
        
        # If no exact match, use geocoding for real addresses
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
        
        try:
            geolocator = Nominatim(user_agent="seattle_data_generator")
            full_address = f"{address}, {city}, {state} {zip_code}"
            location = geolocator.geocode(full_address, timeout=5)
            
            if location:
                return location.latitude, location.longitude, "GEOCODED"
        except (GeocoderTimedOut, GeocoderUnavailable, Exception):
            pass
        
        # Fallback: Use ZIP code based approximation
        seattle_zip_coords = {
            '98101': (47.6062, -122.3321),  # Downtown Seattle
            '98102': (47.6062, -122.3321),  # Capitol Hill
            '98103': (47.6614, -122.3655),  # Wallingford
            '98104': (47.6062, -122.3321),  # Pioneer Square
            '98105': (47.6614, -122.3655),  # University District
            '98106': (47.5206, -122.3344),  # West Seattle
            '98107': (47.6614, -122.3655),  # Ballard
            '98108': (47.5206, -122.3344),  # Georgetown
            '98109': (47.6062, -122.3321),  # Queen Anne
            '98112': (47.6062, -122.3321),  # Madison Park
            '98115': (47.6614, -122.3655),  # Ravenna
            '98116': (47.5206, -122.3344),  # West Seattle
            '98117': (47.6614, -122.3655),  # Crown Hill
            '98118': (47.5206, -122.3344),  # Columbia City
            '98119': (47.6062, -122.3321),  # Queen Anne
            '98121': (47.6062, -122.3321),  # Belltown
            '98122': (47.6062, -122.3321),  # Capitol Hill
            '98125': (47.6614, -122.3655),  # Lake City
            '98126': (47.5206, -122.3344),  # West Seattle
            '98133': (47.6614, -122.3655),  # Northgate
            '98134': (47.6062, -122.3321),  # Industrial District
            '98136': (47.5206, -122.3344),  # West Seattle
            '98144': (47.6062, -122.3321),  # Central District
            '98146': (47.5206, -122.3344),  # West Seattle
            '98148': (47.5206, -122.3344),  # White Center
            '98154': (47.6062, -122.3321),  # Industrial District
            '98155': (47.6614, -122.3655),  # Northgate
            '98158': (47.5206, -122.3344),  # White Center
            '98161': (47.6062, -122.3321),  # Industrial District
            '98164': (47.6062, -122.3321),  # Industrial District
            '98166': (47.5206, -122.3344),  # White Center
            '98168': (47.5206, -122.3344),  # White Center
            '98174': (47.6062, -122.3321),  # Industrial District
            '98177': (47.6614, -122.3655),  # Shoreline
            '98178': (47.5206, -122.3344),  # Rainier Valley
            '98188': (47.5206, -122.3344),  # White Center
            '98195': (47.6614, -122.3655),  # University District
            '98199': (47.6062, -122.3321),  # Magnolia
        }
        
        # Get coordinates for ZIP code, with some random variation
        if zip_code in seattle_zip_coords:
            base_lat, base_lon = seattle_zip_coords[zip_code]
            # Add small random variation (±0.01 degrees ≈ ±0.7 miles)
            lat = base_lat + random.uniform(-0.01, 0.01)
            lon = base_lon + random.uniform(-0.01, 0.01)
            return lat, lon, "ZIP-based approximation"
        else:
            # Fallback to downtown Seattle with variation
            lat = 47.6062 + random.uniform(-0.05, 0.05)
            lon = -122.3321 + random.uniform(-0.05, 0.05)
            return lat, lon, "Seattle area approximation"

    def _generate_ems_narrative(self, incident_type_code, patient_age, patient_sex, priority):
        """Generate realistic EMS incident narrative"""
        narratives = {
            '2301019': f"Responded to {patient_age}yo {patient_sex} found unresponsive. CPR initiated. Transported to hospital with ongoing resuscitation efforts.",
            '2301021': f"Chest pain call for {patient_age}yo {patient_sex}. Patient reports {random.choice(['crushing', 'sharp', 'pressure-like'])} chest pain. ECG performed, oxygen administered.",
            '2301013': f"Respiratory distress call. {patient_age}yo {patient_sex} patient experiencing {random.choice(['shortness of breath', 'wheezing', 'difficulty breathing'])}. Oxygen therapy initiated.",
            '2301073': f"Trauma call. {patient_age}yo {patient_sex} involved in {random.choice(['motor vehicle accident', 'fall', 'industrial accident'])}. Patient assessed and stabilized.",
            '2301053': f"Overdose/poisoning call. {patient_age}yo {patient_sex} found {random.choice(['unconscious', 'confused', 'agitated'])}. Narcan administered. Patient responding to treatment.",
            '2301077': f"Unconscious person call. {patient_age}yo {patient_sex} found unresponsive. Patient assessment performed. {random.choice(['Patient regained consciousness', 'Patient transported unconscious', 'Patient refused transport'])}.",
        }
        
        if incident_type_code in narratives:
            return narratives[incident_type_code]
        
        return f"EMS response to {patient_age}yo {patient_sex} patient. Assessment and treatment provided. Patient {random.choice(['transported', 'refused transport', 'treated on scene'])}."

    def _generate_scene_safety_assessment(self, incident_type_code):
        """Generate scene safety assessment based on incident type"""
        safety_assessments = {
            '2301035': 'FIRE_HAZARD',  # Fire
            '2301007': 'VIOLENCE_POTENTIAL',  # Assault
            '2301059': 'BEHAVIORAL_HAZARD',  # Psychiatric Problem
            '2301063': 'WEAPONS_PRESENT',  # Gunshot/Stab wound
            '2301043': 'ENVIRONMENTAL_HAZARD',  # Heat/Cold exposure
            '2301033': 'TRAFFIC_HAZARD',  # Falls (if traffic-related)
        }
        
        return safety_assessments.get(incident_type_code, 'SCENE_SECURE')

    def _generate_environmental_conditions(self):
        """Generate environmental conditions for incident"""
        conditions = [
            'CLEAR_WEATHER',
            'RAINY_CONDITIONS', 
            'SNOW_ICE_PRESENT',
            'FOGGY_CONDITIONS',
            'HIGH_TEMPERATURE',
            'LOW_TEMPERATURE',
            'WINDY_CONDITIONS',
            'DARK_CONDITIONS'
        ]
        return random.choice(conditions)

    def _generate_destination_facility(self, priority):
        """Generate destination facility based on priority"""
        if priority in ['HIGH', 'CRITICAL']:
            facilities = [
                ('Harborview Medical Center', 'TRAUMA_CENTER'),
                ('Swedish Medical Center', 'TRAUMA_CENTER'),
                ('Virginia Mason Medical Center', 'TERTIARY_CARE'),
                ('University of Washington Medical Center', 'TERTIARY_CARE')
            ]
        else:
            facilities = [
                ('Swedish Medical Center', 'GENERAL_HOSPITAL'),
                ('Virginia Mason Medical Center', 'GENERAL_HOSPITAL'),
                ('Providence Regional Medical Center', 'GENERAL_HOSPITAL'),
                ('Overlake Medical Center', 'GENERAL_HOSPITAL'),
                ('EvergreenHealth Medical Center', 'GENERAL_HOSPITAL')
            ]
        
        return random.choice(facilities)

    def _generate_patient_assessment_scores(self, incident_type_code, patient_age):
        """Generate patient assessment scores based on incident type and age"""
        # Pain scale (0-10)
        if incident_type_code in ['2301073', '2301045', '2301011', '2301001']:  # Trauma, pain-related
            pain_score = random.randint(4, 10)
        elif incident_type_code in ['2301021', '2301013']:  # Chest pain, breathing
            pain_score = random.randint(3, 8)
        else:
            pain_score = random.randint(0, 5)
        
        # Glasgow Coma Scale (3-15)
        if incident_type_code == '2301019':  # Cardiac arrest
            gcs_score = 3
        elif incident_type_code in ['2301077', '2301053']:  # Unconscious, overdose
            gcs_score = random.randint(3, 8)
        elif patient_age >= 65:
            gcs_score = random.randint(12, 15)  # Slightly lower for elderly
        else:
            gcs_score = random.randint(13, 15)  # Normal range
        
        return pain_score if pain_score > 0 else None, gcs_score if gcs_score < 15 else None

    def _should_reuse_existing_patient(self, incident_datetime):
        """Determine if we should reuse an existing patient or create a new one"""
        if len(self._patient_pool) == 0:
            return None, False  # No patients to reuse, create new one
        
        # Realistic frequent caller patterns
        # 70% new patients, 20% 2-3 incidents, 10% frequent callers (4+ incidents)
        reuse_decision = random.choices(
            ['NEW_PATIENT', 'REUSE_EXISTING', 'FREQUENT_CALLER'],
            weights=[0.70, 0.20, 0.10],
            k=1
        )[0]
        
        if reuse_decision == 'NEW_PATIENT':
            return None, False
        
        # For existing patients, filter by realistic criteria
        eligible_patients = []
        for patient in self._patient_pool:
            patient_id = patient['patient_id']
            incident_count = len(self._patient_incident_history.get(patient_id, []))
            
            # Skip patients who already have too many incidents
            if incident_count >= 8:  # Max 8 incidents per patient
                continue
                
            # Check if enough time has passed since last incident
            if patient_id in self._patient_incident_history:
                last_incident_date = max(self._patient_incident_history[patient_id])
                days_since_last = (incident_datetime - last_incident_date).days
                
                # Realistic time gaps between incidents
                if incident_count == 0:  # First reuse
                    min_days = 1  # Can reuse same day
                elif incident_count == 1:  # Second incident
                    min_days = 7  # At least a week
                elif incident_count < 4:  # 3rd-4th incident
                    min_days = 14  # At least 2 weeks
                else:  # Frequent caller (5+ incidents)
                    min_days = 30  # At least a month
                
                if days_since_last >= min_days:
                    eligible_patients.append(patient)
            else:
                eligible_patients.append(patient)
        
        if eligible_patients:
            # Weight selection based on incident frequency (more incidents = higher chance of reuse)
            weights = []
            for patient in eligible_patients:
                patient_id = patient['patient_id']
                incident_count = len(self._patient_incident_history.get(patient_id, []))
                # Higher weight for patients with more incidents (frequent callers)
                weight = 1 + (incident_count * 0.5)
                weights.append(weight)
            
            selected_patient = random.choices(eligible_patients, weights=weights, k=1)[0]
            return selected_patient, True
        
        return None, False

    def _update_patient_incident_history(self, patient_id, incident_datetime):
        """Update the incident history for a patient"""
        if patient_id not in self._patient_incident_history:
            self._patient_incident_history[patient_id] = []
        self._patient_incident_history[patient_id].append(incident_datetime)
    def _generate_incident_for_existing_patient(self, existing_patient, incident_datetime):
        """Generate an incident for an existing patient with realistic progression"""
        patient_id = existing_patient['patient_id']
        incident_count = len(self._patient_incident_history.get(patient_id, []))
        
        # Get patient's existing medical history
        existing_conditions = existing_patient.get('chronic_conditions', [])
        existing_medical_history = existing_patient.get('medical_history', [])
        
        # Determine incident type based on patient's conditions and history
        # Frequent callers often have recurring issues related to their conditions
        if incident_count >= 3:  # Frequent caller - more likely to have condition-related incidents
            incident_type_code = self._select_incident_for_chronic_condition(existing_conditions, existing_medical_history)
        else:
            # Regular incident selection but may be influenced by conditions
            incident_type_code, _ = self._choose_ems_incident_type(incident_datetime)
            if existing_conditions:
                incident_type_code = self._influence_incident_by_medical_history(existing_medical_history, incident_type_code)
        
        incident_type_description = EMS_INCIDENT_CODES[incident_type_code]
        
        # Use existing patient demographics but may age them slightly
        patient_age = existing_patient['patient_age']
        patient_sex = existing_patient['patient_sex']
        patient_race = existing_patient['patient_race']
        
        # Generate new incident location (may be different from home)
        incident_address, incident_city, incident_state, incident_zip = self._get_cached_address()
        
        # Generate incident-specific details
        priority = self._determine_priority_from_incident_type(incident_type_code)
        
        # Generate GPS coordinates for incident location
        lat, lon, accuracy = self._generate_gps_coordinates_from_address(incident_address, incident_city, incident_state, incident_zip)
        
        # Generate GPS coordinates for patient home address
        home_lat, home_lon, _ = self._generate_gps_coordinates_from_address(
            existing_patient['patient_home_address'], 
            existing_patient.get('patient_home_city', 'Seattle'), 
            'WA', 
            existing_patient.get('patient_home_zip', '98101')
        )
        
        # Generate destination facility
        destination_name, destination_type = self._generate_destination_facility(priority)
        
        # Generate patient assessment scores
        pain_score, gcs_score = self._generate_patient_assessment_scores(incident_type_code, patient_age)
        
        # Response times (may be faster for known patients)
        if incident_count >= 3:
            # Frequent callers - EMS may respond faster due to familiarity
            dispatch_delay = timedelta(seconds=random.randint(10, 45))
            en_route_delay = timedelta(seconds=random.randint(20, 90))
            arrive_delay = timedelta(minutes=random.randint(2, 6))
        else:
            # Normal response times
            dispatch_delay = timedelta(seconds=random.randint(15, 60))
            en_route_delay = timedelta(seconds=random.randint(30, 120))
            arrive_delay = timedelta(minutes=random.randint(3, 8))
        
        # Create incident with existing patient data
        incident = EMSIncident(
            incident_id=str(uuid.uuid4()),
            incident_number=f"EMS{incident_datetime.year}{random.randint(100000, 999999)}",
            call_number=f"E{incident_datetime.year}{random.randint(1000000, 9999999)}",
            incident_type=incident_type_description,
            incident_type_code=incident_type_code,
            incident_type_description=incident_type_description,
            incident_subtype=f"{incident_type_description}_SUBTYPE",
            priority=priority,
            call_datetime=incident_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_datetime=(incident_datetime + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            en_route_datetime=(incident_datetime + dispatch_delay + en_route_delay).strftime('%Y-%m-%d %H:%M:%S'),
            arrive_datetime=(incident_datetime + dispatch_delay + en_route_delay + arrive_delay).strftime('%Y-%m-%d %H:%M:%S'),
            transport_datetime=(incident_datetime + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=random.randint(10, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            hospital_arrival_datetime=(incident_datetime + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=random.randint(15, 45))).strftime('%Y-%m-%d %H:%M:%S'),
            clear_datetime=(incident_datetime + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=random.randint(45, 90))).strftime('%Y-%m-%d %H:%M:%S'),
            dispatch_to_enroute_seconds=int(en_route_delay.total_seconds()),
            enroute_to_arrival_seconds=int(arrive_delay.total_seconds()),
            arrival_to_transport_seconds=random.randint(600, 1800),
            transport_to_hospital_seconds=random.randint(900, 2700),
            total_scene_time_seconds=random.randint(2700, 5400),
            total_incident_time_seconds=random.randint(5400, 10800),
            address=incident_address,
            city=incident_city,
            state=incident_state,
            zip_code=incident_zip,
            district=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']),
            responding_unit=self._assign_ems_unit_by_location(incident_city, incident_zip),
            crew_members=[f"PARAMEDIC_{self.fake.last_name().upper()}", f"EMT_{self.fake.last_name().upper()}"],
            patient_person_id=patient_id,
            patient_age=patient_age,
            patient_sex=patient_sex,
            patient_race=patient_race,
            chief_complaint=f"Follow-up: {incident_type_description}",
            primary_impression=incident_type_description,
            vital_signs=self._generate_vital_signs_for_incident(incident_type_code),
            treatment_provided=self._generate_treatment_for_incident(incident_type_code),
            medications_given=self._generate_medications_for_incident(incident_type_code),
            transport_destination=random.choice(['HOSPITAL', 'HOME', 'NURSING_FACILITY', 'CLINIC']),
            transport_mode=random.choice(['GROUND', 'AIR']) if priority in ['HIGH', 'CRITICAL'] else 'GROUND',
            created_date=incident_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            # Enhanced fields
            complaint_reported_by_dispatch=self._generate_complaint_reported_by_dispatch(),
            patient_full_name=existing_patient['patient_full_name'],
            unit_call_sign=self._assign_ems_unit_by_location(incident_city, incident_zip),
            patient_contact=True,
            patient_disposition=self._generate_patient_disposition(),
            crew_disposition=random.choice(['AVAILABLE', 'OUT_OF_SERVICE', 'AT_HOSPITAL']),
            patient_evaluation_care_disposition=random.choice(['TREATED_AND_RELEASED', 'TRANSPORTED', 'REFUSED_CARE']),
            transport_disposition=random.choice(['COMPLETED', 'CANCELLED', 'NO_TRANSPORT']),
            transfer_destination=random.choice(['EMERGENCY_DEPT', 'CARDIAC_UNIT', 'ICU', 'GENERAL_FLOOR']),
            destination_type=random.choice(['HOSPITAL', 'CLINIC', 'HOME', 'NURSING_FACILITY']),
            transportation_method=random.choice(['AMBULANCE', 'HELICOPTER', 'PRIVATE_VEHICLE']),
            unit_level_of_care=self._generate_cad_level_and_provider_type()[0],
            cad_emd_code=f"E{random.randint(1000, 9999)}",
            prearrival_activation=self._generate_dispatched_vs_prearrival_activation('ALS'),
            # Medical Details
            complaint_reported_by_dispatch_code=f"C{random.randint(1000, 9999)}",
            patient_acuity=self._generate_patient_and_situation_acuity()[0],
            attempted_procedures=self._generate_attempted_procedures(),
            successful_procedures=self._generate_successful_procedures([]),
            procedure_complications=self._generate_procedure_complications([]),
            cardiac_arrest_datetime=None,
            cardiac_arrest_resuscitation_discontinuation_datetime=None,
            ecg_findings=random.choice(['NORMAL', 'ST_ELEVATION', 'T_WAVE_INVERSION', 'ARRHYTHMIA']),
            incident_emd_performed=self._generate_incident_emd_performed(),
            incident_emd_performed_code=f"EMD{random.randint(1000, 9999)}",
            cad_level_of_care_provided=self._generate_cad_level_and_provider_type()[0],
            incident_level_of_care_provided=self._generate_cad_level_and_provider_type()[0],
            provider_primary_impression=incident_type_description,
            situation_patient_acuity=self._generate_patient_and_situation_acuity()[1],
            # Crew Details
            crew_member_name=f"{self.fake.first_name()} {self.fake.last_name()}",
            crew_member_level=self._generate_cad_level_and_provider_type()[1],
            crew_badge_number=f"EMS{random.randint(1000, 9999)}",
            # Patient Details
            patient_id=patient_id,
            patient_date_of_birth=existing_patient['patient_date_of_birth'],
            patient_weight=existing_patient['patient_weight'],
            patient_home_address=existing_patient['patient_home_address'],
            patient_medical_history=existing_medical_history,
            patient_chronic_conditions=existing_conditions,
            patient_current_medications=existing_patient.get('current_medications', []),
            is_superuser=False,
            # Unit Details
            agency_number='230',
            agency_name='King County Emergency Medical Services',
            agency_affiliation='COUNTY',
            primary_unit_role=self._generate_primary_unit_role(),
            # Incident Dates/Times
            total_commit_time=random.randint(5400, 10800),
            unit_notified_by_dispatch_datetime=(incident_datetime + dispatch_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_enroute_datetime=(incident_datetime + dispatch_delay + en_route_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_arrive_on_scene_datetime=(incident_datetime + dispatch_delay + en_route_delay + arrive_delay).strftime('%Y-%m-%d %H:%M:%S'),
            unit_clear_datetime=(incident_datetime + dispatch_delay + en_route_delay + arrive_delay + timedelta(minutes=random.randint(45, 90))).strftime('%Y-%m-%d %H:%M:%S'),
            incident_status=self._generate_incident_status(),
            crew_with_als_pt_contact_response_role=random.choice(['PRIMARY', 'SECONDARY']),
            
            # GPS/Geocoding fields
            incident_location_latitude=lat,
            incident_location_longitude=lon,
            incident_location_accuracy=accuracy,
            patient_home_latitude=home_lat,
            patient_home_longitude=home_lon,
            
            # Additional EMS-specific fields
            incident_dispatch_priority=priority,
            incident_response_priority=priority,
            scene_safety_assessment=self._generate_scene_safety_assessment(incident_type_code),
            environmental_conditions=self._generate_environmental_conditions(),
            incident_narrative=self._generate_ems_narrative(incident_type_code, patient_age, patient_sex, priority),
            patient_refusal_reason=random.choice(['PATIENT_DECLINED', 'FAMILY_DECLINED', None]) if random.random() < 0.05 else None,
            transport_refusal_reason=random.choice(['PATIENT_DECLINED', 'FAMILY_DECLINED', None]) if random.random() < 0.08 else None,
            destination_facility_name=destination_name,
            destination_facility_type=destination_type,
            
            # Crew and unit tracking
            responding_unit_type=random.choice(['AMBULANCE', 'PARAMEDIC_UNIT', 'FIRE_ENGINE', 'RESCUE_UNIT']),
            responding_unit_capability=random.choice(['BLS', 'ALS', 'CRITICAL_CARE']),
            backup_unit_assigned=random.choice(['UNIT_2', 'SUPERVISOR', None]) if random.random() < 0.15 else None,
            mutual_aid_requested=random.random() < 0.05,
            
            # Patient assessment details
            initial_patient_assessment=f"Initial assessment: {incident_type_description}",
            final_patient_assessment=f"Final assessment: {random.choice(['STABLE', 'IMPROVING', 'CRITICAL', 'UNCHANGED'])}",
            patient_condition_on_arrival=random.choice(['STABLE', 'UNSTABLE', 'CRITICAL', 'CONSCIOUS', 'UNCONSCIOUS']),
            patient_condition_on_transfer=random.choice(['STABLE', 'IMPROVED', 'CRITICAL', 'UNCHANGED']),
            pain_scale_score=pain_score,
            glasgow_coma_scale=gcs_score
        )
        
        # Update patient incident history
        self._update_patient_incident_history(patient_id, incident_datetime)
        
        return incident

    def _select_incident_for_chronic_condition(self, chronic_conditions, medical_history):
        """Select incident type based on patient's chronic conditions"""
        condition_incident_mapping = {
            'DIABETES_TYPE_2': ['2301027', '2301077', '2301001'],  # Diabetic Problem, Unconscious, Abdominal Pain
            'HYPERTENSION': ['2301021', '2301067', '2301077'],  # Chest Pain, Stroke, Unconscious
            'HEART_DISEASE': ['2301021', '2301019', '2301067'],  # Chest Pain, Cardiac Arrest, Stroke
            'ASTHMA': ['2301013', '2301077'],  # Breathing Problem, Unconscious
            'COPD': ['2301013', '2301077', '2301019'],  # Breathing Problem, Unconscious, Cardiac Arrest
            'DEPRESSION': ['2301059', '2301077'],  # Psychiatric Problem, Unconscious
            'ANXIETY': ['2301059', '2301077'],  # Psychiatric Problem, Unconscious
            'ARTHRITIS': ['2301073', '2301011'],  # Traumatic Injury, Back Pain
            'KIDNEY_DISEASE': ['2301001', '2301077'],  # Abdominal Pain, Unconscious
            'THYROID_DISORDER': ['2301077', '2301001'],  # Unconscious, Abdominal Pain
        }
        
        # Find applicable incident types
        applicable_incidents = []
        for condition in chronic_conditions:
            if condition in condition_incident_mapping:
                applicable_incidents.extend(condition_incident_mapping[condition])
        
        # If we have applicable incidents, choose from them
        if applicable_incidents:
            return random.choice(applicable_incidents)
        
        # Otherwise, use regular incident selection
        incident_code, _ = self._choose_ems_incident_type(datetime.now())
        return incident_code

    def _determine_priority_from_incident_type(self, incident_type_code):
        """Determine incident priority based on incident type code"""
        priority_map = {
            '2301019': 'HIGH',  # Cardiac Arrest/Death
            '2301067': 'HIGH',  # Stroke/CVA
            '2301053': 'HIGH',  # Overdose/Poisoning/Ingestion
            '2301077': 'HIGH',  # Unconscious/Fainting/Near-Fainting
            '2301073': 'HIGH',  # Traumatic Injury
            '2301021': 'HIGH',  # Chest Pain (Non-Traumatic)
            '2301013': 'HIGH',  # Breathing Problem
            '2301045': 'HIGH',  # Hemorrhage/Laceration
            '2301025': 'HIGH',  # Convulsions/Seizure
            '2301061': 'MEDIUM',  # Sick Person
            '2301033': 'MEDIUM',  # Falls
            '2301001': 'MEDIUM',  # Abdominal Pain/Problems
            '2301027': 'MEDIUM',  # Diabetic Problem
            '2301003': 'HIGH',  # Allergic Reaction/Stings
            '2301057': 'HIGH',  # Pregnancy/Childbirth/Miscarriage
            '2301043': 'HIGH',  # Heat/Cold Exposure
            '2301063': 'HIGH',  # Stab/Gunshot Wound/Penetrating Trauma
            '2301059': 'HIGH',  # Psychiatric Problem/Abnormal Behavior/Suicide Attempt
            '2301069': 'HIGH',  # Traffic/Transportation Incident
            '2301007': 'HIGH',  # Assault
            '2301035': 'HIGH',  # Fire
            '2301005': 'MEDIUM',  # Animal Bite
            '2301023': 'HIGH',  # Choking
            '2301037': 'MEDIUM',  # Headache
            '2301011': 'LOW',  # Back Pain (Non-Traumatic)
        }
        return priority_map.get(incident_type_code, 'MEDIUM')

    def _generate_vital_signs_for_incident(self, incident_type_code):
        """Generate realistic vital signs based on incident type"""
        # Base vital signs by incident type
        vital_signs_profiles = {
            '2301019': {  # Cardiac Arrest
                'bp_sys': 0, 'bp_dia': 0, 'hr': 0, 'rr': 0, 'spo2': 0, 'temp': 96.0
            },
            '2301021': {  # Chest Pain
                'bp_sys': (140, 180), 'bp_dia': (80, 110), 'hr': (90, 130), 'rr': (16, 24), 'spo2': (92, 98), 'temp': (97.0, 99.0)
            },
            '2301013': {  # Breathing Problem
                'bp_sys': (120, 160), 'bp_dia': (70, 100), 'hr': (100, 140), 'rr': (20, 35), 'spo2': (85, 95), 'temp': (97.0, 99.0)
            },
            '2301073': {  # Traumatic Injury
                'bp_sys': (85, 120), 'bp_dia': (50, 80), 'hr': (90, 130), 'rr': (18, 28), 'spo2': (92, 98), 'temp': (97.0, 99.0)
            },
            '2301053': {  # Overdose
                'bp_sys': (90, 140), 'bp_dia': (50, 90), 'hr': (60, 120), 'rr': (12, 25), 'spo2': (88, 96), 'temp': (96.0, 99.0)
            }
        }
        
        profile = vital_signs_profiles.get(incident_type_code, {
            'bp_sys': (100, 140), 'bp_dia': (60, 90), 'hr': (70, 110), 'rr': (16, 22), 'spo2': (95, 99), 'temp': (97.5, 98.6)
        })
        
        # Generate vital signs
        if isinstance(profile['bp_sys'], tuple):
            bp_sys = random.randint(profile['bp_sys'][0], profile['bp_sys'][1])
            bp_dia = random.randint(profile['bp_dia'][0], profile['bp_dia'][1])
            hr = random.randint(profile['hr'][0], profile['hr'][1])
            rr = random.randint(profile['rr'][0], profile['rr'][1])
            spo2 = random.randint(profile['spo2'][0], profile['spo2'][1])
            temp = round(random.uniform(profile['temp'][0], profile['temp'][1]), 1)
        else:
            bp_sys = profile['bp_sys']
            bp_dia = profile['bp_dia']
            hr = profile['hr']
            rr = profile['rr']
            spo2 = profile['spo2']
            temp = profile['temp']
        
        return {
            'blood_pressure': f"{bp_sys}/{bp_dia}",
            'heart_rate': hr,
            'respiratory_rate': rr,
            'oxygen_saturation': spo2,
            'temperature': temp
        }

    def _generate_treatment_for_incident(self, incident_type_code):
        """Generate realistic treatments based on incident type"""
        treatment_profiles = {
            '2301019': ['CPR', 'DEFIBRILLATION', 'IV_ACCESS', 'OXYGEN'],
            '2301021': ['OXYGEN', 'IV_ACCESS', 'MONITORING', 'ASPIRIN'],
            '2301013': ['OXYGEN', 'ALBUTEROL', 'IV_ACCESS', 'MONITORING'],
            '2301073': ['SPLINTING', 'IV_FLUIDS', 'BANDAGING', 'OXYGEN'],
            '2301053': ['NALOXONE', 'IV_ACCESS', 'MONITORING', 'OXYGEN']
        }
        
        treatments = treatment_profiles.get(incident_type_code, ['OXYGEN', 'IV_ACCESS', 'MONITORING'])
        num_treatments = random.randint(1, min(len(treatments), 4))
        return random.sample(treatments, num_treatments)

    def _generate_medications_for_incident(self, incident_type_code):
        """Generate realistic medications based on incident type"""
        # Medications for each incident type
        medication_profiles = {
            # Cardiac Arrest/Death
            '2301019': ['EPI 1:10,000', 'AMIODARONE', 'SODIUM BICARBONATE', 'ATROPINE', 'CALCIUM CHLORIDE'],
            
            # Chest Pain (Non-Traumatic) - Cardiac
            '2301021': ['NITROGLYCERIN', 'ASPIRIN', 'MORPHINE', 'OXYGEN', 'METOPROLOL'],
            
            # Breathing Problem - Respiratory
            '2301013': ['ALBUTEROL', 'IPRATROPIUM', 'METHYLPREDNISOLONE', 'OXYGEN', 'EPI 1:1,000'],
            
            # Abdominal Pain/Problems - GI
            '2301001': ['MORPHINE', 'ONDANSETRON', 'NORMAL SALINE', 'OXYGEN'],
            
            # Overdose/Poisoning/Ingestion
            '2301053': ['NALOXONE', 'OXYGEN', 'NORMAL SALINE', 'GLUCOSE', 'DEXTROSE 50%'],
            
            # Convulsions/Seizure - Neurological
            '2301025': ['MIDAZOLAM', 'DIAZEPAM', 'OXYGEN', 'NORMAL SALINE'],
            
            # Allergic Reaction/Stings
            '2301003': ['EPI 1:1,000', 'DIPHENHYDRAMINE', 'OXYGEN', 'NORMAL SALINE', 'METHYLPREDNISOLONE'],
            
            # Unconscious/Fainting/Near-Fainting
            '2301061': ['OXYGEN', 'GLUCOSE', 'NORMAL SALINE', 'NALOXONE', 'DEXTROSE 50%'],
            
            # Stroke/CVA - Neurological
            '2301063': ['OXYGEN', 'NORMAL SALINE', 'ASPIRIN'],
            
            # Traumatic Injury
            '2301065': ['FENTANYL', 'MORPHINE', 'NORMAL SALINE', 'OXYGEN', 'MIDAZOLAM'],
            
            # Diabetic Problem
            '2301027': ['GLUCOSE', 'DEXTROSE 50%', 'GLUCAGON', 'OXYGEN', 'NORMAL SALINE'],
            
            # Psychiatric Problem/Abnormal Behavior/Suicide Attempt
            '2301059': ['MIDAZOLAM', 'HALOPERIDOL', 'OXYGEN'],
            
            # Heart Problems/AICD
            '2301041': ['NITROGLYCERIN', 'ASPIRIN', 'MORPHINE', 'OXYGEN', 'AMIODARONE'],
            
            # Burns/Explosion
            '2301015': ['MORPHINE', 'FENTANYL', 'NORMAL SALINE', 'OXYGEN'],
            
            # Falls
            '2301033': ['FENTANYL', 'MORPHINE', 'NORMAL SALINE', 'OXYGEN'],
            
            # Headache
            '2301037': ['MORPHINE', 'FENTANYL', 'OXYGEN'],
            
            # Back Pain (Non-Traumatic)
            '2301011': ['MORPHINE', 'FENTANYL', 'OXYGEN'],
            
            # Hemorrhage/Laceration
            '2301045': ['NORMAL SALINE', 'OXYGEN', 'MORPHINE'],
            
            # Pregnancy/Childbirth/Miscarriage
            '2301057': ['OXYGEN', 'NORMAL SALINE', 'EPI 1:10,000'],
            
            # Heat/Cold Exposure
            '2301043': ['NORMAL SALINE', 'OXYGEN', 'DEXTROSE 50%'],
            
            # Choking
            '2301023': ['OXYGEN', 'NORMAL SALINE'],
            
            # Eye Problem/Injury
            '2301031': ['TETRACAINE', 'NORMAL SALINE', 'OXYGEN'],
            
            # Animal Bite
            '2301005': ['NORMAL SALINE', 'OXYGEN', 'MORPHINE'],
            
            # Assault
            '2301007': ['MORPHINE', 'FENTANYL', 'NORMAL SALINE', 'OXYGEN'],
            
            # Fire
            '2301035': ['OXYGEN', 'NORMAL SALINE', 'MORPHINE'],
            
            # Carbon Monoxide/Hazmat/Inhalation/CBRN
            '2301017': ['OXYGEN', 'NORMAL SALINE'],
            
            # Electrocution/Lightning
            '2301029': ['NORMAL SALINE', 'OXYGEN', 'MORPHINE'],
            
            # Industrial Accident/Inaccessible Incident/Other Entrapments
            '2301047': ['MORPHINE', 'FENTANYL', 'NORMAL SALINE', 'OXYGEN'],
            
            # Medical Alarm
            '2301049': ['OXYGEN', 'NORMAL SALINE'],
            
            # Healthcare Professional/Admission
            '2301039': ['OXYGEN', 'NORMAL SALINE'],
            
            # Automated Crash Notification
            '2301009': ['MORPHINE', 'FENTANYL', 'NORMAL SALINE', 'OXYGEN'],
            
            # Pandemic/Epidemic/Outbreak
            '2301055': ['OXYGEN', 'NORMAL SALINE'],
            
            # Stroke/CVA - Neurological
            '2301067': ['OXYGEN', 'NORMAL SALINE', 'ASPIRIN'],
            
            # Traffic/Transportation Incident
            '2301069': ['MORPHINE', 'FENTANYL', 'NORMAL SALINE', 'OXYGEN'],
            
            # Default for any other incident types
            'default': ['OXYGEN', 'NORMAL SALINE']
        }
        
        # Get appropriate medications for this incident type
        medications = medication_profiles.get(incident_type_code, medication_profiles['default'])
        
        # Determine number of medications (1-3, weighted toward fewer medications)
        num_medications = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15], k=1)[0]
        
        # Always include OXYGEN for most incident types (except where inappropriate)
        oxygen_inappropriate = ['2301027', '2301031']  # Diabetic Problem, Eye Problem
        if incident_type_code not in oxygen_inappropriate and 'OXYGEN' in medications:
            selected_meds = ['OXYGEN']
            remaining_meds = [med for med in medications if med != 'OXYGEN']
            if remaining_meds and num_medications > 1:
                additional_meds = random.sample(remaining_meds, min(num_medications - 1, len(remaining_meds)))
                selected_meds.extend(additional_meds)
            return selected_meds
        else:
            return random.sample(medications, min(num_medications, len(medications)))

    # Copula modeling methods removed for performance
    def generate_ems_report(self, ems_incident=None, ems_medications=None, ems_patient=None):
        """Generate comprehensive EMS report linking incident, patient, and medications"""
        if not ems_incident:
            return None
        
        # Use provided entities or generate defaults
        if not ems_patient:
            ems_patient = self.generate_ems_patient(ems_incident)
        if not ems_medications:
            ems_medications = [self.generate_ems_medication(ems_incident)]
        
        # Handle both dict and object formats
        def get_attr(obj, attr, default=''):
            if isinstance(obj, dict):
                return obj.get(attr, default)
            else:
                return getattr(obj, attr, default)
        
        # Generate report-specific fields
        incident_number = get_attr(ems_incident, 'incident_number', 'UNKNOWN')
        report_number = f"RPT-{incident_number}-{random.randint(1000, 9999)}"
        
        # Create comprehensive narrative
        narrative_parts = [
            f"EMS Response Report - {get_attr(ems_incident, 'incident_type_description', 'Unknown Incident')}",
            f"Patient: {get_attr(ems_patient, 'patient_full_name', 'Unknown Patient')} ({get_attr(ems_patient, 'patient_age', 'Unknown')}yo {get_attr(ems_patient, 'patient_gender', 'Unknown')})",
            f"Incident: {get_attr(ems_incident, 'incident_type_description', 'Unknown')} at {get_attr(ems_incident, 'address', 'Unknown Location')}",
            f"Dispatch: {get_attr(ems_incident, 'dispatch_datetime', 'Unknown')}",
            f"Arrival: {get_attr(ems_incident, 'arrive_datetime', 'Unknown')}",
            f"Chief Complaint: {get_attr(ems_incident, 'chief_complaint', 'Unknown')}",
            f"Primary Impression: {get_attr(ems_incident, 'primary_impression', 'Unknown')}",
        ]
        
        # Add medication details
        if ems_medications:
            med_names = [get_attr(med, 'medication_name', 'Unknown') for med in ems_medications]
            narrative_parts.append(f"Medications Administered: {', '.join(med_names)}")
        
        # Add procedure details
        attempted_procedures = get_attr(ems_incident, 'attempted_procedures', [])
        if attempted_procedures:
            narrative_parts.append(f"Procedures Attempted: {', '.join(attempted_procedures)}")
        successful_procedures = get_attr(ems_incident, 'successful_procedures', [])
        if successful_procedures:
            narrative_parts.append(f"Procedures Successful: {', '.join(successful_procedures)}")
        
        # Add disposition
        narrative_parts.extend([
            f"Patient Disposition: {get_attr(ems_incident, 'patient_disposition', 'Unknown')}",
            f"Transport Destination: {get_attr(ems_incident, 'transfer_destination', 'Unknown')}",
            f"Transport Method: {get_attr(ems_incident, 'transportation_method', 'Unknown')}",
            f"Incident Status: {get_attr(ems_incident, 'incident_status', 'Unknown')}"
        ])
        
        report_narrative = "\n".join(narrative_parts)
        
        # Generate medication summary
        medications_summary = "; ".join([
            f"{med.medication_name} {med.dosage}{med.dosage_unit} {med.medication_administration_route}"
            for med in ems_medications
        ]) if ems_medications else "None"
        
        # Generate procedure summaries
        attempted_procedures_summary = "; ".join(ems_incident.attempted_procedures) if ems_incident.attempted_procedures else "None"
        successful_procedures_summary = "; ".join(ems_incident.successful_procedures) if ems_incident.successful_procedures else "None"
        
        # Generate location string
        location = f"{ems_incident.incident_location_latitude},{ems_incident.incident_location_longitude}" if hasattr(ems_incident, 'incident_location_latitude') else f"{ems_incident.address}, {ems_incident.city}, {ems_incident.state}"
        
        # Generate linked entity IDs
        linked_medications = [med.medication_id for med in ems_medications] if ems_medications else []
        linked_patients = [ems_patient.patient_id] if ems_patient else []
        linked_incidents = [ems_incident.incident_id]
        
        # Generate quality assurance and billing fields
        quality_assurance = random.choice(['COMPLETE', 'PENDING_REVIEW', 'APPROVED', 'REQUIRES_CORRECTION']) if random.random() < 0.8 else None
        supervisor_approval = random.choice(['APPROVED', 'PENDING', 'REJECTED']) if random.random() < 0.7 else None
        
        report = EMSReport(
            # Report Identification
            report_id=str(uuid.uuid4()),
            report_number=report_number,
            report_date=get_attr(ems_incident, 'call_datetime', ''),
            created_date=get_attr(ems_incident, 'call_datetime', ''),
            last_modified=get_attr(ems_incident, 'call_datetime', ''),
            created_by=get_attr(ems_incident, 'crew_member_name', 'Unknown'),
            
            # Incident Linkage
            incident_id=get_attr(ems_incident, 'incident_id', ''),
            incident_number=get_attr(ems_incident, 'incident_number', ''),
            call_number=get_attr(ems_incident, 'call_number', ''),
            
            # Patient Linkage and Demographics
            patient_id=get_attr(ems_patient, 'patient_id', get_attr(ems_incident, 'patient_id', '')),
            patient_pk=get_attr(ems_patient, 'patient_pk', get_attr(ems_incident, 'patient_pk', '')),
            patient_full_name=get_attr(ems_patient, 'patient_full_name', get_attr(ems_incident, 'patient_full_name', 'Unknown')),
            patient_gender=get_attr(ems_patient, 'patient_gender', get_attr(ems_incident, 'patient_sex', 'Unknown')),
            patient_age=get_attr(ems_patient, 'patient_age', get_attr(ems_incident, 'patient_age', 0)),
            patient_date_of_birth=get_attr(ems_patient, 'patient_date_of_birth', get_attr(ems_incident, 'patient_date_of_birth', '')),
            patient_weight=get_attr(ems_patient, 'patient_weight', get_attr(ems_incident, 'patient_weight', 0.0)),
            patient_home_address=get_attr(ems_patient, 'patient_home_address', get_attr(ems_incident, 'patient_home_address', '')),
            patient_race=get_attr(ems_patient, 'patient_race', get_attr(ems_incident, 'patient_race', 'Unknown')),
            patient_ethnicity=get_attr(ems_patient, 'patient_ethnicity', 'UNKNOWN'),
            
            # Incident Overview
            incident_datetime=ems_incident.call_datetime,
            complaint_reported_by_dispatch=ems_incident.complaint_reported_by_dispatch,
            complaint_reported_by_dispatch_code=ems_incident.complaint_reported_by_dispatch_code,
            unit_call_sign=ems_incident.unit_call_sign,
            location=location,
            
            # Disposition Details
            patient_contact=ems_incident.patient_contact,
            patient_disposition=ems_incident.patient_disposition,
            crew_disposition=ems_incident.crew_disposition,
            patient_evaluation_care_disposition=ems_incident.patient_evaluation_care_disposition,
            transport_disposition=ems_incident.transport_disposition,
            transfer_destination=ems_incident.transfer_destination,
            destination_type=ems_incident.destination_type,
            transportation_method=ems_incident.transportation_method,
            unit_level_of_care=ems_incident.unit_level_of_care,
            cad_emd_code=ems_incident.cad_emd_code,
            prearrival_activation=ems_incident.prearrival_activation,
            
            # Medical Details
            patient_acuity=ems_incident.patient_acuity,
            situation_patient_acuity=ems_incident.situation_patient_acuity,
            medications_given=medications_summary,
            attempted_procedures=attempted_procedures_summary,
            successful_procedures=successful_procedures_summary,
            cardiac_arrest_datetime=ems_incident.cardiac_arrest_datetime,
            cardiac_arrest_resuscitation_discontinuation_datetime=ems_incident.cardiac_arrest_resuscitation_discontinuation_datetime,
            ecg_findings=ems_incident.ecg_findings,
            incident_emd_performed=ems_incident.incident_emd_performed,
            incident_emd_performed_code=ems_incident.incident_emd_performed_code,
            cad_level_of_care_provided=ems_incident.cad_level_of_care_provided,
            incident_level_of_care_provided=ems_incident.incident_level_of_care_provided,
            provider_primary_impression=ems_incident.provider_primary_impression,
            
            # Crew Details
            crew_member_name=ems_incident.crew_member_name,
            crew_member_level=ems_incident.crew_member_level,
            crew_badge_number=ems_incident.crew_badge_number,
            primary_patient_caregiver_on_scene=ems_incident.primary_patient_caregiver_on_scene,
            crew_with_als_pt_contact_response_role=ems_incident.crew_with_als_pt_contact_response_role,
            
            # Unit Details
            agency_number=ems_incident.agency_number,
            agency_name=ems_incident.agency_name,
            agency_affiliation=ems_incident.agency_affiliation,
            primary_unit_role=ems_incident.primary_unit_role,
            
            # Incident Timeline
            total_commit_time=ems_incident.total_commit_time,
            unit_notified_by_dispatch_datetime=ems_incident.unit_notified_by_dispatch_datetime,
            unit_en_route_datetime=ems_incident.unit_enroute_datetime,
            unit_arrived_on_scene_datetime=ems_incident.unit_arrived_at_patient_datetime,
            unit_arrived_at_patient_datetime=ems_incident.unit_arrived_at_patient_datetime,
            transfer_of_ems_patient_care_datetime=ems_incident.transfer_of_ems_patient_care_datetime,
            arrival_at_destination_landing_area_datetime=ems_incident.arrival_at_destination_landing_area_datetime,
            unit_left_scene_datetime=ems_incident.unit_left_scene_datetime,
            patient_arrived_at_destination_datetime=ems_incident.patient_arrived_at_destination_datetime,
            unit_back_in_service_datetime=ems_incident.unit_back_in_service_datetime,
            
            # Status and Classification
            incident_status=ems_incident.incident_status,
            incident_type=ems_incident.incident_type,
            incident_type_code=ems_incident.incident_type_code,
            priority=ems_incident.priority,
            
            # Linked Entity References
            linked_medications=linked_medications,
            linked_patients=linked_patients,
            linked_incidents=linked_incidents,
            
            # Additional Report Fields
            report_narrative=report_narrative,
            quality_assurance_review=quality_assurance,
            supervisor_approval=supervisor_approval,
            billing_status=random.choice(['PENDING', 'BILLED', 'PAID', 'DENIED', 'WRITTEN_OFF']),
            insurance_verified=random.random() < 0.8,
            patient_signature_obtained=random.random() < 0.9
        )
        
        return report


if __name__ == "__main__":
    """Main execution block - creates EMS entities when run directly"""
    import json
    import os
    from faker import Faker
    
    print("EMS Data Generator - Creating EMS Entities")
    print("=" * 50)
    
    # Initialize
    fake = Faker()
    ems_generator = EMSDataGenerator(fake)
    
    # Copula initialization removed for performance
    
    # Create output directory if it doesn't exist
    output_dir = "data/json"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate sample data - 10x scale using batch processing
    num_incidents = 1000
    print(f"Generating {num_incidents} EMS incidents using optimized batch processing...")
    
    # Generate incidents in parallel using batch processing
    incidents = ems_generator.generate_incidents_batch(num_incidents)
    print(f"Generated {len(incidents)} incidents using parallel processing")
    
    # Generate patients and medications for each incident
    patients = []
    medications = []
    
    print("Generating patients and medications...")
    for i, incident in enumerate(incidents):
        if (i + 1) % 200 == 0:
            print(f"  Generated {i + 1}/{len(incidents)} patients and medications...")
        
        # Generate patient for this incident
        patient = ems_generator.generate_ems_patient(incident)
        patients.append(patient.__dict__)
        
        # Generate medications for this incident (0-3 medications per incident)
        num_meds = random.randint(0, 3)
        for _ in range(num_meds):
            medication = ems_generator.generate_ems_medication(incident)
            medications.append(medication.__dict__)
    
    # Generate EMS reports linking all entities (keep incidents as objects for now)
    print("Generating EMS reports...")
    reports = []
    for i, incident in enumerate(incidents):
        if i % 100 == 0:
            print(f"  Generated {i}/{len(incidents)} reports...")
        
        # Find related patient and medications for this incident
        related_patient = next((p for p in patients if p['patient_id'] == incident.patient_id), None)
        related_medications = [m for m in medications if m['incident_id'] == incident.incident_id]
        
        # Generate report
        report = ems_generator.generate_ems_report(incident, related_medications, related_patient)
        if report:
            reports.append(report.__dict__)
    
    # Convert incidents to dict format for JSON serialization after report generation
    incidents = [incident.__dict__ for incident in incidents]
    
    print(f"Generated {len(reports)} EMS reports")
    
    # Save to JSON files
    print(f"\nSaving generated data...")
    
    # Save EMS incidents
    incidents_file = os.path.join(output_dir, "ems_incidents.json")
    with open(incidents_file, 'w') as f:
        json.dump(incidents, f, indent=2)
    print(f"Saved {len(incidents)} EMS incidents to {incidents_file}")
    
    # Save EMS patients
    patients_file = os.path.join(output_dir, "ems_patients.json")
    with open(patients_file, 'w') as f:
        json.dump(patients, f, indent=2)
    print(f"Saved {len(patients)} EMS patients to {patients_file}")
    
    # Save EMS medications
    medications_file = os.path.join(output_dir, "ems_medications.json")
    with open(medications_file, 'w') as f:
        json.dump(medications, f, indent=2)
    print(f"Saved {len(medications)} EMS medications to {medications_file}")
    
    # Save EMS reports
    reports_file = os.path.join(output_dir, "ems_reports.json")
    with open(reports_file, 'w') as f:
        json.dump(reports, f, indent=2)
    print(f"Saved {len(reports)} EMS reports to {reports_file}")

    print(f"\nEMS data generation completed!")
    print(f"Summary:")
    print(f"   - {len(incidents)} EMS incidents")
    print(f"   - {len(patients)} EMS patients")
    print(f"   - {len(medications)} EMS medications")
    print(f"   - {len(reports)} EMS reports")
    print(f"   - All files saved to {output_dir}/")
    
    # Show sample of generated data
    print(f"\nSample EMS Incident:")
    if incidents:
        sample = incidents[0]
        print(f"   Incident ID: {sample['incident_id']}")
        print(f"   Type: {sample['incident_type_description']}")
        print(f"   Address: {sample['address']}, {sample['city']}, {sample['state']} {sample['zip_code']}")
        print(f"   Priority: {sample['priority']}")
        print(f"   Unit: {sample['unit_call_sign']}")
        print(f"   Patient: {sample['patient_full_name']} ({sample['patient_age']} years old, {sample['patient_sex']})")
        print(f"   Primary Impression: {sample['primary_impression']}")