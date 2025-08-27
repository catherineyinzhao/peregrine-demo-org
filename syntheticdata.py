
def generate_fire_incident(self):
    """Generate comprehensive fire incident"""
    incident_types = [
        'STRUCTURE_FIRE', 'VEHICLE_FIRE', 'BRUSH_FIRE', 'ALARM_ACTIVATION',
        'HAZMAT_INCIDENT', 'WATER_RESCUE', 'TECHNICAL_RESCUE', 'SERVICE_CALL'
    ]
    
    weights = [15, 10, 8, 40, 3, 5, 8, 11]  # Alarm activations most common
    incident_type = random.choices(incident_types, weights=weights)[0]
    
    # Generate timing
    alarm_datetime = fake.date_time_between(start_date='-2y', end_date='now')
    dispatch_datetime = alarm_datetime + timedelta(seconds=random.randint(30, 180))
    en_route_datetime = dispatch_datetime + timedelta(seconds=random.randint(45, 300))
    arrive_datetime = en_route_datetime + timedelta(minutes=random.randint(4, 12))
    
    # Control and clear times based on incident type
    if incident_type == 'STRUCTURE_FIRE':
        controlled_datetime = arrive_datetime + timedelta(minutes=random.randint(30, 180))
        last_clear_datetime = controlled_datetime + timedelta(minutes=random.randint(60, 240))
    elif incident_type == 'ALARM_ACTIVATION':
        controlled_datetime = arrive_datetime + timedelta(minutes=random.randint(5, 15))
        last_clear_datetime = controlled_datetime + timedelta(minutes=random.randint(5, 30))
    else:
        controlled_datetime = arrive_datetime + timedelta(minutes=random.randint(15, 90))
        last_clear_datetime = controlled_datetime + timedelta(minutes=random.randint(30, 120))
    
    # Location in Seattle
    location = self.select_location_in_jurisdiction('SEATTLE_FD')
    
    # Generate incident number
    incident_number = f"SFD{alarm_datetime.year}{random.randint(100000, 999999)}"
    call_number = f"F{alarm_datetime.year}{random.randint(1000000, 9999999)}"
    
    # Responding units based on incident type
    units_responding = self.generate_fire_units(incident_type)
    
    # Property damage estimation
    if incident_type == 'STRUCTURE_FIRE':
        property_loss = random.randint(50000, 500000)
        contents_loss = random.randint(10000, 100000)
    elif incident_type == 'VEHICLE_FIRE':
        property_loss = random.randint(5000, 50000)
        contents_loss = random.randint(500, 5000)
    else:
        property_loss = 0
        contents_loss = 0
    
    # Casualties
    casualties = []
    fatalities = 0
    injuries = 0
    
    if incident_type in ['STRUCTURE_FIRE', 'VEHICLE_FIRE'] and random.random() < 0.1:
        num_casualties = random.randint(1, 3)
        for _ in range(num_casualties):
            casualty_type = random.choice(['CIVILIAN_INJURY', 'CIVILIAN_FATALITY'])
            if casualty_type == 'CIVILIAN_FATALITY':
                fatalities += 1
            else:
                injuries += 1
            
            casualties.append({
                'type': casualty_type,
                'age': random.randint(5, 80),
                'sex': random.choice(['M', 'F']),
                'cause': random.choice(['SMOKE_INHALATION', 'BURNS', 'TRAUMA'])
            })
    
    incident = FireIncident(
        incident_id=str(uuid.uuid4()),
        incident_number=incident_number,
        call_number=call_number,
        incident_type=incident_type,
        incident_subtype=self.generate_fire_subtype(incident_type),
        nfirs_code=self.generate_nfirs_code(incident_type),
        alarm_datetime=alarm_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        dispatch_datetime=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        en_route_datetime=en_route_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        arrive_datetime=arrive_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        controlled_datetime=controlled_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        last_unit_cleared_datetime=last_clear_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        address=location['street'],
        city=location['city'],
        latitude=location['latitude'],
        longitude=location['longitude'],
        district=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']),
        first_due_station=random.randint(1, 37),
        units_responding=units_responding,
        incident_commander=f"BC{random.randint(1, 7)}, {fake.last_name().upper()}",
        fire_cause=self.generate_fire_cause(incident_type),
        fire_origin=self.generate_fire_origin(incident_type) if incident_type in ['STRUCTURE_FIRE', 'VEHICLE_FIRE'] else '',
        ignition_factor=self.generate_ignition_factor(incident_type) if incident_type in ['STRUCTURE_FIRE', 'VEHICLE_FIRE'] else '',
        property_type=random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL', 'VEHICLE', 'OUTDOOR']),
        property_use=random.choice(['SINGLE_FAMILY', 'APARTMENT', 'OFFICE', 'RETAIL', 'WAREHOUSE']),
        occupancy_type=random.choice(['RESIDENTIAL', 'BUSINESS', 'EDUCATIONAL', 'INSTITUTIONAL']),
        construction_type=random.choice(['WOOD_FRAME', 'CONCRETE', 'STEEL', 'MASONRY']),
        stories=random.randint(1, 5) if incident_type == 'STRUCTURE_FIRE' else 0,
        total_floor_area=random.randint(1000, 10000) if incident_type == 'STRUCTURE_FIRE' else 0,
        fire_spread=random.choice(['CONFINED_TO_ORIGIN', 'CONFINED_TO_ROOM', 'CONFINED_TO_FLOOR', 'CONFINED_TO_BUILDING', 'BEYOND_BUILDING']) if incident_type in ['STRUCTURE_FIRE'] else '',
        smoke_spread=random.choice(['CONFINED_TO_ROOM', 'CONFINED_TO_FLOOR', 'CONFINED_TO_BUILDING', 'BEYOND_BUILDING']) if incident_type in ['STRUCTURE_FIRE'] else '',
        detector_present=random.choice([True, False]),
        detector_operation=random.choice(['OPERATED', 'FAILED_TO_OPERATE', 'NO_DETECTOR']) if incident_type == 'STRUCTURE_FIRE' else '',
        sprinkler_present=random.choice([True, False]) if incident_type == 'STRUCTURE_FIRE' else False,
        sprinkler_operation=random.choice(['OPERATED', 'FAILED_TO_OPERATE', 'NO_SPRINKLER']) if incident_type == 'STRUCTURE_FIRE' else '',
        estimated_loss_property=property_loss,
        estimated_loss_contents=contents_loss,
        casualties=casualties,
        fatalities=fatalities,
        injuries_fire=injuries,
        injuries_non_fire=0,
        personnel_injuries=1 if random.random() < 0.02 else 0,  # 2% chance of firefighter injury
        apparatus_involved=[unit['unit'] for unit in units_responding],
        mutual_aid_given=['BELLEVUE_FD', 'RENTON_FD'] if random.random() < 0.1 else [],
        mutual_aid_received=['SHORELINE_FD', 'TUKWILA_FD'] if random.random() < 0.15 else [],
        hazmat_release=incident_type == 'HAZMAT_INCIDENT',
        hazmat_id=f"UN{random.randint(1000, 9999)}" if incident_type == 'HAZMAT_INCIDENT' else '',
        narrative=self.generate_fire_narrative(incident_type),
        created_date=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        created_by=f"DISP{random.randint(100, 999)}, {fake.last_name().upper()}"
    )
    
    return incident

def generate_fire_units(self, incident_type):
    """Generate responding fire units"""
    units = []
    
    if incident_type == 'STRUCTURE_FIRE':
        # Full alarm response
        num_engines = random.randint(3, 5)
        num_ladders = random.randint(1, 2)
        num_aid_cars = random.randint(1, 2)
        
        for i in range(num_engines):
            units.append({
                'unit': f'E{random.randint(1, 37)}',
                'station': random.randint(1, 37),
                'personnel': random.randint(3, 4)
            })
        
        for i in range(num_ladders):
            units.append({
                'unit': f'L{random.randint(1, 12)}',
                'station': random.randint(1, 37),
                'personnel': random.randint(3, 4)
            })
        
        for i in range(num_aid_cars):
            units.append({
                'unit': f'A{random.randint(1, 20)}',
                'station': random.randint(1, 37),
                'personnel': 2
            })
            
        # Battalion Chief
        units.append({
            'unit': f'BC{random.randint(1, 7)}',
            'station': 0,
            'personnel': 1
        })
        
    elif incident_type == 'ALARM_ACTIVATION':
        # Single engine response
        units.append({
            'unit': f'E{random.randint(1, 37)}',
            'station': random.randint(1, 37),
            'personnel': random.randint(3, 4)
        })
        
    elif incident_type == 'MEDICAL_EMERGENCY':
        # Aid car response
        units.append({
            'unit': f'A{random.randint(1, 20)}',
            'station': random.randint(1, 37),
            'personnel': 2
        })
        
        # Sometimes engine too
        if random.random() < 0.4:
            units.append({
                'unit': f'E{random.randint(1, 37)}',
                'station': random.randint(1, 37),
                'personnel': random.randint(3, 4)
            })
    
    return units

def generate_fire_subtype(self, incident_type):
    """Generate fire incident subtypes"""
    subtypes = {
        'STRUCTURE_FIRE': ['RESIDENTIAL_FIRE', 'COMMERCIAL_FIRE', 'HIGH_RISE_FIRE'],
        'VEHICLE_FIRE': ['CAR_FIRE', 'TRUCK_FIRE', 'MOTORCYCLE_FIRE'],
        'ALARM_ACTIVATION': ['SMOKE_DETECTOR', 'FIRE_ALARM', 'PULL_STATION'],
        'MEDICAL_EMERGENCY': ['CHEST_PAIN', 'DIFFICULTY_BREATHING', 'UNCONSCIOUS'],
        'HAZMAT_INCIDENT': ['CHEMICAL_SPILL', 'GAS_LEAK', 'UNKNOWN_SUBSTANCE']
    }
    
    return random.choice(subtypes.get(incident_type, ['MISCELLANEOUS']))

def generate_nfirs_code(self, incident_type):
    """Generate NFIRS codes"""
    nfirs_codes = {
        'STRUCTURE_FIRE': '111',
        'VEHICLE_FIRE': '130',
        'BRUSH_FIRE': '140',
        'ALARM_ACTIVATION': '735',
        'MEDICAL_EMERGENCY': '321',
        'HAZMAT_INCIDENT': '412'
    }
    
    return nfirs_codes.get(incident_type, '000')

def generate_fire_cause(self, incident_type):
    """Generate fire cause"""
    if incident_type == 'STRUCTURE_FIRE':
        causes = ['ELECTRICAL', 'COOKING', 'HEATING', 'SMOKING', 'ARSON', 'ACCIDENTAL', 'UNDETERMINED']
        weights = [25, 30, 15, 10, 8, 10, 2]
        return random.choices(causes, weights=weights)[0]
    elif incident_type == 'VEHICLE_FIRE':
        causes = ['MECHANICAL', 'ELECTRICAL', 'ARSON', 'ACCIDENT', 'UNDETERMINED']
        weights = [40, 30, 15, 10, 5]
        return random.choices(causes, weights=weights)[0]
    else:
        return 'N/A'

def generate_fire_origin(self, incident_type):
    """Generate area of fire origin"""
    if incident_type == 'STRUCTURE_FIRE':
        return random.choice(['KITCHEN', 'BEDROOM', 'LIVING_ROOM', 'BASEMENT', 'GARAGE', 'ATTIC'])
    elif incident_type == 'VEHICLE_FIRE':
        return random.choice(['ENGINE_COMPARTMENT', 'PASSENGER_COMPARTMENT', 'TRUNK', 'EXTERIOR'])
    else:
        return ''

def generate_ignition_factor(self, incident_type):
    """Generate ignition factor"""
    if incident_type == 'STRUCTURE_FIRE':
        return random.choice(['UNATTENDED', 'OVERHEATED', 'SHORT_CIRCUIT', 'EXPOSED_WIRING', 'IMPROPER_USE'])
    elif incident_type == 'VEHICLE_FIRE':
        return random.choice(['MECHANICAL_FAILURE', 'COLLISION', 'OVERHEATING', 'FUEL_LEAK'])
    else:
        return ''

def generate_fire_narrative(self, incident_type):
    """Generate fire incident narrative"""
    narratives = {
        'STRUCTURE_FIRE': "UNITS RESPONDED TO REPORTED STRUCTURE FIRE. SMOKE AND FLAMES VISIBLE ON ARRIVAL. FIRE SUPPRESSION OPERATIONS CONDUCTED. FIRE CONTROLLED AND EXTINGUISHED.",
        'VEHICLE_FIRE': "UNITS RESPONDED TO VEHICLE FIRE. FIRE SUPPRESSION FOAM APPLIED. FIRE EXTINGUISHED. NO INJURIES REPORTED.",
        'ALARM_ACTIVATION': "UNITS RESPONDED TO FIRE ALARM ACTIVATION. BUILDING CHECKED FOR SIGNS OF FIRE OR SMOKE. NO FIRE FOUND. ALARM RESET.",
        'MEDICAL_EMERGENCY': "UNITS RESPONDED TO MEDICAL EMERGENCY. PATIENT ASSESSED AND TREATED. TRANSPORT TO HOSPITAL PROVIDED.",
        'HAZMAT_INCIDENT': "UNITS RESPONDED TO HAZMAT INCIDENT. SCENE SECURED AND HAZARDOUS MATERIALS CONTAINED. SPECIALIZED TEAMS DEPLOYED."
    }
    
    return narratives.get(incident_type, "UNITS RESPONDED TO EMERGENCY CALL. APPROPRIATE ACTION TAKEN.")

def generate_ems_incident(self):
    """Generate comprehensive EMS incident"""
    incident_types = [
        'CHEST_PAIN', 'DIFFICULTY_BREATHING', 'UNCONSCIOUS', 'CARDIAC_ARREST',
        'STROKE', 'OVERDOSE', 'TRAUMA', 'PSYCHIATRIC', 'ALLERGIC_REACTION',
        'DIABETIC_EMERGENCY', 'SEIZURE', 'FALL', 'MOTOR_VEHICLE_ACCIDENT'
    ]
    
    weights = [15, 12, 10, 3, 5, 8, 12, 6, 4, 7, 6, 15, 7]
    incident_type = random.choices(incident_types, weights=weights)[0]
    
    # Generate timing
    dispatch_datetime = fake.date_time_between(start_date='-2y', end_date='now')
    en_route_datetime = dispatch_datetime + timedelta(seconds=random.randint(30, 180))
    arrive_datetime = en_route_datetime + timedelta(minutes=random.randint(4, 15))
    patient_contact_datetime = arrive_datetime + timedelta(minutes=random.randint(1, 5))
    
    # Transport decision
    transport_needed = random.choices([True, False], weights=[75, 25])[0]
    
    if transport_needed:
        transport_datetime = patient_contact_datetime + timedelta(minutes=random.randint(10, 45))
        hospital_arrival_datetime = transport_datetime + timedelta(minutes=random.randint(5, 30))
        back_in_service_datetime = hospital_arrival_datetime + timedelta(minutes=random.randint(15, 45))
    else:
        transport_datetime = None
        hospital_arrival_datetime = None
        back_in_service_datetime = patient_contact_datetime + timedelta(minutes=random.randint(30, 90))
    
    # Location
    location = self.select_location_in_jurisdiction('SEATTLE_FD')
    
    # Generate incident identifiers
    incident_number = f"EMS{dispatch_datetime.year}{random.randint(100000, 999999)}"
    call_number = f"M{dispatch_datetime.year}{random.randint(1000000, 9999999)}"
    
    # Patient demographics
    patient_age = self.generate_patient_age(incident_type)
    patient_sex = random.choice(['M', 'F'])
    
    # Chief complaint mapping
    chief_complaints = {
        'CHEST_PAIN': 'CHEST PAIN/DISCOMFORT',
        'DIFFICULTY_BREATHING': 'SHORTNESS OF BREATH',
        'UNCONSCIOUS': 'UNRESPONSIVE',
        'CARDIAC_ARREST': 'CARDIAC/RESPIRATORY ARREST',
        'STROKE': 'STROKE/CVA',
        'OVERDOSE': 'OVERDOSE/POISONING',
        'TRAUMA': 'TRAUMATIC INJURY',
        'FALL': 'FALL'
    }
    
    chief_complaint = chief_complaints.get(incident_type, incident_type)
    
    # Vital signs based on condition
    vital_signs = self.generate_vital_signs(incident_type, patient_age)
    
    # Medications and procedures
    medications = self.generate_medications_administered(incident_type)
    procedures = self.generate_procedures_performed(incident_type)
    
    # Patient disposition
    if transport_needed:
        disposition = random.choices(
            ['TRANSPORTED_ALS', 'TRANSPORTED_BLS', 'TRANSPORTED_TRAUMA'],
            weights=[50, 40, 10]
        )[0]
    else:
        disposition = random.choices(
            ['TREATED_RELEASED', 'REFUSED_TRANSPORT', 'CANCELLED_NO_PATIENT'],
            weights=[60, 35, 5]
        )[0]
    
    incident = EMSIncident(
        incident_id=str(uuid.uuid4()),
        incident_number=incident_number,
        call_number=call_number,
        incident_type=incident_type,
        chief_complaint=chief_complaint,
        dispatch_datetime=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        en_route_datetime=en_route_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        arrive_datetime=arrive_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        patient_contact_datetime=patient_contact_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        transport_datetime=transport_datetime.strftime('%Y-%m-%d %H:%M:%S') if transport_datetime else '',
        hospital_arrival_datetime=hospital_arrival_datetime.strftime('%Y-%m-%d %H:%M:%S') if hospital_arrival_datetime else '',
        back_in_service_datetime=back_in_service_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        address=location['street'],
        city=location['city'],
        latitude=location['latitude'],
        longitude=location['longitude'],
        scene_gps_coordinates=f"{location['latitude']:.6f}, {location['longitude']:.6f}",
        responding_unit=f"A{random.randint(1, 20)}",
        unit_type=random.choices(['BLS', 'ALS', 'MEDIC_ONE'], weights=[40, 50, 10])[0],
        crew_members=[
            {'name': f"{fake.last_name().upper()}, {fake.first_name().upper()}", 'certification': 'EMT'},
            {'name': f"{fake.last_name().upper()}, {fake.first_name().upper()}", 'certification': random.choice(['EMT', 'PARAMEDIC'])}
        ],
        patient_id='',  # Could link to Person if identifiable
        patient_age=patient_age,
        patient_sex=patient_sex,
        patient_race=random.choice(['WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'OTHER', 'UNKNOWN']),
        primary_impression=self.generate_primary_impression(incident_type),
        secondary_impressions=self.generate_secondary_impressions(incident_type),
        vital_signs=vital_signs,
        medications_administered=medications,
        procedures_performed=procedures,
        transport_mode='GROUND' if transport_needed else '',
        transport_destination=random.choice(['HARBORVIEW', 'SWEDISH', 'VIRGINIA_MASON', 'UW_MEDICAL']) if transport_needed else '',
        patient_disposition=disposition,
        trauma_alert_level=self.generate_trauma_level(incident_type),
        stroke_alert=incident_type == 'STROKE',
        cardiac_arrest=incident_type == 'CARDIAC_ARREST',
        narcotic_overdose=incident_type == 'OVERDOSE' and random.random() < 0.6,
        mental_health_crisis=incident_type == 'PSYCHIATRIC',
        domestic_violence_suspected=random.random() < 0.05,
        injury_cause=self.generate_injury_cause(incident_type),
        mechanism_of_injury=self.generate_mechanism_of_injury(incident_type),
        protective_equipment_used=random.choice(['GLOVES', 'GLOVES_MASK', 'FULL_PPE']),
        scene_safety_concerns=['UNSAFE_SCENE', 'COMBATIVE_PATIENT'] if random.random() < 0.1 else [],
        law_enforcement_present=random.random() < 0.3,
        law_enforcement_agency=random.choice(['KCSO', 'BELLEVUE_PD', 'SEATTLE_PD']) if random.random() < 0.3 else '',
        mutual_aid_agency='BELLEVUE_FD' if random.random() < 0.05 else '',
        narrative=self.generate_ems_narrative(incident_type, disposition),
        created_date=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        created_by=f"EMT{random.randint(1000, 9999)}, {fake.last_name().upper()}"
    )
    
    return incident

def generate_patient_age(self, incident_type):
    """Generate age based on incident type"""
    if incident_type in ['CARDIAC_ARREST', 'STROKE', 'CHEST_PAIN']:
        return random.randint(50, 85)  # Older patients more likely
    elif incident_type == 'OVERDOSE':
        return random.randint(18, 45)  # Younger adults
    elif incident_type == 'FALL':
        return random.randint(65, 95)  # Elderly more likely
    else:
        return random.randint(18, 80)

def generate_vital_signs(self, incident_type, age):
    """Generate vital signs based on condition"""
    # Base normal ranges adjusted for age
    if age > 65:
        base_bp_sys = random.randint(130, 150)
        base_hr = random.randint(65, 85)
    else:
        base_bp_sys = random.randint(110, 130)
        base_hr = random.randint(70, 90)
    
    # Adjust for condition
    if incident_type == 'CARDIAC_ARREST':
        return {
            'systolic_bp': 0,
            'diastolic_bp': 0,
            'heart_rate': 0,
            'respiratory_rate': 0,
            'temperature': 0,
            'oxygen_sat': 0
        }
    elif incident_type == 'CHEST_PAIN':
        return {
            'systolic_bp': random.randint(140, 180),
            'diastolic_bp': random.randint(90, 110),
            'heart_rate': random.randint(90, 120),
            'respiratory_rate': random.randint(18, 24),
            'temperature': round(random.uniform(98.0, 99.5), 1),
            'oxygen_sat': random.randint(92, 100)
        }
    else:
        return {
            'systolic_bp': base_bp_sys,
            'diastolic_bp': random.randint(70, 90),
            'heart_rate': base_hr,
            'respiratory_rate': random.randint(16, 20),
            'temperature': round(random.uniform(98.0, 99.0), 1),
            'oxygen_sat': random.randint(95, 100)
        }

def generate_medications_administered(self, incident_type):
    """Generate medications based on condition"""
    medications = []
    
    if incident_type == 'CARDIAC_ARREST':
        medications = [
            {'medication': 'EPINEPHRINE', 'dose': '1mg', 'route': 'IV'},
            {'medication': 'AMIODARONE', 'dose': '300mg', 'route': 'IV'}
        ]
    elif incident_type == 'CHEST_PAIN':
        medications = [
            {'medication': 'ASPIRIN', 'dose': '324mg', 'route': 'PO'},
            {'medication': 'NITROGLYCERIN', 'dose': '0.4mg', 'route': 'SL'}
        ]
    elif incident_type == 'OVERDOSE':
        medications = [
            {'medication': 'NALOXONE', 'dose': '2mg', 'route': 'IN'}
        ]
    elif incident_type == 'DIFFICULTY_BREATHING':
        medications = [
            {'medication': 'ALBUTEROL', 'dose': '2.5mg', 'route': 'NEB'}
        ]
    
    return medications

def generate_procedures_performed(self, incident_type):
    """Generate procedures based on condition"""
    procedures = []
    
    if incident_type == 'CARDIAC_ARREST':
        procedures = [
            {'procedure': 'CPR', 'duration': '15 MINUTES'},
            {'procedure': 'DEFIBRILLATION', 'shocks': '3'},
            {'procedure': 'INTUBATION', 'attempts': '1'}
        ]
    elif incident_type in ['CHEST_PAIN', 'DIFFICULTY_BREATHING']:
        procedures = [
            {'procedure': '12_LEAD_ECG', 'result': 'ABNORMAL'},
            {'procedure': 'IV_ACCESS', 'size': '18G'}
        ]
    elif incident_type == 'TRAUMA':
        procedures = [
            {'procedure': 'SPINE_IMMOBILIZATION', 'device': 'BACKBOARD'},
            {'procedure': 'WOUND_CARE', 'type': 'BANDAGING'}
        ]
    
    return procedures

def generate_primary_impression(self, incident_type):
    """Generate primary medical impression"""
    impressions = {
        'CHEST_PAIN': 'ACUTE CORONARY SYNDROME',
        'DIFFICULTY_BREATHING': 'RESPIRATORY DISTRESS',
        'CARDIAC_ARREST': 'CARDIAC ARREST',
        'STROKE': 'CEREBROVASCULAR ACCIDENT',
        'OVERDOSE': 'DRUG OVERDOSE',
        'TRAUMA': 'TRAUMATIC INJURY',
        'FALL': 'FALL WITH INJURY'
    }
    
    return impressions.get(incident_type, 'MEDICAL EMERGENCY')

def generate_secondary_impressions(self, incident_type):
    """Generate secondary impressions"""
    secondary = {
        'CHEST_PAIN': ['HYPERTENSION', 'ANXIETY'],
        'DIFFICULTY_BREATHING': ['COPD', 'CHF'],
        'OVERDOSE': ['DEPRESSION', 'SUBSTANCE_ABUSE'],
        'FALL': ['OSTEOPOROSIS', 'DEMENTIA']
    }
    
    return secondary.get(incident_type, [])

def generate_trauma_level(self, incident_type):
    """Generate trauma alert level"""
    if incident_type == 'TRAUMA':
        return random.choices(['NONE', 'YELLOW', 'RED'], weights=[60, 30, 10])[0]
    elif incident_type == 'MOTOR_VEHICLE_ACCIDENT':
        return random.choices(['NONE', 'YELLOW', 'RED'], weights=[40, 40, 20])[0]
    else:
        return 'NONE'

def generate_injury_cause(self, incident_type):
    """Generate injury cause"""
    if incident_type == 'FALL':
        return random.choice(['SLIP', 'TRIP', 'FALL_FROM_HEIGHT', 'MECHANICAL_FALL'])
    elif incident_type == 'TRAUMA':
        return random.choice(['ASSAULT', 'MVC', 'FALL', 'SPORTS', 'WORK_RELATED'])
    else:
        return 'N/A'

def generate_mechanism_of_injury(self, incident_type):
    """Generate mechanism of injury"""
    if incident_type == 'TRAUMA':
        return random.choice(['BLUNT_FORCE', 'PENETRATING', 'CRUSH', 'BURN'])
    elif incident_type == 'FALL':
        return 'FALL'
    else:
        return 'N/A'

def generate_ems_narrative(self, incident_type, disposition):
    """Generate EMS narrative"""
    narratives = {
        'CHEST_PAIN': f"RESPONDED TO {incident_type}. PATIENT ASSESSED AND TREATED PER PROTOCOL. {disposition}.",
        'CARDIAC_ARREST': "RESPONDED TO CARDIAC ARREST. CPR IN PROGRESS ON ARRIVAL. RESUSCITATION EFFORTS CONTINUED. ROSC ACHIEVED.",
        'OVERDOSE': "RESPONDED TO POSSIBLE OVERDOSE. NALOXONE ADMINISTERED WITH GOOD EFFECT. PATIENT BECAME RESPONSIVE.",
        'FALL': "RESPONDED TO FALL. PATIENT ASSESSED FOR INJURIES. SPINE PRECAUTIONS TAKEN."
    }
    
    return narratives.get(incident_type, f"RESPONDED TO {incident_type}. PATIENT ASSESSED AND TREATED. {disposition}.")

def create_cross_agency_links(self):
    """Create realistic cross-agency data relationships"""
    print("Creating cross-agency data relationships...")
    
    # Link some police incidents to EMS responses
    for police_incident in self.police_incidents[:1000]:  # Sample for performance
        if police_incident.incident_type in ['ASSAULT', 'DOMESTIC_VIOLENCE'] and random.random() < 0.3:
            # Create corresponding EMS incident
            ems_incident = self.generate_ems_incident()
            ems_incident.incident_type = 'TRAUMA'
            ems_incident.law_enforcement_present = True
            ems_incident.law_enforcement_agency = police_incident.agency
            ems_incident.latitude = police_incident.latitude
            ems_incident.longitude = police_incident.longitude
            ems_incident.address = police_incident.location_address
            ems_incident.city = police_incident.location_city
            self.ems_incidents.append(ems_incident)
    
    # Link some persons across agencies
    person_agency_map = defaultdict(list)
    for person in self.persons:
        person_agency_map[person.created_by_agency].append(person.person_id)
    
    # Create shared person records (same person known to multiple agencies)
    num_shared = min(1000, len(self.persons) // 10)
    for _ in range(num_shared):
        base_person = random.choice(self.persons)
        # This person is also known to other agencies
        for agency in ['KCSO', 'BELLEVUE_PD']:
            if agency != base_person.created_by_agency and random.random() < 0.3:
                # Add cross-reference (in real system would be same person_id)
                pass
    
def generate_all_data(self):
    """Generate all synthetic data with realistic relationships"""
    print("🚀 Multi-Agency Complex Data Generator Starting...")
    start_time = time.time()
    
    # Generate persons
    print(f"📋 Generating {CONFIG['num_persons']:,} persons...")
    for i in range(CONFIG['num_persons']):
        agency = random.choices(['KCSO', 'BELLEVUE_PD'], weights=[70, 30])[0]
        person = self.generate_person(agency)
        self.persons.append(person)
        
        if (i + 1) % 5000 == 0:
            print(f"   Generated {i + 1:,} persons...")
    
    # Generate vehicles
    print(f"🚗 Generating {CONFIG['num_vehicles']:,} vehicles...")
    for i in range(CONFIG['num_vehicles']):
        # 70% of vehicles have known owners
        owner_id = random.choice(self.persons).person_id if random.random() < 0.7 else None
        vehicle = self.generate_vehicle(owner_id)
        self.vehicles.append(vehicle)
        
        if (i + 1) % 10000 == 0:
            print(f"   Generated {i + 1:,} vehicles...")
    
    # Generate police incidents
    print("🚔 Generating police incidents...")
    num_police_incidents = CONFIG['num_persons'] // 3  # ~50k incidents
    for i in range(num_police_incidents):
        agency = random.choices(['KCSO', 'BELLEVUE_PD'], weights=[75, 25])[0]
        incident = self.generate_police_incident(agency)
        self.police_incidents.append(incident)
        
        if (i + 1) % 5000 == 0:
            print(f"   Generated {i + 1:,} police incidents...")
    
    # Generate fire incidents
    if CONFIG['generate_fire_data']:
        print("🔥 Generating fire incidents...")
        num_fire_incidents = 15000  # Seattle FD responds to ~15k calls annually
        for i in range(num_fire_incidents):
            incident = self.generate_fire_incident()
            self.fire_incidents.append(incident)
            
            if (i + 1) % 2500 == 0:
                print(f"   Generated {i + 1:,} fire incidents...")
    
    # Generate EMS incidents
    if CONFIG['generate_ems_data']:
        print("🚑 Generating EMS incidents...")
        num_ems_incidents = 85000  # Seattle FD EMS responds to ~85k calls annually
        for i in range(num_ems_incidents):
            incident = self.generate_ems_incident()
            self.ems_incidents.append(incident)
            
            if (i + 1) % 10000 == 0:
                print(f"   Generated {i + 1:,} EMS incidents...")
    
    # Create cross-agency relationships
    if CONFIG['enable_cross_agency_sharing']:
        self.create_cross_agency_links()
    
    total_time = time.time() - start_time
    print(f"\n⏱️  Total generation time: {total_time:.1f} seconds")
    print(f"📊 Generation rate: {len(self.persons)/total_time:.0f} persons/second")
    
    self.print_summary()

def print_summary(self):
    """Print comprehensive data summary"""
    print(f"\n" + "="*70)
    print(f"📊 MULTI-AGENCY SYNTHETIC DATA GENERATION COMPLETE")
    print(f"="*70)
    
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
            print(f"  • {AGENCIES[agency]['name']}: {count:,}")
    
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
    
    print(f"\n" + "="*70)

def save_data(self):
    """Save all generated data in requested formats"""
    print(f"\n💾 Saving data in formats: {', '.join(CONFIG['output_formats'])}")
    
    if 'json' in CONFIG['output_formats']:
        self.save_json_files()
    
    if 'csv' in CONFIG['output_formats']:
        self.save_csv_files()
    
    if 'sqlite' in CONFIG['output_formats']:
        self.save_sqlite_database()
    
    print("✅ All data saved successfully!")

def save_json_files(self):
    """Save data as JSON files"""
    print("  📄 Saving JSON files...")
    
    # Convert dataclasses to dictionaries
    persons_dict = [asdict(p) for p in self.persons]
    vehicles_dict = [asdict(v) for v in self.vehicles]
    properties_dict = [asdict(p) for p in self.properties]
    police_incidents_dict = [asdict(i) for i in self.police_incidents]
    arrests_dict = [asdict(a) for a in self.arrests]
    jail_bookings_dict = [asdict(b) for b in self.jail_bookings]
    fire_incidents_dict = [asdict(i) for i in self.fire_incidents]
    ems_incidents_dict = [asdict(i) for i in self.ems_incidents]
    
    # Save files
    with open('persons.json', 'w') as f:
        json.dump(persons_dict, f, indent=2, default=str)
    
    with open('vehicles.json', 'w') as f:
        json.dump(vehicles_dict, f, indent=2, default=str)
    
    with open('properties.json', 'w') as f:
        json.dump(properties_dict, f, indent=2, default=str)
    
    with open('police_incidents.json', 'w') as f:
        json.dump(police_incidents_dict, f, indent=2, default=str)
    
    with open('arrests.json', 'w') as f:
        json.dump(arrests_dict, f, indent=2, default=str)
    
    with open('jail_bookings.json', 'w') as f:
        json.dump(jail_bookings_dict, f, indent=2, default=str)
    
    with open('fire_incidents.json', 'w') as f:
        json.dump(fire_incidents_dict, f, indent=2, default=str)
    
    with open('ems_incidents.json', 'w') as f:
        json.dump(ems_incidents_dict, f, indent=2, default=str)

def save_csv_files(self):
    """Save data as CSV files"""
    print("  📊 Saving CSV files...")
    
    # Convert dataclasses to dictionaries and save as CSV
    def save_dataclass_csv(data_list, filename):
        if data_list:
            dict_list = [asdict(item) for item in data_list]
            # Flatten nested structures for CSV
            flattened_list = []
            for item in dict_list:
                flat_item = {}
                for key, value in item.items():
                    if isinstance(value, (list, dict)):
                        flat_item[key] = json.dumps(value)
                    else:
                        flat_item[key] = value
                flattened_list.append(flat_item)
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                if flattened_list:
                    writer = csv.DictWriter(f, fieldnames=flattened_list[0].keys())
                    writer.writeheader()
                    writer.writerows(flattened_list)
    
    save_dataclass_csv(self.persons, 'persons.csv')
    save_dataclass_csv(self.vehicles, 'vehicles.csv')
    save_dataclass_csv(self.properties, 'properties.csv')
    save_dataclass_csv(self.police_incidents, 'police_incidents.csv')
    save_dataclass_csv(self.arrests, 'arrests.csv')
    save_dataclass_csv(self.jail_bookings, 'jail_bookings.csv')
    save_dataclass_csv(self.fire_incidents, 'fire_incidents.csv')
    save_dataclass_csv(self.ems_incidents, 'ems_incidents.csv')

def save_sqlite_database(self):
    """Save data to SQLite database with proper relationships"""
    print("  🗄️  Saving SQLite database...")
    
    conn = sqlite3.connect('multi_agency_data.db')
    cursor = conn.cursor()
    
    # Create tables with proper schemas
    self.create_database_tables(cursor)
    
    # Insert data
    self.insert_database_data(cursor, conn)
    
    conn.close()

def create_database_tables(self, cursor):
    """Create database tables with proper relationships"""
    
    # Persons table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS persons (
            person_id TEXT PRIMARY KEY,
            ssn TEXT UNIQUE,
            first_name TEXT NOT NULL,
            middle_name TEXT,
            last_name TEXT NOT NULL,
            name_suffix TEXT,
            dob TEXT NOT NULL,
            age INTEGER,
            sex TEXT,
            race TEXT,
            ethnicity TEXT,
            height_cm INTEGER,
            weight_lbs INTEGER,
            eye_color TEXT,
            hair_color TEXT,
            build TEXT,
            complexion TEXT,
            facial_hair TEXT,
            glasses TEXT,
            phone_primary TEXT,
            phone_secondary TEXT,
            email TEXT,
            address_street TEXT,
            address_city TEXT,
            address_state TEXT,
            address_zip TEXT,
            latitude REAL,
            longitude REAL,
            dl_number TEXT,
            dl_state TEXT,
            dl_status TEXT,
            state_id TEXT,
            fbi_id TEXT,
            employer TEXT,
            occupation TEXT,
            marital_status TEXT,
            emergency_contact_name TEXT,
            emergency_contact_phone TEXT,
            aliases TEXT,
            gang_affiliation TEXT,
            known_associates TEXT,
            criminal_history_score INTEGER,
            mental_health_flags TEXT,
            medical_conditions TEXT,
            medications TEXT,
            tattoos_scars_marks TEXT,
            fingerprint_classification TEXT,
            dna_profile_id TEXT,
            photos TEXT,
            created_date TEXT,
            updated_date TEXT,
            created_by_agency TEXT,
            housing_status TEXT,
            employment_status TEXT,
            education_level TEXT,
            veteran_status TEXT,
            primary_language TEXT,
            risk_level TEXT,
            probation_status TEXT,
            substance_abuse_flags TEXT
        )
    ''')
    
    # Vehicles table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id TEXT PRIMARY KEY,
            vin TEXT UNIQUE NOT NULL,
            license_plate TEXT,
            plate_state TEXT,
            make TEXT,
            model TEXT,
            year INTEGER,
            color TEXT,
            vehicle_type TEXT,
            owner_person_id TEXT,
            registered_owner_name TEXT,
            registration_status TEXT,
            insurance_company TEXT,
            insurance_policy TEXT,
            insurance_status TEXT,
            stolen_status TEXT,
            impound_status TEXT,
            towed_date TEXT,
            impound_lot TEXT,
            ncic_entry_date TEXT,
            created_date TEXT,
            agency_entered TEXT,
            FOREIGN KEY (owner_person_id) REFERENCES persons (person_id)
        )
    ''')
    
    # Properties table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            property_id TEXT PRIMARY KEY,
            property_type TEXT,
            case_number TEXT,
            incident_number TEXT,
            description TEXT,
            category TEXT,
            subcategory TEXT,
            serial_number TEXT,
            make_model TEXT,
            value_estimated REAL,
            currency_amount REAL,
            quantity INTEGER,
            unit_of_measure TEXT,
            found_location TEXT,
            found_date TEXT,
            found_by_officer TEXT,
            owner_person_id TEXT,
            chain_of_custody TEXT,
            evidence_locker TEXT,
            destruction_date TEXT,
            disposition TEXT,
            created_date TEXT,
            agency TEXT,
            FOREIGN KEY (owner_person_id) REFERENCES persons (person_id)
        )
    ''')
    
    # Police Incidents table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS police_incidents (
            incident_id TEXT PRIMARY KEY,
            incident_number TEXT UNIQUE NOT NULL,
            case_number TEXT,
            call_number TEXT,
            incident_type TEXT,
            incident_subtype TEXT,
            ucr_code TEXT,
            nibrs_code TEXT,
            location_address TEXT,
            location_city TEXT,
            location_beat TEXT,
            location_precinct TEXT,
            latitude REAL,
            longitude REAL,
            occurred_datetime TEXT,
            reported_datetime TEXT,
            dispatch_datetime TEXT,
            arrived_datetime TEXT,
            cleared_datetime TEXT,
            reporting_officer TEXT,
            supervisor_officer TEXT,
            assisting_officers TEXT,
            persons_involved TEXT,
            vehicles_involved TEXT,
            property_involved TEXT,
            narrative TEXT,
            status TEXT,
            clearance_type TEXT,
            modus_operandi TEXT,
            domestic_violence BOOLEAN,
            gang_related BOOLEAN,
            hate_crime BOOLEAN,
            drug_related BOOLEAN,
            alcohol_related BOOLEAN,
            weapon_used TEXT,
            injury_severity TEXT,
            agency TEXT,
            created_date TEXT,
            updated_date TEXT
        )
    ''')
    
    # Fire Incidents table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fire_incidents (
            incident_id TEXT PRIMARY KEY,
            incident_number TEXT UNIQUE NOT NULL,
            call_number TEXT,
            incident_type TEXT,
            incident_subtype TEXT,
            nfirs_code TEXT,
            alarm_datetime TEXT,
            dispatch_datetime TEXT,
            en_route_datetime TEXT,
            arrive_datetime TEXT,
            controlled_datetime TEXT,
            last_unit_cleared_datetime TEXT,
            address TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            district TEXT,
            first_due_station INTEGER,
            units_responding TEXT,
            incident_commander TEXT,
            fire_cause TEXT,
            fire_origin TEXT,
            ignition_factor TEXT,
            property_type TEXT,
            property_use TEXT,
            occupancy_type TEXT,
            construction_type TEXT,
            stories INTEGER,
            total_floor_area INTEGER,
            fire_spread TEXT,
            smoke_spread TEXT,
            detector_present BOOLEAN,
            detector_operation TEXT,
            sprinkler_present BOOLEAN,
            sprinkler_operation TEXT,
            estimated_loss_property REAL,
            estimated_loss_contents REAL,
            casualties TEXT,
            fatalities INTEGER,
            injuries_fire INTEGER,
            injuries_non_fire INTEGER,
            personnel_injuries INTEGER,
            apparatus_involved TEXT,
            mutual_aid_given TEXT,
            mutual_aid_received TEXT,
            hazmat_release BOOLEAN,
            hazmat_id TEXT,
            narrative TEXT,
            created_date TEXT,
            created_by TEXT
        )
    ''')
    
    # EMS Incidents table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ems_incidents (
            incident_id TEXT PRIMARY KEY,
            incident_number TEXT UNIQUE NOT NULL,
            call_number TEXT,
            incident_type TEXT,
            chief_complaint TEXT,
            dispatch_datetime TEXT,
            en_route_datetime TEXT,
            arrive_datetime TEXT,
            patient_contact_datetime TEXT,
            transport_datetime TEXT,
            hospital_arrival_datetime TEXT,
            back_in_service_datetime TEXT,
            address TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            scene_gps_coordinates TEXT,
            responding_unit TEXT,
            unit_type TEXT,
            crew_members TEXT,
            patient_id TEXT,
            patient_age INTEGER,
            patient_sex TEXT,
            patient_race TEXT,
            primary_impression TEXT,
            secondary_impressions TEXT,
            vital_signs TEXT,
            medications_administered TEXT,
            procedures_performed TEXT,
            transport_mode TEXT,
            transport_destination TEXT,
            patient_disposition TEXT,
            trauma_alert_level TEXT,
            stroke_alert BOOLEAN,
            cardiac_arrest BOOLEAN,
            narcotic_overdose BOOLEAN,
            mental_health_crisis BOOLEAN,
            domestic_violence_suspected BOOLEAN,
            injury_cause TEXT,
            mechanism_of_injury TEXT,
            protective_equipment_used TEXT,
            scene_safety_concerns TEXT,
            law_enforcement_present BOOLEAN,
            law_enforcement_agency TEXT,
            mutual_aid_agency TEXT,
            narrative TEXT,
            created_date TEXT,
            created_by TEXT,
            FOREIGN KEY (patient_id) REFERENCES persons (person_id)
        )
    ''')
    
    # Create indexes for performance
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_persons_name ON persons (last_name, first_name)",
        "CREATE INDEX IF NOT EXISTS idx_persons_dob ON persons (dob)",
        "CREATE INDEX IF NOT EXISTS idx_persons_ssn ON persons (ssn)",
        "CREATE INDEX IF NOT EXISTS idx_vehicles_plate ON vehicles (license_plate, plate_state)",
        "CREATE INDEX IF NOT EXISTS idx_vehicles_vin ON vehicles (vin)",
        "CREATE INDEX IF NOT EXISTS idx_incidents_date ON police_incidents (occurred_datetime)",
        "CREATE INDEX IF NOT EXISTS idx_incidents_type ON police_incidents (incident_type)",
        "CREATE INDEX IF NOT EXISTS idx_incidents_location ON police_incidents (latitude, longitude)",
        "CREATE INDEX IF NOT EXISTS idx_fire_incidents_date ON fire_incidents (alarm_datetime)",
        "CREATE INDEX IF NOT EXISTS idx_ems_incidents_date ON ems_incidents (dispatch_datetime)"
    ]
    
    for index_sql in indexes:
        cursor.execute(index_sql)

def insert_database_data(self, cursor, conn):
    """Insert all data into database tables"""
    
    # Insert persons
    if self.persons:
        person_data = []
        for person in self.persons:
            person_dict = asdict(person)
            # Convert lists to JSON strings
            for key, value in person_dict.items():
                if isinstance(value, list):
                    person_dict[key] = json.dumps(value)
            person_data.append(tuple(person_dict.values()))
        
        cursor.executemany(f'''
            INSERT INTO persons VALUES ({','.join(['?' for _ in asdict(self.persons[0])])})
        ''', person_data)
    
    # Insert vehicles
    if self.vehicles:
        vehicle_data = []
        for vehicle in self.vehicles:
            vehicle_dict = asdict(vehicle)
            vehicle_data.append(tuple(vehicle_dict.values()))
        
        cursor.executemany(f'''
            INSERT INTO vehicles VALUES ({','.join(['?' for _ in asdict(self.vehicles[0])])})
        ''', vehicle_data)
    
    # Insert properties
    if self.properties:
        property_data = []
        for prop in self.properties:
            prop_dict = asdict(prop)
            # Convert lists to JSON strings
            for key, value in prop_dict.items():
                if isinstance(value, list):
                    prop_dict[key] = json.dumps(value)
            property_data.append(tuple(prop_dict.values()))
        
        cursor.executemany(f'''
            INSERT INTO properties VALUES ({','.join(['?' for _ in asdict(self.properties[0])])})
        ''', property_data)
    
    # Insert police incidents
    if self.police_incidents:
        incident_data = []
        for incident in self.police_incidents:
            incident_dict = asdict(incident)
            # Convert lists to JSON strings
            for key, value in incident_dict.items():
                if isinstance(value, list):
                    incident_dict[key] = json.dumps(value)
            incident_data.append(tuple(incident_dict.values()))
        
        cursor.executemany(f'''
            INSERT INTO police_incidents VALUES ({','.join(['?' for _ in asdict(self.police_incidents[0])])})
        ''', incident_data)
    
    # Insert fire incidents
    if self.fire_incidents:
        fire_data = []
        for incident in self.fire_incidents:
            incident_dict = asdict(incident)
            # Convert lists to JSON strings
            for key, value in incident_dict.items():
                if isinstance(value, list):
                    incident_dict[key] = json.dumps(value)
            fire_data.append(tuple(incident_dict.values()))
        
        cursor.executemany(f'''
            INSERT INTO fire_incidents VALUES ({','.join(['?' for _ in asdict(self.fire_incidents[0])])})
        ''', fire_data)
    
    # Insert EMS incidents
    if self.ems_incidents:
        ems_data = []
        for incident in self.ems_incidents:
            incident_dict = asdict(incident)
            # Convert lists/dicts to JSON strings
            for key, value in incident_dict.items():
                if isinstance(value, (list, dict)):
                    incident_dict[key] = json.dumps(value)
            ems_data.append(tuple(incident_dict.values()))
        
        cursor.executemany(f'''
            INSERT INTO ems_incidents VALUES ({','.join(['?' for _ in asdict(self.ems_incidents[0])])})
        ''', ems_data)
    
    conn.commit()

def main():
"""Main execution function"""
print("🚀 Multi-Agency Complex Synthetic Data Generator")
print("Generating data for King County SO, Bellevue PD, and Seattle FD")
print(f"Configuration: {CONFIG}")

generator = DataGenerator()

try:
    # Generate all data
    generator.generate_all_data()
    
    # Save in requested formats
    generator.save_data()
    
    print("\n🎉 Multi-agency synthetic data generation completed successfully!")
    print("📁 Files created:")
    
    if 'json' in CONFIG['output_formats']:
        print("   • *.json files for each data type")
    if 'csv' in CONFIG['output_formats']:
        print("   • *.csv files for each data type")
    if 'sqlite' in CONFIG['output_formats']:
        print("   • multi_agency_data.db (SQLite database)")
    
    print(f"\n📈 Total records: {sum([
        len(generator.persons),
        len(generator.vehicles), 
        len(generator.properties),
        len(generator.police_incidents),
        len(generator.arrests),
        len(generator.jail_bookings),
        len(generator.fire_incidents),
        len(generator.ems_incidents)
    ]):,}")
    
except Exception as e:
    print(f"❌ Error during generation: {str(e)}")
    raise

if __name__ == "__main__":
main()#!/usr/bin/env python3
"""
Multi-Agency Complex Data Generator
Replicates data structures across:
- King County Sheriff's Office (including Seattle jurisdiction)
- Bellevue Police Department  
- Seattle Fire Department

Generates interconnected data with realistic cross-agency relationships,
shared databases, and complex multi-jurisdictional scenarios.
"""

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

class AgencyType(Enum):
SHERIFF = "SHERIFF"
POLICE = "POLICE" 
FIRE = "FIRE"
EMS = "EMS"
CORRECTIONS = "CORRECTIONS"

class IncidentType(Enum):
ARREST = "ARREST"
CITATION = "CITATION"
FIRE_RESPONSE = "FIRE_RESPONSE"
MEDICAL_RESPONSE = "MEDICAL_RESPONSE"
TRAFFIC_STOP = "TRAFFIC_STOP"
DOMESTIC_CALL = "DOMESTIC_CALL"
WARRANT_SERVICE = "WARRANT_SERVICE"
COURT_TRANSPORT = "COURT_TRANSPORT"
WELFARE_CHECK = "WELFARE_CHECK"
SUSPICIOUS_ACTIVITY = "SUSPICIOUS_ACTIVITY"

# Configuration
CONFIG = {
'num_persons': 150_000,  # Increased for multi-agency scope
'num_vehicles': 200_000,
'num_properties': 75_000,
'simulation_years': 10,
'output_formats': ['json', 'csv', 'sqlite'],
'enable_cross_agency_sharing': True,
'generate_rms_data': True,  # Records Management System
'generate_cad_data': True,  # Computer Aided Dispatch
'generate_jail_data': True,
'generate_fire_data': True,
'generate_ems_data': True,
'realistic_response_times': True,
'inter_agency_mutual_aid': True,
}

# Agency definitions with realistic jurisdictions and capabilities
AGENCIES = {
'KCSO': {
    'name': 'KING COUNTY SHERIFF\'S OFFICE',
    'type': AgencyType.SHERIFF,
    'jurisdiction_cities': [
        'UNINCORPORATED KING COUNTY', 'BURIEN', 'SKYKOMISH', 'WOODINVILLE',
        'SAMMAMISH', 'SHORELINE', 'LAKE FOREST PARK', 'CARNATION',
        'DUVALL', 'SNOQUALMIE', 'NORTH BEND', 'ALGONA', 'PACIFIC'
    ],
    'contracts_police_services': ['BURIEN', 'SKYKOMISH', 'WOODINVILLE'],
    'operates_jail': True,
    'patrol_zones': ['NORTH', 'SOUTH', 'EAST', 'WEST', 'METRO'],
    'specialties': ['CORRECTIONS', 'MARINE_PATROL', 'AVIATION', 'SWAT', 'K9'],
    'staff_count': 1200,
    'coordinates': (47.5480, -122.1430)  # Kent area (KCSO HQ area)
},
'BELLEVUE_PD': {
    'name': 'BELLEVUE POLICE DEPARTMENT',
    'type': AgencyType.POLICE,
    'jurisdiction_cities': ['BELLEVUE'],
    'contracts_police_services': [],
    'operates_jail': False,  # Uses KCSO jail
    'patrol_zones': ['NORTH', 'SOUTH', 'EAST', 'WEST', 'DOWNTOWN'],
    'specialties': ['TRAFFIC', 'DETECTIVES', 'SWAT', 'K9', 'SCHOOL_RESOURCE'],
    'staff_count': 180,
    'coordinates': (47.6101, -122.2015)  # Bellevue
},
'SEATTLE_FD': {
    'name': 'SEATTLE FIRE DEPARTMENT',
    'type': AgencyType.FIRE,
    'jurisdiction_cities': ['SEATTLE'],
    'contracts_services': ['TUKWILA', 'BURIEN'],  # Mutual aid agreements
    'operates_ems': True,
    'fire_stations': 33,
    'specialties': ['FIRE_SUPPRESSION', 'EMS', 'HAZMAT', 'MARINE', 'TECHNICAL_RESCUE'],
    'staff_count': 1000,
    'coordinates': (47.6062, -122.3321),  # Seattle
    'response_districts': ['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']
}
}

# Detailed King County geography with realistic beat/district assignments
KING_COUNTY_GEOGRAPHY = {
'SEATTLE': {
    'population': 737015,
    'area_sq_miles': 142.5,
    'beats': ['1A', '1B', '1C', '2A', '2B', '2C', '3A', '3B', '3C', '4A', '4B', '4C', '5A', '5B', '5C'],
    'precincts': ['NORTH', 'SOUTH', 'EAST', 'WEST', 'SOUTHWEST'],
    'fire_stations': [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37],
    'coordinates': (47.6062, -122.3321),
    'zipcodes': ['98101', '98102', '98103', '98104', '98105', '98106', '98107', '98108', '98109', '98112', '98115', '98116', '98117', '98118', '98119', '98121', '98122', '98125', '98126', '98133', '98134', '98136', '98144', '98146', '98148', '98154', '98164', '98174', '98177', '98178', '98195', '98199']
},
'BELLEVUE': {
    'population': 151854,
    'area_sq_miles': 33.5,
    'beats': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6'],
    'precincts': ['MAIN'],
    'fire_stations': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],  # Bellevue FD stations
    'coordinates': (47.6101, -122.2015),
    'zipcodes': ['98004', '98005', '98006', '98007', '98008', '98015', '98039', '98052', '98053', '98059']
},
'UNINCORPORATED_KING_COUNTY': {
    'population': 400000,  # Estimated
    'area_sq_miles': 1500,
    'beats': ['UC1', 'UC2', 'UC3', 'UC4', 'UC5', 'UC6', 'UC7', 'UC8'],
    'precincts': ['NORTH_COUNTY', 'SOUTH_COUNTY', 'EAST_COUNTY'],
    'coordinates': (47.5000, -121.8000),
    'zipcodes': ['98001', '98010', '98019', '98024', '98027', '98028', '98029', '98045', '98050', '98051', '98065', '98070', '98071', '98092']
}
}

# Complex incident types with realistic frequencies and agency responses
INCIDENT_TYPES = {
'TRAFFIC_VIOLATION': {'weight': 35, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'citation_likely': 0.8},
'DOMESTIC_VIOLENCE': {'weight': 15, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.6},
'THEFT': {'weight': 20, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'report_only': 0.7},
'ASSAULT': {'weight': 10, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.5},
'DUI': {'weight': 8, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.95},
'DRUG_POSSESSION': {'weight': 12, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.4},
'BURGLARY': {'weight': 5, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'report_investigation': 0.9},
'ROBBERY': {'weight': 3, 'agencies': ['KCSO', 'BELLEVUE_PD'], 'arrest_likely': 0.3},
'MEDICAL_EMERGENCY': {'weight': 40, 'agencies': ['SEATTLE_FD'], 'transport': 0.7},
'FIRE_ALARM': {'weight': 20, 'agencies': ['SEATTLE_FD'], 'false_alarm': 0.6},
'STRUCTURE_FIRE': {'weight': 8, 'agencies': ['SEATTLE_FD'], 'multi_unit': 0.8},
'VEHICLE_FIRE': {'weight': 5, 'agencies': ['SEATTLE_FD'], 'total_loss': 0.4},
'HAZMAT_INCIDENT': {'weight': 2, 'agencies': ['SEATTLE_FD'], 'specialist_required': 1.0},
'WATER_RESCUE': {'weight': 3, 'agencies': ['SEATTLE_FD'], 'marine_unit': 1.0},
'CARDIAC_ARREST': {'weight': 12, 'agencies': ['SEATTLE_FD'], 'als_required': 1.0},
'OVERDOSE': {'weight': 8, 'agencies': ['SEATTLE_FD'], 'narcan_admin': 0.6},
'MENTAL_HEALTH_CRISIS': {'weight': 10, 'agencies': ['KCSO', 'BELLEVUE_PD', 'SEATTLE_FD'], 'crisis_team': 0.4}
}

# Cross-agency shared databases and systems
SHARED_SYSTEMS = {
'NCIC': ['KCSO', 'BELLEVUE_PD'],  # National Crime Information Center
'WACIC': ['KCSO', 'BELLEVUE_PD'],  # Washington Crime Information Center  
'CJIS': ['KCSO', 'BELLEVUE_PD'],   # Criminal Justice Information Services
'JIMS': ['KCSO'],                  # Jail Information Management System
'CAD': ['KCSO', 'BELLEVUE_PD', 'SEATTLE_FD'],  # Computer Aided Dispatch (shared)
'RMS': ['KCSO', 'BELLEVUE_PD'],   # Records Management System
'FIRE_RMS': ['SEATTLE_FD'],       # Fire Records Management
'EMS_PATIENT_CARE': ['SEATTLE_FD'] # Patient Care Reporting
}

@dataclass
class Person:
person_id: str
ssn: str
first_name: str
middle_name: str
last_name: str
name_suffix: str
dob: str
age: int
sex: str
race: str
ethnicity: str
height_cm: int
weight_lbs: int
eye_color: str
hair_color: str
build: str
complexion: str
facial_hair: str
glasses: str
phone_primary: str
phone_secondary: str
email: str
address_street: str
address_city: str
address_state: str
address_zip: str
latitude: float
longitude: float
dl_number: str
dl_state: str
dl_status: str  # VALID, SUSPENDED, REVOKED, EXPIRED
state_id: str
fbi_id: str
employer: str
occupation: str
marital_status: str
emergency_contact_name: str
emergency_contact_phone: str
aliases: List[str]
gang_affiliation: str
known_associates: List[str]  # Person IDs
criminal_history_score: int  # 0-10 scale
mental_health_flags: List[str]
medical_conditions: List[str]
medications: List[str]
tattoos_scars_marks: str
fingerprint_classification: str
dna_profile_id: str
photos: List[str]  # Photo IDs
created_date: str
updated_date: str
created_by_agency: str
housing_status: str
employment_status: str
education_level: str
veteran_status: bool
primary_language: str
risk_level: str
probation_status: str
substance_abuse_flags: List[str]

@dataclass 
class Vehicle:
vehicle_id: str
vin: str
license_plate: str
plate_state: str
make: str
model: str
year: int
color: str
vehicle_type: str  # SEDAN, SUV, TRUCK, MOTORCYCLE, etc.
owner_person_id: str
registered_owner_name: str
registration_status: str  # CURRENT, EXPIRED, SUSPENDED
insurance_company: str
insurance_policy: str
insurance_status: str  # ACTIVE, EXPIRED, CANCELLED
stolen_status: str  # CLEAR, STOLEN, RECOVERED
impound_status: str
towed_date: str
impound_lot: str
ncic_entry_date: str
created_date: str
agency_entered: str

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
class PoliceIncident:
incident_id: str
incident_number: str
case_number: str
call_number: str  # CAD call number
incident_type: str
incident_subtype: str
ucr_code: str  # Uniform Crime Reporting code
nibrs_code: str  # National Incident-Based Reporting System
location_address: str
location_city: str
location_beat: str
location_precinct: str
latitude: float
longitude: float
occurred_datetime: str
reported_datetime: str
dispatch_datetime: str
arrived_datetime: str
cleared_datetime: str
reporting_officer: str
supervisor_officer: str
assisting_officers: List[str]
persons_involved: List[str]  # Person IDs
vehicles_involved: List[str]  # Vehicle IDs
property_involved: List[str]  # Property IDs
narrative: str
status: str  # ACTIVE, CLEARED, SUSPENDED
clearance_type: str  # ARREST, CITATION, REPORT_ONLY
modus_operandi: List[str]
domestic_violence: bool
gang_related: bool
hate_crime: bool
drug_related: bool
alcohol_related: bool
weapon_used: str
injury_severity: str
agency: str
created_date: str
updated_date: str

@dataclass
class Arrest:
arrest_id: str
person_id: str
incident_id: str
arrest_number: str
arrest_datetime: str
arrest_location: str
arrest_agency: str
arresting_officer: str
assisting_officers: List[str]
arrest_type: str  # ON_VIEW, WARRANT, CITIZEN_ARREST
resistance_level: str  # NONE, PASSIVE, ACTIVE, AGGRESSIVE
force_used: List[str]  # NONE, VERBAL, HANDS_ON, TASER, BATON, etc.
charges: List[Dict]  # Charge details
booking_id: str  # Links to jail booking if booked
citation_issued: bool
miranda_given: bool
miranda_datetime: str
transport_to: str  # JAIL, HOSPITAL, JUVENILE_DETENTION
property_seized: List[str]  # Property IDs
photos_taken: List[str]
created_date: str
agency: str

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
class FireIncident:
incident_id: str
incident_number: str
call_number: str
incident_type: str
incident_subtype: str
nfirs_code: str  # National Fire Incident Reporting System
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
first_due_station: str
units_responding: List[Dict]  # Unit, station, personnel
incident_commander: str
fire_cause: str
fire_origin: str
ignition_factor: str
property_type: str  # RESIDENTIAL, COMMERCIAL, VEHICLE, WILDLAND
property_use: str
occupancy_type: str
construction_type: str
stories: int
total_floor_area: int
fire_spread: str
smoke_spread: str
detector_present: bool
detector_operation: str
sprinkler_present: bool
sprinkler_operation: str
estimated_loss_property: float
estimated_loss_contents: float
casualties: List[Dict]  # Type, severity, age, sex
fatalities: int
injuries_fire: int
injuries_non_fire: int
personnel_injuries: int
apparatus_involved: List[str]
mutual_aid_given: List[str]  # Agencies
mutual_aid_received: List[str]
hazmat_release: bool
hazmat_id: str
narrative: str
created_date: str
created_by: str

@dataclass
class EMSIncident:
incident_id: str
incident_number: str
call_number: str
incident_type: str
chief_complaint: str
dispatch_datetime: str
en_route_datetime: str
arrive_datetime: str
patient_contact_datetime: str
transport_datetime: str
hospital_arrival_datetime: str
back_in_service_datetime: str
address: str
city: str
latitude: float
longitude: float
scene_gps_coordinates: str
responding_unit: str
unit_type: str  # BLS, ALS, MEDIC_ONE
crew_members: List[Dict]  # Name, certification level
patient_id: str  # Links to Person if identifiable
patient_age: int
patient_sex: str
patient_race: str
primary_impression: str
secondary_impressions: List[str]
vital_signs: Dict  # BP, pulse, resp, temp, etc.
medications_administered: List[Dict]
procedures_performed: List[Dict]
transport_mode: str  # GROUND, AIR
transport_destination: str
patient_disposition: str  # TREATED_RELEASED, TRANSPORTED, REFUSED, DOA
trauma_alert_level: str
stroke_alert: bool
cardiac_arrest: bool
narcotic_overdose: bool
mental_health_crisis: bool
domestic_violence_suspected: bool
injury_cause: str
mechanism_of_injury: str
protective_equipment_used: str
scene_safety_concerns: List[str]
law_enforcement_present: bool
law_enforcement_agency: str
mutual_aid_agency: str
narrative: str
created_date: str
created_by: str

class DataGenerator:
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
    
def generate_unique_ssn(self):
    """Generate unique SSN"""
    while True:
        area = random.randint(1, 899)
        if area == 666 or 900 <= area <= 999:
            continue
        group = random.randint(1, 99)
        serial = random.randint(1, 9999)
        ssn = f"{area:03d}{group:02d}{serial:04d}"
        if ssn not in self.used_ssns:
            self.used_ssns.add(ssn)
            return ssn

def generate_unique_license_plate(self, state='WA'):
    """Generate unique license plate"""
    while True:
        if state == 'WA':
            # Washington format: ABC1234 or 123ABC
            if random.random() < 0.7:
                plate = f"{fake.random_letter().upper()}{fake.random_letter().upper()}{fake.random_letter().upper()}{random.randint(1000, 9999)}"
            else:
                plate = f"{random.randint(100, 999)}{fake.random_letter().upper()}{fake.random_letter().upper()}{fake.random_letter().upper()}"
        else:
            plate = f"{fake.license_plate()}"
        
        if plate not in self.used_license_plates:
            self.used_license_plates.add(plate)
            return plate

def generate_unique_vin(self):
    """Generate unique VIN"""
    while True:
        vin = fake.vin()
        if vin not in self.used_vins:
            self.used_vins.add(vin)
            return vin

def select_location_in_jurisdiction(self, agency_key):
    """Select realistic location within agency jurisdiction"""
    agency = AGENCIES[agency_key]
    
    if agency_key == 'KCSO':
        # King County SO covers multiple jurisdictions
        cities = ['UNINCORPORATED_KING_COUNTY', 'SEATTLE'] + agency['jurisdiction_cities']
        weights = [30, 20] + [5] * len(agency['jurisdiction_cities'])
        city = random.choices(cities, weights=weights)[0]
    elif agency_key == 'BELLEVUE_PD':
        city = 'BELLEVUE'
    elif agency_key == 'SEATTLE_FD':
        city = 'SEATTLE'
    else:
        city = 'SEATTLE'  # Default
    
    # Get city data or use Seattle as fallback
    city_key = city if city in KING_COUNTY_GEOGRAPHY else 'SEATTLE'
    city_data = KING_COUNTY_GEOGRAPHY[city_key]
    
    # Add realistic scatter around city center
    center_lat, center_lng = city_data['coordinates']
    scatter_radius = random.uniform(0.01, 0.15)  # ~1-10 mile radius
    angle = random.uniform(0, 2 * math.pi)
    
    lat_offset = scatter_radius * math.cos(angle)
    lng_offset = scatter_radius * math.sin(angle)
    
    latitude = center_lat + lat_offset
    longitude = center_lng + lng_offset
    
    # Select random address components
    street_address = fake.street_address()
    zipcode = random.choice(city_data['zipcodes'])
    beat = random.choice(city_data['beats'])
    
    return {
        'street': street_address,
        'city': city_key,
        'state': 'WA',
        'zip': zipcode,
        'latitude': round(latitude, 6),
        'longitude': round(longitude, 6),
        'beat': beat
    }

def generate_person(self, agency_context=None):
    """Generate comprehensive person record"""
    sex = random.choice(['Male', 'Female'])
    first_name = fake.first_name_male() if sex == 'Male' else fake.first_name_female()
    last_name = fake.last_name()
    middle_name = fake.first_name() if random.random() < 0.7 else ""
    
    # Age distribution favoring adult population but including juveniles
    if random.random() < 0.05:
        age = random.randint(16, 17)  # Juveniles
    else:
        age = random.randint(18, 85)  # Adults
        
    dob = fake.date_of_birth(minimum_age=age, maximum_age=age)
    
    # Location in King County area
    location = self.select_location_in_jurisdiction('KCSO')  # Default to KCSO jurisdiction
    
    # Generate complex identifiers
    person_id = str(uuid.uuid4())
    ssn = self.generate_unique_ssn()
    
    # Criminal history scoring (0-10, most people are 0-2)
    criminal_score = random.choices(
        range(11), 
        weights=[40, 25, 15, 8, 5, 3, 2, 1, 0.5, 0.3, 0.2]
    )[0]
    
    # Mental health and medical flags (realistic prevalence)
    mental_health_flags = []
    if random.random() < 0.15:  # 15% have mental health considerations
        flags = ['DEPRESSION', 'ANXIETY', 'BIPOLAR', 'PTSD', 'SCHIZOPHRENIA', 'SUBSTANCE_ABUSE']
        mental_health_flags = random.sample(flags, random.randint(1, 2))
    
    medical_conditions = []
    medications = []
    if random.random() < 0.25:  # 25% have medical conditions
        conditions = ['DIABETES', 'HYPERTENSION', 'ASTHMA', 'HEART_DISEASE', 'EPILEPSY']
        medical_conditions = random.sample(conditions, random.randint(1, 2))
        # Generate corresponding medications
        if 'DIABETES' in medical_conditions:
            medications.append('INSULIN')
        if 'HYPERTENSION' in medical_conditions:
            medications.append('LISINOPRIL')
        if 'ASTHMA' in medical_conditions:
            medications.append('ALBUTEROL')

    # Socioeconomic and risk factors
    housing_status = random.choices(
        ['STABLE_HOUSING', 'UNSTABLE_HOUSING', 'HOMELESS'],
        weights=[85, 12, 3]
    )[0]
    
    employment_status = random.choices(
        ['EMPLOYED_FULL_TIME', 'EMPLOYED_PART_TIME', 'UNEMPLOYED', 'STUDENT', 'RETIRED'],
        weights=[50, 15, 20, 5, 10] if age > 17 else [0, 20, 10, 70, 0]
    )[0]
    
    education_level = random.choices(
        ['LESS_THAN_HIGH_SCHOOL', 'HIGH_SCHOOL_DIPLOMA', 'SOME_COLLEGE', 'ASSOCIATES_DEGREE', 'BACHELORS_DEGREE', 'GRADUATE_DEGREE'],
        weights=[10, 35, 25, 10, 15, 5]
    )[0]

    veteran_status = random.random() < 0.07 # ~7% of WA population are veterans

    primary_language = random.choices(
        ['English', 'Spanish', 'Chinese', 'Vietnamese', 'Somali', 'Other'],
        weights=[80, 8, 4, 3, 1, 4]
    )[0]

    # Risk level based on criminal history
    if criminal_score == 0:
        risk_level = 'LOW'
    elif criminal_score <= 4:
        risk_level = 'MEDIUM'
    else:
        risk_level = 'HIGH'

    probation_status = 'NONE'
    if criminal_score > 0 and random.random() < 0.3: # 30% chance if they have a record
        probation_status = random.choices(['ACTIVE', 'INACTIVE'], weights=[70, 30])[0]

    substance_abuse_flags = []
    if random.random() < (0.10 + criminal_score * 0.05): # Higher chance with higher score
        flags = ['ALCOHOL', 'OPIOIDS', 'CANNABIS', 'METHAMPHETAMINE', 'COCAINE']
        substance_abuse_flags = random.sample(flags, random.randint(1, 2))
    
    # Generate known associates (person IDs will be filled in later)
    num_associates = random.choices([0, 1, 2, 3, 4, 5], weights=[60, 20, 10, 5, 3, 2])[0]
    
    person = Person(
        person_id=person_id,
        ssn=ssn,
        first_name=first_name.upper(),
        middle_name=middle_name.upper(),
        last_name=last_name.upper(),
        name_suffix=random.choice(['', 'JR', 'SR', 'III']) if random.random() < 0.08 else '',
        dob=dob.strftime('%Y-%m-%d'),
        age=age,
        sex=sex,
        race=random.choices(
            ['White', 'Black', 'Hispanic', 'Asian', 'American Indian', 'Pacific Islander', 'Other'],
            weights=[55, 20, 15, 8, 1, 0.5, 0.5]
        )[0],
        ethnicity=random.choice(['Hispanic', 'Non-Hispanic']) if random.random() < 0.2 else 'Non-Hispanic',
        height_cm=random.randint(150, 200),
        weight_lbs=random.randint(100, 350),
        eye_color=random.choice(['Brown', 'Blue', 'Green', 'Hazel', 'Gray', 'Black']),
        hair_color=random.choice(['Black', 'Brown', 'Blonde', 'Red', 'Gray', 'White']),
        build=random.choice(['Slender', 'Medium', 'Heavy', 'Stocky', 'Athletic']),
        complexion=random.choice(['Fair', 'Medium', 'Dark', 'Light', 'Olive']),
        facial_hair=random.choice(['Mustache', 'Beard', 'Goatee', 'Clean Shaven']) if sex == 'Male' else 'N/A',
        glasses=random.choice(['Yes', 'No']) if random.random() < 0.3 else 'No',
        phone_primary=self.generate_phone_number('WA'),
        phone_secondary=self.generate_phone_number('WA') if random.random() < 0.4 else '',
        email=f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])}",
        address_street=location['street'],
        address_city=location['city'],
        address_state=location['state'],
        address_zip=location['zip'],
        latitude=location['latitude'],
        longitude=location['longitude'],
        dl_number=self.generate_dl_number('WA'),
        dl_state='WA',
        dl_status=random.choices(
            ['VALID', 'EXPIRED', 'SUSPENDED', 'REVOKED'],
            weights=[75, 15, 8, 2]
        )[0],
        state_id=f"WA{random.randint(1000000, 9999999)}",
        fbi_id=f"{random.randint(1, 9)}{fake.random_letter().upper()}{fake.random_letter().upper()}{random.randint(100000, 999999)}",
        employer=fake.company().upper() if random.random() < 0.6 else '',
        occupation=fake.job().upper() if random.random() < 0.6 else 'UNEMPLOYED',
        marital_status=random.choices(
            ['Single', 'Married', 'Divorced', 'Widowed', 'Separated'],
            weights=[45, 35, 15, 3, 2]
        )[0],
        emergency_contact_name=f"{fake.first_name().upper()} {fake.last_name().upper()}",
        emergency_contact_phone=self.generate_phone_number('WA'),
        aliases=self.generate_aliases(first_name, last_name),
        gang_affiliation=random.choice([
            'EASTSIDE CREW', 'WESTSIDE GANG', 'NORTHSIDE KINGS', 'SOUTHSIDE RIDERS',
            'RED DRAGONS', 'BLUE DEVILS', 'GREEN MACHINE', 'PURPLE KNIGHTS'
        ]) if random.random() < 0.08 else '',
        known_associates=[],  # Will be populated later
        criminal_history_score=criminal_score,
        mental_health_flags=mental_health_flags,
        medical_conditions=medical_conditions,
        medications=medications,
        tattoos_scars_marks=self.generate_ncic_marks(),
        fingerprint_classification=f"CLASS_{random.randint(1, 10)}_{fake.random_letter().upper()}{fake.random_letter().upper()}",
        dna_profile_id=f"DNA{random.randint(100000, 999999)}" if random.random() < 0.3 else '',
        photos=[f"PHOTO_{person_id}_{i}" for i in range(random.randint(1, 4))],
        created_date=fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        updated_date=fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        created_by_agency=agency_context or random.choice(['KCSO', 'BELLEVUE_PD']),
        housing_status=housing_status,
        employment_status=employment_status,
        education_level=education_level,
        veteran_status=veteran_status,
        primary_language=primary_language,
        risk_level=risk_level,
        probation_status=probation_status,
        substance_abuse_flags=substance_abuse_flags
    )
    
    return person

def generate_phone_number(self, state='WA'):
    """Generate realistic phone number"""
    if state == 'WA':
        area_codes = [206, 253, 360, 425, 509, 564]
        area_code = random.choice(area_codes)
    else:
        area_code = random.randint(200, 999)
    
    exchange = random.randint(200, 999)
    number = random.randint(0, 9999)
    return f"{area_code:03d}{exchange:03d}{number:04d}"

def generate_dl_number(self, state='WA'):
    """Generate driver's license number"""
    if state == 'WA':
        return f"WDL{random.randint(1000000, 9999999)}{fake.random_letter().upper()}{fake.random_letter().upper()}"
    else:
        return f"{fake.random_letter().upper()}{random.randint(100000000, 999999999)}"

def generate_aliases(self, first_name, last_name):
    """Generate realistic aliases"""
    aliases = []
    num_aliases = random.choices([0, 1, 2, 3], weights=[70, 20, 8, 2])[0]
    
    monikers = [
        'BIG MIKE', 'LITTLE TONY', 'SLIM', 'TANK', 'SPIDER', 'RED', 'LEFTY', 'SHORTY',
        'THE KID', 'DOC', 'DIESEL', 'FLASH', 'GHOST', 'HAWK', 'ICE', 'JOKER'
    ]
    
    for _ in range(num_aliases):
        if random.random() < 0.6:
            # Moniker/street name
            alias = random.choice(monikers)
        else:
            # Name variation
            alias_first = fake.first_name() if random.random() < 0.4 else first_name
            alias_last = fake.last_name() if random.random() < 0.3 else last_name
            alias = f"{alias_first.upper()} {alias_last.upper()}"
        
        if alias not in aliases:
            aliases.append(alias)
    
    return aliases

def generate_ncic_marks(self):
    """Generate NCIC-compliant marks"""
    marks = []
    body_locations = [
        'ARM', 'BACK', 'CHEST', 'FACE', 'HAND', 'LEG', 'NECK', 'SHLD', 'THGH', 'WRIST',
        'L ARM', 'R ARM', 'L LEG', 'R LEG', 'L HND', 'R HND'
    ]
    mark_types = [('TAT', 60), ('SC', 30), ('MOLE', 10)]
    
    num_marks = random.choices([0, 1, 2, 3], weights=[70, 20, 8, 2])[0]
    
    for _ in range(num_marks):
        mark_type = random.choices(
            [mt[0] for mt in mark_types],
            weights=[mt[1] for mt in mark_types]
        )[0]
        location = random.choice(body_locations)
        marks.append(f"{mark_type} {location}")
    
    return '; '.join(marks)

def generate_vehicle(self, owner_person_id=None):
    """Generate comprehensive vehicle record"""
    makes_models = {
        'FORD': ['F-150', 'MUSTANG', 'EXPLORER', 'ESCAPE', 'FUSION'],
        'CHEVROLET': ['SILVERADO', 'CAMARO', 'EQUINOX', 'MALIBU', 'TAHOE'],
        'TOYOTA': ['CAMRY', 'COROLLA', 'PRIUS', 'RAV4', 'TACOMA'],
        'HONDA': ['CIVIC', 'ACCORD', 'CRV', 'PILOT', 'FIT'],
        'NISSAN': ['ALTIMA', 'SENTRA', 'ROGUE', 'MAXIMA', 'PATHFINDER'],
        'BMW': ['325I', '528I', 'X3', 'X5', '740I'],
        'MERCEDES-BENZ': ['C300', 'E350', 'GLE350', 'S550', 'CLA250']
    }
    
    make = random.choice(list(makes_models.keys()))
    model = random.choice(makes_models[make])
    year = random.randint(1990, 2024)
    
    colors = ['WHITE', 'BLACK', 'GRAY', 'SILVER', 'RED', 'BLUE', 'GREEN', 'BROWN', 'GOLD', 'YELLOW']
    color = random.choice(colors)
    
    vehicle_types = ['SEDAN', 'SUV', 'TRUCK', 'COUPE', 'HATCHBACK', 'CONVERTIBLE', 'WAGON', 'VAN']
    vehicle_type = random.choice(vehicle_types)
    
    # Insurance status based on realistic patterns
    insurance_status = random.choices(
        ['ACTIVE', 'EXPIRED', 'CANCELLED', 'UNKNOWN'],
        weights=[75, 15, 8, 2]
    )[0]
    
    # Stolen status (very low rate)
    stolen_status = random.choices(
        ['CLEAR', 'STOLEN', 'RECOVERED'],
        weights=[99.5, 0.3, 0.2]
    )[0]
    
    vehicle = Vehicle(
        vehicle_id=str(uuid.uuid4()),
        vin=self.generate_unique_vin(),
        license_plate=self.generate_unique_license_plate('WA'),
        plate_state='WA',
        make=make,
        model=model,
        year=year,
        color=color,
        vehicle_type=vehicle_type,
        owner_person_id=owner_person_id or '',
        registered_owner_name=f"{fake.first_name().upper()} {fake.last_name().upper()}",
        registration_status=random.choices(
            ['CURRENT', 'EXPIRED', 'SUSPENDED'],
            weights=[80, 15, 5]
        )[0],
        insurance_company=random.choice([
            'STATE FARM', 'GEICO', 'PROGRESSIVE', 'ALLSTATE', 'FARMERS',
            'USAA', 'LIBERTY MUTUAL', 'NATIONWIDE', 'AMERICAN FAMILY'
        ]) if insurance_status == 'ACTIVE' else '',
        insurance_policy=f"POL{random.randint(1000000, 9999999)}" if insurance_status == 'ACTIVE' else '',
        insurance_status=insurance_status,
        stolen_status=stolen_status,
        impound_status='CLEAR' if random.random() < 0.95 else 'IMPOUNDED',
        towed_date=fake.date_time_between(start_date='-2y', end_date='now').strftime('%Y-%m-%d %H:%M:%S') if random.random() < 0.1 else '',
        impound_lot=random.choice(['LINCOLN TOWING', 'FOSS TOWING', 'SEATTLE IMPOUND']) if random.random() < 0.1 else '',
        ncic_entry_date=fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S') if stolen_status != 'CLEAR' else '',
        created_date=fake.date_time_between(start_date='-10y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        agency_entered=random.choice(['KCSO', 'BELLEVUE_PD'])
    )
    
    return vehicle

def generate_police_incident(self, agency='KCSO'):
    """Generate comprehensive police incident"""
    # Select incident type based on agency and weights
    incident_types = list(INCIDENT_TYPES.keys())
    weights = [INCIDENT_TYPES[it]['weight'] for it in incident_types]
    
    # Filter by agency capability
    available_types = [it for it in incident_types if agency in INCIDENT_TYPES[it]['agencies']]
    if available_types:
        incident_type = random.choice(available_types)
    else:
        incident_type = random.choice(incident_types)
    
    # Generate timing - incidents cluster around certain times
    base_date = fake.date_time_between(start_date='-2y', end_date='now')
    
    # Adjust for realistic incident timing patterns
    if incident_type in ['DUI', 'DOMESTIC_VIOLENCE']:
        # More common on weekends and evenings
        if base_date.weekday() < 5:  # Weekday
            base_date = base_date.replace(hour=random.randint(18, 23))
        else:  # Weekend
            base_date = base_date.replace(hour=random.randint(20, 2))
    
    occurred_datetime = base_date
    reported_datetime = occurred_datetime + timedelta(minutes=random.randint(-30, 240))
    dispatch_datetime = reported_datetime + timedelta(minutes=random.randint(1, 15))
    arrived_datetime = dispatch_datetime + timedelta(minutes=random.randint(3, 45))
    cleared_datetime = arrived_datetime + timedelta(minutes=random.randint(15, 480))
    
    # Location within agency jurisdiction
    location = self.select_location_in_jurisdiction(agency)
    
    # Generate case numbers and identifiers
    incident_number = f"{agency[:4]}{base_date.year}{random.randint(100000, 999999)}"
    case_number = f"{incident_number}-{random.randint(1, 99):02d}"
    call_number = f"C{base_date.year}{random.randint(1000000, 9999999)}"
    
    # Officer assignments
    reporting_officer = f"{random.randint(1000, 9999)}, {fake.last_name().upper()}"
    supervisor_officer = f"{random.randint(1000, 9999)}, {fake.last_name().upper()}" if random.random() < 0.7 else ''
    
    # Assisting officers based on incident severity
    num_assisting = 0
    if incident_type in ['ROBBERY', 'ASSAULT', 'DOMESTIC_VIOLENCE']:
        num_assisting = random.randint(1, 3)
    elif incident_type in ['BURGLARY', 'THEFT']:
        num_assisting = random.randint(0, 2)
    
    assisting_officers = [
        f"{random.randint(1000, 9999)}, {fake.last_name().upper()}"
        for _ in range(num_assisting)
    ]
    
    # Generate narrative
    narratives = {
        'TRAFFIC_VIOLATION': "OFFICER CONDUCTED TRAFFIC STOP FOR SPEEDING VIOLATION. DRIVER CITED AND RELEASED.",
        'DOMESTIC_VIOLENCE': "RESPONDED TO DOMESTIC DISPUTE. INVESTIGATION REVEALED PROBABLE CAUSE FOR DOMESTIC VIOLENCE ASSAULT. SUSPECT ARRESTED.",
        'THEFT': "VICTIM REPORTS THEFT OF PERSONAL PROPERTY FROM VEHICLE. NO SUSPECTS AT THIS TIME. REPORT TAKEN FOR INSURANCE PURPOSES.",
        'DUI': "OFFICER OBSERVED VEHICLE WEAVING IN TRAFFIC. FIELD SOBRIETY TESTS ADMINISTERED. DRIVER ARRESTED FOR DUI.",
        'BURGLARY': "VICTIM RETURNED HOME TO FIND RESIDENCE BURGLARIZED. MULTIPLE ITEMS MISSING. EVIDENCE COLLECTED."
    }
    
    narrative = narratives.get(incident_type, "OFFICER RESPONDED TO CALL FOR SERVICE. INVESTIGATION CONDUCTED.")
    
    # Status and clearance
    clearance_types = {
        'TRAFFIC_VIOLATION': 'CITATION',
        'DUI': 'ARREST',
        'DOMESTIC_VIOLENCE': 'ARREST',
        'THEFT': 'REPORT_ONLY',
        'BURGLARY': 'REPORT_ONLY'
    }
    
    incident = PoliceIncident(
        incident_id=str(uuid.uuid4()),
        incident_number=incident_number,
        case_number=case_number,
        call_number=call_number,
        incident_type=incident_type,
        incident_subtype=self.generate_incident_subtype(incident_type),
        ucr_code=self.generate_ucr_code(incident_type),
        nibrs_code=self.generate_nibrs_code(incident_type),
        location_address=location['street'],
        location_city=location['city'],
        location_beat=location['beat'],
        location_precinct=random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST']),
        latitude=location['latitude'],
        longitude=location['longitude'],
        occurred_datetime=occurred_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        reported_datetime=reported_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        dispatch_datetime=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        arrived_datetime=arrived_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        cleared_datetime=cleared_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        reporting_officer=reporting_officer,
        supervisor_officer=supervisor_officer,
        assisting_officers=assisting_officers,
        persons_involved=[],  # Will be populated with person IDs
        vehicles_involved=[],  # Will be populated with vehicle IDs
        property_involved=[],  # Will be populated with property IDs
        narrative=narrative,
        status=random.choices(['ACTIVE', 'CLEARED', 'SUSPENDED'], weights=[10, 85, 5])[0],
        clearance_type=clearance_types.get(incident_type, 'REPORT_ONLY'),
        modus_operandi=self.generate_modus_operandi(incident_type),
        domestic_violence=incident_type == 'DOMESTIC_VIOLENCE',
        gang_related=random.random() < 0.05,
        hate_crime=random.random() < 0.01,
        drug_related=incident_type == 'DRUG_POSSESSION' or random.random() < 0.1,
        alcohol_related=incident_type == 'DUI' or random.random() < 0.15,
        weapon_used=self.generate_weapon_used(incident_type),
        injury_severity=self.generate_injury_severity(incident_type),
        agency=agency,
        created_date=dispatch_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        updated_date=cleared_datetime.strftime('%Y-%m-%d %H:%M:%S')
    )
    
    return incident

def generate_incident_subtype(self, incident_type):
    """Generate realistic incident subtypes"""
    subtypes = {
        'TRAFFIC_VIOLATION': ['SPEEDING', 'RECKLESS_DRIVING', 'EQUIPMENT_VIOLATION', 'LICENSE_VIOLATION'],
        'DOMESTIC_VIOLENCE': ['PHYSICAL_ASSAULT', 'THREATS', 'HARASSMENT', 'VIOLATION_OF_ORDER'],
        'THEFT': ['SHOPLIFTING', 'THEFT_FROM_VEHICLE', 'BICYCLE_THEFT', 'PURSE_SNATCHING'],
        'ASSAULT': ['SIMPLE_ASSAULT', 'ASSAULT_2ND', 'ASSAULT_3RD', 'ASSAULT_4TH'],
        'BURGLARY': ['RESIDENTIAL_BURGLARY', 'COMMERCIAL_BURGLARY', 'VEHICLE_PROWL']
    }
    
    return random.choice(subtypes.get(incident_type, ['MISCELLANEOUS']))

def generate_ucr_code(self, incident_type):
    """Generate UCR codes"""
    ucr_codes = {
        'TRAFFIC_VIOLATION': '90Z',
        'DOMESTIC_VIOLENCE': '13A',
        'THEFT': '23F',
        'ASSAULT': '13A',
        'BURGLARY': '220',
        'ROBBERY': '120',
        'DUI': '90D',
        'DRUG_POSSESSION': '35A'
    }
    
    return ucr_codes.get(incident_type, '90Z')

def generate_nibrs_code(self, incident_type):
    """Generate NIBRS codes"""
    nibrs_codes = {
        'DOMESTIC_VIOLENCE': '13A',
        'THEFT': '23F',
        'ASSAULT': '13A',
        'BURGLARY': '220',
        'ROBBERY': '120',
        'DUI': '90D',
        'DRUG_POSSESSION': '35A'
    }
    
    return nibrs_codes.get(incident_type, '90Z')

def generate_modus_operandi(self, incident_type):
    """Generate modus operandi codes"""
    mo_codes = {
        'BURGLARY': ['FORCED_ENTRY', 'UNLOCKED_DOOR', 'WINDOW_ENTRY', 'KEY_USED'],
        'THEFT': ['OPPORTUNITY', 'DISTRACTION', 'CONCEALMENT'],
        'ROBBERY': ['FORCE', 'INTIMIDATION', 'WEAPON_DISPLAYED'],
        'ASSAULT': ['HANDS_FISTS', 'WEAPON', 'DOMESTIC_RELATED']
    }
    
    return random.sample(mo_codes.get(incident_type, ['UNKNOWN']), 
                        random.randint(1, len(mo_codes.get(incident_type, ['UNKNOWN']))))

def generate_weapon_used(self, incident_type):
    """Generate weapon information"""
    if incident_type in ['ASSAULT', 'ROBBERY']:
        weapons = ['HANDS_FISTS', 'KNIFE', 'HANDGUN', 'CLUB', 'OTHER_WEAPON', 'NONE']
        weights = [40, 20, 15, 10, 10, 5]
        return random.choices(weapons, weights=weights)[0]
    elif incident_type == 'DUI':
        return 'VEHICLE'
    else:
        return 'NONE'

def generate_injury_severity(self, incident_type):
    """Generate injury severity"""
    if incident_type in ['ASSAULT', 'DOMESTIC_VIOLENCE', 'ROBBERY']:
        severities = ['NONE', 'MINOR', 'MODERATE', 'SEVERE']
        weights = [60, 25, 12, 3]
        return random.choices(severities, weights=weights)[0]
    else:
        return 'NONE'