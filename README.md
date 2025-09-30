# Public Safety Data Generator

A Python-based synthetic data generation system for Emergency Medical Services (EMS), Law Enforcement (LE), Fire, and Corrections data, designed to create realistic but anonymized datasets for testing, development, and analysis purposes.

## Overview

This system generates comprehensive public safety data across multiple domains:

### EMS (Emergency Medical Services)
- **EMS Incidents**: Realistic incident types, locations, and timing patterns
- **Patient Data**: Demographics, medical history, vital signs, and conditions
- **Medications**: Real medication names with synthetic dosage distributions
- **Treatments**: Medical procedures and interventions
- **EMS Reports**: Complete EMS run reports linking all entities

### Law Enforcement
- **Police Incidents**: Crime types, locations, and response patterns
- **Arrests**: Suspect demographics, charges, and booking information
- **Citations**: Traffic violations and enforcement data
- **Field Interviews**: Officer interactions and case documentation

### Fire Services
- **Fire Incidents**: Fire types, locations, and response data
- **Fire Personnel**: Staff assignments, certifications, and schedules
- **Fire Reports**: Incident documentation and investigation data
- **Fire Shifts**: Personnel scheduling and deployment patterns

### Corrections
- **Jail Bookings**: Inmate intake, demographics, and charges
- **Jail Incidents**: Facility incidents and security events
- **Jail Logs**: Daily operations and inmate management
- **Jail Programs**: Inmate rehabilitation and educational programs
- **Jail Sentences**: Sentencing data and release information

## Key Features

- **Synthetic Distributions**: Uses statistical distributions instead of raw data values
- **Real Terminology**: Preserves authentic medical, legal, and procedural terminology
- **Geographic Realism**: Generates Seattle-area addresses and zip codes
- **Temporal Patterns**: Realistic time-of-day, day-of-week, and seasonal variations
- **Cross-Domain Consistency**: Relationships between incidents, personnel, and outcomes
- **Scalable Generation**: Batch processing for large datasets across all domains

## Quick Start

```bash
# Generate sample EMS data
python ems_data_generator.py

# Generate other public safety data
python synthetic_data.py

# Output files created in data/json/:
# EMS Data:
# - ems_incidents.json
# - ems_patients.json  
# - ems_medications.json
# - ems_reports.json

# Law Enforcement Data:
# - police_incidents.json
# - arrests.json
# - citations.json
# - field_interviews.json

# Fire Services Data:
# - fire_incidents.json
# - fire_personnel.json
# - fire_reports.json
# - fire_shifts.json

# Corrections Data:
# - jail_bookings.json
# - jail_incidents.json
# - jail_logs.json
# - jail_programs.json
# - jail_sentences.json
```

## Data Privacy

- **No Real Data**: All distributions are synthetic, no raw sensitive values
- **Anonymized**: Uses synthetic addresses and statistical patterns only
- **Real Terminology**: Preserves accuracy with real medical, legal, and procedural terminology
- **Safe for Testing**: Suitable for development and analysis environments

## Generated Data Structure

### EMS Data
- **Incidents**: EMS incidents with realistic incident types and timing
- **Patients**: Demographics, medical history, allergies, and current medications
- **Medications**: Real medication names with synthetic dosage and administration data
- **Reports**: Complete EMS run reports with patient assessments and treatments

### Law Enforcement Data
- **Police Incidents**: Crime reports with realistic patterns and classifications
- **Arrests**: Booking data with demographics and charges
- **Citations**: Traffic enforcement with violation types and locations
- **Field Interviews**: Officer interactions and case documentation

### Fire Services Data
- **Fire Incidents**: Fire calls with response times and incident classifications
- **Fire Personnel**: Staff data with certifications and assignments
- **Fire Reports**: Incident documentation and investigation details
- **Fire Shifts**: Personnel scheduling and deployment patterns

### Corrections Data
- **Jail Bookings**: Inmate intake with demographics and charges
- **Jail Incidents**: Facility security events and inmate management
- **Jail Logs**: Daily operations and administrative records
- **Jail Programs**: Rehabilitation and educational program participation
- **Jail Sentences**: Sentencing data and release information

## Requirements

- Python 3.7+
- pandas, numpy, faker
- Standard library modules (json, random, datetime, etc.)

---

*This system generates synthetic data for legitimate testing, development, and research purposes only.*
