# EMS Data Generator

A Python-based synthetic data generation system for Emergency Medical Services (EMS), Law Enforcement (LE), Fire, and Corrections data, designed to create realistic but anonymized datasets for testing, development, and analysis.

## Quick Start

```bash
# Generate sample EMS data
python ems_data_generator.py

# Output files created in data/json/:
# - ems_incidents.json
# - ems_patients.json  
# - ems_medications.json
# - ems_reports.json
```

## Data Privacy

- **No Real Data**: All distributions are synthetic, no raw sensitive values
- **Anonymized**: Uses synthetic addresses and statistical patterns only
- **Real Terminology**: Preserves medical accuracy with real medication/condition names
- **Safe for Testing**: Suitable for development and analysis environments

## Generated Data Structure

- **Incidents**: EMS incidents with realistic incident types and timing
- **Patients**: Demographics, medical history, allergies, and current medications
- **Medications**: Real medication names with synthetic dosage and administration data
- **Reports**: Complete EMS run reports with patient assessments and treatments

## Requirements

- Python 3.7+
- pandas, numpy, faker
- Standard library modules (json, random, datetime, etc.)

*This system generates synthetic data for legitimate testing and development purposes only.*
