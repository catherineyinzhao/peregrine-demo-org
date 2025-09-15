#!/usr/bin/env python3
"""
Analyze and identify orphaned data class entities that don't connect to any person.
"""

import json
from collections import defaultdict, Counter

def load_json_file(filepath: str) -> list:
    """Load a JSON file and return the data as a list."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def analyze_orphaned_entities():
    """Analyze and identify orphaned entities that don't connect to any person."""
    
    print("ANALYZING ORPHANED ENTITIES")
    print("=" * 60)
    
    # Define data files and their person_id field mappings
    data_files = {
        'Arrest': {
            'file': '../data/json/arrests.json',
            'person_field': 'person_id'
        },
        'Vehicle': {
            'file': '../data/json/vehicles.json',
            'person_field': 'owner_person_id'
        },
        'JailBooking': {
            'file': '../data/json/jail_bookings.json',
            'person_field': 'person_id'
        },
        'JailSentence': {
            'file': '../data/json/jail_sentences.json',
            'person_field': 'person_id'
        },
        'JailIncident': {
            'file': '../data/json/jail_incidents.json',
            'person_field': 'person_id'
        },
        'BailBond': {
            'file': '../data/json/bail_bonds.json',
            'person_field': 'person_id'
        },
        'JailLog': {
            'file': '../data/json/jail_logs.json',
            'person_field': 'person_id'
        },
        'JailProgram': {
            'file': '../data/json/jail_programs.json',
            'person_field': 'enrolled_person_ids'  # This is a list
        },
        'Property': {
            'file': '../data/json/properties.json',
            'person_field': 'owner_person_id'
        },
        'EMSIncident': {
            'file': '../data/json/ems_incidents.json',
            'person_field': 'patient_person_id'
        },
        'FireIncident': {
            'file': '../data/json/fire_incidents.json',
            'person_field': 'caller_person_id'
        },
        'PoliceIncident': {
            'file': '../data/json/police_incidents.json',
            'person_field': 'caller_person_id'
        },
        'Case': {
            'file': '../data/json/cases.json',
            'person_field': 'jacket_id'
        },
        'FirePersonnel': {
            'file': '../data/json/fire_personnel.json',
            'person_field': None  # This doesn't link to Person entities
        },
        'FireReport': {
            'file': '../data/json/fire_reports.json',
            'person_field': None  # This links to FirePersonnel, not Person
        },
        'FireRMSIncident': {
            'file': '../data/json/fire_rms_incidents.json',
            'person_field': None  # This doesn't link to Person entities
        },
        'FireShift': {
            'file': '../data/json/fire_shifts.json',
            'person_field': None  # This doesn't link to Person entities
        },
    }
    
    # Load all person IDs
    print("Loading person data...")
    persons = load_json_file('../data/json/persons.json')
    person_ids = set(person.get('person_id') for person in persons if person.get('person_id'))
    print(f"Total persons loaded: {len(person_ids):,}")
    
    # Analyze each data class
    orphaned_analysis = {}
    total_orphaned = 0
    
    for data_class, config in data_files.items():
        print(f"\nAnalyzing {data_class}...")
        
        if config['person_field'] is None:
            print(f"  Skipping {data_class} - no person field mapping")
            continue
            
        records = load_json_file(config['file'])
        print(f"  Total records: {len(records):,}")
        
        orphaned_count = 0
        orphaned_records = []
        connected_count = 0
        
        for i, record in enumerate(records):
            is_connected = False
            
            if config['person_field'] == 'enrolled_person_ids':
                # Special handling for JailProgram
                enrolled_ids = record.get('enrolled_person_ids', [])
                if enrolled_ids:
                    for person_id in enrolled_ids:
                        if person_id in person_ids:
                            is_connected = True
                            break
            else:
                # Standard person field
                person_id = record.get(config['person_field'])
                if person_id and person_id in person_ids:
                    is_connected = True
            
            if is_connected:
                connected_count += 1
            else:
                orphaned_count += 1
                if len(orphaned_records) < 5:  # Keep first 5 examples
                    orphaned_records.append({
                        'index': i,
                        'record_id': record.get('id', f"record_{i}"),
                        'person_field_value': record.get(config['person_field']),
                        'sample_data': {k: v for k, v in list(record.items())[:3]}  # First 3 fields
                    })
        
        orphaned_analysis[data_class] = {
            'total_records': len(records),
            'connected_records': connected_count,
            'orphaned_records': orphaned_count,
            'orphaned_percentage': (orphaned_count / len(records)) * 100 if records else 0,
            'person_field': config['person_field'],
            'sample_orphaned': orphaned_records
        }
        
        total_orphaned += orphaned_count
        
        print(f"  Connected: {connected_count:,} ({connected_count/len(records)*100:.1f}%)")
        print(f"  Orphaned: {orphaned_count:,} ({orphaned_count/len(records)*100:.1f}%)")
    
    # Summary analysis
    print(f"\n" + "=" * 60)
    print("ORPHANED ENTITIES SUMMARY")
    print("=" * 60)
    
    print(f"Total orphaned records across all data classes: {total_orphaned:,}")
    
    # Sort by orphaned percentage
    sorted_analysis = sorted(orphaned_analysis.items(), 
                           key=lambda x: x[1]['orphaned_percentage'], reverse=True)
    
    print(f"\nData classes with orphaned records (sorted by percentage):")
    print(f"{'Data Class':<20} {'Total':<10} {'Orphaned':<10} {'%':<8} {'Person Field'}")
    print("-" * 70)
    
    for data_class, analysis in sorted_analysis:
        if analysis['orphaned_records'] > 0:
            print(f"{data_class:<20} {analysis['total_records']:<10,} {analysis['orphaned_records']:<10,} "
                  f"{analysis['orphaned_percentage']:<7.1f}% {analysis['person_field']}")
    
    # Detailed analysis of most problematic data classes
    print(f"\nDETAILED ANALYSIS OF ORPHANED RECORDS:")
    print("=" * 60)
    
    for data_class, analysis in sorted_analysis[:5]:  # Top 5 most problematic
        if analysis['orphaned_records'] > 0:
            print(f"\n{data_class} - {analysis['orphaned_records']:,} orphaned records:")
            print(f"Person field: {analysis['person_field']}")
            
            # Show sample orphaned records
            for sample in analysis['sample_orphaned']:
                print(f"  Record {sample['index']}: {sample['record_id']}")
                print(f"    Person field value: {sample['person_field_value']}")
                print(f"    Sample data: {sample['sample_data']}")
    
    # Identify potential issues
    print(f"\nPOTENTIAL DATA QUALITY ISSUES:")
    print("=" * 60)
    
    issues_found = []
    
    for data_class, analysis in orphaned_analysis.items():
        if analysis['orphaned_percentage'] > 10:  # More than 10% orphaned
            issues_found.append(f"{data_class}: {analysis['orphaned_percentage']:.1f}% orphaned")
        elif analysis['orphaned_percentage'] > 0:
            issues_found.append(f"{data_class}: {analysis['orphaned_percentage']:.1f}% orphaned (minor)")
    
    if issues_found:
        for issue in issues_found:
            print(f"  - {issue}")
    else:
        print("  No significant orphaned entity issues found!")
    
    # Check for invalid person IDs
    print(f"\nINVALID PERSON ID ANALYSIS:")
    print("=" * 60)
    
    invalid_person_ids = defaultdict(int)
    
    for data_class, config in data_files.items():
        if config['person_field'] is None:
            continue
            
        records = load_json_file(config['file'])
        
        for record in records:
            if config['person_field'] == 'enrolled_person_ids':
                enrolled_ids = record.get('enrolled_person_ids', [])
                for person_id in enrolled_ids:
                    if person_id and person_id not in person_ids:
                        invalid_person_ids[person_id] += 1
            else:
                person_id = record.get(config['person_field'])
                if person_id and person_id not in person_ids:
                    invalid_person_ids[person_id] += 1
    
    if invalid_person_ids:
        print(f"Found {len(invalid_person_ids):,} invalid person IDs:")
        for person_id, count in sorted(invalid_person_ids.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {person_id}: {count} references")
    else:
        print("No invalid person IDs found!")
    
    # Save detailed results
    results = {
        'summary': {
            'total_orphaned_records': total_orphaned,
            'data_classes_analyzed': len(orphaned_analysis),
            'data_classes_with_orphans': len([a for a in orphaned_analysis.values() if a['orphaned_records'] > 0])
        },
        'orphaned_analysis': orphaned_analysis,
        'invalid_person_ids': dict(invalid_person_ids)
    }
    
    with open('orphaned_entities_analysis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDetailed analysis saved to: orphaned_entities_analysis.json")
    
    return orphaned_analysis, invalid_person_ids

if __name__ == "__main__":
    analyze_orphaned_entities()
