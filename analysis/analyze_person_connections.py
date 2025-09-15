#!/usr/bin/env python3
"""
Script to analyze person_id connections across all data classes.
This script reads all JSON files and counts how many data classes each person_id appears in.
"""

import json
import os
from collections import defaultdict, Counter
from typing import Dict, List, Set

def load_json_file(filepath: str) -> List[Dict]:
    """Load a JSON file and return the data as a list of dictionaries."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            # Handle both list and dict formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                return []
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load {filepath}: {e}")
        return []

def extract_person_ids_from_record(record: Dict, data_class: str) -> Set[str]:
    """Extract all person_id values from a single record."""
    person_ids = set()
    
    # Direct person_id field
    if 'person_id' in record and record['person_id']:
        person_ids.add(record['person_id'])
    
    # Fields that contain person_id in their name
    for key, value in record.items():
        if 'person_id' in key.lower() and value:
            if isinstance(value, str):
                person_ids.add(value)
            elif isinstance(value, list):
                person_ids.update([pid for pid in value if pid])
    
    # Special handling for specific data classes
    if data_class == 'JailProgram':
        # Handle enrolled_person_ids and waitlist_person_ids
        if 'enrolled_person_ids' in record and record['enrolled_person_ids']:
            person_ids.update(record['enrolled_person_ids'])
        if 'waitlist_person_ids' in record and record['waitlist_person_ids']:
            person_ids.update(record['waitlist_person_ids'])
    
    return person_ids

def analyze_person_connections():
    """Analyze connections between persons across all data classes."""
    
    # Define the data classes and their corresponding JSON files
    data_classes = {
        'Person': '../data/json/persons.json',
        'JailBooking': '../data/json/jail_bookings.json',
        'JailSentence': '../data/json/jail_sentences.json',
        'JailIncident': '../data/json/jail_incidents.json',
        'BailBond': '../data/json/bail_bonds.json',
        'JailProgram': '../data/json/jail_programs.json',
        'JailLog': '../data/json/jail_logs.json',
        'Arrest': '../data/json/arrests.json',
        'Property': '../data/json/properties.json',
        'EMSIncident': '../data/json/ems_incidents.json',
        'FireIncident': '../data/json/fire_incidents.json',
        'PoliceIncident': '../data/json/police_incidents.json',
        'Vehicle': '../data/json/vehicles.json',
        'FirePersonnel': '../data/json/fire_personnel.json',
        'FireReport': '../data/json/fire_reports.json',
        'FireRMSIncident': '../data/json/fire_rms_incidents.json',
        'FireShift': '../data/json/fire_shifts.json',
    }
    
    # Track connections for each person_id
    person_connections = defaultdict(set)  # person_id -> set of data classes
    data_class_counts = defaultdict(int)   # data_class -> count of records
    person_id_counts = defaultdict(int)    # person_id -> total occurrences
    
    print("Analyzing person connections across data classes...")
    print("=" * 60)
    
    # Process each data class
    for data_class, filename in data_classes.items():
        filepath = filename
        
        if not os.path.exists(filepath):
            print(f"File not found: {filename}")
            continue
            
        print(f"Processing {data_class} ({filename})...")
        
        records = load_json_file(filepath)
        data_class_counts[data_class] = len(records)
        
        for record in records:
            person_ids = extract_person_ids_from_record(record, data_class)
            
            for person_id in person_ids:
                person_connections[person_id].add(data_class)
                person_id_counts[person_id] += 1
    
    # Generate statistics
    print("\n" + "=" * 60)
    print("CONNECTION ANALYSIS RESULTS")
    print("=" * 60)
    
    # Data class statistics
    print("\nData Class Statistics:")
    for data_class, count in sorted(data_class_counts.items()):
        print(f"  {data_class:20} : {count:>8,} records")
    
    # Person connection statistics
    connection_counts = Counter(len(classes) for classes in person_connections.values())
    
    print(f"\nPerson Connection Statistics:")
    print(f"  Total unique persons: {len(person_connections):,}")
    print(f"  Total person occurrences: {sum(person_id_counts.values()):,}")
    
    print(f"\nConnections per person:")
    for num_connections in sorted(connection_counts.keys()):
        count = connection_counts[num_connections]
        percentage = (count / len(person_connections)) * 100
        print(f"  {num_connections:2d} data classes: {count:>6,} persons ({percentage:5.1f}%)")
    
    # Find most connected persons
    print(f"\nMost Connected Persons (top 10):")
    most_connected = sorted(person_connections.items(), 
                          key=lambda x: len(x[1]), reverse=True)[:10]
    
    for i, (person_id, classes) in enumerate(most_connected, 1):
        total_occurrences = person_id_counts[person_id]
        print(f"  {i:2d}. {person_id} : {len(classes):2d} classes, {total_occurrences:3d} total occurrences")
        print(f"      Classes: {', '.join(sorted(classes))}")
    
    # Find persons with specific connection patterns
    print(f"\nConnection Pattern Analysis:")
    
    # Persons in jail system
    jail_persons = {pid for pid, classes in person_connections.items() 
                   if any(cls in classes for cls in ['JailBooking', 'JailSentence', 'JailIncident', 'BailBond', 'JailLog'])}
    print(f"  Persons in jail system: {len(jail_persons):,}")
    
    # Persons with arrests
    arrested_persons = {pid for pid, classes in person_connections.items() if 'Arrest' in classes}
    print(f"  Persons with arrests: {len(arrested_persons):,}")
    
    # Persons with EMS incidents
    ems_persons = {pid for pid, classes in person_connections.items() if 'EMSIncident' in classes}
    print(f"  Persons with EMS incidents: {len(ems_persons):,}")
    
    # Persons with fire incidents
    fire_persons = {pid for pid, classes in person_connections.items() if 'FireIncident' in classes}
    print(f"  Persons with fire incidents: {len(fire_persons):,}")
    
    # Persons with police incidents
    police_persons = {pid for pid, classes in person_connections.items() if 'PoliceIncident' in classes}
    print(f"  Persons with police incidents: {len(police_persons):,}")
    
    # Cross-agency connections
    cross_agency = {pid for pid, classes in person_connections.items() 
                   if len(classes) >= 2 and any(cls in classes for cls in ['JailBooking', 'Arrest']) 
                   and any(cls in classes for cls in ['EMSIncident', 'FireIncident', 'PoliceIncident'])}
    print(f"  Persons with cross-agency connections: {len(cross_agency):,}")
    
    # Save detailed results to file
    results = {
        'summary': {
            'total_unique_persons': len(person_connections),
            'total_person_occurrences': sum(person_id_counts.values()),
            'data_class_counts': dict(data_class_counts),
            'connection_distribution': dict(connection_counts),
        },
        'person_connections': {
            person_id: {
                'data_classes': list(classes),
                'num_connections': len(classes),
                'total_occurrences': person_id_counts[person_id]
            }
            for person_id, classes in person_connections.items()
        }
    }
    
    with open('person_connection_analysis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDetailed results saved to: person_connection_analysis.json")
    
    return person_connections, data_class_counts, person_id_counts

if __name__ == "__main__":
    analyze_person_connections()
