#!/usr/bin/env python3
"""
Simple summary of person connection analysis results.
"""

import json

def print_connection_summary():
    """Print a concise summary of person connections."""
    
    with open('person_connection_analysis.json', 'r') as f:
        results = json.load(f)
    
    summary = results['summary']
    person_connections = results['person_connections']
    
    print("🔗 PERSON CONNECTION ANALYSIS SUMMARY")
    print("=" * 50)
    
    print(f"\nOVERVIEW:")
    print(f"  • Total unique persons: {summary['total_unique_persons']:,}")
    print(f"  • Total person occurrences: {summary['total_person_occurrences']:,}")
    print(f"  • Average connections per person: {summary['total_person_occurrences']/summary['total_unique_persons']:.1f}")
    
    print(f"\nCONNECTION DISTRIBUTION:")
    connection_dist = summary['connection_distribution']
    for num_connections in sorted(connection_dist.keys()):
        count = connection_dist[num_connections]
        percentage = (count / summary['total_unique_persons']) * 100
        print(f"  • {int(num_connections):2d} data classes: {count:>6,} persons ({percentage:5.1f}%)")
    
    print(f"\nAGENCY BREAKDOWN:")
    data_class_counts = summary['data_class_counts']
    
    # Group by agency
    agencies = {
        'Jail System': ['JailBooking', 'JailSentence', 'JailIncident', 'BailBond', 'JailLog', 'JailProgram'],
        'Law Enforcement': ['Arrest', 'PoliceIncident'],
        'Emergency Services': ['EMSIncident', 'FireIncident', 'FireRMSIncident'],
        'Fire Department': ['FirePersonnel', 'FireReport', 'FireShift'],
        'Property/Assets': ['Property', 'Vehicle'],
        'Core': ['Person']
    }
    
    for agency, classes in agencies.items():
        total_records = sum(data_class_counts.get(cls, 0) for cls in classes)
        print(f"  • {agency:20}: {total_records:>8,} records")
    
    print(f"\nMOST CONNECTED PERSONS:")
    most_connected = sorted(person_connections.items(), 
                          key=lambda x: x[1]['num_connections'], reverse=True)[:5]
    
    for i, (person_id, data) in enumerate(most_connected, 1):
        print(f"  {i}. {person_id} - {data['num_connections']} classes, {data['total_occurrences']} occurrences")
        print(f"     Classes: {', '.join(sorted(data['data_classes']))}")
    
    print(f"\nKEY INSIGHTS:")
    
    # Calculate some key metrics
    single_connection = connection_dist.get(1, 0)
    multi_connection = summary['total_unique_persons'] - single_connection
    
    print(f"  • {single_connection:,} persons ({single_connection/summary['total_unique_persons']*100:.1f}%) appear in only 1 data class")
    print(f"  • {multi_connection:,} persons ({multi_connection/summary['total_unique_persons']*100:.1f}%) appear in multiple data classes")
    
    # Find persons with maximum connections
    max_connections = max(data['num_connections'] for data in person_connections.values())
    max_connected_count = sum(1 for data in person_connections.values() 
                            if data['num_connections'] == max_connections)
    
    print(f"  • Maximum connections: {max_connections} data classes")
    print(f"  • {max_connected_count} persons have the maximum number of connections")
    
    # Most common connection pattern
    pattern_counts = {}
    for data in person_connections.values():
        pattern = tuple(sorted(data['data_classes']))
        pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
    
    most_common_pattern = max(pattern_counts.items(), key=lambda x: x[1])
    print(f"  • Most common pattern: {', '.join(most_common_pattern[0])} ({most_common_pattern[1]:,} persons)")

if __name__ == "__main__":
    print_connection_summary()
