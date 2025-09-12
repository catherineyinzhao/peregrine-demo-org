#!/usr/bin/env python3
"""
Comprehensive script to check for empty/null parameters across all generated data files.
"""

import json
import os
from collections import defaultdict, Counter

def load_json_file(filepath: str) -> list:
    """Load a JSON file and return the data as a list."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading {filepath}: {e}")
        return []

def check_empty_null_values(data: list, filename: str) -> dict:
    """Check for empty/null values in a dataset."""
    if not data:
        return {}
    
    # Get all possible field names from the first record
    all_fields = set()
    for record in data:
        if isinstance(record, dict):
            all_fields.update(record.keys())
    
    # Count empty/null values for each field
    field_stats = {}
    total_records = len(data)
    
    for field in all_fields:
        empty_count = 0
        null_count = 0
        empty_string_count = 0
        
        for record in data:
            if isinstance(record, dict):
                value = record.get(field)
                if value is None:
                    null_count += 1
                elif value == "":
                    empty_string_count += 1
                elif isinstance(value, str) and value.strip() == "":
                    empty_count += 1
        
        total_empty = empty_count + null_count + empty_string_count
        if total_empty > 0:
            field_stats[field] = {
                'null_count': null_count,
                'empty_string_count': empty_string_count,
                'whitespace_only_count': empty_count,
                'total_empty': total_empty,
                'percentage': (total_empty / total_records) * 100
            }
    
    return field_stats

def main():
    """Main function to check all data files for empty/null parameters."""
    print("CHECKING FOR EMPTY/NULL PARAMETERS")
    print("=" * 60)
    
    # List of all JSON data files
    data_files = [
        'persons.json',
        'vehicles.json', 
        'properties.json',
        'police_incidents.json',
        'arrests.json',
        'cases.json',
        'jail_bookings.json',
        'jail_sentences.json',
        'jail_incidents.json',
        'bail_bonds.json',
        'jail_programs.json',
        'jail_logs.json',
        'corrections_facilities.json',
        'fire_incidents.json',
        'ems_incidents.json',
        'fire_rms_incidents.json',
        'fire_shifts.json',
        'fire_personnel.json',
        'fire_reports.json'
    ]
    
    overall_stats = {}
    files_with_issues = []
    
    for filename in data_files:
        if not os.path.exists(filename):
            print(f"⚠️  File not found: {filename}")
            continue
            
        print(f"\nAnalyzing {filename}...")
        data = load_json_file(filename)
        
        if not data:
            print(f"   No data found in {filename}")
            continue
            
        field_stats = check_empty_null_values(data, filename)
        
        if field_stats:
            files_with_issues.append(filename)
            overall_stats[filename] = field_stats
            
            print(f"   📊 Found {len(field_stats)} fields with empty/null values:")
            for field, stats in field_stats.items():
                print(f"      • {field}: {stats['total_empty']:,} empty ({stats['percentage']:.1f}%)")
                if stats['null_count'] > 0:
                    print(f"        - {stats['null_count']:,} null values")
                if stats['empty_string_count'] > 0:
                    print(f"        - {stats['empty_string_count']:,} empty strings")
                if stats['whitespace_only_count'] > 0:
                    print(f"        - {stats['whitespace_only_count']:,} whitespace-only strings")
        else:
            print(f"   ✅ No empty/null values found")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if files_with_issues:
        print(f"📋 Files with empty/null parameters: {len(files_with_issues)}")
        print("\nDetailed breakdown:")
        
        for filename in files_with_issues:
            print(f"\n🔍 {filename}:")
            field_stats = overall_stats[filename]
            
            # Sort by percentage of empty values
            sorted_fields = sorted(field_stats.items(), key=lambda x: x[1]['percentage'], reverse=True)
            
            for field, stats in sorted_fields:
                print(f"   • {field}: {stats['total_empty']:,} empty ({stats['percentage']:.1f}%)")
                
                # Show sample of empty records
                if stats['total_empty'] <= 10:
                    data = load_json_file(filename)
                    empty_samples = []
                    for record in data:
                        if isinstance(record, dict):
                            value = record.get(field)
                            if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                                empty_samples.append(record)
                                if len(empty_samples) >= 3:
                                    break
                    
                    if empty_samples:
                        print(f"     Sample empty records:")
                        for i, sample in enumerate(empty_samples[:3]):
                            # Show a few key fields from the sample
                            key_fields = list(sample.keys())[:3]
                            sample_info = {k: sample[k] for k in key_fields}
                            print(f"       {i+1}. {sample_info}")
    else:
        print("✅ No empty/null parameters found in any files!")
    
    # Save detailed report
    with open('empty_null_analysis.json', 'w') as f:
        json.dump(overall_stats, f, indent=2)
    print(f"\n📄 Detailed analysis saved to: empty_null_analysis.json")

if __name__ == "__main__":
    main()
