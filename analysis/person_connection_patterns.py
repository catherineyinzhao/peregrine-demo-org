#!/usr/bin/env python3
"""
Analyze and map out the most common types of connections formed from Person models.
"""

import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import networkx as nx
from itertools import combinations

def load_json_file(filepath: str) -> list:
    """Load a JSON file and return the data as a list."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def analyze_person_connection_patterns():
    """Analyze the most common connection patterns from Person models."""
    
    print("ANALYZING PERSON CONNECTION PATTERNS")
    print("=" * 60)
    
    # Load all data files
    data_files = {
        'Person': '../data/json/persons.json',
        'Arrest': '../data/json/arrests.json',
        'Vehicle': '../data/json/vehicles.json',
        'JailBooking': '../data/json/jail_bookings.json',
        'JailSentence': '../data/json/jail_sentences.json',
        'JailIncident': '../data/json/jail_incidents.json',
        'BailBond': '../data/json/bail_bonds.json',
        'JailLog': '../data/json/jail_logs.json',
        'JailProgram': '../data/json/jail_programs.json',
        'Property': '../data/json/properties.json',
        'EMSIncident': '../data/json/ems_incidents.json',
        'FireIncident': '../data/json/fire_incidents.json',
        'PoliceIncident': '../data/json/police_incidents.json',
        'FirePersonnel': '../data/json/fire_personnel.json',
        'FireReport': '../data/json/fire_reports.json',
        'FireRMSIncident': '../data/json/fire_rms_incidents.json',
        'FireShift': '../data/json/fire_shifts.json',
    }
    
    # Load all data
    all_data = {}
    for data_class, filename in data_files.items():
        all_data[data_class] = load_json_file(filename)
        print(f"📊 Loaded {len(all_data[data_class]):,} {data_class} records")
    
    # Analyze person connections
    person_connections = defaultdict(set)  # person_id -> set of data classes
    data_class_connections = defaultdict(int)  # data_class -> count of persons
    connection_pairs = defaultdict(int)  # (class1, class2) -> count
    
    print(f"\nANALYZING CONNECTIONS...")
    
    # Process each data class
    for data_class, records in all_data.items():
        if data_class == 'Person':
            continue  # Skip person records themselves
            
        for record in records:
            person_id = None
            
            # Extract person_id based on data class
            if 'person_id' in record:
                person_id = record['person_id']
            elif 'owner_person_id' in record:
                person_id = record['owner_person_id']
            elif 'patient_person_id' in record:
                person_id = record['patient_person_id']
            elif 'caller_person_id' in record:
                person_id = record['caller_person_id']
            elif 'enrolled_person_ids' in record:
                # Handle lists of person IDs
                for pid in record['enrolled_person_ids']:
                    if pid:
                        person_connections[pid].add(data_class)
                        data_class_connections[data_class] += 1
                continue
            
            if person_id:
                person_connections[person_id].add(data_class)
                data_class_connections[data_class] += 1
    
    # Analyze connection patterns
    print(f"\n📈 CONNECTION PATTERN ANALYSIS")
    print("=" * 60)
    
    # 1. Single connections (persons with only one data class)
    single_connections = Counter()
    multi_connections = Counter()
    
    for person_id, classes in person_connections.items():
        if len(classes) == 1:
            single_connections[list(classes)[0]] += 1
        else:
            multi_connections[len(classes)] += 1
    
    print(f"Total unique persons with connections: {len(person_connections):,}")
    print(f"Persons with single connections: {sum(single_connections.values()):,}")
    print(f"Persons with multiple connections: {sum(multi_connections.values()):,}")
    
    # 2. Most common single connections
    print(f"\n🔍 MOST COMMON SINGLE CONNECTIONS:")
    for data_class, count in single_connections.most_common(10):
        percentage = (count / len(person_connections)) * 100
        print(f"  {data_class:20}: {count:>6,} persons ({percentage:5.1f}%)")
    
    # 3. Multi-connection distribution
    print(f"\nMULTI-CONNECTION DISTRIBUTION:")
    for num_connections in sorted(multi_connections.keys()):
        count = multi_connections[num_connections]
        percentage = (count / len(person_connections)) * 100
        print(f"  {num_connections:2d} connections: {count:>6,} persons ({percentage:5.1f}%)")
    
    # 4. Most common data class connections
    print(f"\nMOST CONNECTED DATA CLASSES:")
    for data_class, count in sorted(data_class_connections.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(person_connections)) * 100
        print(f"  {data_class:20}: {count:>6,} persons ({percentage:5.1f}%)")
    
    # 5. Connection pair analysis
    print(f"\n ANALYZING CONNECTION PAIRS...")
    for person_id, classes in person_connections.items():
        if len(classes) > 1:
            for class1, class2 in combinations(sorted(classes), 2):
                connection_pairs[(class1, class2)] += 1
    
    print(f"\n MOST COMMON CONNECTION PAIRS:")
    for (class1, class2), count in sorted(connection_pairs.items(), key=lambda x: x[1], reverse=True)[:15]:
        percentage = (count / len(person_connections)) * 100
        print(f"  {class1:15} + {class2:15}: {count:>4,} persons ({percentage:4.1f}%)")
    
    # 6. Agency-based analysis
    print(f"\n AGENCY-BASED CONNECTION ANALYSIS:")
    agencies = {
        'Jail System': ['JailBooking', 'JailSentence', 'JailIncident', 'BailBond', 'JailLog', 'JailProgram'],
        'Law Enforcement': ['Arrest', 'PoliceIncident'],
        'Emergency Services': ['EMSIncident', 'FireIncident', 'FireRMSIncident'],
        'Fire Department': ['FirePersonnel', 'FireReport', 'FireShift'],
        'Property/Assets': ['Property', 'Vehicle'],
    }
    
    agency_connections = defaultdict(set)
    for person_id, classes in person_connections.items():
        for agency, agency_classes in agencies.items():
            if any(cls in classes for cls in agency_classes):
                agency_connections[person_id].add(agency)
    
    print(f"  Cross-agency connections:")
    cross_agency_counts = Counter(len(agencies) for agencies in agency_connections.values())
    for num_agencies, count in sorted(cross_agency_counts.items()):
        percentage = (count / len(person_connections)) * 100
        print(f"    {num_agencies} agencies: {count:>6,} persons ({percentage:5.1f}%)")
    
    # 7. Create visualizations
    create_connection_visualizations(person_connections, data_class_connections, 
                                   single_connections, multi_connections, connection_pairs)
    
    # 8. Save detailed results
    save_connection_analysis(person_connections, data_class_connections, 
                           single_connections, multi_connections, connection_pairs)
    
    return person_connections, data_class_connections, single_connections, multi_connections, connection_pairs

def create_connection_visualizations(person_connections, data_class_connections, 
                                   single_connections, multi_connections, connection_pairs):
    """Create visualizations of connection patterns."""
    
    print(f"\n📊 CREATING VISUALIZATIONS...")
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("Set2")
    
    # Create a figure with multiple subplots
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle('Person Connection Pattern Analysis', fontsize=20, fontweight='bold', y=0.98)
    fig.patch.set_facecolor('#f8f9fa')
    
    # 1. Data Class Connection Counts
    ax1 = axes[0, 0]
    classes = list(data_class_connections.keys())
    counts = list(data_class_connections.values())
    
    # Sort by count
    sorted_data = sorted(zip(classes, counts), key=lambda x: x[1], reverse=True)
    classes_sorted, counts_sorted = zip(*sorted_data)
    
    colors1 = plt.cm.viridis([i/len(classes_sorted) for i in range(len(classes_sorted))])
    bars1 = ax1.barh(range(len(classes_sorted)), counts_sorted, color=colors1, alpha=0.8)
    ax1.set_yticks(range(len(classes_sorted)))
    ax1.set_yticklabels(classes_sorted, fontsize=10)
    ax1.set_xlabel('Number of Persons', fontsize=12, fontweight='bold')
    ax1.set_title('Persons Connected to Each Data Class', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, linestyle='--')
    
    # Add value labels
    for i, (bar, count) in enumerate(zip(bars1, counts_sorted)):
        width = bar.get_width()
        ax1.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                f'{count:,}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    # 2. Single vs Multi-Connection Distribution
    ax2 = axes[0, 1]
    single_count = sum(single_connections.values())
    multi_count = sum(multi_connections.values())
    
    labels = ['Single Connection', 'Multiple Connections']
    sizes = [single_count, multi_count]
    colors2 = ['#FF6B6B', '#4ECDC4']
    
    wedges, texts, autotexts = ax2.pie(sizes, labels=labels, colors=colors2, autopct='%1.1f%%', 
                                      startangle=90, textprops={'fontsize': 10, 'fontweight': 'bold'})
    ax2.set_title('Single vs Multiple Connections', fontsize=14, fontweight='bold')
    
    # 3. Multi-Connection Distribution
    ax3 = axes[0, 2]
    connections = sorted(multi_connections.keys())
    counts = [multi_connections[c] for c in connections]
    
    colors3 = plt.cm.plasma([i/len(connections) for i in range(len(connections))])
    bars3 = ax3.bar(connections, counts, color=colors3, alpha=0.8, edgecolor='white', linewidth=1.5)
    ax3.set_xlabel('Number of Connections', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Number of Persons', fontsize=12, fontweight='bold')
    ax3.set_title('Distribution of Multi-Connections', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, linestyle='--')
    
    # Add value labels
    for bar, count in zip(bars3, counts):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{count:,}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # 4. Top Single Connections
    ax4 = axes[1, 0]
    top_single = single_connections.most_common(8)
    if top_single:
        classes, counts = zip(*top_single)
        colors4 = plt.cm.coolwarm([i/len(classes) for i in range(len(classes))])
        bars4 = ax4.barh(range(len(classes)), counts, color=colors4, alpha=0.8)
        ax4.set_yticks(range(len(classes)))
        ax4.set_yticklabels(classes, fontsize=10)
        ax4.set_xlabel('Number of Persons', fontsize=12, fontweight='bold')
        ax4.set_title('Top Single Connections', fontsize=14, fontweight='bold')
        ax4.grid(True, alpha=0.3, linestyle='--')
        
        # Add value labels
        for i, (bar, count) in enumerate(zip(bars4, counts)):
            width = bar.get_width()
            ax4.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                    f'{count:,}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    # 5. Top Connection Pairs
    ax5 = axes[1, 1]
    top_pairs = sorted(connection_pairs.items(), key=lambda x: x[1], reverse=True)[:8]
    if top_pairs:
        pair_labels = [f"{pair[0][0]}\n+ {pair[0][1]}" for pair in top_pairs]
        pair_counts = [pair[1] for pair in top_pairs]
        colors5 = plt.cm.Set3([i/len(pair_labels) for i in range(len(pair_labels))])
        bars5 = ax5.barh(range(len(pair_labels)), pair_counts, color=colors5, alpha=0.8)
        ax5.set_yticks(range(len(pair_labels)))
        ax5.set_yticklabels(pair_labels, fontsize=9)
        ax5.set_xlabel('Number of Persons', fontsize=12, fontweight='bold')
        ax5.set_title('Top Connection Pairs', fontsize=14, fontweight='bold')
        ax5.grid(True, alpha=0.3, linestyle='--')
        
        # Add value labels
        for i, (bar, count) in enumerate(zip(bars5, pair_counts)):
            width = bar.get_width()
            ax5.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                    f'{count:,}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    # 6. Connection Heatmap
    ax6 = axes[1, 2]
    
    # Create a matrix of connection pairs
    all_classes = sorted(set([cls for classes in person_connections.values() for cls in classes]))
    heatmap_data = pd.DataFrame(0, index=all_classes, columns=all_classes)
    
    for (class1, class2), count in connection_pairs.items():
        heatmap_data.loc[class1, class2] = count
        heatmap_data.loc[class2, class1] = count
    
    # Only show classes that have connections
    non_zero_classes = [cls for cls in all_classes if heatmap_data[cls].sum() > 0]
    heatmap_subset = heatmap_data.loc[non_zero_classes, non_zero_classes]
    
    sns.heatmap(heatmap_subset, annot=True, fmt='d', cmap='YlOrRd', 
                ax=ax6, cbar_kws={'label': 'Number of Persons', 'shrink': 0.8},
                linewidths=0.5, linecolor='white', square=True)
    ax6.set_title('Connection Pair Heatmap', fontsize=14, fontweight='bold')
    ax6.set_xlabel('Data Class', fontsize=12, fontweight='bold')
    ax6.set_ylabel('Data Class', fontsize=12, fontweight='bold')
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('person_connection_patterns.png', dpi=300, bbox_inches='tight', 
                facecolor='#f8f9fa', edgecolor='none')
    plt.show()
    
    print(f"📊 Visualizations saved to: person_connection_patterns.png")

def save_connection_analysis(person_connections, data_class_connections, 
                           single_connections, multi_connections, connection_pairs):
    """Save detailed connection analysis to JSON file."""
    
    # Prepare data for saving
    analysis_results = {
        'summary': {
            'total_persons_with_connections': len(person_connections),
            'total_single_connections': sum(single_connections.values()),
            'total_multi_connections': sum(multi_connections.values()),
            'data_class_counts': dict(data_class_connections),
            'single_connection_counts': dict(single_connections),
            'multi_connection_counts': dict(multi_connections),
            'connection_pair_counts': {f"{pair[0]}+{pair[1]}": count for pair, count in connection_pairs.items()}
        },
        'person_connections': {
            person_id: {
                'data_classes': list(classes),
                'num_connections': len(classes),
                'connection_type': 'single' if len(classes) == 1 else 'multiple'
            }
            for person_id, classes in person_connections.items()
        }
    }
    
    with open('person_connection_patterns.json', 'w') as f:
        json.dump(analysis_results, f, indent=2)
    
    print(f"💾 Detailed analysis saved to: person_connection_patterns.json")

if __name__ == "__main__":
    analyze_person_connection_patterns()
