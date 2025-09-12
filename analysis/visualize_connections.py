#!/usr/bin/env python3
"""
Script to create visualizations of person connections across data classes.
"""

import json
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
import pandas as pd

def load_analysis_results():
    """Load the connection analysis results."""
    with open('person_connection_analysis.json', 'r') as f:
        return json.load(f)

def create_connection_visualizations():
    """Create various visualizations of person connections."""
    
    # Load the analysis results
    results = load_analysis_results()
    summary = results['summary']
    person_connections = results['person_connections']
    
    # Set up the plotting style with better aesthetics
    plt.style.use('seaborn-v0_8')
    sns.set_palette("Set2")
    
    # Create a figure with multiple subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Person Connection Analysis Across Data Classes', fontsize=18, fontweight='bold', y=0.98)
    
    # Set background color
    fig.patch.set_facecolor('#f8f9fa')
    
    # 1. Connection Distribution
    ax1 = axes[0, 0]
    connection_dist = summary['connection_distribution']
    connections = sorted(connection_dist.keys())
    counts = [connection_dist[c] for c in connections]
    
    # Create gradient colors
    colors = plt.cm.viridis([i/len(connections) for i in range(len(connections))])
    bars = ax1.bar(connections, counts, color=colors, edgecolor='white', linewidth=1.5, alpha=0.8)
    ax1.set_xlabel('Number of Data Classes Connected', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Number of Persons', fontsize=12, fontweight='bold')
    ax1.set_title('Distribution of Connections per Person', fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3, linestyle='--')
    ax1.set_facecolor('#fafafa')
    
    # Add value labels on bars with better formatting
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{count:,}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # 2. Data Class Record Counts
    ax2 = axes[0, 1]
    data_classes = list(summary['data_class_counts'].keys())
    record_counts = list(summary['data_class_counts'].values())
    
    # Sort by count for better visualization
    sorted_data = sorted(zip(data_classes, record_counts), key=lambda x: x[1], reverse=True)
    data_classes_sorted, record_counts_sorted = zip(*sorted_data)
    
    # Create horizontal bar chart with gradient colors
    colors2 = plt.cm.plasma([i/len(data_classes_sorted) for i in range(len(data_classes_sorted))])
    bars2 = ax2.barh(range(len(data_classes_sorted)), record_counts_sorted, 
                     color=colors2, edgecolor='white', linewidth=1.5, alpha=0.8)
    ax2.set_yticks(range(len(data_classes_sorted)))
    ax2.set_yticklabels(data_classes_sorted, fontsize=10, fontweight='bold')
    ax2.set_xlabel('Number of Records', fontsize=12, fontweight='bold')
    ax2.set_title('Records per Data Class', fontsize=14, fontweight='bold', pad=20)
    ax2.grid(True, alpha=0.3, linestyle='--')
    ax2.set_facecolor('#fafafa')
    
    # Add value labels with better positioning
    for i, (bar, count) in enumerate(zip(bars2, record_counts_sorted)):
        width = bar.get_width()
        ax2.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                f'{count:,}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    # 3. Connection Pattern Heatmap
    ax3 = axes[1, 0]
    
    # Create a matrix of connections between data classes
    data_class_pairs = defaultdict(int)
    for person_data in person_connections.values():
        classes = person_data['data_classes']
        for i, class1 in enumerate(classes):
            for class2 in classes[i+1:]:
                pair = tuple(sorted([class1, class2]))
                data_class_pairs[pair] += 1
    
    # Create a DataFrame for the heatmap
    all_classes = sorted(set([cls for classes in person_connections.values() 
                             for cls in classes['data_classes']]))
    
    heatmap_data = pd.DataFrame(0, index=all_classes, columns=all_classes)
    for (class1, class2), count in data_class_pairs.items():
        heatmap_data.loc[class1, class2] = count
        heatmap_data.loc[class2, class1] = count
    
    # Only show classes that have connections
    non_zero_classes = [cls for cls in all_classes if heatmap_data[cls].sum() > 0]
    heatmap_subset = heatmap_data.loc[non_zero_classes, non_zero_classes]
    
    # Create a more visually appealing heatmap
    sns.heatmap(heatmap_subset, annot=True, fmt='d', cmap='RdYlBu_r', 
                ax=ax3, cbar_kws={'label': 'Number of Persons', 'shrink': 0.8},
                linewidths=0.5, linecolor='white', square=True)
    ax3.set_title('Data Class Connection Heatmap', fontsize=14, fontweight='bold', pad=20)
    ax3.set_xlabel('Data Class', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Data Class', fontsize=12, fontweight='bold')
    ax3.set_facecolor('#fafafa')
    
    # 4. Top Connected Persons
    ax4 = axes[1, 1]
    
    # Get top 15 most connected persons
    top_persons = sorted(person_connections.items(), 
                        key=lambda x: x[1]['num_connections'], reverse=True)[:15]
    
    person_ids = [pid[:12] + '...' for pid, _ in top_persons]  # Truncate for display
    connection_counts = [data['num_connections'] for _, data in top_persons]
    
    # Create horizontal bar chart with gradient colors
    colors4 = plt.cm.coolwarm([i/len(person_ids) for i in range(len(person_ids))])
    bars4 = ax4.barh(range(len(person_ids)), connection_counts, 
                     color=colors4, edgecolor='white', linewidth=1.5, alpha=0.8)
    ax4.set_yticks(range(len(person_ids)))
    ax4.set_yticklabels(person_ids, fontsize=9, fontweight='bold')
    ax4.set_xlabel('Number of Data Classes', fontsize=12, fontweight='bold')
    ax4.set_title('Top 15 Most Connected Persons', fontsize=14, fontweight='bold', pad=20)
    ax4.grid(True, alpha=0.3, linestyle='--')
    ax4.set_facecolor('#fafafa')
    
    # Add value labels with better formatting
    for i, (bar, count) in enumerate(zip(bars4, connection_counts)):
        width = bar.get_width()
        ax4.text(width + 0.1, bar.get_y() + bar.get_height()/2.,
                f'{count}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    # Improve overall layout and styling
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    
    # Add a subtle border to the entire figure
    for ax in axes.flat:
        for spine in ax.spines.values():
            spine.set_edgecolor('#cccccc')
            spine.set_linewidth(1.5)
    
    plt.savefig('person_connection_analysis.png', dpi=300, bbox_inches='tight', 
                facecolor='#f8f9fa', edgecolor='none')
    plt.show()
    
    print("📊 Enhanced visualizations saved to: person_connection_analysis.png")

def create_detailed_connection_report():
    """Create a detailed text report of connection patterns."""
    
    results = load_analysis_results()
    person_connections = results['person_connections']
    
    # Analyze connection patterns
    connection_patterns = defaultdict(list)
    
    for person_id, data in person_connections.items():
        classes = tuple(sorted(data['data_classes']))
        connection_patterns[classes].append(person_id)
    
    # Sort patterns by frequency
    sorted_patterns = sorted(connection_patterns.items(), 
                           key=lambda x: len(x[1]), reverse=True)
    
    print("\n" + "="*80)
    print("DETAILED CONNECTION PATTERN ANALYSIS")
    print("="*80)
    
    print(f"\n📊 Total unique connection patterns: {len(connection_patterns)}")
    print(f"📊 Total persons analyzed: {len(person_connections)}")
    
    print(f"\n🔍 Top 20 Most Common Connection Patterns:")
    print("-" * 80)
    
    for i, (pattern, person_ids) in enumerate(sorted_patterns[:20], 1):
        print(f"{i:2d}. Pattern: {', '.join(pattern)}")
        print(f"    Count: {len(person_ids):,} persons")
        print(f"    Percentage: {(len(person_ids)/len(person_connections)*100):.2f}%")
        if len(person_ids) <= 5:
            print(f"    Person IDs: {', '.join(person_ids)}")
        else:
            print(f"    Sample Person IDs: {', '.join(person_ids[:3])}... (+{len(person_ids)-3} more)")
        print()
    
    # Analyze specific agency connections
    print(f"\n🏛️  Agency-Specific Connection Analysis:")
    print("-" * 50)
    
    agency_stats = {
        'Jail System': ['JailBooking', 'JailSentence', 'JailIncident', 'BailBond', 'JailLog', 'JailProgram'],
        'Law Enforcement': ['Arrest', 'PoliceIncident'],
        'Emergency Services': ['EMSIncident', 'FireIncident', 'FireRMSIncident'],
        'Fire Department': ['FirePersonnel', 'FireReport', 'FireShift'],
        'Property/Assets': ['Property', 'Vehicle']
    }
    
    for agency, classes in agency_stats.items():
        agency_persons = set()
        for person_id, data in person_connections.items():
            if any(cls in data['data_classes'] for cls in classes):
                agency_persons.add(person_id)
        
        print(f"{agency:20}: {len(agency_persons):,} persons")
    
    # Cross-agency analysis
    print(f"\n🔗 Cross-Agency Connection Analysis:")
    print("-" * 50)
    
    cross_agency_patterns = defaultdict(int)
    for person_id, data in person_connections.items():
        classes = data['data_classes']
        agencies_involved = set()
        
        for agency, agency_classes in agency_stats.items():
            if any(cls in classes for cls in agency_classes):
                agencies_involved.add(agency)
        
        if len(agencies_involved) > 1:
            pattern = tuple(sorted(agencies_involved))
            cross_agency_patterns[pattern] += 1
    
    for pattern, count in sorted(cross_agency_patterns.items(), key=lambda x: x[1], reverse=True):
        print(f"{' + '.join(pattern):40}: {count:,} persons")

if __name__ == "__main__":
    try:
        create_connection_visualizations()
        create_detailed_connection_report()
    except ImportError as e:
        print(f"Missing required packages: {e}")
        print("Please install matplotlib and seaborn: pip install matplotlib seaborn")
        # Still run the text report
        create_detailed_connection_report()
