#!/usr/bin/env python3
"""
Create a network graph visualization showing connections for the most connected person.
"""

import json
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from collections import defaultdict
import random

def load_analysis_results():
    """Load the connection analysis results."""
    with open('person_connection_analysis.json', 'r') as f:
        return json.load(f)

def create_network_graph():
    """Create a network graph for the most connected person."""
    
    # Load the analysis results
    results = load_analysis_results()
    person_connections = results['person_connections']
    
    # Find the most connected person
    most_connected = max(person_connections.items(), key=lambda x: x[1]['num_connections'])
    person_id, person_data = most_connected
    
    print(f"Creating network graph for person: {person_id}")
    print(f"Number of connections: {person_data['num_connections']}")
    print(f"Data classes: {', '.join(person_data['data_classes'])}")
    
    # Create a directed graph
    G = nx.DiGraph()
    
    # Add the central person node
    G.add_node(person_id, node_type='person', size=2000, color='#FF6B6B')
    
    # Define data class categories and their colors
    data_class_categories = {
        'Jail System': {
            'classes': ['JailBooking', 'JailSentence', 'JailIncident', 'BailBond', 'JailLog', 'JailProgram'],
            'color': '#4ECDC4'
        },
        'Law Enforcement': {
            'classes': ['Arrest', 'PoliceIncident'],
            'color': '#45B7D1'
        },
        'Emergency Services': {
            'classes': ['EMSIncident', 'FireIncident', 'FireRMSIncident'],
            'color': '#96CEB4'
        },
        'Fire Department': {
            'classes': ['FirePersonnel', 'FireReport', 'FireShift'],
            'color': '#FFEAA7'
        },
        'Property/Assets': {
            'classes': ['Property', 'Vehicle'],
            'color': '#DDA0DD'
        },
        'Core': {
            'classes': ['Person'],
            'color': '#98D8C8'
        }
    }
    
    # Add data class nodes and connections
    for data_class in person_data['data_classes']:
        # Find the category for this data class
        category = 'Other'
        category_color = '#95A5A6'
        for cat_name, cat_info in data_class_categories.items():
            if data_class in cat_info['classes']:
                category = cat_name
                category_color = cat_info['color']
                break
        
        # Add the data class node
        G.add_node(data_class, 
                  node_type='data_class', 
                  category=category,
                  size=800, 
                  color=category_color)
        
        # Add edge from person to data class
        G.add_edge(person_id, data_class, weight=2, color='#2C3E50')
    
    # Create the visualization
    plt.figure(figsize=(16, 12))
    plt.title(f'Network Graph: Most Connected Person\n{person_id}\n({person_data["num_connections"]} data classes)', 
              fontsize=16, fontweight='bold', pad=20)
    
    # Use spring layout for better positioning
    pos = nx.spring_layout(G, k=3, iterations=50, seed=42)
    
    # Draw nodes
    node_sizes = [G.nodes[node]['size'] for node in G.nodes()]
    node_colors = [G.nodes[node]['color'] for node in G.nodes()]
    
    # Draw the central person node first (larger)
    person_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'person']
    data_class_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'data_class']
    
    # Draw data class nodes
    nx.draw_networkx_nodes(G, pos, 
                          nodelist=data_class_nodes,
                          node_size=[G.nodes[node]['size'] for node in data_class_nodes],
                          node_color=[G.nodes[node]['color'] for node in data_class_nodes],
                          alpha=0.8, edgecolors='white', linewidths=2)
    
    # Draw the central person node
    nx.draw_networkx_nodes(G, pos, 
                          nodelist=person_nodes,
                          node_size=[G.nodes[node]['size'] for node in person_nodes],
                          node_color=[G.nodes[node]['color'] for node in person_nodes],
                          alpha=0.9, edgecolors='black', linewidths=3)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, 
                          edge_color='#34495E', 
                          width=3, 
                          alpha=0.6,
                          arrows=True,
                          arrowsize=20,
                          arrowstyle='->')
    
    # Add labels
    labels = {}
    for node in G.nodes():
        if G.nodes[node]['node_type'] == 'person':
            labels[node] = f"Person\n{node[:12]}..."
        else:
            labels[node] = node
    
    nx.draw_networkx_labels(G, pos, labels, font_size=10, font_weight='bold')
    
    # Create legend
    legend_elements = []
    for category, info in data_class_categories.items():
        if any(cls in person_data['data_classes'] for cls in info['classes']):
            legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', 
                                            markerfacecolor=info['color'], 
                                            markersize=12, label=category))
    
    # Add person legend element
    legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', 
                                    markerfacecolor='#FF6B6B', 
                                    markersize=12, label='Person'))
    
    plt.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Set background
    plt.gca().set_facecolor('#F8F9FA')
    plt.axis('off')
    
    # Add statistics text box
    stats_text = f"""Connection Statistics:
• Total Data Classes: {person_data['num_connections']}
• Total Occurrences: {person_data['total_occurrences']}
• Categories: {len(set(G.nodes[node]['category'] for node in data_class_nodes))}"""
    
    plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes, 
             fontsize=12, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig('most_connected_person_network.png', dpi=300, bbox_inches='tight', 
                facecolor='#F8F9FA', edgecolor='none')
    plt.show()
    
    print(f"Network graph saved to: most_connected_person_network.png")
    
    return G, person_id, person_data

def create_multiple_person_network():
    """Create a network graph showing multiple highly connected persons."""
    
    # Load the analysis results
    results = load_analysis_results()
    person_connections = results['person_connections']
    
    # Get top 5 most connected persons
    top_persons = sorted(person_connections.items(), 
                        key=lambda x: x[1]['num_connections'], reverse=True)[:5]
    
    print(f"Creating network graph for top 5 most connected persons")
    
    # Create a graph
    G = nx.Graph()
    
    # Define colors for different persons
    person_colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
    
    # Add person nodes and their connections
    for i, (person_id, person_data) in enumerate(top_persons):
        person_color = person_colors[i % len(person_colors)]
        
        # Add person node
        G.add_node(person_id, node_type='person', size=1500, color=person_color)
        
        # Add data class nodes and connections
        for data_class in person_data['data_classes']:
            if not G.has_node(data_class):
                G.add_node(data_class, node_type='data_class', size=600, color='#95A5A6')
            
            # Add edge between person and data class
            G.add_edge(person_id, data_class, weight=1)
    
    # Create the visualization
    plt.figure(figsize=(20, 14))
    plt.title('Network Graph: Top 5 Most Connected Persons', 
              fontsize=18, fontweight='bold', pad=20)
    
    # Use spring layout
    pos = nx.spring_layout(G, k=2, iterations=100, seed=42)
    
    # Separate person and data class nodes
    person_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'person']
    data_class_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'data_class']
    
    # Draw data class nodes first (background)
    nx.draw_networkx_nodes(G, pos, 
                          nodelist=data_class_nodes,
                          node_size=[G.nodes[node]['size'] for node in data_class_nodes],
                          node_color=[G.nodes[node]['color'] for node in data_class_nodes],
                          alpha=0.7, edgecolors='white', linewidths=1)
    
    # Draw person nodes (foreground)
    nx.draw_networkx_nodes(G, pos, 
                          nodelist=person_nodes,
                          node_size=[G.nodes[node]['size'] for node in person_nodes],
                          node_color=[G.nodes[node]['color'] for node in person_nodes],
                          alpha=0.9, edgecolors='black', linewidths=2)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, 
                          edge_color='#7F8C8D', 
                          width=2, 
                          alpha=0.5)
    
    # Add labels
    labels = {}
    for node in G.nodes():
        if G.nodes[node]['node_type'] == 'person':
            labels[node] = f"P-{node.split('-')[1][:8]}..."
        else:
            labels[node] = node
    
    nx.draw_networkx_labels(G, pos, labels, font_size=9, font_weight='bold')
    
    # Create legend
    legend_elements = []
    for i, (person_id, person_data) in enumerate(top_persons):
        legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', 
                                        markerfacecolor=person_colors[i % len(person_colors)], 
                                        markersize=12, 
                                        label=f"{person_id[:12]}... ({person_data['num_connections']} classes)"))
    
    plt.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Set background
    plt.gca().set_facecolor('#F8F9FA')
    plt.axis('off')
    
    plt.tight_layout()
    plt.savefig('top_5_connected_persons_network.png', dpi=300, bbox_inches='tight', 
                facecolor='#F8F9FA', edgecolor='none')
    plt.show()
    
    print(f"Multi-person network graph saved to: top_5_connected_persons_network.png")
    
    return G

if __name__ == "__main__":
    try:
        # Create network graph for the most connected person
        G1, person_id, person_data = create_network_graph()
        
        # Create network graph for top 5 most connected persons
        G2 = create_multiple_person_network()
        
    except ImportError as e:
        print(f"Missing required packages: {e}")
        print("Please install networkx: pip install networkx")
    except Exception as e:
        print(f"Error creating network graph: {e}")
