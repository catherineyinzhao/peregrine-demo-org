#!/usr/bin/env python3
"""
Network Graph Visualizer for Multi-Agency Data
Creates an interactive network graph showing one person and all related entities
"""

import json
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import random
from collections import defaultdict

def load_data():
    """Load the generated JSON data files"""
    try:
        with open('persons.json', 'r') as f:
            persons = json.load(f)
        with open('police_incidents.json', 'r') as f:
            police_incidents = json.load(f)
        with open('cad_incidents.json', 'r') as f:
            cad_incidents = json.load(f)
        with open('arrests.json', 'r') as f:
            arrests = json.load(f)
        with open('fire_incidents.json', 'r') as f:
            fire_incidents = json.load(f)
        with open('ems_incidents.json', 'r') as f:
            ems_incidents = json.load(f)
        with open('properties.json', 'r') as f:
            properties = json.load(f)
        return persons, police_incidents, cad_incidents, arrests, fire_incidents, ems_incidents, properties
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run syntheticdata_enhanced.py first to generate the data files.")
        return None, None, None, None, None, None, None

def create_network_graph(person_id, persons, police_incidents, cad_incidents, arrests, fire_incidents, ems_incidents, properties):
    """Create a network graph for a specific person"""
    G = nx.Graph()
    
    # Find the target person
    target_person = None
    for person in persons:
        if person['person_id'] == person_id:
            target_person = person
            break
    
    if not target_person:
        print(f"Person with ID {person_id} not found.")
        return None
    
    # Add the target person as the central node
    G.add_node(person_id, 
               type='PERSON', 
               label=f"{target_person['first_name']} {target_person['last_name']}",
               details=target_person)
    
    # Track all related entities
    related_entities = defaultdict(list)
    
    # Find related police incidents
    for incident in police_incidents:
        if incident.get('suspect_id') == person_id or incident.get('victim_id') == person_id:
            incident_id = incident['incident_id']
            G.add_node(incident_id, 
                      type='POLICE_INCIDENT', 
                      label=f"Police: {incident['incident_type']}",
                      details=incident)
            G.add_edge(person_id, incident_id, relationship='INVOLVED_IN')
            related_entities['police_incidents'].append(incident_id)
    
    # Find related CAD incidents
    for incident in cad_incidents:
        for related_person in incident.get('related_persons', []):
            if related_person['person_id'] == person_id:
                incident_id = incident['cad_id']
                G.add_node(incident_id, 
                          type='CAD_INCIDENT', 
                          label=f"CAD: {incident['call_type']}",
                          details=incident)
                G.add_edge(person_id, incident_id, relationship='LINKED_TO')
                related_entities['cad_incidents'].append(incident_id)
    
    # Find related arrests
    for arrest in arrests:
        if arrest['person_id'] == person_id:
            arrest_id = arrest['arrest_id']
            G.add_node(arrest_id, 
                      type='ARREST', 
                      label=f"Arrest: {arrest['arrest_type']}",
                      details=arrest)
            G.add_edge(person_id, arrest_id, relationship='ARRESTED_IN')
            related_entities['arrests'].append(arrest_id)
    
    # Find related fire incidents
    for incident in fire_incidents:
        if incident.get('victim_id') == person_id or incident.get('witness_id') == person_id:
            incident_id = incident['incident_id']
            G.add_node(incident_id, 
                      type='FIRE_INCIDENT', 
                      label=f"Fire: {incident['incident_type']}",
                      details=incident)
            G.add_edge(person_id, incident_id, relationship='INVOLVED_IN')
            related_entities['fire_incidents'].append(incident_id)
    
    # Find related EMS incidents
    for incident in ems_incidents:
        if incident.get('patient_id') == person_id or incident.get('witness_id') == person_id:
            incident_id = incident['incident_id']
            G.add_node(incident_id, 
                      type='EMS_INCIDENT', 
                      label=f"EMS: {incident['incident_type']}",
                      details=incident)
            G.add_edge(person_id, incident_id, relationship='INVOLVED_IN')
            related_entities['ems_incidents'].append(incident_id)
    
    # Find related properties
    for property_item in properties:
        if property_item.get('person_id') == person_id:
            property_id = property_item['property_id']
            G.add_node(property_id, 
                      type='PROPERTY', 
                      label=f"Property: {property_item['property_type']}",
                      details=property_item)
            G.add_edge(person_id, property_id, relationship='OWNED_BY')
            related_entities['properties'].append(property_id)
    
    # Add connections between related entities
    for arrest_id in related_entities['arrests']:
        arrest_details = next(a for a in arrests if a['arrest_id'] == arrest_id)
        if arrest_details.get('cad_incident_id'):
            cad_id = arrest_details['cad_incident_id']
            if cad_id in related_entities['cad_incidents']:
                G.add_edge(arrest_id, cad_id, relationship='RESULTED_FROM')
    
    return G, target_person, related_entities

def visualize_network(G, target_person, related_entities):
    """Create and display the network visualization"""
    if not G:
        return
    
    plt.figure(figsize=(16, 12))
    
    # Use spring layout for better node positioning
    pos = nx.spring_layout(G, k=3, iterations=50)
    
    # Define colors and styles for different node types
    node_colors = {
        'PERSON': '#1f77b4',      # Blue
        'POLICE_INCIDENT': '#ff7f0e',  # Orange
        'CAD_INCIDENT': '#2ca02c',     # Green
        'ARREST': '#d62728',           # Red
        'FIRE_INCIDENT': '#9467bd',    # Purple
        'EMS_INCIDENT': '#8c564b',     # Brown
        'PROPERTY': '#e377c2'          # Pink
    }
    
    # Draw nodes
    for node_type in node_colors:
        nodes = [n for n, d in G.nodes(data=True) if d['type'] == node_type]
        nx.draw_networkx_nodes(G, pos, 
                              nodelist=nodes,
                              node_color=node_colors[node_type],
                              node_size=1000,
                              alpha=0.8)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, alpha=0.6, edge_color='gray', width=2)
    
    # Draw labels
    labels = {n: d['label'] for n, d in G.nodes(data=True)}
    nx.draw_networkx_labels(G, pos, labels, font_size=8, font_weight='bold')
    
    # Create legend
    legend_elements = [mpatches.Patch(color=color, label=node_type.replace('_', ' ').title()) 
                      for node_type, color in node_colors.items()]
    plt.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1, 1))
    
    # Add title and statistics
    plt.title(f"Network Graph for {target_person['first_name']} {target_person['last_name']}\n"
              f"Person ID: {target_person['person_id']}", fontsize=16, fontweight='bold')
    
    # Add statistics text
    stats_text = f"Total Related Entities: {len(G.nodes()) - 1}\n"
    for entity_type, entities in related_entities.items():
        if entities:
            stats_text += f"{entity_type.replace('_', ' ').title()}: {len(entities)}\n"
    
    plt.figtext(0.02, 0.02, stats_text, fontsize=10, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    
    plt.tight_layout()
    plt.show()
    
    # Print detailed information
    print(f"\n=== NETWORK ANALYSIS FOR {target_person['first_name']} {target_person['last_name']} ===")
    print(f"Person ID: {target_person['person_id']}")
    print(f"Total Related Entities: {len(G.nodes()) - 1}")
    print(f"Total Relationships: {len(G.edges())}")
    
    for entity_type, entities in related_entities.items():
        if entities:
            print(f"\n{entity_type.replace('_', ' ').title()} ({len(entities)}):")
            for entity_id in entities:
                entity_data = G.nodes[entity_id]['details']
                if entity_type == 'police_incidents':
                    print(f"  - {entity_data['incident_type']} on {entity_data['incident_date']}")
                elif entity_type == 'cad_incidents':
                    print(f"  - {entity_data['call_type']} on {entity_data['call_datetime']}")
                elif entity_type == 'arrests':
                    print(f"  - {entity_data['arrest_type']} on {entity_data['arrest_datetime']}")
                elif entity_type == 'fire_incidents':
                    print(f"  - {entity_data['incident_type']} on {entity_data['alarm_datetime']}")
                elif entity_type == 'ems_incidents':
                    print(f"  - {entity_type} on {entity_data['incident_datetime']}")
                elif entity_type == 'properties':
                    print(f"  - {entity_data['property_type']} found on {entity_data['found_date']}")

def main():
    """Main function to run the network visualizer"""
    print("Loading data files...")
    data = load_data()
    if not data[0]:
        return
    
    persons, police_incidents, cad_incidents, arrests, fire_incidents, ems_incidents, properties = data
    
    print(f"Loaded {len(persons)} persons")
    print(f"Loaded {len(police_incidents)} police incidents")
    print(f"Loaded {len(cad_incidents)} CAD incidents")
    print(f"Loaded {len(arrests)} arrests")
    print(f"Loaded {len(fire_incidents)} fire incidents")
    print(f"Loaded {len(ems_incidents)} EMS incidents")
    print(f"Loaded {len(properties)} properties")
    
    # Select a person to visualize (you can change this ID)
    if persons:
        # Select a person with the most connections for demonstration
        target_person = persons[0]  # You can change this to any person ID
        person_id = target_person['person_id']
        
        print(f"\nCreating network graph for: {target_person['first_name']} {target_person['last_name']}")
        
        # Create and visualize the network
        G, target_person, related_entities = create_network_graph(
            person_id, persons, police_incidents, cad_incidents, arrests, 
            fire_incidents, ems_incidents, properties
        )
        
        if G:
            visualize_network(G, target_person, related_entities)
        else:
            print("No network could be created.")
    else:
        print("No persons found in the data.")

if __name__ == "__main__":
    main()
