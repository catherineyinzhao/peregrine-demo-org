#!/usr/bin/env python3
"""
Create an enhanced network graph with detailed attributes and context for connected data.
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, ConnectionPatch
import networkx as nx
import numpy as np
from collections import defaultdict
import random

def load_json_file(filepath: str) -> list:
    """Load a JSON file and return the data as a list."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def get_person_details(person_id: str) -> dict:
    """Get detailed information about a person."""
    persons = load_json_file('../data/json/persons.json')
    for person in persons:
        if person.get('person_id') == person_id:
            return person
    return {}

def get_arrest_details(person_id: str) -> list:
    """Get arrest details for a person."""
    arrests = load_json_file('../data/json/arrests.json')
    return [arrest for arrest in arrests if arrest.get('person_id') == person_id]

def get_vehicle_details(person_id: str) -> list:
    """Get vehicle details for a person."""
    vehicles = load_json_file('../data/json/vehicles.json')
    return [vehicle for vehicle in vehicles if vehicle.get('owner_person_id') == person_id]

def get_jail_booking_details(person_id: str) -> list:
    """Get jail booking details for a person."""
    bookings = load_json_file('../data/json/jail_bookings.json')
    return [booking for booking in bookings if booking.get('person_id') == person_id]

def get_property_details(person_id: str) -> list:
    """Get property details for a person."""
    properties = load_json_file('../data/json/properties.json')
    return [prop for prop in properties if prop.get('owner_person_id') == person_id]

def get_ems_incident_details(person_id: str) -> list:
    """Get EMS incident details for a person."""
    incidents = load_json_file('../data/json/ems_incidents.json')
    return [incident for incident in incidents if incident.get('patient_person_id') == person_id]

def create_enhanced_network_graph():
    """Create an enhanced network graph with detailed attributes."""
    
    # Load the analysis results
    with open('person_connection_analysis.json', 'r') as f:
        results = json.load(f)
    
    person_connections = results['person_connections']
    
    # Find the most connected person
    most_connected = max(person_connections.items(), key=lambda x: x[1]['num_connections'])
    person_id, person_data = most_connected
    
    print(f"Creating enhanced network graph for person: {person_id}")
    print(f"Number of connections: {person_data['num_connections']}")
    
    # Get detailed information
    person_details = get_person_details(person_id)
    arrest_details = get_arrest_details(person_id)
    vehicle_details = get_vehicle_details(person_id)
    booking_details = get_jail_booking_details(person_id)
    property_details = get_property_details(person_id)
    ems_details = get_ems_incident_details(person_id)
    
    # Create the visualization
    fig, ax = plt.subplots(figsize=(20, 16))
    fig.patch.set_facecolor('#f8f9fa')
    
    # Set up the layout
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Define colors and styles
    colors = {
        'person': '#FF6B6B',
        'arrest': '#E74C3C',
        'vehicle': '#3498DB',
        'jail': '#9B59B6',
        'property': '#F39C12',
        'ems': '#2ECC71',
        'connection': '#34495E'
    }
    
    # Central person node
    person_x, person_y = 5, 5
    person_circle = plt.Circle((person_x, person_y), 0.8, color=colors['person'], alpha=0.9, zorder=10)
    ax.add_patch(person_circle)
    
    # Person details text
    person_name = f"{person_details.get('first_name', 'Unknown')} {person_details.get('last_name', 'Unknown')}"
    person_age = person_details.get('age', 'Unknown')
    person_ssn = person_details.get('ssn', 'Unknown')[:3] + '**-**' + person_details.get('ssn', 'Unknown')[-4:]
    
    ax.text(person_x, person_y + 0.3, person_name, ha='center', va='center', 
            fontsize=12, fontweight='bold', color='white', zorder=11)
    ax.text(person_x, person_y, f"Age: {person_age}", ha='center', va='center', 
            fontsize=10, color='white', zorder=11)
    ax.text(person_x, person_y - 0.3, f"SSN: {person_ssn}", ha='center', va='center', 
            fontsize=9, color='white', zorder=11)
    
    # Position data nodes around the person
    data_nodes = []
    angle_step = 2 * np.pi / len(person_data['data_classes'])
    
    for i, data_class in enumerate(person_data['data_classes']):
        angle = i * angle_step
        x = person_x + 2.5 * np.cos(angle)
        y = person_y + 2.5 * np.sin(angle)
        
        # Determine color based on data class
        if 'Arrest' in data_class:
            color = colors['arrest']
        elif 'Vehicle' in data_class:
            color = colors['vehicle']
        elif any(jail_class in data_class for jail_class in ['Jail', 'Bail']):
            color = colors['jail']
        elif 'Property' in data_class:
            color = colors['property']
        elif 'EMS' in data_class:
            color = colors['ems']
        else:
            color = '#95A5A6'
        
        # Create data node
        data_circle = plt.Circle((x, y), 0.6, color=color, alpha=0.8, zorder=5)
        ax.add_patch(data_circle)
        
        # Add connection line
        connection = ConnectionPatch((person_x, person_y), (x, y), "data", "data",
                                   arrowstyle="->", shrinkA=0.8, shrinkB=0.6,
                                   mutation_scale=20, fc=colors['connection'], 
                                   alpha=0.7, linewidth=3, zorder=1)
        ax.add_patch(connection)
        
        # Add data class label
        ax.text(x, y + 0.8, data_class, ha='center', va='center', 
                fontsize=11, fontweight='bold', color='black', zorder=6)
        
        # Add detailed information based on data class
        details = []
        if data_class == 'Arrest' and arrest_details:
            arrest = arrest_details[0]  # Get first arrest
            details.append(f"Date: {arrest.get('arrest_date', 'Unknown')}")
            details.append(f"Charge: {arrest.get('primary_charge', 'Unknown')[:20]}...")
            details.append(f"Location: {arrest.get('arrest_location', 'Unknown')[:15]}...")
        elif data_class == 'Vehicle' and vehicle_details:
            vehicle = vehicle_details[0]  # Get first vehicle
            details.append(f"Make: {vehicle.get('make', 'Unknown')}")
            details.append(f"Model: {vehicle.get('model', 'Unknown')}")
            details.append(f"Year: {vehicle.get('year', 'Unknown')}")
            details.append(f"Plate: {vehicle.get('license_plate', 'Unknown')}")
        elif data_class == 'JailBooking' and booking_details:
            booking = booking_details[0]  # Get first booking
            details.append(f"Date: {booking.get('booking_datetime', 'Unknown')[:10]}")
            details.append(f"Facility: {booking.get('facility_name', 'Unknown')[:15]}...")
            details.append(f"Status: {booking.get('booking_status', 'Unknown')}")
        elif data_class == 'Property' and property_details:
            prop = property_details[0]  # Get first property
            details.append(f"Type: {prop.get('property_type', 'Unknown')}")
            details.append(f"Value: ${prop.get('estimated_value', 'Unknown')}")
            details.append(f"Location: {prop.get('location', 'Unknown')[:15]}...")
        elif data_class == 'EMSIncident' and ems_details:
            ems = ems_details[0]  # Get first incident
            details.append(f"Date: {ems.get('incident_date', 'Unknown')[:10]}")
            details.append(f"Complaint: {ems.get('chief_complaint', 'Unknown')[:20]}...")
            details.append(f"Priority: {ems.get('priority', 'Unknown')}")
        
        # Add details text
        for j, detail in enumerate(details[:3]):  # Limit to 3 details
            ax.text(x, y - 0.4 - j*0.15, detail, ha='center', va='center', 
                    fontsize=8, color='black', zorder=6)
    
    # Add title and statistics
    title = f"Enhanced Network Graph: {person_name}\nPerson ID: {person_id}\n{person_data['num_connections']} Data Classes Connected"
    ax.text(5, 9.5, title, ha='center', va='center', fontsize=16, fontweight='bold', 
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9))
    
    # Add legend
    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['person'], 
                   markersize=15, label='Person'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['arrest'], 
                   markersize=12, label='Arrest'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['vehicle'], 
                   markersize=12, label='Vehicle'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['jail'], 
                   markersize=12, label='Jail System'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['property'], 
                   markersize=12, label='Property'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors['ems'], 
                   markersize=12, label='EMS Incident')
    ]
    
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(0.02, 0.98),
              frameon=True, fancybox=True, shadow=True)
    
    # Add summary statistics box
    stats_text = f"""Connection Summary:
• Total Records: {person_data['total_occurrences']}
• Arrests: {len(arrest_details)}
• Vehicles: {len(vehicle_details)}
• Jail Bookings: {len(booking_details)}
• Properties: {len(property_details)}
• EMS Incidents: {len(ems_details)}"""
    
    ax.text(0.02, 0.02, stats_text, transform=ax.transAxes, fontsize=10,
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9),
            verticalalignment='bottom')
    
    plt.tight_layout()
    plt.savefig('enhanced_network_graph.png', dpi=300, bbox_inches='tight', 
                facecolor='#f8f9fa', edgecolor='none')
    plt.show()
    
    print(f"Enhanced network graph saved to: enhanced_network_graph.png")
    
    # Print detailed information
    print(f"\nDetailed Information for {person_name}:")
    print(f"Person ID: {person_id}")
    print(f"Age: {person_details.get('age', 'Unknown')}")
    print(f"Address: {person_details.get('address', 'Unknown')}")
    
    if arrest_details:
        print(f"\nArrests ({len(arrest_details)}):")
        for i, arrest in enumerate(arrest_details[:3], 1):
            print(f"  {i}. Date: {arrest.get('arrest_date', 'Unknown')}")
            print(f"     Charge: {arrest.get('primary_charge', 'Unknown')}")
            print(f"     Location: {arrest.get('arrest_location', 'Unknown')}")
    
    if vehicle_details:
        print(f"\nVehicles ({len(vehicle_details)}):")
        for i, vehicle in enumerate(vehicle_details[:3], 1):
            print(f"  {i}. {vehicle.get('year', 'Unknown')} {vehicle.get('make', 'Unknown')} {vehicle.get('model', 'Unknown')}")
            print(f"     Plate: {vehicle.get('license_plate', 'Unknown')}")
            print(f"     VIN: {vehicle.get('vin', 'Unknown')[:10]}...")
    
    if booking_details:
        print(f"\nJail Bookings ({len(booking_details)}):")
        for i, booking in enumerate(booking_details[:3], 1):
            print(f"  {i}. Date: {booking.get('booking_datetime', 'Unknown')[:10]}")
            print(f"     Facility: {booking.get('facility_name', 'Unknown')}")
            print(f"     Status: {booking.get('booking_status', 'Unknown')}")

if __name__ == "__main__":
    create_enhanced_network_graph()
