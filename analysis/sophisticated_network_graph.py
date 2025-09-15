#!/usr/bin/env python3
"""
Create a sophisticated network graph with detailed attributes, better layout, and rich context.
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Rectangle, Circle
import numpy as np
from collections import defaultdict
import textwrap

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

def create_sophisticated_network_graph():
    """Create a sophisticated network graph with detailed attributes and better layout."""
    
    # Load the analysis results
    with open('person_connection_analysis.json', 'r') as f:
        results = json.load(f)
    
    person_connections = results['person_connections']
    
    # Find the most connected person
    most_connected = max(person_connections.items(), key=lambda x: x[1]['num_connections'])
    person_id, person_data = most_connected
    
    print(f"Creating sophisticated network graph for person: {person_id}")
    print(f"Number of connections: {person_data['num_connections']}")
    
    # Get detailed information
    person_details = get_person_details(person_id)
    arrest_details = get_arrest_details(person_id)
    vehicle_details = get_vehicle_details(person_id)
    booking_details = get_jail_booking_details(person_id)
    property_details = get_property_details(person_id)
    ems_details = get_ems_incident_details(person_id)
    
    # Create the visualization with multiple subplots
    fig = plt.figure(figsize=(24, 18))
    fig.patch.set_facecolor('#f5f5f5')
    
    # Main network graph
    ax_main = plt.subplot2grid((3, 4), (0, 0), colspan=3, rowspan=2)
    ax_main.set_facecolor('#ffffff')
    
    # Detail panels
    ax_details = plt.subplot2grid((3, 4), (0, 3), rowspan=2)
    ax_details.set_facecolor('#f8f9fa')
    
    # Summary panel
    ax_summary = plt.subplot2grid((3, 4), (2, 0), colspan=4)
    ax_summary.set_facecolor('#e9ecef')
    
    # Define colors and styles
    colors = {
        'person': '#FF6B6B',
        'arrest': '#E74C3C',
        'vehicle': '#3498DB',
        'jail': '#9B59B6',
        'property': '#F39C12',
        'ems': '#2ECC71',
        'connection': '#34495E',
        'background': '#ffffff'
    }
    
    # Main network visualization
    ax_main.set_xlim(0, 10)
    ax_main.set_ylim(0, 10)
    ax_main.axis('off')
    
    # Central person node
    person_x, person_y = 5, 5
    person_circle = Circle((person_x, person_y), 0.8, color=colors['person'], alpha=0.9, zorder=10)
    ax_main.add_patch(person_circle)
    
    # Person details
    person_name = f"{person_details.get('first_name', 'Unknown')} {person_details.get('last_name', 'Unknown')}"
    person_age = person_details.get('age', 'Unknown')
    person_dob = person_details.get('date_of_birth', 'Unknown')[:10] if person_details.get('date_of_birth') else 'Unknown'
    
    ax_main.text(person_x, person_y + 0.4, person_name, ha='center', va='center', 
                fontsize=14, fontweight='bold', color='white', zorder=11)
    ax_main.text(person_x, person_y, f"Age: {person_age}", ha='center', va='center', 
                fontsize=11, color='white', zorder=11)
    ax_main.text(person_x, person_y - 0.4, f"DOB: {person_dob}", ha='center', va='center', 
                fontsize=10, color='white', zorder=11)
    
    # Position data nodes around the person
    data_nodes = []
    angle_step = 2 * np.pi / len(person_data['data_classes'])
    
    for i, data_class in enumerate(person_data['data_classes']):
        angle = i * angle_step
        x = person_x + 3.2 * np.cos(angle)
        y = person_y + 3.2 * np.sin(angle)
        
        # Determine color based on data class
        if 'Arrest' in data_class:
            color = colors['arrest']
            icon = '🚨'
        elif 'Vehicle' in data_class:
            color = colors['vehicle']
            icon = '🚗'
        elif any(jail_class in data_class for jail_class in ['Jail', 'Bail']):
            color = colors['jail']
            icon = '🏛️'
        elif 'Property' in data_class:
            color = colors['property']
            icon = '🏠'
        elif 'EMS' in data_class:
            color = colors['ems']
            icon = '🚑'
        else:
            color = '#95A5A6'
            icon = '📄'
        
        # Create data node with icon
        data_circle = Circle((x, y), 0.7, color=color, alpha=0.8, zorder=5)
        ax_main.add_patch(data_circle)
        
        # Add icon
        ax_main.text(x, y, icon, ha='center', va='center', fontsize=16, zorder=6)
        
        # Add connection line with arrow
        ax_main.annotate('', xy=(x, y), xytext=(person_x, person_y),
                        arrowprops=dict(arrowstyle='->', lw=3, color=colors['connection'], alpha=0.7))
        
        # Add data class label
        ax_main.text(x, y + 1.0, data_class, ha='center', va='center', 
                    fontsize=10, fontweight='bold', color='black', zorder=6,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        
        data_nodes.append((data_class, x, y, color))
    
    # Add title
    ax_main.text(5, 9.2, f"Network Graph: {person_name}", ha='center', va='center', 
                fontsize=18, fontweight='bold', 
                bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9))
    
    # Details panel
    ax_details.axis('off')
    ax_details.set_xlim(0, 1)
    ax_details.set_ylim(0, 1)
    
    # Add detailed information
    details_y = 0.95
    ax_details.text(0.5, details_y, "DETAILED INFORMATION", ha='center', va='top', 
                   fontsize=14, fontweight='bold', color='#2C3E50')
    
    details_y -= 0.08
    
    # Person details
    ax_details.text(0.05, details_y, "👤 PERSON:", ha='left', va='top', 
                   fontsize=12, fontweight='bold', color=colors['person'])
    details_y -= 0.05
    ax_details.text(0.1, details_y, f"Name: {person_name}", ha='left', va='top', fontsize=10)
    details_y -= 0.04
    ax_details.text(0.1, details_y, f"Age: {person_age}", ha='left', va='top', fontsize=10)
    details_y -= 0.04
    ax_details.text(0.1, details_y, f"DOB: {person_dob}", ha='left', va='top', fontsize=10)
    details_y -= 0.04
    ax_details.text(0.1, details_y, f"Address: {person_details.get('address', 'Unknown')[:30]}...", 
                   ha='left', va='top', fontsize=10)
    details_y -= 0.06
    
    # Arrest details
    if arrest_details:
        ax_details.text(0.05, details_y, "🚨 ARRESTS:", ha='left', va='top', 
                       fontsize=12, fontweight='bold', color=colors['arrest'])
        details_y -= 0.05
        for i, arrest in enumerate(arrest_details[:2], 1):
            ax_details.text(0.1, details_y, f"{i}. {arrest.get('arrest_date', 'Unknown')[:10]}", 
                           ha='left', va='top', fontsize=10)
            details_y -= 0.04
            charge = arrest.get('primary_charge', 'Unknown')
            wrapped_charge = textwrap.fill(charge, width=25)
            ax_details.text(0.1, details_y, f"   {wrapped_charge}", ha='left', va='top', fontsize=9)
            details_y -= 0.06
    
    # Vehicle details
    if vehicle_details:
        ax_details.text(0.05, details_y, "🚗 VEHICLES:", ha='left', va='top', 
                       fontsize=12, fontweight='bold', color=colors['vehicle'])
        details_y -= 0.05
        for i, vehicle in enumerate(vehicle_details[:2], 1):
            make_model = f"{vehicle.get('year', 'Unknown')} {vehicle.get('make', 'Unknown')} {vehicle.get('model', 'Unknown')}"
            ax_details.text(0.1, details_y, f"{i}. {make_model}", ha='left', va='top', fontsize=10)
            details_y -= 0.04
            ax_details.text(0.1, details_y, f"   Plate: {vehicle.get('license_plate', 'Unknown')}", 
                           ha='left', va='top', fontsize=9)
            details_y -= 0.05
    
    # Jail booking details
    if booking_details:
        ax_details.text(0.05, details_y, "🏛️ JAIL BOOKINGS:", ha='left', va='top', 
                       fontsize=12, fontweight='bold', color=colors['jail'])
        details_y -= 0.05
        for i, booking in enumerate(booking_details[:2], 1):
            booking_date = booking.get('booking_datetime', 'Unknown')[:10]
            ax_details.text(0.1, details_y, f"{i}. {booking_date}", ha='left', va='top', fontsize=10)
            details_y -= 0.04
            facility = booking.get('facility_name', 'Unknown')
            ax_details.text(0.1, details_y, f"   Facility: {facility[:20]}...", 
                           ha='left', va='top', fontsize=9)
            details_y -= 0.05
    
    # Summary panel
    ax_summary.axis('off')
    ax_summary.set_xlim(0, 1)
    ax_summary.set_ylim(0, 1)
    
    # Create summary statistics
    summary_text = f"""
    CONNECTION SUMMARY FOR {person_name.upper()}
    
    📊 Total Data Classes: {person_data['num_connections']} | 📈 Total Records: {person_data['total_occurrences']} | 🆔 Person ID: {person_id}
    
    🚨 Arrests: {len(arrest_details)} | 🚗 Vehicles: {len(vehicle_details)} | 🏛️ Jail Bookings: {len(booking_details)} | 🏠 Properties: {len(property_details)} | 🚑 EMS Incidents: {len(ems_details)}
    
    Connected Data Classes: {', '.join(person_data['data_classes'])}
    """
    
    ax_summary.text(0.5, 0.5, summary_text, ha='center', va='center', 
                   fontsize=12, fontweight='bold', 
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9))
    
    plt.tight_layout()
    plt.savefig('sophisticated_network_graph.png', dpi=300, bbox_inches='tight', 
                facecolor='#f5f5f5', edgecolor='none')
    plt.show()
    
    print(f"Sophisticated network graph saved to: sophisticated_network_graph.png")
    
    # Print comprehensive summary
    print(f"\n{'='*80}")
    print(f"COMPREHENSIVE ANALYSIS FOR {person_name.upper()}")
    print(f"{'='*80}")
    print(f"Person ID: {person_id}")
    print(f"Total Connections: {person_data['num_connections']} data classes")
    print(f"Total Records: {person_data['total_occurrences']} occurrences")
    print(f"Data Classes: {', '.join(person_data['data_classes'])}")
    
    if arrest_details:
        print(f"\n🚨 ARREST RECORDS ({len(arrest_details)}):")
        for i, arrest in enumerate(arrest_details, 1):
            print(f"  {i}. Date: {arrest.get('arrest_date', 'Unknown')}")
            print(f"     Charge: {arrest.get('primary_charge', 'Unknown')}")
            print(f"     Location: {arrest.get('arrest_location', 'Unknown')}")
            print(f"     Agency: {arrest.get('arresting_agency', 'Unknown')}")
    
    if vehicle_details:
        print(f"\n🚗 VEHICLE RECORDS ({len(vehicle_details)}):")
        for i, vehicle in enumerate(vehicle_details, 1):
            print(f"  {i}. {vehicle.get('year', 'Unknown')} {vehicle.get('make', 'Unknown')} {vehicle.get('model', 'Unknown')}")
            print(f"     Plate: {vehicle.get('license_plate', 'Unknown')}")
            print(f"     VIN: {vehicle.get('vin', 'Unknown')}")
            print(f"     Color: {vehicle.get('color', 'Unknown')}")
    
    if booking_details:
        print(f"\n🏛️ JAIL BOOKING RECORDS ({len(booking_details)}):")
        for i, booking in enumerate(booking_details, 1):
            print(f"  {i}. Date: {booking.get('booking_datetime', 'Unknown')}")
            print(f"     Facility: {booking.get('facility_name', 'Unknown')}")
            print(f"     Status: {booking.get('booking_status', 'Unknown')}")
            print(f"     Housing: {booking.get('housing_assignment', 'Unknown')}")

if __name__ == "__main__":
    create_sophisticated_network_graph()
