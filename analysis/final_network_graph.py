#!/usr/bin/env python3
"""
Create the final, most polished network graph with detailed attributes and professional styling.
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
    persons = load_json_file('persons.json')
    for person in persons:
        if person.get('person_id') == person_id:
            return person
    return {}

def get_arrest_details(person_id: str) -> list:
    """Get arrest details for a person."""
    arrests = load_json_file('arrests.json')
    return [arrest for arrest in arrests if arrest.get('person_id') == person_id]

def get_vehicle_details(person_id: str) -> list:
    """Get vehicle details for a person."""
    vehicles = load_json_file('vehicles.json')
    return [vehicle for vehicle in vehicles if vehicle.get('owner_person_id') == person_id]

def get_jail_booking_details(person_id: str) -> list:
    """Get jail booking details for a person."""
    bookings = load_json_file('jail_bookings.json')
    return [booking for booking in bookings if booking.get('person_id') == person_id]

def get_property_details(person_id: str) -> list:
    """Get property details for a person."""
    properties = load_json_file('properties.json')
    return [prop for prop in properties if prop.get('owner_person_id') == person_id]

def get_ems_incident_details(person_id: str) -> list:
    """Get EMS incident details for a person."""
    incidents = load_json_file('ems_incidents.json')
    return [incident for incident in incidents if incident.get('patient_person_id') == person_id]

def create_final_network_graph():
    """Create the final, most polished network graph."""
    
    # Load the analysis results
    with open('person_connection_analysis.json', 'r') as f:
        results = json.load(f)
    
    person_connections = results['person_connections']
    
    # Find the most connected person
    most_connected = max(person_connections.items(), key=lambda x: x[1]['num_connections'])
    person_id, person_data = most_connected
    
    print(f"Creating final network graph for person: {person_id}")
    print(f"Number of connections: {person_data['num_connections']}")
    
    # Get detailed information
    person_details = get_person_details(person_id)
    arrest_details = get_arrest_details(person_id)
    vehicle_details = get_vehicle_details(person_id)
    booking_details = get_jail_booking_details(person_id)
    property_details = get_property_details(person_id)
    ems_details = get_ems_incident_details(person_id)
    
    # Create the visualization with professional styling
    fig = plt.figure(figsize=(24, 16))
    fig.patch.set_facecolor('#f8f9fa')
    
    # Main network graph
    ax_main = plt.subplot2grid((3, 4), (0, 0), colspan=3, rowspan=2)
    ax_main.set_facecolor('#ffffff')
    
    # Detail panels
    ax_details = plt.subplot2grid((3, 4), (0, 3), rowspan=2)
    ax_details.set_facecolor('#f8f9fa')
    
    # Summary panel
    ax_summary = plt.subplot2grid((3, 4), (2, 0), colspan=4)
    ax_summary.set_facecolor('#e9ecef')
    
    # Professional color scheme
    colors = {
        'person': '#E74C3C',
        'arrest': '#C0392B',
        'vehicle': '#3498DB',
        'jail': '#8E44AD',
        'property': '#F39C12',
        'ems': '#27AE60',
        'connection': '#2C3E50',
        'background': '#ffffff',
        'text': '#2C3E50',
        'light_gray': '#BDC3C7'
    }
    
    # Main network visualization
    ax_main.set_xlim(0, 10)
    ax_main.set_ylim(0, 10)
    ax_main.axis('off')
    
    # Central person node with professional styling
    person_x, person_y = 5, 5
    person_circle = Circle((person_x, person_y), 0.8, color=colors['person'], alpha=0.9, zorder=10)
    ax_main.add_patch(person_circle)
    
    # Add subtle shadow effect
    shadow_circle = Circle((person_x + 0.05, person_y - 0.05), 0.8, color='black', alpha=0.2, zorder=9)
    ax_main.add_patch(shadow_circle)
    
    # Person details with better typography
    person_name = f"{person_details.get('first_name', 'Unknown')} {person_details.get('last_name', 'Unknown')}"
    person_age = person_details.get('age', 'Unknown')
    person_dob = person_details.get('date_of_birth', 'Unknown')[:10] if person_details.get('date_of_birth') else 'Unknown'
    
    ax_main.text(person_x, person_y + 0.4, person_name, ha='center', va='center', 
                fontsize=14, fontweight='bold', color='white', zorder=11)
    ax_main.text(person_x, person_y, f"Age: {person_age}", ha='center', va='center', 
                fontsize=11, color='white', zorder=11)
    ax_main.text(person_x, person_y - 0.4, f"DOB: {person_dob}", ha='center', va='center', 
                fontsize=10, color='white', zorder=11)
    
    # Position data nodes around the person with better spacing
    data_nodes = []
    angle_step = 2 * np.pi / len(person_data['data_classes'])
    
    for i, data_class in enumerate(person_data['data_classes']):
        angle = i * angle_step
        x = person_x + 3.5 * np.cos(angle)
        y = person_y + 3.5 * np.sin(angle)
        
        # Determine color and label based on data class
        if 'Arrest' in data_class:
            color = colors['arrest']
            label = 'ARREST'
        elif 'Vehicle' in data_class:
            color = colors['vehicle']
            label = 'VEHICLE'
        elif any(jail_class in data_class for jail_class in ['Jail', 'Bail']):
            color = colors['jail']
            label = 'JAIL'
        elif 'Property' in data_class:
            color = colors['property']
            label = 'PROPERTY'
        elif 'EMS' in data_class:
            color = colors['ems']
            label = 'EMS'
        else:
            color = colors['light_gray']
            label = 'OTHER'
        
        # Create data node with shadow
        shadow_circle = Circle((x + 0.03, y - 0.03), 0.6, color='black', alpha=0.2, zorder=4)
        ax_main.add_patch(shadow_circle)
        
        data_circle = Circle((x, y), 0.6, color=color, alpha=0.9, zorder=5)
        ax_main.add_patch(data_circle)
        
        # Add connection line with professional styling
        ax_main.annotate('', xy=(x, y), xytext=(person_x, person_y),
                        arrowprops=dict(arrowstyle='->', lw=4, color=colors['connection'], 
                                      alpha=0.8, connectionstyle="arc3,rad=0.1"))
        
        # Add data class label with professional styling
        ax_main.text(x, y + 1.1, label, ha='center', va='center', 
                    fontsize=10, fontweight='bold', color=colors['text'], zorder=6,
                    bbox=dict(boxstyle='round,pad=0.4', facecolor='white', alpha=0.95, 
                            edgecolor=color, linewidth=2))
        
        data_nodes.append((data_class, x, y, color))
    
    # Add professional title
    ax_main.text(5, 9.3, f"PERSON CONNECTION NETWORK", ha='center', va='center', 
                fontsize=20, fontweight='bold', color=colors['text'])
    ax_main.text(5, 8.8, f"{person_name}", ha='center', va='center', 
                fontsize=16, fontweight='bold', color=colors['person'])
    
    # Details panel with professional styling
    ax_details.axis('off')
    ax_details.set_xlim(0, 1)
    ax_details.set_ylim(0, 1)
    
    # Add detailed information with better organization
    details_y = 0.95
    ax_details.text(0.5, details_y, "DETAILED RECORDS", ha='center', va='top', 
                   fontsize=16, fontweight='bold', color=colors['text'])
    
    details_y -= 0.08
    
    # Person details section
    ax_details.text(0.05, details_y, "PERSON INFORMATION", ha='left', va='top', 
                   fontsize=12, fontweight='bold', color=colors['person'])
    details_y -= 0.06
    ax_details.text(0.1, details_y, f"Name: {person_name}", ha='left', va='top', fontsize=10)
    details_y -= 0.04
    ax_details.text(0.1, details_y, f"Age: {person_age}", ha='left', va='top', fontsize=10)
    details_y -= 0.04
    ax_details.text(0.1, details_y, f"DOB: {person_dob}", ha='left', va='top', fontsize=10)
    details_y -= 0.04
    address = person_details.get('address', 'Unknown')
    if len(address) > 35:
        address = address[:35] + "..."
    ax_details.text(0.1, details_y, f"Address: {address}", ha='left', va='top', fontsize=10)
    details_y -= 0.06
    
    # Arrest details section
    if arrest_details:
        ax_details.text(0.05, details_y, "ARREST RECORDS", ha='left', va='top', 
                       fontsize=12, fontweight='bold', color=colors['arrest'])
        details_y -= 0.06
        for i, arrest in enumerate(arrest_details[:2], 1):
            arrest_date = arrest.get('arrest_date', 'Unknown')[:10]
            ax_details.text(0.1, details_y, f"{i}. Date: {arrest_date}", ha='left', va='top', fontsize=10)
            details_y -= 0.04
            charge = arrest.get('primary_charge', 'Unknown')
            if len(charge) > 25:
                charge = charge[:25] + "..."
            ax_details.text(0.1, details_y, f"   Charge: {charge}", ha='left', va='top', fontsize=9)
            details_y -= 0.05
    
    # Vehicle details section
    if vehicle_details:
        ax_details.text(0.05, details_y, "VEHICLE RECORDS", ha='left', va='top', 
                       fontsize=12, fontweight='bold', color=colors['vehicle'])
        details_y -= 0.06
        for i, vehicle in enumerate(vehicle_details[:2], 1):
            make_model = f"{vehicle.get('year', 'Unknown')} {vehicle.get('make', 'Unknown')} {vehicle.get('model', 'Unknown')}"
            if len(make_model) > 25:
                make_model = make_model[:25] + "..."
            ax_details.text(0.1, details_y, f"{i}. {make_model}", ha='left', va='top', fontsize=10)
            details_y -= 0.04
            plate = vehicle.get('license_plate', 'Unknown')
            ax_details.text(0.1, details_y, f"   Plate: {plate}", ha='left', va='top', fontsize=9)
            details_y -= 0.05
    
    # Jail booking details section
    if booking_details:
        ax_details.text(0.05, details_y, "JAIL BOOKINGS", ha='left', va='top', 
                       fontsize=12, fontweight='bold', color=colors['jail'])
        details_y -= 0.06
        for i, booking in enumerate(booking_details[:2], 1):
            booking_date = booking.get('booking_datetime', 'Unknown')[:10]
            ax_details.text(0.1, details_y, f"{i}. Date: {booking_date}", ha='left', va='top', fontsize=10)
            details_y -= 0.04
            facility = booking.get('facility_name', 'Unknown')
            if len(facility) > 20:
                facility = facility[:20] + "..."
            ax_details.text(0.1, details_y, f"   Facility: {facility}", ha='left', va='top', fontsize=9)
            details_y -= 0.05
    
    # Summary panel with professional styling
    ax_summary.axis('off')
    ax_summary.set_xlim(0, 1)
    ax_summary.set_ylim(0, 1)
    
    # Create comprehensive summary
    summary_text = f"""
    CONNECTION ANALYSIS SUMMARY
    
    Person: {person_name} | ID: {person_id} | Total Data Classes: {person_data['num_connections']} | Total Records: {person_data['total_occurrences']}
    
    Records by Category: Arrests: {len(arrest_details)} | Vehicles: {len(vehicle_details)} | Jail Bookings: {len(booking_details)} | Properties: {len(property_details)} | EMS Incidents: {len(ems_details)}
    
    Connected Data Classes: {', '.join(person_data['data_classes'])}
    """
    
    ax_summary.text(0.5, 0.5, summary_text, ha='center', va='center', 
                   fontsize=14, fontweight='bold', color=colors['text'],
                   bbox=dict(boxstyle='round,pad=0.8', facecolor='white', alpha=0.95, 
                           edgecolor=colors['light_gray'], linewidth=1))
    
    plt.tight_layout()
    plt.savefig('final_network_graph.png', dpi=300, bbox_inches='tight', 
                facecolor='#f8f9fa', edgecolor='none')
    plt.show()
    
    print(f"Final network graph saved to: final_network_graph.png")
    
    # Print comprehensive summary
    print(f"\n{'='*100}")
    print(f"COMPREHENSIVE PERSON CONNECTION ANALYSIS")
    print(f"{'='*100}")
    print(f"Person: {person_name}")
    print(f"Person ID: {person_id}")
    print(f"Total Connections: {person_data['num_connections']} data classes")
    print(f"Total Records: {person_data['total_occurrences']} occurrences")
    print(f"Data Classes: {', '.join(person_data['data_classes'])}")
    
    if arrest_details:
        print(f"\nARREST RECORDS ({len(arrest_details)}):")
        for i, arrest in enumerate(arrest_details, 1):
            print(f"  {i}. Date: {arrest.get('arrest_date', 'Unknown')}")
            print(f"     Charge: {arrest.get('primary_charge', 'Unknown')}")
            print(f"     Location: {arrest.get('arrest_location', 'Unknown')}")
            print(f"     Agency: {arrest.get('arresting_agency', 'Unknown')}")
    
    if vehicle_details:
        print(f"\nVEHICLE RECORDS ({len(vehicle_details)}):")
        for i, vehicle in enumerate(vehicle_details, 1):
            print(f"  {i}. {vehicle.get('year', 'Unknown')} {vehicle.get('make', 'Unknown')} {vehicle.get('model', 'Unknown')}")
            print(f"     Plate: {vehicle.get('license_plate', 'Unknown')}")
            print(f"     VIN: {vehicle.get('vin', 'Unknown')}")
            print(f"     Color: {vehicle.get('color', 'Unknown')}")
    
    if booking_details:
        print(f"\nJAIL BOOKING RECORDS ({len(booking_details)}):")
        for i, booking in enumerate(booking_details, 1):
            print(f"  {i}. Date: {booking.get('booking_datetime', 'Unknown')}")
            print(f"     Facility: {booking.get('facility_name', 'Unknown')}")
            print(f"     Status: {booking.get('booking_status', 'Unknown')}")
            print(f"     Housing: {booking.get('housing_assignment', 'Unknown')}")

if __name__ == "__main__":
    create_final_network_graph()
