import os
import json

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/utility_data.json'

def get_property_id(filename):
    base = os.path.basename(filename)
    return base.split('.')[0]

def extract_utility_from_html(html, property_id):
    # Stub: Fill with nulls for schema compliance
    utility = {
        "source_http_request": {
            "method": "GET",
            "url": f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={property_id}"
        },
        "request_identifier": property_id,
        "cooling_system_type": None,
        "heating_system_type": None,
        "public_utility_type": None,
        "sewer_type": None,
        "water_source_type": None,
        "plumbing_system_type": None,
        "plumbing_system_type_other_description": None,
        "electrical_panel_capacity": None,
        "electrical_wiring_type": None,
        "hvac_condensing_unit_present": None,
        "electrical_wiring_type_other_description": None,
        "solar_panel_present": False,
        "solar_panel_type": None,
        "solar_panel_type_other_description": None,
        "smart_home_features": None,
        "smart_home_features_other_description": None,
        "hvac_unit_condition": None,
        "solar_inverter_visible": False,
        "hvac_unit_issues": None
    }
    return utility

def main():
    data = {}
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.html') or filename.endswith('.json'):
            path = os.path.join(INPUT_DIR, filename)
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            property_id = get_property_id(filename)
            if filename.endswith('.html'):
                utility = extract_utility_from_html(content, property_id)
            else:
                utility = extract_utility_from_html('', property_id)
            data[f"property_{property_id}"] = utility
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
