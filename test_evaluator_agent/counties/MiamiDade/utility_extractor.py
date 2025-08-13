import os
import json
from typing import Dict, Any

def extract_utility_from_property(property_json: Dict[str, Any], property_id: str) -> Dict[str, Any]:
    source_http_request = {
        "method": "GET",
        "url": f"https://property-data.local/property/{property_id}"
    }
    utility = {
        "source_http_request": source_http_request,
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
    # Example extraction logic (expand as needed)
    # No clear mapping in sample input, so leave as default/null/False
    return utility

def main():
    input_dir = './input'
    output_path = './owners/utility_data.json'
    result = {}
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            property_id = filename.replace('.json', '')
            with open(os.path.join(input_dir, filename), 'r') as f:
                property_json = json.load(f)
            utility = extract_utility_from_property(property_json, property_id)
            result[f'property_{property_id}'] = utility
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
