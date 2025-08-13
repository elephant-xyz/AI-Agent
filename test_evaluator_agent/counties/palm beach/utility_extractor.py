import os
import json
import re
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/utility_data.json'

def extract_utility_from_html(html, file_id):
    soup = BeautifulSoup(html, 'html.parser')
    utility = {
        'request_identifier': file_id,
        'source_http_request': {},
        'cooling_system_type': None,
        'heating_system_type': None,
        'public_utility_type': None,
        'sewer_type': None,
        'water_source_type': None,
        'plumbing_system_type': None,
        'plumbing_system_type_other_description': None,
        'electrical_panel_capacity': None,
        'electrical_wiring_type': None,
        'hvac_condensing_unit_present': None,
        'electrical_wiring_type_other_description': None,
        'solar_panel_present': False,
        'solar_panel_type': None,
        'solar_panel_type_other_description': None,
        'smart_home_features': None,
        'smart_home_features_other_description': None,
        'hvac_unit_condition': None,
        'solar_inverter_visible': False,
        'hvac_unit_issues': None
    }
    # HVAC
    ac = soup.find(text=re.compile(r'Air Condition'))
    if ac:
        val = ac.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'AC' in val.upper() or 'CENTRAL' in val.upper():
            utility['cooling_system_type'] = 'CentralAir'
        elif 'DUCTLESS' in val.upper():
            utility['cooling_system_type'] = 'Ductless'
    heat = soup.find(text=re.compile(r'Heat Type'))
    if heat:
        val = heat.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'FORCED AIR' in val.upper():
            utility['heating_system_type'] = 'ElectricFurnace'
        elif 'ELECTRIC' in val.upper():
            utility['heating_system_type'] = 'Electric'
    # Plumbing
    # Not directly available, so leave as None
    # Electrical
    # Not directly available, so leave as None
    # Utilities
    # Only extract if explicitly present in input
    utility['public_utility_type'] = None
    utility['sewer_type'] = None
    utility['water_source_type'] = None
    return utility

def main():
    result = {}
    for fname in os.listdir(INPUT_DIR):
        if not fname.endswith('.html'):
            continue
        file_id = fname.replace('.html', '')
        with open(os.path.join(INPUT_DIR, fname), 'r', encoding='utf-8') as f:
            html = f.read()
        utility = extract_utility_from_html(html, file_id)
        result[f'property_{file_id}'] = utility
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
