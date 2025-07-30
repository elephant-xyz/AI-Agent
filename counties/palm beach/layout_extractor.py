import os
import json
import re
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/layout_data.json'

# Map for space_type schema enums
BEDROOM_ENUM = 'Bedroom'
FULL_BATH_ENUM = 'Full Bathroom'
HALF_BATH_ENUM = 'Half Bathroom / Powder Room'

# For this schema, use 'Bedroom', 'Full Bathroom', 'Half Bathroom / Powder Room' as space_type

def extract_layout_from_html(html, file_id):
    soup = BeautifulSoup(html, 'html.parser')
    layouts = []
    # Bedrooms
    bed = soup.find(text=re.compile(r'Bed ?Rooms|No of Bedroom'))
    if bed:
        val = bed.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        try:
            n_bed = int(val)
        except:
            n_bed = 0
        for i in range(n_bed):
            layouts.append({
                'request_identifier': file_id,
                'source_http_request': {},
                'space_type': 'Bedroom',
                'flooring_material_type': None,
                'size_square_feet': None,
                'floor_level': None,
                'has_windows': None,
                'window_design_type': None,
                'window_material_type': None,
                'window_treatment_type': None,
                'is_finished': None,
                'furnished': None,
                'paint_condition': None,
                'flooring_wear': None,
                'clutter_level': None,
                'visible_damage': None,
                'countertop_material': None,
                'cabinet_style': None,
                'fixture_finish_quality': None,
                'design_style': None,
                'natural_light_quality': None,
                'decor_elements': None,
                'pool_type': None,
                'pool_equipment': None,
                'spa_type': None,
                'safety_features': None,
                'view_type': None,
                'lighting_features': None,
                'condition_issues': None,
                'is_exterior': False,
                'pool_condition': None,
                'pool_surface_type': None,
                'pool_water_quality': None
            })
    # Full Baths
    full_bath = soup.find(text=re.compile(r'Full Bath|No of Bath'))
    if full_bath:
        val = full_bath.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        try:
            n_full = int(val)
        except:
            n_full = 0
        for i in range(n_full):
            layouts.append({
                'request_identifier': file_id,
                'source_http_request': {},
                'space_type': 'Full Bathroom',
                'flooring_material_type': None,
                'size_square_feet': None,
                'floor_level': None,
                'has_windows': None,
                'window_design_type': None,
                'window_material_type': None,
                'window_treatment_type': None,
                'is_finished': None,
                'furnished': None,
                'paint_condition': None,
                'flooring_wear': None,
                'clutter_level': None,
                'visible_damage': None,
                'countertop_material': None,
                'cabinet_style': None,
                'fixture_finish_quality': None,
                'design_style': None,
                'natural_light_quality': None,
                'decor_elements': None,
                'pool_type': None,
                'pool_equipment': None,
                'spa_type': None,
                'safety_features': None,
                'view_type': None,
                'lighting_features': None,
                'condition_issues': None,
                'is_exterior': False,
                'pool_condition': None,
                'pool_surface_type': None,
                'pool_water_quality': None
            })
    # Half Baths
    half_bath = soup.find(text=re.compile(r'Half Bath'))
    if half_bath:
        val = half_bath.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        try:
            n_half = int(val)
        except:
            n_half = 0
        for i in range(n_half):
            layouts.append({
                'request_identifier': file_id,
                'source_http_request': {},
                'space_type': 'Half Bathroom / Powder Room',
                'flooring_material_type': None,
                'size_square_feet': None,
                'floor_level': None,
                'has_windows': None,
                'window_design_type': None,
                'window_material_type': None,
                'window_treatment_type': None,
                'is_finished': None,
                'furnished': None,
                'paint_condition': None,
                'flooring_wear': None,
                'clutter_level': None,
                'visible_damage': None,
                'countertop_material': None,
                'cabinet_style': None,
                'fixture_finish_quality': None,
                'design_style': None,
                'natural_light_quality': None,
                'decor_elements': None,
                'pool_type': None,
                'pool_equipment': None,
                'spa_type': None,
                'safety_features': None,
                'view_type': None,
                'lighting_features': None,
                'condition_issues': None,
                'is_exterior': False,
                'pool_condition': None,
                'pool_surface_type': None,
                'pool_water_quality': None
            })
    return layouts

def main():
    result = {}
    for fname in os.listdir(INPUT_DIR):
        if not fname.endswith('.html'):
            continue
        file_id = fname.replace('.html', '')
        with open(os.path.join(INPUT_DIR, fname), 'r', encoding='utf-8') as f:
            html = f.read()
        layouts = extract_layout_from_html(html, file_id)
        result[f'property_{file_id}'] = {'layouts': layouts}
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
