import os
import json
import re
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/layout_data.json'

# Allowed enums in your schema that we can map to
ENUM_MAPPING = {
    'main area': 'Living Room',  # or choose another appropriate interior type
    'attached garage': 'Attached Garage',
    'detached garage': 'Detached Garage',
    'open porch': 'Porch',
    'patio': 'Patio',
    'porch': 'Porch',
    'water well and septic system': 'Utility Closet'
}

BEDROOM_ENUM = 'Bedroom'
FULL_BATH_ENUM = 'Full Bathroom'
HALF_BATH_ENUM = 'Half Bathroom / Powder Room'

def _new_layout(file_id, space_type, size_sqft=None, is_exterior=False, index=1):
    """Create a schema-compliant layout object."""
    return {
        "source_http_request": {
            "method": "GET",
            "url": f"https://example.com/property/{file_id}"
        },
        "request_identifier": file_id,
        "space_type": space_type,
        "space_index": index,
        "flooring_material_type": None,
        "size_square_feet": size_sqft,
        "floor_level": None,
        "has_windows": None,
        "window_design_type": None,
        "window_material_type": None,
        "window_treatment_type": None,
        "is_finished": None,
        "furnished": None,
        "paint_condition": None,
        "flooring_wear": None,
        "clutter_level": None,
        "visible_damage": None,
        "countertop_material": None,
        "cabinet_style": None,
        "fixture_finish_quality": None,
        "design_style": None,
        "natural_light_quality": None,
        "decor_elements": None,
        "pool_type": None,
        "pool_equipment": None,
        "spa_type": None,
        "safety_features": None,
        "view_type": None,
        "lighting_features": None,
        "condition_issues": None,
        "is_exterior": is_exterior,
        "pool_condition": None,
        "pool_surface_type": None,
        "pool_water_quality": None
    }

def _to_float(val):
    if not val:
        return None
    s = re.sub(r'[,\s]', '', str(val))
    try:
        return float(s)
    except:
        return None

def extract_layout_from_html(html, file_id):
    soup = BeautifulSoup(html, 'html.parser')
    layouts = []
    idx = 1

    # Bedrooms
    bed = soup.find(string=re.compile(r'Bed ?Rooms|No of Bedroom', re.I))
    if bed:
        tr = bed.find_parent('tr')
        if tr:
            val = tr.find_all('td')[-1].get_text(strip=True)
            try:
                n_bed = int(re.sub(r'[^\d]', '', val) or 0)
            except:
                n_bed = 0
            for _ in range(n_bed):
                layouts.append(_new_layout(file_id, BEDROOM_ENUM, index=idx))
                idx += 1

    # Full Baths
    full_bath = soup.find(string=re.compile(r'Full Bath|No of Bath', re.I))
    if full_bath:
        tr = full_bath.find_parent('tr')
        if tr:
            val = tr.find_all('td')[-1].get_text(strip=True)
            try:
                n_full = int(re.sub(r'[^\d]', '', val) or 0)
            except:
                n_full = 0
            for _ in range(n_full):
                layouts.append(_new_layout(file_id, FULL_BATH_ENUM, index=idx))
                idx += 1

    # Half Baths
    half_bath = soup.find(string=re.compile(r'Half Bath', re.I))
    if half_bath:
        tr = half_bath.find_parent('tr')
        if tr:
            val = tr.find_all('td')[-1].get_text(strip=True)
            try:
                n_half = int(re.sub(r'[^\d]', '', val) or 0)
            except:
                n_half = 0
            for _ in range(n_half):
                layouts.append(_new_layout(file_id, HALF_BATH_ENUM, index=idx))
                idx += 1

    # Improvements table
    for tbl in soup.find_all('table'):
        th_text = ' '.join(th.get_text(strip=True).lower() for th in tbl.find_all('th'))
        if all(x in th_text for x in ['type', 'description', 'class', 'year', 'sqft']):
            for tr in tbl.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) < 5:
                    continue
                desc = tds[1].get_text(" ", strip=True).lower()
                sqft = _to_float(tds[4].get_text(strip=True))
                if desc in ENUM_MAPPING:
                    space_type = ENUM_MAPPING[desc]
                    is_exterior = space_type in [
                        "Attached Garage", "Detached Garage", "Porch", "Patio"
                    ]
                    layouts.append(_new_layout(file_id, space_type, size_sqft=sqft,
                                               is_exterior=is_exterior, index=idx))
                    idx += 1
            break

    return layouts

def main():
    result = {}
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    for fname in os.listdir(INPUT_DIR):
        if not fname.lower().endswith('.html'):
            continue
        file_id = fname[:-5]
        with open(os.path.join(INPUT_DIR, fname), 'r', encoding='utf-8') as f:
            html = f.read()
        layouts = extract_layout_from_html(html, file_id)
        result[f'property_{file_id}'] = {'layouts': layouts}

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()

