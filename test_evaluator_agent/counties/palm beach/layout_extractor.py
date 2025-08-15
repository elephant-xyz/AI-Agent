import os, re, json
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/layout_data.json'

# Allowed enums (subset)
ALLOWED_SPACE_TYPES = {
    "Living Room","Family Room","Great Room","Dining Room","Kitchen","Breakfast Nook","Pantry",
    "Primary Bedroom","Secondary Bedroom","Guest Bedroom","Children’s Bedroom","Nursery",
    "Full Bathroom","Three-Quarter Bathroom","Half Bathroom / Powder Room","En-Suite Bathroom",
    "Jack-and-Jill Bathroom","Primary Bathroom","Laundry Room","Mudroom","Closet","Bedroom",
    "Walk-in Closet","Mechanical Room","Storage Room","Server/IT Closet","Home Office","Library",
    "Den","Study","Media Room / Home Theater","Game Room","Home Gym","Music Room",
    "Craft Room / Hobby Room","Prayer Room / Meditation Room","Safe Room / Panic Room","Wine Cellar",
    "Bar Area","Greenhouse","Attached Garage","Detached Garage","Carport","Workshop","Storage Loft",
    "Porch","Screened Porch","Sunroom","Deck","Patio","Pergola","Balcony","Terrace","Gazebo",
    "Pool House","Outdoor Kitchen","Lobby / Entry Hall","Common Room","Utility Closet","Elevator Lobby",
    "Mail Room","Janitor’s Closet","Pool Area","Indoor Pool","Outdoor Pool","Hot Tub / Spa Area","Shed"
}

BEDROOM_ENUM = 'Bedroom'
FULL_BATH_ENUM = 'Full Bathroom'
HALF_BATH_ENUM = 'Half Bathroom / Powder Room'

def _to_int(s):
    s = (s or '').strip()
    s = re.sub(r'[^\d]', '', s)
    return int(s) if s.isdigit() else 0

def _add_layout(layouts, file_id, space_type, size_sqft=None, is_exterior=False):
    st = space_type if space_type in ALLOWED_SPACE_TYPES else None
    layouts.append({
        'request_identifier': file_id,
        'source_http_request': {},
        'space_type': st,
        'flooring_material_type': None,
        'size_square_feet': size_sqft,
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
        'is_exterior': is_exterior,
        'pool_condition': None,
        'pool_surface_type': None,
        'pool_water_quality': None
    })

def extract_layout_from_html(html, file_id):
    soup = BeautifulSoup(html, 'html.parser')
    layouts = []

    # Bedrooms
    bed = soup.find(string=re.compile(r'\b(Bed ?Rooms|No of Bedroom\(s\)|No of Bedroom)\b', re.I))
    if bed:
        try:
            val = bed.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        except Exception:
            val = ''
        n_bed = _to_int(val)
        for _ in range(n_bed):
            _add_layout(layouts, file_id, BEDROOM_ENUM, is_exterior=False)

    # Full Baths
    full_bath = soup.find(string=re.compile(r'\b(Full Bath|No of Bath\(s\)|No of Bath)\b', re.I))
    if full_bath:
        try:
            val = full_bath.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        except Exception:
            val = ''
        n_full = _to_int(val)
        for _ in range(n_full):
            _add_layout(layouts, file_id, FULL_BATH_ENUM, is_exterior=False)

    # Half Baths
    half_bath = soup.find(string=re.compile(r'\b(Half Bath|No of Half Bath\(s\))\b', re.I))
    if half_bath:
        try:
            val = half_bath.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        except Exception:
            val = ''
        n_half = _to_int(val)
        for _ in range(n_half):
            _add_layout(layouts, file_id, HALF_BATH_ENUM, is_exterior=False)

    # SUBAREA rows like FOP/FGR
    for table in soup.find_all('table'):
        header_text = ' '.join(th.get_text(' ', strip=True) for th in table.find_all('th'))
        if re.search(r'Code Description', header_text, re.I) and re.search(r'Square\s*Foot', header_text, re.I):
            for tr in table.find_all('tr'):
                tds = tr.find_all(['td'])
                if len(tds) < 2:
                    continue
                code_desc = tds[0].get_text(' ', strip=True)
                sqft_text = tds[-1].get_text(' ', strip=True)
                sqft = _to_int(sqft_text)
                if sqft <= 0:
                    continue

                if re.search(r'\bFOP\b.*Finished\s+Open\s+Porch', code_desc, re.I):
                    _add_layout(layouts, file_id, 'Porch', size_sqft=sqft, is_exterior=True)
                elif re.search(r'\bFGR\b.*Finished\s+Garage', code_desc, re.I):
                    _add_layout(layouts, file_id, 'Attached Garage', size_sqft=sqft, is_exterior=False)

    return layouts

def main():
    result = {}
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    for fname in os.listdir(INPUT_DIR):
        if not fname.lower().endswith('.html'):
            continue
        file_id = os.path.splitext(fname)[0]
        with open(os.path.join(INPUT_DIR, fname), 'r', encoding='utf-8', errors='ignore') as f:
            html = f.read()
        layouts = extract_layout_from_html(html, file_id)
        result[f'property_{file_id}'] = {'layouts': layouts}
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()

