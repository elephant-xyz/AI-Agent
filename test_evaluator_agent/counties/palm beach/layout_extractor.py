import os, re, json
from bs4 import BeautifulSoup
# Inline utility functions to avoid import issues
def find_area_table(soup):
    """
    Find the 'SUBAREA AND SQUARE FOOTAGE' table in the HTML.
    
    Args:
        soup: BeautifulSoup object of the HTML content
        
    Returns:
        The area table element if found, None otherwise
    """
    all_h3 = soup.find_all('h3')
    area_table = None
    for h3 in all_h3:
        if 'SUBAREA AND SQUARE FOOTAGE' in h3.get_text(strip=True):
            area_table = h3
            break
    
    if area_table:
        area_table = area_table.find_next('table')
        return area_table
    
    return None

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/layout_data.json'

# Allowed enums (subset)
ALLOWED_SPACE_TYPES = {
    "Living Room","Family Room","Great Room","Dining Room","Kitchen","Breakfast Nook","Pantry",
    "Primary Bedroom","Secondary Bedroom","Guest Bedroom","Children's Bedroom","Nursery",
    "Full Bathroom","Three-Quarter Bathroom","Half Bathroom / Powder Room","En-Suite Bathroom",
    "Jack-and-Jill Bathroom","Primary Bathroom","Laundry Room","Mudroom","Closet","Bedroom",
    "Walk-in Closet","Mechanical Room","Storage Room","Server/IT Closet","Home Office","Library",
    "Den","Study","Media Room / Home Theater","Game Room","Home Gym","Music Room",
    "Craft Room / Hobby Room","Prayer Room / Meditation Room","Safe Room / Panic Room","Wine Cellar",
    "Bar Area","Greenhouse","Attached Garage","Detached Garage","Carport","Workshop","Storage Loft",
    "Porch","Screened Porch","Sunroom","Deck","Patio","Pergola","Balcony","Terrace","Gazebo",
    "Pool House","Outdoor Kitchen","Lobby / Entry Hall","Common Room","Utility Closet","Elevator Lobby",
    "Mail Room","Janitor's Closet","Pool Area","Indoor Pool","Outdoor Pool","Hot Tub / Spa Area","Shed"
}

BEDROOM_ENUM = 'Bedroom'
FULL_BATH_ENUM = 'Full Bathroom'
HALF_BATH_ENUM = 'Half Bathroom / Powder Room'

def _to_int(s):
    s = (s or '').strip()
    s = re.sub(r'[^\d]', '', s)
    return int(s) if s.isdigit() else 0

def _add_layout(layouts, file_id, space_type, source_http_request=None, size_sqft=None, is_exterior=False):
    layouts.append({
        'source_http_request': source_http_request or {},
        'request_identifier': str(file_id),
        'space_type': space_type,
        'space_index': len(layouts) + 1,
        'flooring_material_type': None,
        'size_square_feet': size_sqft,
        'floor_level': None,
        'has_windows': None,
        'window_design_type': None,
        'window_material_type': None,
        'window_treatment_type': None,
        'is_finished': True,
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

def extract_layout_from_html(html, file_id, source_http_request=None):
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
            _add_layout(layouts, file_id, BEDROOM_ENUM, source_http_request, is_exterior=False)

    # Full Baths
    full_bath = soup.find(string=re.compile(r'\b(Full Baths?|No of Bath\(s\)|No of Bath)\b', re.I))
    if full_bath:
        try:
            val = full_bath.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        except Exception:
            val = ''
        n_full = _to_int(val)
        for _ in range(n_full):
            _add_layout(layouts, file_id, FULL_BATH_ENUM, source_http_request, is_exterior=False)

    # Half Baths
    half_bath = soup.find(string=re.compile(r'\b(Half Baths?|No of Half Bath\(s\))\b', re.I))
    if half_bath:
        try:
            val = half_bath.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        except Exception:
            val = ''
        n_half = _to_int(val)
        for _ in range(n_half):
            _add_layout(layouts, file_id, HALF_BATH_ENUM, source_http_request, is_exterior=False)

    # Extract area information from SUBAREA AND SQUARE FOOTAGE table
    area_table = find_area_table(soup)
    if area_table:
            for row in area_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code_desc = cells[0].get_text(strip=True)
                    sqft_text = cells[1].get_text(strip=True)
                    
                    try:
                        sqft = int(sqft_text)
                    except (ValueError, TypeError):
                        continue
                    
                    # FOP - Finished Open Porch (exterior space) - each gets its own layout entry
                    if 'FOP' in code_desc and 'Finished Open Porch' in code_desc:
                        _add_layout(layouts, file_id, 'Porch', source_http_request, size_sqft=sqft, is_exterior=True)
                    
                    # FGR - Finished Garage
                    elif 'FGR' in code_desc and 'Finished Garage' in code_desc:
                        _add_layout(layouts, file_id, 'Attached Garage', source_http_request, size_sqft=sqft, is_exterior=False)

    return layouts

def main():
    result = {}
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Read property_seed.json if it exists to get source_http_request
    property_seed = {}
    if os.path.exists('./property_seed.json'):
        try:
            with open('./property_seed.json', 'r', encoding='utf-8') as f:
                property_seed = json.load(f)
        except Exception:
            pass
    
    for fname in os.listdir(INPUT_DIR):
        if not fname.lower().endswith('.html'):
            continue
        file_id = os.path.splitext(fname)[0]
        
        # Get source_http_request for this property
        source_http_request = property_seed.get("source_http_request", {})
        
        with open(os.path.join(INPUT_DIR, fname), 'r', encoding='utf-8', errors='ignore') as f:
            html = f.read()
        layouts = extract_layout_from_html(html, file_id, source_http_request)
        result[f'property_{file_id}'] = {'layouts': layouts}
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()

