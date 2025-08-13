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


def get_property_id(filename):
    base = os.path.basename(filename)
    return base.split('.')[0]


def create_layout_object(property_id, space_type, space_index):
    """Create a layout object with the specified space_type and all other fields as None"""
    return {
        "source_http_request": {
            "method": "GET",
            "url": f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={property_id}"
        },
        "request_identifier": property_id,
        "space_index": space_index,
        "space_type": space_type,
        "flooring_material_type": None,
        "size_square_feet": None,
        "floor_level": None,
        "has_windows": None,
        "window_design_type": None,
        "window_material_type": None,
        "window_treatment_type": None,
        "is_finished": False,
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
        "is_exterior": False,
        "pool_condition": None,
        "pool_surface_type": None,
        "pool_water_quality": None
    }


def extract_bedroom_bathroom_counts(html):
    """Extract bedroom and bathroom counts from HTML using precise pattern matching"""

    # Method 1: Look for the specific table structure with "Total Bedrooms / Bathrooms"
    total_pattern = r'<th[^>]*>\s*Total\s+Bedrooms?\s*/\s*Bathrooms?[\s\S]*?</th>\s*<td[^>]*>\s*([^<]+)\s*</td>'
    total_match = re.search(total_pattern, html, re.IGNORECASE)

    if total_match:
        cell_content = total_match.group(1).strip()

        # Check if it's in "X / Y.Z" format
        split_match = re.match(r'^(\d+)\s*/\s*([\d.]+)$', cell_content)
        if split_match:
            bedrooms = int(split_match.group(1))
            bathrooms = int(float(split_match.group(2)))
            return bedrooms, bathrooms

        # Check if it's just a single number (like "0")
        single_match = re.match(r'^(\d+)$', cell_content)
        if single_match:
            num = int(single_match.group(1))
            # If it's 0, assume 0 bedrooms and 0 bathrooms
            # If it's another single number, we might need to interpret it differently
            return num, 0 if num == 0 else num

    # Method 2: Look for individual "Bedrooms" and "Bathrooms" entries in building details
    soup = BeautifulSoup(html, 'html.parser')

    bedrooms = 0
    bathrooms = 0

    # Look for individual bedroom/bathroom entries in building characteristics
    # This pattern appears in the building details sections
    building_rows = soup.find_all('tr')

    for row in building_rows:
        th = row.find('th')
        td = row.find('td')

        if th and td:
            th_text = th.get_text(strip=True).lower()
            td_text = td.get_text(strip=True)

            if 'bedroom' in th_text and not 'bathroom' in th_text:
                bedroom_match = re.search(r'(\d+)', td_text)
                if bedroom_match:
                    bedrooms = max(bedrooms, int(bedroom_match.group(1)))

            elif 'bathroom' in th_text and not 'bedroom' in th_text:
                bathroom_match = re.search(r'([\d.]+)', td_text)
                if bathroom_match:
                    bathrooms = max(bathrooms, int(float(bathroom_match.group(1))))

    if bedrooms > 0 or bathrooms > 0:
        print(f"  Found individual entries: {bedrooms} bedrooms, {bathrooms} bathrooms")
        return bedrooms, bathrooms

    print(f"  No bedroom/bathroom information found")
    return 0, 0


def extract_layouts_from_html(html, property_id):
    """Extract layout information from HTML content"""
    if not html.strip():
        return []

    layouts = []

    # Extract bedroom and bathroom counts
    n_bed, n_full = extract_bedroom_bathroom_counts(html)

    # Create bedroom layout objects
    for i in range(n_bed):
        layouts.append(create_layout_object(property_id, BEDROOM_ENUM, i + 1))

    # Create full bathroom layout objects
    for i in range(n_full):
        layouts.append(create_layout_object(property_id, FULL_BATH_ENUM, i + 1))

    # Extract Half Bathrooms (these might be listed separately)
    soup = BeautifulSoup(html, 'html.parser')
    half_bath_patterns = [
        r'Half\s*Baths?',
        r'Powder\s*Rooms?',
        r'1/2\s*Baths?'
    ]

    n_half = 0
    for pattern in half_bath_patterns:
        half_bath_element = soup.find(text=re.compile(pattern, re.IGNORECASE))
        if half_bath_element:
            try:
                parent_row = half_bath_element.find_parent('tr')
                if parent_row:
                    cells = parent_row.find_all('td')
                    if cells:
                        val = cells[-1].get_text(strip=True)
                        half_match = re.search(r'\d+', val)
                        if half_match:
                            n_half = int(half_match.group())
                            break
            except (AttributeError, ValueError, TypeError):
                continue

    # Create half bathroom layout objects
    for i in range(n_half):
        layouts.append(create_layout_object(property_id, HALF_BATH_ENUM, i + 1))

    return layouts


def main():
    """Main function to process all HTML files and generate layout data"""
    if not os.path.exists(INPUT_DIR):
        print(f"Input directory {INPUT_DIR} does not exist!")
        return

    data = {}
    processed_files = 0


    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.html'):
            filepath = os.path.join(INPUT_DIR, filename)

            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                property_id = get_property_id(filename)
                layouts = extract_layouts_from_html(content, property_id)

                data[f"property_{property_id}"] = {"layouts": layouts}

                # Print summary for this property
                if layouts:
                    layout_summary = {}
                    for layout in layouts:
                        space_type = layout['space_type']
                        layout_summary[space_type] = layout_summary.get(space_type, 0) + 1


                processed_files += 1

            except Exception as e:
                print(f"  Error processing {filename}: {str(e)}")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Save results
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


    # Print summary statistics

    # Print breakdown by space type
    space_type_counts = {}
    for prop_data in data.values():
        for layout in prop_data['layouts']:
            space_type = layout['space_type']
            space_type_counts[space_type] = space_type_counts.get(space_type, 0) + 1



if __name__ == '__main__':
    main()