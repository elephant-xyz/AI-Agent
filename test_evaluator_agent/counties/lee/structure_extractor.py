import os
import re
import json
from bs4 import BeautifulSoup

# Directory containing input files
INPUT_DIR = './input/'
OUTPUT_FILE = './owners/structure_data.json'

# Helper function to extract property id from filename

def get_property_id(filename):
    base = os.path.basename(filename)
    return base.split('.')[0]

# Helper function to extract structure data from HTML

def extract_structure_from_html(html, property_id):
    # This is a stub. Real extraction logic should be implemented based on the HTML structure.
    # For now, we will fill with nulls and required fields for schema compliance.
    # You can add regex or BeautifulSoup logic here to extract real values.
    # Example: soup = BeautifulSoup(html, 'html.parser')
    structure = {
        "source_http_request": {
            "method": "GET",
            "url": f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={property_id}"
        },
        "request_identifier": property_id,
        "architectural_style_type": None,
        "attachment_type": None,
        "exterior_wall_material_primary": None,
        "exterior_wall_material_secondary": None,
        "exterior_wall_condition": None,
        "exterior_wall_insulation_type": None,
        "flooring_material_primary": None,
        "flooring_material_secondary": None,
        "subfloor_material": None,
        "flooring_condition": None,
        "interior_wall_structure_material": None,
        "interior_wall_surface_material_primary": None,
        "interior_wall_surface_material_secondary": None,
        "interior_wall_finish_primary": None,
        "interior_wall_finish_secondary": None,
        "interior_wall_condition": None,
        "roof_covering_material": None,
        "roof_underlayment_type": None,
        "roof_structure_material": None,
        "roof_design_type": None,
        "roof_condition": None,
        "roof_age_years": None,
        "gutters_material": None,
        "gutters_condition": None,
        "roof_material_type": None,
        "foundation_type": None,
        "foundation_material": None,
        "foundation_waterproofing": None,
        "foundation_condition": None,
        "ceiling_structure_material": None,
        "ceiling_surface_material": None,
        "ceiling_insulation_type": None,
        "ceiling_height_average": None,
        "ceiling_condition": None,
        "exterior_door_material": None,
        "interior_door_material": None,
        "window_frame_material": None,
        "window_glazing_type": None,
        "window_operation_type": None,
        "window_screen_material": None,
        "primary_framing_material": None,
        "secondary_framing_material": None,
        "structural_damage_indicators": None
    }
    return structure

def main():
    data = {}
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.html') or filename.endswith('.json'):
            path = os.path.join(INPUT_DIR, filename)
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            property_id = get_property_id(filename)
            if filename.endswith('.html'):
                structure = extract_structure_from_html(content, property_id)
            else:
                # For JSON input, you could add a similar extraction function
                structure = extract_structure_from_html('', property_id)
            data[f"property_{property_id}"] = structure
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

if __name__ == '__main__':
    main()
