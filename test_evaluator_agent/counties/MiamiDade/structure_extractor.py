import os
import json
from typing import Dict, Any

def extract_structure_from_property(property_json: Dict[str, Any], property_id: str) -> Dict[str, Any]:
    # Compose a dummy HTTP request for source_http_request
    source_http_request = {
        "method": "GET",
        "url": f"https://property-data.local/property/{property_id}"
    }
    # Default values for all required fields (null or enum first value)
    structure = {
        "source_http_request": source_http_request,
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
    # Example extraction logic (expand as needed)
    property_info = property_json.get('PropertyInfo', {})
    dor_desc = property_info.get('DORDescription', '')
    if 'SINGLE FAMILY' in dor_desc:
        structure['attachment_type'] = 'Detached'
        structure['architectural_style_type'] = 'Ranch'
    # Add more extraction logic here as needed
    return structure

def main():
    input_dir = './input'
    output_path = './owners/structure_data.json'
    result = {}
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            property_id = filename.replace('.json', '')
            with open(os.path.join(input_dir, filename), 'r') as f:
                property_json = json.load(f)
            structure = extract_structure_from_property(property_json, property_id)
            result[f'property_{property_id}'] = structure
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
