import os
import json
from typing import Dict, Any

def extract_layout_from_property(property_json: Dict[str, Any], property_id: str) -> Dict[str, Any]:
    source_http_request = {
        "method": "GET",
        "url": f"https://property-data.local/property/{property_id}"
    }
    property_info = property_json.get('PropertyInfo', {})
    layouts = []
    # Extract bedrooms
    for _ in range(property_info.get('BedroomCount', 0)):
        layouts.append({
            "source_http_request": source_http_request,
            "request_identifier": property_id,
            "space_type": "Bedroom",
            "flooring_material_type": None,
            "size_square_feet": None,
            "floor_level": None,
            "has_windows": None,
            "window_design_type": None,
            "window_material_type": None,
            "window_treatment_type": None,
            "is_finished": True,
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
        })
    # Extract full bathrooms
    for _ in range(property_info.get('BathroomCount', 0)):
        layouts.append({
            "source_http_request": source_http_request,
            "request_identifier": property_id,
            "space_type": "Full Bathroom",
            "flooring_material_type": None,
            "size_square_feet": None,
            "floor_level": None,
            "has_windows": None,
            "window_design_type": None,
            "window_material_type": None,
            "window_treatment_type": None,
            "is_finished": True,
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
        })
    # Extract half bathrooms
    for _ in range(property_info.get('HalfBathroomCount', 0)):
        layouts.append({
            "source_http_request": source_http_request,
            "request_identifier": property_id,
            "space_type": "Half Bathroom / Powder Room",
            "flooring_material_type": None,
            "size_square_feet": None,
            "floor_level": None,
            "has_windows": None,
            "window_design_type": None,
            "window_material_type": None,
            "window_treatment_type": None,
            "is_finished": True,
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
        })
    return {"layouts": layouts}

def main():
    input_dir = './input'
    output_path = './owners/layout_data.json'
    result = {}
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            property_id = filename.replace('.json', '')
            with open(os.path.join(input_dir, filename), 'r') as f:
                property_json = json.load(f)
            layout = extract_layout_from_property(property_json, property_id)
            result[f'property_{property_id}'] = layout
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
