import os
import json
import re
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/structure_data.json'

def extract_structure_from_html(html, file_id):
    soup = BeautifulSoup(html, 'html.parser')
    # Required fields from schema
    def safe_enum(val, allowed):
        if val is None or val == '' or val == 'N/A':
            return None
        if val in allowed:
            return val
        return None

    structure = {
        'request_identifier': str(file_id),
        'source_http_request': {
            'method': 'GET',
            'url': f'https://www.pbcgov.org/papa/Property/Details?parcelID={file_id}'
        },
        'architectural_style_type': None,  # Only if explicitly present in input
        'attachment_type': None,  # Only if explicitly present in input
        'exterior_wall_material_primary': None,
        'exterior_wall_material_secondary': None,
        'exterior_wall_condition': None,
        'exterior_wall_insulation_type': None,
        'flooring_material_primary': None,
        'flooring_material_secondary': None,
        'subfloor_material': None,
        'flooring_condition': None,
        'interior_wall_structure_material': None,
        'interior_wall_surface_material_primary': None,
        'interior_wall_surface_material_secondary': None,
        'interior_wall_finish_primary': None,
        'interior_wall_finish_secondary': None,
        'interior_wall_condition': None,
        'roof_covering_material': None,
        'roof_underlayment_type': None,
        'roof_structure_material': None,
        'roof_design_type': None,
        'roof_condition': None,
        'roof_age_years': None,
        'gutters_material': None,
        'gutters_condition': None,
        'roof_material_type': None,
        'foundation_type': None,
        'foundation_material': None,
        'foundation_waterproofing': None,
        'foundation_condition': None,
        'ceiling_structure_material': None,
        'ceiling_surface_material': None,
        'ceiling_insulation_type': None,
        'ceiling_height_average': None,
        'ceiling_condition': None,
        'exterior_door_material': None,
        'interior_door_material': None,
        'window_frame_material': None,
        'window_glazing_type': None,
        'window_operation_type': None,
        'window_screen_material': None,
        'primary_framing_material': None,
        'secondary_framing_material': None,
        'structural_damage_indicators': None
    }
    # Building type/attachment and architectural_style_type
    # Only extract if explicitly present in input (not inferred from use code)
    # Try to extract attachment_type from 'Property Use Code' or similar
    use_code = soup.find(string=re.compile(r'Property Use Code'))
    if use_code:
        val = use_code.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'CONDOMINIUM' in val.upper():
            structure['attachment_type'] = 'Attached'
        elif 'TOWNHOUSE' in val.upper():
            structure['attachment_type'] = 'Attached'
        elif 'DUPLEX' in val.upper():
            structure['attachment_type'] = 'SemiDetached'
        elif 'SINGLE FAMILY' in val.upper():
            structure['attachment_type'] = 'Detached'
        else:
            structure['attachment_type'] = None
    else:
        structure['attachment_type'] = None
    structure['architectural_style_type'] = None
    # Exterior wall
    ext_wall = soup.find(string=re.compile(r'Exterior Wall 1'))
    if ext_wall:
        val = ext_wall.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        # Use exact text if matches enum, else null
        if 'CB' in val.upper() or 'CONCRETE BLOCK' in val.upper():
            structure['exterior_wall_material_primary'] = 'Concrete Block'
            if 'STUCCO' in val.upper():
                structure['exterior_wall_material_secondary'] = 'Stucco Accent'
        elif 'STUCCO' in val.upper():
            structure['exterior_wall_material_primary'] = 'Stucco'
        else:
            structure['exterior_wall_material_primary'] = None
    # Secondary wall
    ext_wall2 = soup.find(string=re.compile(r'Exterior Wall 2'))
    if ext_wall2:
        val = ext_wall2.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'STUCCO' in val.upper():
            structure['exterior_wall_material_secondary'] = 'Stucco Accent'
        elif 'NONE' in val.upper():
            structure['exterior_wall_material_secondary'] = None  # Schema only allows enum, so None
        else:
            structure['exterior_wall_material_secondary'] = None
    # Roof
    roof_struct = soup.find(string=re.compile(r'Roof Structure'))
    if roof_struct:
        val = roof_struct.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'WOOD' in val.upper():
            structure['roof_structure_material'] = 'Wood Truss'
        elif 'CONCRETE' in val.upper():
            structure['roof_structure_material'] = 'Concrete Beam'
        else:
            structure['roof_structure_material'] = None
    roof_cover = soup.find(string=re.compile(r'Roof Cover'))
    if roof_cover:
        val = roof_cover.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'CONCRETE TILE' in val.upper():
            structure['roof_covering_material'] = 'Concrete Tile'
        elif 'MIN. ROOFING' in val.upper() or 'CORR/SH.M' in val.upper():
            structure['roof_covering_material'] = 'Metal Corrugated'
        else:
            structure['roof_covering_material'] = None

    # Flooring
    floor1 = soup.find(string=re.compile(r'Floor Type 1'))
    if floor1:
        val = floor1.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'CARPET' in val.upper():
            structure['flooring_material_primary'] = 'Carpet'
        elif 'TILE' in val.upper():
            structure['flooring_material_primary'] = 'Ceramic Tile'
        else:
            structure['flooring_material_primary'] = None
    floor2 = soup.find(string=re.compile(r'Floor Type 2'))
    if floor2:
        val = floor2.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'TILE' in val.upper():
            structure['flooring_material_secondary'] = 'Ceramic Tile'
        elif 'CARPET' in val.upper():
            structure['flooring_material_secondary'] = 'Carpet'
        else:
            structure['flooring_material_secondary'] = None
    # Interior wall surface material (primary)
    int_wall1 = soup.find(string=re.compile(r'Interior Wall 1'))
    if int_wall1:
        val = int_wall1.find_parent('tr').find_all('td')[-1].get_text(strip=True)
        if 'DRYWALL' in val.upper():
            structure['interior_wall_surface_material_primary'] = 'Drywall'
        elif 'PLASTER' in val.upper():
            structure['interior_wall_surface_material_primary'] = 'Plaster'
        else:
            structure['interior_wall_surface_material_primary'] = None
    # Year Built
    year_built = soup.find(string=re.compile(r'Year Built'))
    if year_built:
        possible_values = year_built.find_parent('tr').find_all('td')
        if possible_values:
            val = possible_values[-1].get_text(strip=True)
            try:
                structure['year_built'] = int(val)
            except:
                structure['year_built'] = None
        else:
            structure['year_built'] = None
    # All other fields remain None unless explicitly present in input
    return structure

def main():
    result = {}
    for fname in os.listdir(INPUT_DIR):
        if not fname.endswith('.html'):
            continue
        file_id = fname.replace('.html', '')
        with open(os.path.join(INPUT_DIR, fname), 'r', encoding='utf-8') as f:
            html = f.read()
        structure = extract_structure_from_html(html, file_id)
        result[f'property_{file_id}'] = structure
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
