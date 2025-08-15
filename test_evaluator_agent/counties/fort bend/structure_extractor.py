import os
import json
import re
from bs4 import BeautifulSoup

INPUT_DIR = './input/'
OUTPUT_FILE = './owners/structure_data.json'

# Helper: get text from a table row by header
def get_table_value(soup, header_regex):
    th = soup.find(lambda tag: tag.name in ['th', 'td'] and re.search(header_regex, tag.get_text(), re.I))
    if th:
        tr = th.find_parent('tr')
        tds = tr.find_all('td')
        # Find the first <td> that is not the header itself
        for td in tds:
            if td != th:
                val = td.get_text(strip=True)
                if val:
                    return val
        # If not found, try next sibling
        sib = th.find_next_sibling('td')
        if sib:
            return sib.get_text(strip=True)
    return None

def extract_structure_from_html(html, file_id):
    soup = BeautifulSoup(html, 'html.parser')
    # All fields required by schema
    structure = {
        'request_identifier': str(file_id),
        'source_http_request': {
            'method': 'GET',
            'url': f'https://www.pbcgov.org/papa/Property/Details?parcelID={file_id}'
        },
        'architectural_style_type': None,
        'attachment_type': None,
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
    # Example: extract year built from improvement table
    # Find the 'Property Improvement - Building' panel
    panel = soup.find('div', class_='panel-heading', string=re.compile(r'Improvement.*Building', re.I))
    year_built = None
    if panel:
        table = panel.find_next('table')
        if table:
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 4:
                    if re.search(r'Main Area', cells[1].get_text(), re.I):
                        try:
                            year_built = int(cells[3].get_text(strip=True))
                        except Exception:
                            year_built = None
    # If not found, fallback to any Year Built field
    if not year_built:
        val = get_table_value(soup, r'Year Built')
        try:
            year_built = int(val)
        except Exception:
            year_built = None
    structure['year_built'] = year_built
    # All other fields remain None unless explicitly present in input
    return structure

def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
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
