import os
import re
import json
from bs4 import BeautifulSoup

def clean_money(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return round(float(val), 2)
    try:
        return round(float(re.sub(r'[^\d.]', '', val)), 2) if val else None
    except Exception:
        return None

def clean_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        return None

def clean_str(val):
    if val is None:
        return None
    return str(val).strip()

def parse_date(val):
    if not val:
        return None
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", val)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    return val

def remove_null_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    if isinstance(data, dict) and all(v in (None, '', [], {}) for v in data.values()):
                        os.remove(path)
                except Exception:
                    continue

with open("./owners/addresses_mapping.json") as f:
    address_map = json.load(f)
with open("./owners/owners_schema.json") as f:
    owners_schema = json.load(f)
with open("./owners/layout_data.json") as f:
    layout_data = json.load(f)
with open("./owners/structure_data.json") as f:
    structure_data = json.load(f)
with open("./owners/utility_data.json") as f:
    utility_data = json.load(f)

os.makedirs("./data", exist_ok=True)
input_dir = "./input/"
input_files = [f for f in os.listdir(input_dir) if f.endswith(".html")]

for input_file in input_files:
    parcel_id = os.path.splitext(input_file)[0]
    property_dir = os.path.join("./data", parcel_id)
    os.makedirs(property_dir, exist_ok=True)
    with open(os.path.join(input_dir, input_file), encoding="utf-8") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    addr_key = f"property_{parcel_id}"
    address = address_map.get(addr_key, {}).get("address", {})
    # --- ADDRESS ---
    address_schema_fields = [
        "source_http_request", "request_identifier", "city_name", "country_code", "county_name", "latitude", "longitude", "plus_four_postal_code", "postal_code", "state_code", "street_name", "street_post_directional_text", "street_pre_directional_text", "street_number", "street_suffix_type", "unit_identifier", "township", "range", "section", "block"
    ]
    if address:
        for k in address_schema_fields:
            if k not in address:
                address[k] = None
        with open(os.path.join(property_dir, "address.json"), "w") as f:
            json.dump(address, f, indent=2)
    # --- PROPERTY ---
    property_json = {
        "source_http_request": address.get("source_http_request", {}),
        "request_identifier": parcel_id,
        "livable_floor_area": None,
        "number_of_units_type": None,
        "parcel_identifier": None,
        "property_legal_description_text": None,
        "property_structure_built_year": None,
        "property_type": None
    }
    pcn = soup.find(id="MainContent_lblPCN")
    if pcn:
        property_json["parcel_identifier"] = clean_str(pcn.text)
    legal = soup.find(id="MainContent_lblLegalDesc")
    if legal:
        property_json["property_legal_description_text"] = clean_str(legal.text)
    if addr_key in structure_data and structure_data[addr_key].get("year_built"):
        property_json["property_structure_built_year"] = structure_data[addr_key]["year_built"]
    # Extract number_of_units_type and lot_area_sqft from structural details
    number_of_units = None
    lot_area_sqft = None
    struct_tables = soup.find_all("table", class_="structural_elements")
    for struct_table in struct_tables:
        rows = struct_table.find_all("tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) == 2:
                label = tds[0].text.strip().lower()
                val = tds[1].text.strip()
                if ("number of units" in label or "units" in label) and val.isdigit():
                    number_of_units = int(val)
                if ("total square feet" in label or "area" == label) and val.isdigit():
                    lot_area_sqft = int(val)
    # Set number_of_units_type
    if number_of_units == 1:
        property_json["number_of_units_type"] = "One"
    elif number_of_units == 2:
        property_json["number_of_units_type"] = "Two"
    elif number_of_units == 3:
        property_json["number_of_units_type"] = "Three"
    elif number_of_units == 4:
        property_json["number_of_units_type"] = "Four"
    elif number_of_units and 2 <= number_of_units <= 4:
        property_json["number_of_units_type"] = "TwoToFour"
    # Set lot_area_sqft as string for property (schema allows string or null)
    if lot_area_sqft:
        property_json["livable_floor_area"] = str(lot_area_sqft)
    # Set property_type
    property_type_set = False
    # Try to extract from Subdivision
    subdiv = soup.find(id="MainContent_lblSubdiv")
    if subdiv:
        val = subdiv.text.strip().lower()
        if "condo" in val:
            property_json["property_type"] = "Condominium"
            property_type_set = True
        elif "townhouse" in val:
            property_json["property_type"] = "Townhouse"
            property_type_set = True
        elif "single family" in val:
            property_json["property_type"] = "SingleFamily"
            property_type_set = True
        elif "duplex" in val:
            property_json["property_type"] = "Duplex"
            property_type_set = True
        elif "cooperative" in val:
            property_json["property_type"] = "Cooperative"
            property_type_set = True
    # If not set, try to extract from Property Use Code
    if not property_type_set:
        # Find "Property Use Code" in structural_elements tables
        for struct_table in soup.find_all("table", class_="structural_elements"):
            rows = struct_table.find_all("tr")
            for row in rows:
                tds = row.find_all("td")
                if len(tds) == 2:
                    label = tds[0].text.strip().lower()
                    val = tds[1].text.strip().lower()
                    if "property use code" in label:
                        # Map code or text to property_type
                        if "condo" in val:
                            property_json["property_type"] = "Condominium"
                        elif "townhouse" in val:
                            property_json["property_type"] = "Townhouse"
                        elif "single family" in val:
                            property_json["property_type"] = "SingleFamily"
                        elif "duplex" in val:
                            property_json["property_type"] = "Duplex"
                        elif "cooperative" in val:
                            property_json["property_type"] = "Cooperative"
                        elif "0400" in val:
                            property_json["property_type"] = "Condominium"
                        elif "0100" in val:
                            property_json["property_type"] = "SingleFamily"
                        elif "0200" in val:
                            property_json["property_type"] = "Duplex"
                        elif "0300" in val:
                            property_json["property_type"] = "Triplex"
                        elif "0500" in val:
                            property_json["property_type"] = "Townhouse"
                        else:
                            property_json["property_type"] = None
                        property_type_set = True
                        break
            if property_type_set:
                break
    with open(os.path.join(property_dir, "property.json"), "w") as f:
        json.dump(property_json, f, indent=2)

    # --- SALES ---
    sales_tables = soup.find_all("h2", string=re.compile("Sales INFORMATION", re.I))
    sales_jsons = []
    sales_years = []
    if sales_tables:
        sales_table = sales_tables[0].find_next("table")
        if sales_table:
            rows = sales_table.find_all("tr")[1:]
            for i, row in enumerate(rows):
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue
                date = parse_date(cols[0].text.strip())
                price = clean_money(cols[1].text.strip())
                if price == 0:
                    price = None
                sales_json = {
                    "source_http_request": address.get("source_http_request", {}),
                    "request_identifier": f"{parcel_id}_sale_{i+1}",
                    "ownership_transfer_date": date,
                    "purchase_price_amount": price
                }
                sales_jsons.append(sales_json)
                sales_years.append(date[:4] if date else None)
                with open(os.path.join(property_dir, f"sales_{i+1}.json"), "w") as f:
                    json.dump(sales_json, f, indent=2)
    # --- TAXES ---
    tax_years = set()
    assessed = {}
    taxable = {}
    market = {}
    building = {}
    land = {}
    monthly_tax = {}
    for h2 in soup.find_all('h2', string=re.compile('Assessed & taxable values', re.I)):
        for tab in h2.find_all_next('div', class_='table_scroll'):
            ths = tab.find_all('th')
            if len(ths) > 1:
                years = [th.text.strip() for th in ths[1:]]
                trs = tab.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    if not tds:
                        continue
                    label = tds[0].text.strip().lower()
                    for j, year in enumerate(years):
                        tax_years.add(year)
                        val = clean_money(tds[j+1].text) if j+1 < len(tds) else None
                        if val == 0:
                            val = None
                        if 'assessed value' in label:
                            assessed[year] = val
                        elif 'taxable value' in label:
                            taxable[year] = val
    for h2 in soup.find_all('h2', string=re.compile('Appraisals', re.I)):
        for tab in h2.find_all_next('div', class_='table_scroll'):
            ths = tab.find_all('th')
            if len(ths) > 1:
                years = [th.text.strip() for th in ths[1:]]
                trs = tab.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    if not tds:
                        continue
                    label = tds[0].text.strip().lower()
                    for j, year in enumerate(years):
                        tax_years.add(year)
                        val = clean_money(tds[j+1].text) if j+1 < len(tds) else None
                        if val == 0:
                            val = None
                        if 'total market value' in label:
                            market[year] = val
                        elif 'improvement value' in label:
                            building[year] = val
                        elif 'land value' in label:
                            land[year] = val
    for h2 in soup.find_all('h2', string=re.compile('Taxes', re.I)):
        for tab in h2.find_all_next('div', class_='table_scroll'):
            ths = tab.find_all('th')
            if len(ths) > 1:
                years = [th.text.strip() for th in ths[1:]]
                trs = tab.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    if not tds:
                        continue
                    label = tds[0].text.strip().lower()
                    for j, year in enumerate(years):
                        if 'total tax' in label:
                            val = clean_money(tds[j+1].text) if j+1 < len(tds) else None
                            if val == 0:
                                val = None
                            monthly_tax[year] = val
    for year in sorted(tax_years):
        try:
            yint = int(year)
        except Exception:
            continue
        def safe_val(val):
            try:
                if val is None:
                    return 0.01
                v = float(val)
                v = round(v, 2)
                if v < 0:
                    return 0.01
                # Ensure at least 0.01 for positive, nonzero
                if v == 0:
                    return 0.01
                return v
            except Exception:
                return 0.01
        tax_json = {
            "source_http_request": address.get("source_http_request", {}),
            "request_identifier": f"{parcel_id}_tax_{year}",
            "tax_year": clean_int(year),
            "property_assessed_value_amount": safe_val(assessed.get(year)),
            "property_market_value_amount": safe_val(market.get(year)),
            "property_building_amount": safe_val(building.get(year)),
            "property_land_amount": safe_val(land.get(year)),
            "property_taxable_value_amount": safe_val(taxable.get(year)),
            "monthly_tax_amount": monthly_tax.get(year) if monthly_tax.get(year) is not None else 0.0,
            "period_end_date": None,
            "period_start_date": None
        }
        with open(os.path.join(property_dir, f"tax_{year}.json"), "w") as f:
            json.dump(tax_json, f, indent=2)
    # --- OWNERS (PERSON/COMPANY) ---
    if parcel_id in owners_schema:
        owners_by_date = owners_schema[parcel_id]["owners_by_date"]
        for i, (date, owners) in enumerate(owners_by_date.items()):
            for j, owner in enumerate(owners):
                if owner["type"] == "person":
                    person_json = {
                        "source_http_request": address.get("source_http_request", {}),
                        "request_identifier": f"{parcel_id}_person_{i+1}_{j+1}",
                        "birth_date": None,
                        "first_name": owner.get("first_name"),
                        "last_name": owner.get("last_name"),
                        "middle_name": owner.get("middle_name"),
                        "prefix_name": None,
                        "suffix_name": None,
                        "us_citizenship_status": None,
                        "veteran_status": None
                    }
                    with open(os.path.join(property_dir, f"person_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(person_json, f, indent=2)
                elif owner["type"] == "company":
                    company_json = {
                        "source_http_request": address.get("source_http_request", {}),
                        "request_identifier": f"{parcel_id}_company_{i+1}_{j+1}",
                        "name": owner.get("name")
                    }
                    with open(os.path.join(property_dir, f"company_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(company_json, f, indent=2)
    # --- RELATIONSHIP FILES ---
    if parcel_id in owners_schema:
        owners_by_date = owners_schema[parcel_id]["owners_by_date"]
        for i, (date, owners) in enumerate(owners_by_date.items()):
            sales_file = f"sales_{i+1}.json"
            for j, owner in enumerate(owners):
                if owner["type"] == "person":
                    rel = {
                        "to": {"/": f"./person_{i+1}_{j+1}.json"},
                        "from": {"/": f"./{sales_file}"}
                    }
                    with open(os.path.join(property_dir, f"relationship_sales_person_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(rel, f, indent=2)
                elif owner["type"] == "company":
                    rel = {
                        "to": {"/": f"./company_{i+1}_{j+1}.json"},
                        "from": {"/": f"./{sales_file}"}
                    }
                    with open(os.path.join(property_dir, f"relationship_sales_company_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(rel, f, indent=2)
    # --- STRUCTURE ---
    if addr_key in structure_data:
        struct = structure_data[addr_key].copy()
        if 'year_built' in struct:
            del struct['year_built']
        required_structure_fields = [
            "source_http_request", "request_identifier", "architectural_style_type", "attachment_type", "exterior_wall_material_primary", "exterior_wall_material_secondary", "exterior_wall_condition", "exterior_wall_insulation_type", "flooring_material_primary", "flooring_material_secondary", "subfloor_material", "flooring_condition", "interior_wall_structure_material", "interior_wall_surface_material_primary", "interior_wall_surface_material_secondary", "interior_wall_finish_primary", "interior_wall_finish_secondary", "interior_wall_condition", "roof_covering_material", "roof_underlayment_type", "roof_structure_material", "roof_design_type", "roof_condition", "roof_age_years", "gutters_material", "gutters_condition", "roof_material_type", "foundation_type", "foundation_material", "foundation_waterproofing", "foundation_condition", "ceiling_structure_material", "ceiling_surface_material", "ceiling_insulation_type", "ceiling_height_average", "ceiling_condition", "exterior_door_material", "interior_door_material", "window_frame_material", "window_glazing_type", "window_operation_type", "window_screen_material", "primary_framing_material", "secondary_framing_material", "structural_damage_indicators"
        ]
        for k in required_structure_fields:
            if k not in struct:
                struct[k] = None
        struct["source_http_request"] = address.get("source_http_request", {})
        struct["request_identifier"] = parcel_id
        with open(os.path.join(property_dir, "structure.json"), "w") as f:
            json.dump(struct, f, indent=2)
    # --- UTILITY ---
    if addr_key in utility_data:
        util = utility_data[addr_key]
        util["source_http_request"] = address.get("source_http_request", {})
        util["request_identifier"] = parcel_id
        with open(os.path.join(property_dir, "utility.json"), "w") as f:
            json.dump(util, f, indent=2)
    # --- LAYOUT ---
    bedroom_count = 0
    bathroom_count = 0
    half_bath_count = 0
    struct_tables = soup.find_all("table", class_="structural_elements")
    for struct_table in struct_tables:
        rows = struct_table.find_all("tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) == 2:
                label = tds[0].text.strip().lower()
                val = tds[1].text.strip()
                if ("bedroom" in label or "bed room" in label) and val.isdigit():
                    bedroom_count = int(val)
                if ("full bath" in label or ("bath" in label and "half" not in label)) and val.isdigit():
                    bathroom_count = int(val)
                if ("half bath" in label or ("half" in label and "bath" in label)) and val.isdigit():
                    half_bath_count = int(val)
    for f in os.listdir(property_dir):
        if f.startswith("layout_") and f.endswith(".json"):
            os.remove(os.path.join(property_dir, f))
    layout_idx = 1
    # Only create bedroom layouts if bedroom_count > 0
    if bedroom_count > 0:
        for i in range(bedroom_count):
            layout = {
                "source_http_request": address.get("source_http_request", {}),
                "request_identifier": f"{parcel_id}_layout_bedroom_{i+1}",
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
        }
        with open(os.path.join(property_dir, f"layout_{layout_idx}.json"), "w") as f:
            json.dump(layout, f, indent=2)
        layout_idx += 1
    for i in range(bathroom_count):
        layout = {
            "source_http_request": address.get("source_http_request", {}),
            "request_identifier": f"{parcel_id}_layout_bathroom_{i+1}",
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
        }
        with open(os.path.join(property_dir, f"layout_{layout_idx}.json"), "w") as f:
            json.dump(layout, f, indent=2)
        layout_idx += 1
    for i in range(half_bath_count):
        layout = {
            "source_http_request": address.get("source_http_request", {}),
            "request_identifier": f"{parcel_id}_layout_halfbath_{i+1}",
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
        }
        with open(os.path.join(property_dir, f"layout_{layout_idx}.json"), "w") as f:
            json.dump(layout, f, indent=2)
        layout_idx += 1
    # --- LOT ---
    lot_json = None
    lot_schema_fields = [
        "source_http_request", "request_identifier", "lot_type", "lot_length_feet", "lot_width_feet", "lot_area_sqft", "landscaping_features", "view", "fencing_type", "fence_height", "fence_length", "driveway_material", "driveway_condition", "lot_condition_issues"
    ]
    lot_json = {k: None for k in lot_schema_fields}
    lot_json["source_http_request"] = address.get("source_http_request", {})
    lot_json["request_identifier"] = parcel_id

    with open(os.path.join(property_dir, "lot.json"), "w") as f:
        json.dump(lot_json, f, indent=2)
    # --- REMOVE NULL/EMPTY FILES ---
    remove_null_files(property_dir)
# End of script
