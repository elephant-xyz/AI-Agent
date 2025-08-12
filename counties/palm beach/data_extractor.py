import os
import re
import json
import csv
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


def load_property_seed_json():
    """Load property seed JSON data"""
    try:
        with open('property_seed.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Warning: property_seed.json not found")
        return {}


def load_unnormalized_address_json():
    """Load unnormalized address JSON data"""
    try:
        with open('unnormalized_address.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Warning: unnormalized_address.json not found")
        return {}

def main():
    # Load owner and structure data
    with open("./owners/owners_schema.json") as f:
        owners_schema = json.load(f)
    with open("./owners/layout_data.json") as f:
        layout_data = json.load(f)
    with open("./owners/structure_data.json") as f:
        structure_data = json.load(f)
    with open("./owners/utility_data.json") as f:
        utility_data = json.load(f)

    # Load property and address data instead of seed.csv
    property_seed = load_property_seed_json()
    address_data = load_unnormalized_address_json()

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

        # Get source info from property_seed
        request_identifier = property_seed.get("parcel_id", parcel_id)
        source_http_request = property_seed.get("source_http_request", {
            "method": "GET",
            "url": f"https://property-data.local/property/{parcel_id}"
        })

        # --- PROPERTY ---
        property_json = {
            "source_http_request": source_http_request,
            "request_identifier": request_identifier,
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
                        "source_http_request": source_http_request,
                        "request_identifier": f"{request_identifier}_sale_{i + 1}",
                        "ownership_transfer_date": date,
                        "purchase_price_amount": price
                    }
                    sales_jsons.append(sales_json)
                    sales_years.append(date[:4] if date else None)
                    with open(os.path.join(property_dir, f"sales_{i + 1}.json"), "w") as f:
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
                            val = clean_money(tds[j + 1].text) if j + 1 < len(tds) else None
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
                            val = clean_money(tds[j + 1].text) if j + 1 < len(tds) else None
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
                                val = clean_money(tds[j + 1].text) if j + 1 < len(tds) else None
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
                        return 0.0
                    v = float(val)
                    if v < 0:
                        return 0.0
                    # Round to 2 decimal places, and if 0, set to 0.01
                    v = round(v, 2)
                    if v == 0:
                        return 0.0
                    return v
                except Exception:
                    return 0.0


            # Ensure property_taxable_value_amount is a positive number with at most 2 decimal places
            taxable_val = safe_val(taxable.get(year))
            assessed_val = safe_val(assessed.get(year))
            market_val = safe_val(market.get(year))
            building_val = safe_val(building.get(year))
            land_val = safe_val(land.get(year))
            monthly_tax_val = safe_val(monthly_tax.get(year))
            tax_json = {
                "source_http_request": source_http_request,
                "request_identifier": f"{request_identifier}_tax_{year}",
                "tax_year": clean_int(year),
                "property_assessed_value_amount": assessed_val,
                "property_market_value_amount": market_val,
                "property_building_amount": building_val,
                "property_land_amount": land_val,
                "property_taxable_value_amount": taxable_val,
                "monthly_tax_amount": monthly_tax_val,
                "period_end_date": None,
                "period_start_date": None
            }
            with open(os.path.join(property_dir, f"tax_{year}.json"), "w") as f:
                json.dump(tax_json, f, indent=2)

        # --- OWNERS (PERSON/COMPANY) ---
        if parcel_id in owners_schema:
            owners_by_date = owners_schema[parcel_id]["owners_by_date"]
            for i, (date, owners) in enumerate(owners_by_date.items()):
                person_count = 0
                company_count = 0
                seen_persons = set()
                seen_companies = set()
                for j, owner in enumerate(owners):
                    if owner["type"] == "person":
                        person_key = (owner.get("first_name"), owner.get("last_name"), owner.get("middle_name"))
                        if person_key in seen_persons:
                            continue
                        seen_persons.add(person_key)
                        person_count += 1
                        person_json = {
                            "source_http_request": source_http_request,
                            "request_identifier": f"{request_identifier}_person_{i + 1}_{person_count}",
                            "birth_date": None,
                            "first_name": owner.get("first_name"),
                            "last_name": owner.get("last_name"),
                            "middle_name": owner.get("middle_name"),
                            "prefix_name": None,
                            "suffix_name": None,
                            "us_citizenship_status": None,
                            "veteran_status": None
                        }
                        with open(os.path.join(property_dir, f"person_{i + 1}_{person_count}.json"), "w") as f:
                            json.dump(person_json, f, indent=2)
                    elif owner["type"] == "company":
                        company_key = owner.get("name")
                        if company_key in seen_companies:
                            continue
                        seen_companies.add(company_key)
                        company_count += 1
                        company_json = {
                            "source_http_request": source_http_request,
                            "request_identifier": f"{request_identifier}_company_{i + 1}_{company_count}",
                            "name": owner.get("name")
                        }
                        with open(os.path.join(property_dir, f"company_{i + 1}_{company_count}.json"), "w") as f:
                            json.dump(company_json, f, indent=2)

        # --- RELATIONSHIP FILES ---
        if parcel_id in owners_schema:
            owners_by_date = owners_schema[parcel_id]["owners_by_date"]
            for i, (date, owners) in enumerate(owners_by_date.items()):
                sales_file = f"sales_{i + 1}.json"
                person_count = 0
                company_count = 0
                seen_persons = set()
                seen_companies = set()
                for j, owner in enumerate(owners):
                    if owner["type"] == "person":
                        person_key = (owner.get("first_name"), owner.get("last_name"), owner.get("middle_name"))
                        if person_key in seen_persons:
                            continue
                        seen_persons.add(person_key)
                        person_count += 1
                        rel = {
                            "to": {"/": f"./person_{i + 1}_{person_count}.json"},
                            "from": {"/": f"./{sales_file}"}
                        }
                        with open(os.path.join(property_dir, f"relationship_sales_person_{i + 1}_{person_count}.json"),
                                  "w") as f:
                            json.dump(rel, f, indent=2)
                    elif owner["type"] == "company":
                        company_key = owner.get("name")
                        if company_key in seen_companies:
                            continue
                        seen_companies.add(company_key)
                        company_count += 1
                        rel = {
                            "to": {"/": f"./company_{i + 1}_{company_count}.json"},
                            "from": {"/": f"./{sales_file}"}
                        }
                        with open(os.path.join(property_dir, f"relationship_sales_company_{i + 1}_{company_count}.json"),
                                  "w") as f:
                            json.dump(rel, f, indent=2)

        # --- STRUCTURE ---
        if addr_key in structure_data:
            struct = structure_data[addr_key].copy()
            if 'year_built' in struct:
                del struct['year_built']
            required_structure_fields = [
                "source_http_request", "request_identifier", "architectural_style_type", "attachment_type",
                "exterior_wall_material_primary", "exterior_wall_material_secondary", "exterior_wall_condition",
                "exterior_wall_insulation_type", "flooring_material_primary", "flooring_material_secondary",
                "subfloor_material", "flooring_condition", "interior_wall_structure_material",
                "interior_wall_surface_material_primary", "interior_wall_surface_material_secondary",
                "interior_wall_finish_primary", "interior_wall_finish_secondary", "interior_wall_condition",
                "roof_covering_material", "roof_underlayment_type", "roof_structure_material", "roof_design_type",
                "roof_condition", "roof_age_years", "gutters_material", "gutters_condition", "roof_material_type",
                "foundation_type", "foundation_material", "foundation_waterproofing", "foundation_condition",
                "ceiling_structure_material", "ceiling_surface_material", "ceiling_insulation_type",
                "ceiling_height_average", "ceiling_condition", "exterior_door_material", "interior_door_material",
                "window_frame_material", "window_glazing_type", "window_operation_type", "window_screen_material",
                "primary_framing_material", "secondary_framing_material", "structural_damage_indicators"
            ]
            for k in required_structure_fields:
                if k not in struct:
                    struct[k] = None
            struct["source_http_request"] = source_http_request
            struct["request_identifier"] = request_identifier
            with open(os.path.join(property_dir, "structure.json"), "w") as f:
                json.dump(struct, f, indent=2)

        # --- UTILITY ---
        if addr_key in utility_data:
            util = utility_data[addr_key]
            util["source_http_request"] = source_http_request
            util["request_identifier"] = request_identifier
            with open(os.path.join(property_dir, "utility.json"), "w") as f:
                json.dump(util, f, indent=2)

        # --- LAYOUT ---
        # Count bedrooms, full baths, half baths from structural_elements table
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
        # Remove any existing layout files
        for f in os.listdir(property_dir):
            if f.startswith("layout_") and f.endswith(".json"):
                os.remove(os.path.join(property_dir, f))
        # Create layout files for each bedroom, full bath, half bath
        for i in range(bedroom_count):
            layout = {
                "source_http_request": source_http_request,
                "request_identifier": f"{request_identifier}_layout_bedroom_{i + 1}",
                "space_type": "Bedroom",
                "space_index": i + 1,
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
            with open(os.path.join(property_dir, f"layout_bedroom_{i + 1}.json"), "w") as f:
                json.dump(layout, f, indent=2)
        for i in range(bathroom_count):
            layout = {
                "source_http_request": source_http_request,
                "request_identifier": f"{request_identifier}_layout_bathroom_{i + 1}",
                "space_type": "Full Bathroom",
                "space_index": i + 1,
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
            with open(os.path.join(property_dir, f"layout_bathroom_{i + 1}.json"), "w") as f:
                json.dump(layout, f, indent=2)
        for i in range(half_bath_count):
            layout = {
                "source_http_request": source_http_request,
                "request_identifier": f"{request_identifier}_layout_halfbath_{i + 1}",
                "space_type": "Half Bathroom / Powder Room",
                "space_index": i + 1,
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
            with open(os.path.join(property_dir, f"layout_halfbath_{i + 1}.json"), "w") as f:
                json.dump(layout, f, indent=2)
        # Ensure total layout files = bedrooms + full baths + half baths
        layout_files = [f for f in os.listdir(property_dir) if f.startswith("layout_") and f.endswith(".json")]
        assert len(
            layout_files) == bedroom_count + bathroom_count + half_bath_count, f"Layout file count mismatch for {parcel_id}"

        # --- LOT ---
        lot_json = None
        lot_schema_fields = [
            "source_http_request", "request_identifier", "lot_type", "lot_length_feet", "lot_width_feet", "lot_area_sqft",
            "landscaping_features", "view", "fencing_type", "fence_height", "fence_length", "driveway_material",
            "driveway_condition", "lot_condition_issues"
        ]
        lot_json = {k: None for k in lot_schema_fields}
        lot_json["source_http_request"] = source_http_request
        lot_json["request_identifier"] = request_identifier

        with open(os.path.join(property_dir, "lot.json"), "w") as f:
            json.dump(lot_json, f, indent=2)

        # --- ADDRESS EXTRACTION ---
        address_json = {
            "source_http_request": source_http_request,
            "request_identifier": request_identifier,
            "city_name": None,
            "country_code": "US",
            "county_name": address_data.get("county_jurisdiction", "PALM BEACH"),
            "latitude": None,
            "longitude": None,
            "plus_four_postal_code": None,
            "postal_code": None,
            "state_code": "FL",
            "street_name": None,
            "street_post_directional_text": None,
            "street_pre_directional_text": None,
            "street_number": None,
            "street_suffix_type": None,
            "unit_identifier": None,
            "route_number": None,
            "township": None,
            "range": None,
            "section": None,
            "block": None
        }

        # FIRST: Parse from address_data if available
        if address_data and address_data.get('full_address'):
            addr = address_data['full_address']

            # Extract unit identifier from # symbol
            if '#' in addr:
                parts = addr.split('#')
                if len(parts) > 1:
                    unit_part = parts[1].split(',')[0].strip()
                    address_json["unit_identifier"] = f"#{unit_part}" if unit_part else None

            # Parse full address from address data
            addr_parts = [a.strip() for a in addr.split(',')]
            if len(addr_parts) >= 1:
                # Parse street address (before first comma)
                street_addr = addr_parts[0]
                # Remove unit part if it exists
                if '#' in street_addr:
                    street_addr = street_addr.split('#')[0].strip()

                address_parts = street_addr.split()
                print(f"Address parts for {parcel_id}:", address_parts)
                if address_parts and address_parts[0].isdigit():
                    address_json["street_number"] = address_parts[0]

                    suffix_map = {
                        "BLVD": "Blvd", "AVE": "Ave", "ST": "St", "DR": "Dr", "LN": "Ln", "RD": "Rd",
                        "CT": "Ct", "PL": "Pl", "WAY": "Way", "CIR": "Cir", "PKWY": "Pkwy",
                        "TRAIL": "Trl", "TER": "Ter", "PLZ": "Plz", "HWY": "Hwy", "HIGHWAY": "Hwy",
                        "LOOP": "Loop", "BND": "Bnd", "CV": "Cv", "CRK": "Crk", "MNR": "Mnr",
                        "TRL": "Trl", "PT": "Pt", "SQ": "Sq", "RUN": "Run", "ROW": "Row",
                        "XING": "Xing", "WALK": "Walk", "PATH": "Path", "BCH": "Bch", "ISLE": "Isle",
                        "PLS": "Pls", "PLN": "Pln", "PASS": "Pass", "RTE": "Rte", "EST": "Est",
                        "ESTS": "Ests", "VLG": "Vlg", "VLGS": "Vlgs", "VILLAGE": "Vlg",
                        "VILLAGES": "Vlgs", "COVE": "Cv", "HILL": "Hl", "HILLS": "Hls",
                        "VIEW": "Vw", "VIEWS": "Vws", "COURT": "Ct", "CRES": "Cres",
                        "CRESCENT": "Cres", "GROVE": "Grv", "GROVES": "Grvs", "MEADOW": "Mdw",
                        "MEADOWS": "Mdws", "RIDGE": "Rdg", "RIDGES": "Rdgs", "TERRACE": "Ter",
                        "TERR": "Ter", "PARK": "Park", "PARKWAY": "Pkwy", "CROSSING": "Xing",
                        "CROSSINGS": "Xing", "LANDING": "Lndg", "LANDINGS": "Lndg", "GLEN": "Gln",
                        "GLENS": "Glns", "LAKE": "Lk", "LAKES": "Lks", "CREEK": "Crk",
                        "CREEKS": "Crk", "BAY": "Bay", "BAYS": "Bay", "POINT": "Pt",
                        "POINTS": "Pt", "SHORE": "Shr", "SHORES": "Shrs", "WOOD": "Wd",
                        "WOODS": "Wds", "FOREST": "Frst", "FORESTS": "Frst", "ESTATE": "Est",
                        "ESTATES": "Ests", "MOUNT": "Mt", "MOUNTAIN": "Mtn", "MOUNTAINS": "Mtns",
                        "CAMP": "Cp", "CAMPS": "Cp", "CAMPUS": "Cp", "CENTER": "Ctr",
                        "CENTERS": "Ctrs", "PLAZA": "Plz", "PLAZAS": "Plz", "SQUARE": "Sq",
                        "SQUARES": "Sqs", "STREET": "St", "AVENUE": "Ave", "DRIVE": "Dr",
                        "ROAD": "Rd", "LANE": "Ln", "PLACE": "Pl", "CIRCLE": "Cir",
                        "BEND": "Bnd"
                    }

                    # Find suffix
                    suffix = None
                    suffix_idx = None
                    for i in range(len(address_parts) - 1, 0, -1):
                        part = address_parts[i].upper().replace(",", "")
                        if part in suffix_map:
                            suffix = suffix_map[part]
                            suffix_idx = i
                            address_json["street_suffix_type"] = suffix
                            break

                    # Get street name (between street number and suffix)
                    if suffix_idx:
                        street_name_parts = address_parts[1:suffix_idx]
                    else:
                        street_name_parts = address_parts[1:]

                    # Handle directionals
                    directionals = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}
                    street_name_parts = []

                    # Get parts between street number and suffix (or end if no suffix)
                    end_idx = suffix_idx if suffix_idx else len(address_parts)
                    remaining_parts = address_parts[1:]

                    for idx, part in enumerate(remaining_parts):
                        up = part.upper()
                        if up in directionals:
                            if idx == 0:  # First part is pre-directional
                                address_json["street_pre_directional_text"] = up
                            elif idx == len(remaining_parts) - 1:  # Last part might be post-directional
                                address_json["street_post_directional_text"] = up
                            else:
                                street_name_parts.append(part)
                        elif part.isdigit():
                            # This is likely a route number (like "1" in "US Hwy 1")
                            route_number = part
                            address_json["route_number"] = route_number  # SET THE ROUTE NUMBER
                        elif up in suffix_map:
                            continue
                        else:
                            street_name_parts.append(part)

                    address_json["street_name"] = " ".join(street_name_parts) if street_name_parts else None

            # Parse city
            if len(addr_parts) >= 2:
                address_json["city_name"] = addr_parts[1].strip().upper()

            # Parse state and zip
            if len(addr_parts) >= 3:
                state_zip = addr_parts[2].split()
                if len(state_zip) >= 2:
                    address_json["postal_code"] = state_zip[1][:5]
                if len(state_zip) >= 3:
                    plus4 = state_zip[2]
                    if len(plus4) == 4 and plus4.isdigit():
                        address_json["plus_four_postal_code"] = plus4

        # SECOND: Fill missing fields from HTML if address_data didn't provide them
        if not address_json["city_name"]:
            city = soup.find(id="MainContent_lblMunicipality")
            if city:
                address_json["city_name"] = city.text.strip().upper()

        # Only parse from HTML if address_data didn't provide address info
        if not address_data or not address_data.get('full_address'):
            location = soup.find(id="MainContent_lblLocation")
            if location:
                address_line = location.text.strip()
                # Try to parse street number, name, suffix, unit
                address_parts = address_line.split()
                if address_parts and address_parts[0].isdigit():
                    address_json["street_number"] = address_parts[0]
                    suffix_map = {
                        "BLVD": "Blvd", "AVE": "Ave", "ST": "St", "DR": "Dr", "LN": "Ln", "RD": "Rd", "CT": "Ct",
                        "PL": "Pl", "WAY": "Way", "CIR": "Cir", "PKWY": "Pkwy", "TRAIL": "Trl", "TER": "Ter", "PLZ": "Plz",
                        "HWY": "Hwy", "LOOP": "Loop", "BND": "Bnd", "CV": "Cv", "CRK": "Crk", "MNR": "Mnr", "TRL": "Trl",
                        "PT": "Pt", "SQ": "Sq", "RUN": "Run", "ROW": "Row", "XING": "Xing", "WALK": "Walk", "PATH": "Path",
                        "BCH": "Bch", "ISLE": "Isle", "PLS": "Pls", "PLN": "Pln", "PASS": "Pass", "RTE": "Rte",
                        "EST": "Est", "ESTS": "Ests", "VLG": "Vlg", "VLGS": "Vlgs", "VILLAGE": "Vlg", "VILLAGES": "Vlgs",
                        "COVE": "Cv", "HILL": "Hl", "HILLS": "Hls", "VIEW": "Vw", "VIEWS": "Vws", "COURT": "Ct",
                        "CRES": "Cres", "CRESCENT": "Cres", "GROVE": "Grv", "GROVES": "Grvs", "MEADOW": "Mdw",
                        "MEADOWS": "Mdws", "RIDGE": "Rdg", "RIDGES": "Rdgs", "TERRACE": "Ter", "TERR": "Ter",
                        "PARK": "Park", "PARKWAY": "Pkwy", "CROSSING": "Xing", "CROSSINGS": "Xing", "LANDING": "Lndg",
                        "LANDINGS": "Lndg", "GLEN": "Gln", "GLENS": "Glns", "LAKE": "Lk", "LAKES": "Lks", "CREEK": "Crk",
                        "CREEKS": "Crk", "BAY": "Bay", "BAYS": "Bay", "POINT": "Pt", "POINTS": "Pt", "SHORE": "Shr",
                        "SHORES": "Shrs", "WOOD": "Wd", "WOODS": "Wds", "FOREST": "Frst", "FORESTS": "Frst",
                        "ESTATE": "Est", "ESTATES": "Ests", "MOUNT": "Mt", "MOUNTAIN": "Mtn", "MOUNTAINS": "Mtns",
                        "CAMP": "Cp", "CAMPS": "Cp", "CAMPUS": "Cp", "CENTER": "Ctr", "CENTERS": "Ctrs", "PLAZA": "Plz",
                        "PLAZAS": "Plz", "SQUARE": "Sq", "SQUARES": "Sqs", "STREET": "St", "AVENUE": "Ave", "DRIVE": "Dr",
                        "ROAD": "Rd", "LANE": "Ln", "COURT": "Ct", "PLACE": "Pl", "WAY": "Way", "CIRCLE": "Cir",
                        "HIGHWAY": "Hwy", "BEND": "Bnd"
                    }
                    # Find suffix
                    suffix = None
                    suffix_idx = None
                    for i in range(len(address_parts) - 1, 0, -1):
                        part = address_parts[i].upper().replace(",", "")
                        if part in suffix_map:
                            suffix = suffix_map[part]
                            suffix_idx = i
                            address_json["street_suffix_type"] = suffix
                            break
                    # Remove directionals from street_name
                    street_name_parts = address_parts[1:suffix_idx] if suffix_idx else address_parts[1:]
                    # Remove any part that is a directional
                    directionals = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}
                    street_name_clean = " ".join([p for p in street_name_parts if p.upper() not in directionals])
                    address_json["street_name"] = street_name_clean if street_name_clean else None
                    if suffix_idx and suffix_idx + 1 < len(address_parts):
                        address_json["unit_identifier"] = " ".join(address_parts[suffix_idx + 1:])
                    if not suffix:
                        address_json["street_suffix_type"] = None
                        # Remove directionals from street_name
                        street_name_parts = address_parts[1:]
                        street_name_clean = " ".join([p for p in street_name_parts if p.upper() not in directionals])
                        address_json["street_name"] = street_name_clean if street_name_clean else None

                # Try to extract pre/post directionals
                for idx, part in enumerate(address_parts):
                    up = part.upper()
                    if up in ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]:
                        if idx == 1:
                            address_json["street_pre_directional_text"] = up
                        elif idx == len(address_parts) - 2:
                            address_json["street_post_directional_text"] = up

        with open(os.path.join(property_dir, "address.json"), "w") as f:
            json.dump(address_json, f, indent=2)

        remove_null_files(property_dir)


if __name__ == '__main__':
    main()
