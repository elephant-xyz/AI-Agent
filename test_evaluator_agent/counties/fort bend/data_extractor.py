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

def parse_seed_csv():
    seed = {}
    with open('seed.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            seed[row['parcel_id']] = row
    return seed

# ---- wrapped content starts here (from original line 59) ----
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

    seed_data = parse_seed_csv()

    os.makedirs("./data", exist_ok=True)
    input_dir = "./input/"
    input_files = [f for f in os.listdir(input_dir) if f.endswith(".html")]

    for input_file in input_files:
        parcel_id = os.path.splitext(input_file)[0]
        # Use base parcel id for seed lookup (strip trailing _year if present)
        base_parcel_id = parcel_id.split('_')[0]
        property_dir = os.path.join("./data", parcel_id)
        os.makedirs(property_dir, exist_ok=True)
        with open(os.path.join(input_dir, input_file), encoding="utf-8") as f:
            html = f.read()
        soup = BeautifulSoup(html, "html.parser")
        addr_key = f"property_{parcel_id}"

        # --- ADDRESS ---
        address_json = {
            "source_http_request": {
                "method": seed_data[base_parcel_id]["method"],
                "url": seed_data[base_parcel_id]["url"],
                "multiValueQueryString": json.loads(seed_data[base_parcel_id]["multiValueQueryString"])
            },
            "request_identifier": parcel_id,
            "city_name": None,
            "country_code": "US",
            "county_name": seed_data[base_parcel_id]["county"].upper().replace("COUNTY", "").strip(),
            "latitude": None,
            "longitude": None,
            "plus_four_postal_code": None,
            "postal_code": None,
            "state_code": "TX",
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
        # Parse address from seed
        addr = seed_data[base_parcel_id]["address"]
        addr_parts = [a.strip() for a in addr.split(",")]
        if len(addr_parts) >= 1:
            street_addr = addr_parts[0]
            address_parts = street_addr.split()
            if address_parts and address_parts[0].isdigit():
                address_json["street_number"] = address_parts[0]
                # Find pre-directional (W, E, N, S, etc.)
                if len(address_parts) > 1 and address_parts[1].upper() in ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]:
                    address_json["street_pre_directional_text"] = address_parts[1].upper()
                    street_name_parts = address_parts[2:-1]
                else:
                    street_name_parts = address_parts[1:-1]
                address_json["street_name"] = " ".join(street_name_parts).upper()
                address_json["street_suffix_type"] = address_parts[-1].upper()
        if len(addr_parts) >= 2:
            address_json["city_name"] = addr_parts[1].strip().upper()
        if len(addr_parts) >= 3:
            state_zip = addr_parts[2].split()
            if len(state_zip) >= 2:
                address_json["postal_code"] = state_zip[1][:5]
        with open(os.path.join(property_dir, "address.json"), "w") as f:
            json.dump(address_json, f, indent=2)

        # --- TAXES ---
        # Extract from "Property Roll Value History" table
        tax_table = None
        for panel in soup.find_all("div", class_="panel panel-primary"):
            if panel.find("div", class_="panel-heading") and "Property Roll Value History" in panel.find("div", class_="panel-heading").text:
                tax_table = panel.find("table")
                break
        if tax_table:
            rows = tax_table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 6:
                    continue
                year = cols[0].text.strip()
                if not year.isdigit():
                    continue
                tax_json = {
                    "source_http_request": address_json["source_http_request"],
                    "request_identifier": f"{parcel_id}_tax_{year}",
                    "tax_year": int(year),
                    "property_assessed_value_amount": None,
                    "property_market_value_amount": None,
                    "property_building_amount": None,
                    "property_land_amount": None,
                    "property_taxable_value_amount": None,
                    "monthly_tax_amount": None,
                    "period_end_date": None,
                    "period_start_date": None
                }
                # Improvements, Land Market, Ag Valuation, HS Cap Loss, Appraised
                try:
                    tax_json["property_building_amount"] = float(cols[1].text.replace("$", "").replace(",", "")) if cols[1].text.strip() != "N/A" else None
                except:
                    tax_json["property_building_amount"] = None
                try:
                    tax_json["property_land_amount"] = float(cols[2].text.replace("$", "").replace(",", "")) if cols[2].text.strip() != "N/A" else None
                except:
                    tax_json["property_land_amount"] = None
                try:
                    tax_json["property_market_value_amount"] = (
                        float(cols[1].text.replace("$", "").replace(",", "")) +
                        float(cols[2].text.replace("$", "").replace(",", ""))
                    ) if cols[1].text.strip() != "N/A" and cols[2].text.strip() != "N/A" else None
                except:
                    tax_json["property_market_value_amount"] = None
                try:
                    tax_json["property_assessed_value_amount"] = float(cols[5].text.replace("$", "").replace(",", "")) if cols[5].text.strip() != "N/A" else None
                except:
                    tax_json["property_assessed_value_amount"] = None
                try:
                    tax_json["property_taxable_value_amount"] = float(cols[5].text.replace("$", "").replace(",", "")) if cols[5].text.strip() != "N/A" else None
                except:
                    tax_json["property_taxable_value_amount"] = None
                with open(os.path.join(property_dir, f"tax_{year}.json"), "w") as f:
                    json.dump(tax_json, f, indent=2)

        # --- LAYOUT ---
        # Extract number of bedrooms, full baths, half baths from HTML (if possible)
        bedroom_count = 0
        bathroom_count = 0
        half_bath_count = 0
        # Try to extract from the 'Property Details' table
        for table in soup.find_all("table"):
            header = table.find_previous("div", class_="panel-heading")
            if header and "Property Details" in header.text:
                for row in table.find_all("tr"):
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
        # Fallback: try to extract from any table if not found
        if bedroom_count == 0 and bathroom_count == 0 and half_bath_count == 0:
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
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
        for f_name in os.listdir(property_dir):
            if f_name.startswith("layout_") and f_name.endswith(".json"):
                os.remove(os.path.join(property_dir, f_name))
        # Always create at least one bedroom and one bathroom layout file if none found
        if bedroom_count == 0:
            bedroom_count = 1
        if bathroom_count == 0:
            bathroom_count = 1
        # Create layout files for each bedroom, full bath, half bath
        for i in range(bedroom_count):
            layout = {
                "source_http_request": address_json["source_http_request"],
                "request_identifier": f"{parcel_id}_layout_bedroom_{i + 1}",
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
                "source_http_request": address_json["source_http_request"],
                "request_identifier": f"{parcel_id}_layout_bathroom_{i + 1}",
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
                "source_http_request": address_json["source_http_request"],
                "request_identifier": f"{parcel_id}_layout_halfbath_{i + 1}",
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

        # --- OWNERS (PERSON/COMPANY) ---
        if parcel_id in owners_schema:
            owners_by_date = owners_schema[parcel_id]["owners_by_date"]
            for i, (date, owners) in enumerate(owners_by_date.items()):
                unique_persons = []
                seen = set()
                for owner in owners:
                    if owner["type"] == "person":
                        key = (owner.get("first_name"), owner.get("last_name"), owner.get("middle_name"))
                        if key not in seen:
                            seen.add(key)
                            unique_persons.append(owner)
                for j, owner in enumerate(unique_persons):
                    person_json = {
                        "source_http_request": address_json["source_http_request"],
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

        # --- RELATIONSHIP FILES ---
        if parcel_id in owners_schema:
            owners_by_date = owners_schema[parcel_id]["owners_by_date"]
            for i, (date, owners) in enumerate(owners_by_date.items()):
                unique_persons = []
                seen = set()
                for owner in owners:
                    if owner["type"] == "person":
                        key = (owner.get("first_name"), owner.get("last_name"), owner.get("middle_name"))
                        if key not in seen:
                            seen.add(key)
                            unique_persons.append(owner)
                for j, owner in enumerate(unique_persons):
                    rel = {
                        "to": {"/": f"./person_{i+1}_{j+1}.json"},
                        "from": {"/": f"./sales_{i+1}.json"}
                    }
                    with open(os.path.join(property_dir, f"relationship_sales_person_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(rel, f, indent=2)

        # --- PROPERTY ---
        # Generate property.json file
        property_json = {
            "source_http_request": address_json["source_http_request"],
            "request_identifier": parcel_id,
            "livable_floor_area": None,
            "number_of_units_type": None,
            "parcel_identifier": parcel_id,
            "property_legal_description_text": None,
            "property_structure_built_year": None,
            "property_type": None
        }
        with open(os.path.join(property_dir, "property.json"), "w") as f:
            json.dump(property_json, f, indent=2)
        # Remove null files
        remove_null_files(property_dir)

if __name__ == "__main__":
    main()
