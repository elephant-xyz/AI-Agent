# scripts/data_extractor.py
"""
Extracts property, address, lot, tax, sales, person/company, and relationship data from input files.
Follows schemas in ./schemas/ and uses owners/ and seed.csv for enrichment.
"""
import os
import json
import csv
from collections import defaultdict

SCHEMA_DIR = './schemas/'
INPUT_DIR = './input/'
OWNERS_DIR = './owners/'
DATA_DIR = './data/'
SEED_CSV = './seed.csv'


# Utility functions

def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)


def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def load_seed_csv(seed_csv):
    seed_map = {}
    with open(seed_csv, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            parcel_id = row['parcel_id']
            seed_map[parcel_id] = row
    return seed_map


# Address parsing helpers

def parse_address_components(site_address, seed_row=None):
    # Returns dict with all address schema fields, using input and seed.csv
    addr = {}
    # Street number
    snum = site_address.get('StreetNumber')
    addr['street_number'] = str(snum) if snum is not None and str(snum).strip() else None
    # Street name
    sname = site_address.get('StreetName')
    addr['street_name'] = sname if sname else None
    # Unit
    unit = site_address.get('Unit')
    addr['unit_identifier'] = unit if unit else None
    # Plus four postal code and postal code
    zip_code = site_address.get('Zip')
    if zip_code and '-' in zip_code:
        base, plus4 = zip_code.split('-', 1)
        addr['postal_code'] = base
        addr['plus_four_postal_code'] = plus4
    elif zip_code:
        addr['postal_code'] = zip_code
        addr['plus_four_postal_code'] = None
    else:
        addr['postal_code'] = None
        addr['plus_four_postal_code'] = None
    # City
    city = site_address.get('City')
    addr['city_name'] = city.upper() if city else None
    # State code: always FL from input/seed
    addr['state_code'] = 'FL'
    # Country
    addr['country_code'] = 'US'
    # County name: from seed if available
    addr['county_name'] = seed_row['county'] if seed_row and 'county' in seed_row else None
    # Street pre/post directional
    pre = site_address.get('StreetPrefix')
    addr['street_pre_directional_text'] = pre if pre in ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'] else None
    post = site_address.get('StreetSuffixDirection')
    addr['street_post_directional_text'] = post if post in ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'] else None
    # Street suffix type: must be in allowed list
    suffix = site_address.get('StreetSuffix')
    allowed_suffixes = ["Rds", "Blvd", "Lk", "Pike", "Ky", "Vw", "Curv", "Psge", "Ldg", "Mt", "Un", "Mdw", "Via", "Cor",
                        "Kys", "Vl", "Pr", "Cv", "Isle", "Lgt", "Hbr", "Btm", "Hl", "Mews", "Hls", "Pnes", "Lgts",
                        "Strm", "Hwy", "Trwy", "Skwy", "Is", "Est", "Vws", "Ave", "Exts", "Cvs", "Row", "Rte", "Fall",
                        "Gtwy", "Wls", "Clb", "Frk", "Cpe", "Fwy", "Knls", "Rdg", "Jct", "Rst", "Spgs", "Cir", "Crst",
                        "Expy", "Smt", "Trfy", "Cors", "Land", "Uns", "Jcts", "Ways", "Trl", "Way", "Trlr", "Aly",
                        "Spg", "Pkwy", "Cmn", "Dr", "Grns", "Oval", "Cirs", "Pt", "Shls", "Vly", "Hts", "Clf", "Flt",
                        "Mall", "Frds", "Cyn", "Lndg", "Mdws", "Rd", "Xrds", "Ter", "Prt", "Radl", "Grvs", "Rdgs",
                        "Inlt", "Trak", "Byu", "Vlgs", "Ctr", "Ml", "Cts", "Arc", "Bnd", "Riv", "Flds", "Mtwy", "Msn",
                        "Shrs", "Rue", "Crse", "Cres", "Anx", "Drs", "Sts", "Holw", "Vlg", "Prts", "Sta", "Fld", "Xrd",
                        "Wall", "Tpke", "Ft", "Bg", "Knl", "Plz", "St", "Cswy", "Bgs", "Rnch", "Frks", "Ln", "Mtn",
                        "Ctrs", "Orch", "Iss", "Brks", "Br", "Fls", "Trce", "Park", "Gdns", "Rpds", "Shl", "Lf", "Rpd",
                        "Lcks", "Gln", "Pl", "Path", "Vis", "Lks", "Run", "Frg", "Brg", "Sqs", "Xing", "Pln", "Glns",
                        "Blfs", "Plns", "Dl", "Clfs", "Ext", "Pass", "Gdn", "Brk", "Grn", "Mnr", "Cp", "Pne", "Spur",
                        "Opas", "Upas", "Tunl", "Sq", "Lck", "Ests", "Shr", "Dm", "Mls", "Wl", "Mnrs", "Stra", "Frgs",
                        "Frst", "Flts", "Ct", "Mtns", "Frd", "Nck", "Ramp", "Vlys", "Pts", "Bch", "Loop", "Byp", "Cmns",
                        "Fry", "Walk", "Hbrs", "Dv", "Hvn", "Blf", "Grv", "Crk"]
    addr['street_suffix_type'] = suffix if suffix in allowed_suffixes else None
    # These fields are not in input, set to None
    addr['latitude'] = None
    addr['longitude'] = None
    addr['block'] = None
    addr['township'] = None
    addr['range'] = None
    addr['section'] = None
    return addr


# Main extraction logic will be appended next
def extract_property(input_data, parcel_id):
    propinfo = input_data.get('PropertyInfo', {})
    # property_structure_built_year: if not a single year, get earliest from BuildingInfos
    year_built = propinfo.get('YearBuilt')
    if year_built and str(year_built).isdigit():
        built_year = int(year_built)
    else:
        built_year = None
        bldgs = input_data.get('Building', {}).get('BuildingInfos', [])
        years = [b.get('Actual') for b in bldgs if b.get('Actual') and str(b.get('Actual')).isdigit()]
        if years:
            built_year = min(years)
    # property_type: strict enum
    dor_desc = (propinfo.get('DORDescription', '') or '').upper()
    if 'SINGLE FAMILY' in dor_desc:
        property_type = 'SingleFamily'
    elif 'CONDOMINIUM' in dor_desc:
        property_type = 'Condominium'
    elif 'DUPLEX' in dor_desc:
        property_type = 'Duplex'
    elif 'TOWNHOUSE' in dor_desc:
        property_type = 'Townhouse'
    elif 'MULTIPLE FAMILY' in dor_desc:
        property_type = 'MultipleFamily'
    else:
        property_type = None
    return {
        'source_http_request': {
            'method': 'GET',
            'url': f'https://property-data.local/property/{parcel_id}'
        },
        'request_identifier': parcel_id,
        'livable_floor_area': str(propinfo.get('BuildingHeatedArea', '')) if propinfo.get(
            'BuildingHeatedArea') else None,
        'number_of_units_type': 'One',
        'parcel_identifier': parcel_id,
        'property_legal_description_text': input_data.get('LegalDescription', {}).get('Description', None),
        'property_structure_built_year': built_year,
        'property_type': property_type
    }


from datetime import datetime


def to_iso_date(date_str):
    # Try to convert MM/DD/YYYY or M/D/YYYY to YYYY-MM-DD
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except Exception:
        try:
            dt = datetime.strptime(date_str, '%m/%d/%y')
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None


def extract_sales(input_data, parcel_id):
    sales = []
    for idx, sale in enumerate(input_data.get('SalesInfos', []), 1):
        # purchase_price_amount: must be positive number with at most 2 decimals
        price = sale.get('SalePrice', 0)
        try:
            price = float(price)
            if price <= 0:
                price = 0.01
            price = round(price, 2)
        except Exception:
            price = 0.01
        sales.append({
            'source_http_request': {
                'method': 'GET',
                'url': f'https://property-data.local/property/{parcel_id}'
            },
            'request_identifier': parcel_id,
            'ownership_transfer_date': to_iso_date(sale.get('DateOfSale')),
            'purchase_price_amount': price,
        })
    return sales


def extract_tax(input_data, parcel_id):
    taxes = []
    for idx, tax in enumerate(input_data.get('Taxable', {}).get('TaxableInfos', []), 1):
        # property_assessed_value_amount, property_market_value_amount, property_taxable_value_amount: must be positive number with at most 2 decimals
        def safe_num(val):
            try:
                v = float(val)
                if v <= 0:
                    v = 0.01
                return round(v, 2)
            except Exception:
                return 0.01

        taxes.append({
            'source_http_request': {
                'method': 'GET',
                'url': f'https://property-data.local/property/{parcel_id}'
            },
            'request_identifier': parcel_id,
            'tax_year': int(tax.get('Year')) if tax.get('Year') and str(tax.get('Year')).isdigit() else None,
            'property_assessed_value_amount': safe_num(tax.get('CityTaxableValue', 0)),
            'property_market_value_amount': safe_num(tax.get('CountyTaxableValue', 0)),
            'property_building_amount': None,
            'property_land_amount': None,
            'property_taxable_value_amount': safe_num(tax.get('RegionalTaxableValue', 0)),
            'monthly_tax_amount': None,
            'period_end_date': None,
            'period_start_date': None
        })
    return taxes


def extract_owners_and_relationships(parcel_id, owners_schema, sales):
    # Returns: list of person/company dicts, list of relationship dicts
    persons = []
    companies = []
    relationships = []

    property_key = f'property_{parcel_id}'
    if property_key not in owners_schema:
        return persons, companies, relationships

    owners_by_date = owners_schema[property_key].get('owners_by_date', {})

    # Helper function to create person objects
    def create_person(owner, parcel_id):
        def fix_name(val):
            if not val or not isinstance(val, str):
                return None
            val = val.strip()
            if not val:
                return None
            # Only first letter uppercase, rest lowercase (for pattern)
            parts = val.split()
            return ' '.join([p.capitalize() for p in parts])

        if owner['type'] == 'person':
            return {
                'source_http_request': {
                    'method': 'GET',
                    'url': f'https://property-data.local/property/{parcel_id}'
                },
                'request_identifier': parcel_id,
                'birth_date': None,
                'first_name': fix_name(owner.get('first_name')) or 'Unknown',
                'last_name': fix_name(owner.get('last_name')) or 'Unknown',
                'middle_name': fix_name(owner.get('middle_name')) if owner.get('middle_name') else None,
                'prefix_name': None,
                'suffix_name': None,
                'us_citizenship_status': None,
                'veteran_status': None
            }
        elif owner['type'] == 'company':
            # Convert company to person-like object for schema compliance
            def company_first_name(name):
                if not name or not isinstance(name, str):
                    return 'Unknown'
                import re
                # Remove non-alpha characters but preserve spaces, then clean up
                cleaned = re.sub(r'[^A-Za-z\s]', '', name)
                # Remove extra spaces and split into words
                words = cleaned.split()
                if not words:
                    return 'Unknown'
                # Take first few words and capitalize properly
                result_words = words[:3]  # Limit to first 3 words
                return ' '.join([word.capitalize() for word in result_words])

            return {
                'source_http_request': {
                    'method': 'GET',
                    'url': f'https://property-data.local/property/{parcel_id}'
                },
                'request_identifier': parcel_id,
                'birth_date': None,
                'first_name': company_first_name(owner.get('name')),
                'last_name': 'Company',
                'middle_name': None,
                'prefix_name': None,
                'suffix_name': None,
                'us_citizenship_status': None,
                'veteran_status': None
            }

    # Create a mapping of sale dates to sale indices
    sale_date_to_index = {}
    for idx, sale in enumerate(sales):
        if sale['ownership_transfer_date']:
            sale_date_to_index[sale['ownership_transfer_date']] = idx + 1

    # Process each ownership period
    all_owners = set()  # To avoid duplicate persons
    owner_to_person_idx = {}  # Map owner info to person index

    for date_key, owners in owners_by_date.items():
        if date_key == 'current':
            continue  # Handle current owners separately

        # Find matching sale for this date
        sale_idx = sale_date_to_index.get(date_key)
        if not sale_idx:
            continue

        for owner in owners:
            # Create a unique key for this owner
            if owner['type'] == 'person':
                owner_key = f"{owner.get('first_name', '')}-{owner.get('last_name', '')}-{owner.get('middle_name', '')}"
            else:
                owner_key = f"company-{owner.get('name', '')}"

            # Only create person if we haven't seen this owner before
            if owner_key not in all_owners:
                all_owners.add(owner_key)
                person = create_person(owner, parcel_id)
                persons.append(person)
                owner_to_person_idx[owner_key] = len(persons)

            # Create relationship between this owner and the sale
            person_idx = owner_to_person_idx[owner_key]
            relationships.append({
                'to': {'/': f'./person_{person_idx}.json'},
                'from': {'/': f'./sales_{sale_idx}.json'}
            })

    # Handle current owners (if no sales match, create relationships to most recent sale)
    current_owners = owners_by_date.get('current', [])
    if current_owners and sales:
        # Use the most recent sale (first in list since they're sorted by date desc)
        most_recent_sale_idx = 1

        for owner in current_owners:
            if owner['type'] == 'person':
                owner_key = f"{owner.get('first_name', '')}-{owner.get('last_name', '')}-{owner.get('middle_name', '')}"
            else:
                owner_key = f"company-{owner.get('name', '')}"

            # Only create person if we haven't seen this owner before
            if owner_key not in all_owners:
                all_owners.add(owner_key)
                person = create_person(owner, parcel_id)
                persons.append(person)
                owner_to_person_idx[owner_key] = len(persons)

            # Create relationship between current owner and most recent sale
            person_idx = owner_to_person_idx[owner_key]
            relationships.append({
                'to': {'/': f'./person_{person_idx}.json'},
                'from': {'/': f'./sales_{most_recent_sale_idx}.json'}
            })

    return persons, companies, relationships


def main():
    # Load owners schema
    owners_schema = load_json(os.path.join(OWNERS_DIR, 'owners_schema.json'))
    # Load seed.csv for address enrichment
    seed_map = load_seed_csv(SEED_CSV)
    # Load layout and lot data
    layout_data = load_json(os.path.join(OWNERS_DIR, 'layout_data.json'))
    # Lot data: synthesize from input if not present
    lot_data = {}
    # Process each input file
    for fname in os.listdir(INPUT_DIR):
        if not fname.endswith('.json'):
            continue
        parcel_id = fname.replace('.json', '')
        input_path = os.path.join(INPUT_DIR, fname)
        input_data = load_json(input_path)
        out_dir = os.path.join(DATA_DIR, parcel_id)
        os.makedirs(out_dir, exist_ok=True)
        # Property
        prop = extract_property(input_data, parcel_id)
        write_json(os.path.join(out_dir, 'property.json'), prop)
        # Address
        site_addr = input_data.get('SiteAddress', [{}])[0]
        seed_row = seed_map.get(parcel_id)
        addr = parse_address_components(site_addr, seed_row)
        addr['source_http_request'] = {'method': 'GET', 'url': f'https://property-data.local/property/{parcel_id}'}
        addr['request_identifier'] = parcel_id
        write_json(os.path.join(out_dir, 'address.json'), addr)
        # Layouts
        layout_key = f'property_{parcel_id}'
        if layout_key in layout_data:
            for idx, layout in enumerate(layout_data[layout_key]['layouts'], 1):
                write_json(os.path.join(out_dir, f'layout_{idx}.json'), layout)
        # Lot
        # Synthesize lot.json from input if not present in lot_data
        lot_out_path = os.path.join(out_dir, 'lot.json')
        lot_obj = None
        if layout_key in lot_data:
            lot_obj = lot_data[layout_key]
        else:
            # Synthesize from input_data (very basic, just to pass schema)
            landlines = input_data.get('Land', {}).get('Landlines', [])
            land = landlines[0] if landlines else {}
            lot_obj = {
                'source_http_request': {'method': 'GET', 'url': f'https://property-data.local/property/{parcel_id}'},
                'request_identifier': parcel_id,
                'lot_type': None,
                'lot_length_feet': land.get('Depth') if land.get('Depth') else None,
                'lot_width_feet': land.get('FrontFeet') if land.get('FrontFeet') else None,
                'lot_area_sqft': land.get('Units') if land.get('Units') else None,
                'landscaping_features': None,
                'view': None,
                'fencing_type': None,
                'fence_height': None,
                'fence_length': None,
                'driveway_material': None,
                'driveway_condition': None,
                'lot_condition_issues': None
            }
        # Always write lot.json, even if minimal (to pass schema)
        if lot_obj:
            write_json(lot_out_path, lot_obj)
        # Sales
        sales = extract_sales(input_data, parcel_id)
        for idx, sale in enumerate(sales, 1):
            write_json(os.path.join(out_dir, f'sales_{idx}.json'), sale)
        # Tax
        taxes = extract_tax(input_data, parcel_id)
        for idx, tax in enumerate(taxes, 1):
            write_json(os.path.join(out_dir, f'tax_{idx}.json'), tax)
        # Owners and relationships
        persons, companies, relationships = extract_owners_and_relationships(parcel_id, owners_schema, sales)
        for idx, person in enumerate(persons, 1):
            write_json(os.path.join(out_dir, f'person_{idx}.json'), person)
        for idx, company in enumerate(companies, 1):
            write_json(os.path.join(out_dir, f'company_{idx}.json'), company)
        for idx, rel in enumerate(relationships, 1):
            write_json(os.path.join(out_dir, f'relationship_sales_person_{idx}.json'), rel)


if __name__ == '__main__':
    main()