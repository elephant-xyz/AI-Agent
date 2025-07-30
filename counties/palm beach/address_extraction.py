import os
import json
import csv
import re
from difflib import SequenceMatcher

INPUT_DIR = './input/'
POSSIBLE_ADDRESSES_DIR = './possible_addresses/'
SEED_CSV = './seed.csv'
OUTPUT_FILE = './owners/addresses_mapping.json'
SCHEMA_FILE = './schemas/address.json'

# Helper: Parse address string (e.g., '1605 S US HIGHWAY 1 3E')
def parse_address(address_str):
    # Regex: number, pre-directional, street, suffix, post-directional, unit
    # This is a simple version, can be improved for edge cases
    pattern = r'^(\d+)\s+((?:[NSEW]{1,2})\s+)?([A-Za-z0-9\s]+?)(?:\s+(AVE|ST|BLVD|RD|PKWY|LN|DR|CT|PL|HWY|WAY|CIR|TRL|TER|PLZ|PARKWAY|COURT|ROAD|STREET|AVENUE|BOULEVARD|LANE|DRIVE|CIRCLE|TRAIL|TERRACE|PLACE|PLAZA|HIGHWAY))?(?:\s+([NSEW]{1,2}))?(?:\s+(\w+))?$'
    m = re.match(pattern, address_str.strip(), re.IGNORECASE)
    if not m:
        # fallback: just number and rest
        parts = address_str.strip().split(' ', 1)
        return {
            'number': parts[0],
            'street': parts[1] if len(parts) > 1 else '',
            'unit': None,
            'pre_dir': None,
            'post_dir': None,
            'suffix': None
        }
    number, pre_dir, street, suffix, post_dir, unit = m.groups()
    return {
        'number': number,
        'street': street.strip() if street else '',
        'unit': unit,
        'pre_dir': pre_dir.strip() if pre_dir else None,
        'post_dir': post_dir,
        'suffix': suffix
    }

# Helper: Fuzzy match

def fuzzy_match(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# Helper: Validate address against schema (basic required fields check)
def validate_address(address, schema):
    required = schema.get('required', [])
    for field in required:
        if field not in address:
            return False, f'Missing field: {field}'
        if address[field] is None and 'null' not in schema['properties'][field]['type']:
            return False, f'Field {field} is null but not allowed.'
    return True, ''

# Load schema
def load_schema():
    with open(SCHEMA_FILE, 'r') as f:
        return json.load(f)

# Load seed.csv into a dict: parcel_id -> {address, county}
def load_seed():
    mapping = {}
    with open(SEED_CSV, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row['parcel_id']] = row
    return mapping

# Main processing
def main():
    schema = load_schema()
    seed = load_seed()
    result = {}
    for fname in os.listdir(INPUT_DIR):
        if not fname.endswith('.html'):
            continue
        parcel_id = fname.replace('.html', '')
        if parcel_id not in seed:
            continue
        address_str = seed[parcel_id]['Address']
        county = seed[parcel_id]['County']
        parsed = parse_address(address_str)
        # Load possible addresses
        pa_path = os.path.join(POSSIBLE_ADDRESSES_DIR, f'{parcel_id}.json')
        if not os.path.exists(pa_path):
            print(f'Warning: possible_addresses file missing for {parcel_id}')
            continue
        with open(pa_path, 'r') as f:
            pa_data = json.load(f)
        # Support both formats: dict (already mapped) or list (raw candidates)
        if isinstance(pa_data, dict) and f'property_{parcel_id}' in pa_data:
            # Already mapped, just copy
            result[f'property_{parcel_id}'] = pa_data[f'property_{parcel_id}']
            continue
        candidates = pa_data if isinstance(pa_data, list) else []
        # Try exact match first
        match = None
        for cand in candidates:
            if (str(cand['number']) == parsed['number'] and
                cand['street'].replace('.', '').replace(',', '').replace('  ', ' ').strip().lower() == parsed['street'].replace('.', '').replace(',', '').replace('  ', ' ').strip().lower() and
                (not parsed['unit'] or (cand['unit'] or '').lower() == (parsed['unit'] or '').lower())):
                match = cand
                break
        # Fuzzy match if needed
        if not match:
            best_score = 0
            for cand in candidates:
                score = fuzzy_match(f"{cand['number']} {cand['street']} {(cand['unit'] or '')}",
                                   f"{parsed['number']} {parsed['street']} {(parsed['unit'] or '')}")
                if score > best_score and score > 0.85:
                    best_score = score
                    match = cand
        if not match and candidates:
            match = candidates[0]  # fallback: pick first candidate if only one
        if not match:
            print(f'No match found for {parcel_id}')
            continue  # skip if no match
        # Build address object (fix: use candidate for street fields, clean up street_name)
        # Extract directional and suffix from candidate street
        street_parts = cand['street'].split()
        pre_dir = None
        post_dir = None
        suffix = None
        street_name_parts = []
        # Directional abbreviations
        dirs = {'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'}
        suffixes = {'Rds','Blvd','Lk','Pike','Ky','Vw','Curv','Psge','Ldg','Mt','Un','Mdw','Via','Cor','Kys','Vl','Pr','Cv','Isle','Lgt','Hbr','Btm','Hl','Mews','Hls','Pnes','Lgts','Strm','Hwy','Trwy','Skwy','Is','Est','Vws','Ave','Exts','Cvs','Row','Rte','Fall','Gtwy','Wls','Clb','Frk','Cpe','Fwy','Knls','Rdg','Jct','Rst','Spgs','Cir','Crst','Expy','Smt','Trfy','Cors','Land','Uns','Jcts','Ways','Trl','Way','Trlr','Aly','Spg','Pkwy','Cmn','Dr','Grns','Oval','Cirs','Pt','Shls','Vly','Hts','Clf','Flt','Mall','Frds','Cyn','Lndg','Mdws','Rd','Xrds','Ter','Prt','Radl','Grvs','Rdgs','Inlt','Trak','Byu','Vlgs','Ctr','Ml','Cts','Arc','Bnd','Riv','Flds','Mtwy','Msn','Shrs','Rue','Crse','Cres','Anx','Drs','Sts','Holw','Vlg','Prts','Sta','Fld','Xrd','Wall','Tpke','Ft','Bg','Knl','Plz','St','Cswy','Bgs','Rnch','Frks','Ln','Mtn','Ctrs','Orch','Iss','Brks','Br','Fls','Trce','Park','Gdns','Rpds','Shl','Lf','Rpd','Lcks','Gln','Pl','Path','Vis','Lks','Run','Frg','Brg','Sqs','Xing','Pln','Glns','Blfs','Plns','Dl','Clfs','Ext','Pass','Gdn','Brk','Grn','Mnr','Cp','Pne','Spur','Opas','Upas','Tunl','Sq','Lck','Ests','Shr','Dm','Mls','Wl','Mnrs','Stra','Frgs','Frst','Flts','Ct','Mtns','Frd','Nck','Ramp','Vlys','Pts','Bch','Loop','Byp','Cmns','Fry','Walk','Hbrs','Dv','Hvn','Blf','Grv','Crk'}
        # Pre-directional
        if street_parts and street_parts[0].upper() in dirs:
            pre_dir = street_parts[0].upper()
            street_parts = street_parts[1:]
        # Post-directional
        if street_parts and street_parts[-1].upper() in dirs:
            post_dir = street_parts[-1].upper()
            street_parts = street_parts[:-1]
        # Suffix
        if street_parts and street_parts[-1].replace('.','').capitalize() in suffixes:
            suffix = street_parts[-1].replace('.','').capitalize()
            street_parts = street_parts[:-1]
        # The rest is street name
        street_name = ' '.join(street_parts).upper()
        # Remove any unit or city name from street_name
        if cand['unit'] and street_name.endswith(cand['unit'].upper()):
            street_name = street_name[:-(len(cand['unit'])+1)].strip()
        if cand['city'] and street_name.endswith(cand['city'].upper()):
            street_name = street_name[:-(len(cand['city'])+1)].strip()
        address_obj = {
            'source_http_request': {
                'method': seed[parcel_id]['method'],
                'url': seed[parcel_id]['url'],
                'multiValueQueryString': json.loads(seed[parcel_id]['multiValueQueryString']) if seed[parcel_id]['multiValueQueryString'] else {},
            },
            'request_identifier': parcel_id,
            'city_name': (cand['city'] or '').upper(),
            'country_code': 'US',
            'county_name': county,
            'latitude': cand['coordinates'][1],
            'longitude': cand['coordinates'][0],
            'plus_four_postal_code': None,  # Not available
            'postal_code': cand['postcode'],
            'state_code': 'FL',  # Assume FL for now
            'street_name': street_name,
            'street_post_directional_text': post_dir,
            'street_pre_directional_text': pre_dir,
            'street_number': cand['number'],
            'street_suffix_type': suffix,
            'unit_identifier': cand['unit'] if cand['unit'] else None,
            'township': None,
            'range': None,
            'section': None,
            'block': None
        }
        # Validate
        valid, msg = validate_address(address_obj, schema)
        if not valid:
            print(f'Validation failed for {parcel_id}: {msg}')
            continue
        result[f'property_{parcel_id}'] = {'address': address_obj}
    # Write output
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(result, f, indent=2)

if __name__ == '__main__':
    main()
