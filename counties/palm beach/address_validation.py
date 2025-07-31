import json
import os
from jsonschema import validate, ValidationError

SCHEMA_PATH = './schemas/address.json'
MAPPING_PATH = './owners/addresses_mapping.json'
INPUT_DIR = './input/'

# Load schema
def load_schema():
    with open(SCHEMA_PATH, 'r') as f:
        return json.load(f)

# Load mapping
def load_mapping():
    with open(MAPPING_PATH, 'r') as f:
        return json.load(f)

# Validate address object
def validate_address_obj(address, schema):
    try:
        validate(instance=address, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e)

# Main validation
if __name__ == '__main__':
    schema = load_schema()
    mapping = load_mapping()
    errors = []
    for prop_id, data in mapping.items():
        address = data['address'] if 'address' in data else data
        valid, err = validate_address_obj(address, schema)
        if not valid:
            errors.append((prop_id, err))
    if errors:
        print('SCHEMA VALIDATION ERRORS:')
        for prop_id, err in errors:
            print(f'{prop_id}: {err}')
    else:
        print('All addresses are valid against the schema.')
    # Check completeness
    input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.html')]
    missing = []
    for f in input_files:
        pid = f.replace('.html', '')
        if f'property_{pid}' not in mapping:
            missing.append(pid)
    if missing:
        print('MISSING ADDRESSES FOR:', ', '.join(missing))
    else:
        print('All input properties have address mappings.')
