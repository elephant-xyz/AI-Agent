import json
import os
import sys
from jsonschema import validate, ValidationError

SCHEMA_PATH = './schemas/address.json'
MAPPING_PATH = './owners/addresses_mapping.json'


def load_schema():
    with open(SCHEMA_PATH, 'r') as f:
        return json.load(f)

def load_mapping():
    with open(MAPPING_PATH, 'r') as f:
        return json.load(f)

def validate_address(address, schema):
    try:
        validate(instance=address, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e)

def main():
    schema = load_schema()
    mapping = load_mapping()
    errors = []
    total = 0
    valid = 0
    for prop_id, data in mapping.items():
        address = data['address'] if 'address' in data else data
        is_valid, err = validate_address(address, schema)
        total += 1
        if is_valid:
            valid += 1
        else:
            errors.append((prop_id, err))
    print(f'Validation results: {valid}/{total} addresses valid.')
    if errors:
        print('Validation errors:')
        for prop_id, err in errors:
            print(f'  {prop_id}: {err}')
    else:
        print('All addresses are valid against the schema.')

if __name__ == '__main__':
    main()
