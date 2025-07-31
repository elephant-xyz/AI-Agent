import json
import os
from jsonschema import validate, ValidationError

SCHEMA_PATH = './schemas/address.json'
MAPPING_PATH = './owners/addresses_mapping.json'


def main():
    # Load schema
    with open(SCHEMA_PATH, 'r') as f:
        schema = json.load(f)
    # Load mapping
    with open(MAPPING_PATH, 'r') as f:
        mapping = json.load(f)
    errors = []
    count = 0
    for prop_id, data in mapping.items():
        address = data['address'] if 'address' in data else data
        try:
            validate(instance=address, schema=schema)
        except ValidationError as e:
            errors.append((prop_id, str(e)))
        count += 1
    print(f'Validated {count} addresses.')
    if errors:
        print(f'Found {len(errors)} validation errors:')
        for prop_id, err in errors:
            print(f'  {prop_id}: {err}')
    else:
        print('All addresses are valid against the schema.')

if __name__ == '__main__':
    main()
