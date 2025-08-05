import os
import json
from datetime import datetime


def parse_owner_name(name):
    # Company detection keywords
    company_keywords = [
        'INC', 'LLC', 'LTD', 'CORP', 'CO', 'FOUNDATION', 'ALLIANCE', 'SOLUTIONS', 'SERVICES', 'SYSTEMS', 'COUNCIL',
        'VETERANS', 'FIRST RESPONDERS', 'HEROES', 'INITIATIVE', 'ASSOCIATION', 'GROUP', 'TRUST', 'PARTNERS',
        'PROPERTIES', 'HOLDINGS', 'ENTERPRISES', 'INVESTMENTS', 'FUND', 'BANK', 'SAVINGS', 'MORTGAGE', 'REALTY',
        'COMPANY', 'LP', 'LLP', 'PLC', 'PC', 'PLLC', 'P.A.', 'P.C.'
    ]
    if not name or not name.strip():
        return None
    upper_name = name.upper()
    for kw in company_keywords:
        if kw in upper_name:
            return {'type': 'company', 'name': name.strip()}
    # Person name parsing
    parts = name.replace('&', 'and').replace('  ', ' ').strip().split()
    if len(parts) == 2:
        return {'type': 'person', 'first_name': parts[0], 'last_name': parts[1], 'middle_name': None}
    elif len(parts) == 3:
        return {'type': 'person', 'first_name': parts[0], 'middle_name': parts[1], 'last_name': parts[2]}
    elif len(parts) > 3:
        return {'type': 'person', 'first_name': parts[0], 'middle_name': ' '.join(parts[1:-1]), 'last_name': parts[-1]}
    else:
        return {'type': 'person', 'first_name': name.strip(), 'last_name': None, 'middle_name': None}


def extract_owners_from_json(data):
    owners = []
    # Current owner(s)
    if 'OwnerInfos' in data:
        for owner in data['OwnerInfos']:
            if owner.get('Name'):
                owners.append(owner['Name'])
    # Previous owners from SalesInfos
    if 'SalesInfos' in data:
        for sale in data['SalesInfos']:
            for key in ['GranteeName1', 'GranteeName2']:
                if sale.get(key):
                    owners.append(sale[key])
    return owners


def extract_owners_from_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except Exception:
            return []
    # Try to find the main property dict
    if 'OwnerInfos' in data or 'SalesInfos' in data:
        return extract_owners_from_json(data)
    # Sometimes nested
    for k, v in data.items():
        if isinstance(v, dict) and ('OwnerInfos' in v or 'SalesInfos' in v):
            return extract_owners_from_json(v)
    return []


def main():
    input_dir = './input/'
    output_dir = './owners/'
    os.makedirs(output_dir, exist_ok=True)
    extracted = {}
    schema = {}

    for fname in os.listdir(input_dir):
        if not fname.endswith('.json'):
            continue
        fpath = os.path.join(input_dir, fname)
        property_id = fname.replace('.json', '')
        owners = extract_owners_from_file(fpath)
        # Remove empty/nulls
        owners = [o for o in owners if o and o.strip()]
        extracted[property_id] = owners

        # Now, build schema by date
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        owners_by_date = {}

        # Previous owners by sale date - using GRANTEES (buyers)
        if 'SalesInfos' in data:
            # Sort sales by date to get chronological order
            sales = sorted(data['SalesInfos'],
                           key=lambda x: datetime.strptime(x.get('DateOfSale', '01/01/1900'), '%m/%d/%Y')
                           if x.get('DateOfSale') else datetime(1900, 1, 1),
                           reverse=True)

            for idx, sale in enumerate(sales):
                date = sale.get('DateOfSale')
                if not date:
                    continue

                # Convert to ISO
                try:
                    iso_date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
                except Exception:
                    continue

                # Extract GRANTEES (buyers who became owners on this date)
                sale_owners = []
                for key in ['GranteeName1', 'GranteeName2']:
                    grantee_name = sale.get(key, '')
                    if grantee_name and grantee_name.strip():  # Only process non-empty names
                        parsed = parse_owner_name(grantee_name)
                        if parsed:
                            sale_owners.append(parsed)

                if sale_owners:
                    owners_by_date[iso_date] = sale_owners

        # Remove the optional grantor tracking section to keep it simple
        schema[f'property_{property_id}'] = {'owners_by_date': owners_by_date}

    # Write outputs
    with open(os.path.join(output_dir, 'owners_extracted.json'), 'w', encoding='utf-8') as f:
        json.dump(extracted, f, indent=2)

    with open(os.path.join(output_dir, 'owners_schema.json'), 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)


if __name__ == '__main__':
    main()