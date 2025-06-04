import os
import json
from collections import defaultdict

def extract_addresses_by_pid(input_file, output_dir='Lee_addresses'):
    os.makedirs(output_dir, exist_ok=True)

    # Dictionary to group addresses by pid
    pid_to_addresses = defaultdict(list)

    # Read and parse each line of the geojson file
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                feature = json.loads(line)
                props = feature.get('properties', {})
                pid = props.get('pid')
                if pid:
                    address = {
                        "number": props.get("number", ""),
                        "street": props.get("street", ""),
                        "unit": props.get("unit", ""),
                        "city": props.get("city", ""),
                        "district": props.get("district", ""),
                        "region": props.get("region", ""),
                        "postcode": props.get("postcode", "")
                    }
                    pid_to_addresses[pid].append(address)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {e}")

    # Write one JSON file per PID
    for pid, addresses in pid_to_addresses.items():
        output_path = os.path.join(output_dir, f"{pid}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(addresses, f, indent=2)

    print(f"Finished writing {len(pid_to_addresses)} files to '{output_dir}'")

# Example usage:
extract_addresses_by_pid('lee-parcels-parcel-address.geojson')
