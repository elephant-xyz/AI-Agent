import os
import csv
import shutil

def copy_matching_json_files(csv_file, source_dir='Lee_addresses', target_dir='lee_addresses_test'):
    os.makedirs(target_dir, exist_ok=True)

    # Read parcel IDs from CSV
    parcel_ids = set()
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            parcel_id = row.get('parcel_id')
            if parcel_id:
                parcel_ids.add(parcel_id)

    # Copy matching JSON files
    for pid in parcel_ids:
        src_file = os.path.join(source_dir, f"{pid}.json")
        dest_file = os.path.join(target_dir, f"{pid}.json")
        if os.path.exists(src_file):
            shutil.copy(src_file, dest_file)
        else:
            print(f"Warning: File not found for PID {pid}")

    print(f"Copied {len(parcel_ids)} files to '{target_dir}'")

copy_matching_json_files('strap_output.csv')
