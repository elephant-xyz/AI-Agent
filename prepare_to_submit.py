import os
import shutil
import json
import pandas as pd
import subprocess
import hashlib
import logging
from typing import List, Dict, Any

# === Logger Configuration ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COUNTY_DATA_GROUP_CID = "bafkreia23qtrtvkbfa2emegfjpgf5esircbn5mqb7y5rmmpnqfed3v2bm4"

# === Utilities ===
def unescape_http_request(http_request):
    if not http_request:
        return None
    return (http_request
            .replace('\\r\\n', '\r\n')
            .replace('\\r', '\r')
            .replace('\\n', '\n')
            .replace('\\"', '"')
            .replace('\\\
', '\\'))

def create_county_data_group(relationship_files: List[str]) -> Dict[str, Any]:
    county_data = {"label": "County", "relationships": {}}
    all_relationships = {
        "person_has_property": None,
        "company_has_property": None,
        "property_has_address": None,
        "property_has_lot": None,
        "property_has_tax": None,
        "property_has_sales_history": None,
        "property_has_layout": None,
        "property_has_flood_storm_information": None,
        "property_has_file": None
    }
    categories = {
        "person": [], "company": [], "tax": [], "sales": [], "layout": []
    }
    for rel_file in relationship_files:
        ref = {"/": f"./{rel_file}"}
        if "person" in rel_file and "property" in rel_file:
            categories["person"].append(ref)
        elif "company" in rel_file and "property" in rel_file:
            categories["company"].append(ref)
        elif "property_address" in rel_file:
            all_relationships["property_has_address"] = ref
        elif "property_lot" in rel_file:
            all_relationships["property_has_lot"] = ref
        elif "property_tax" in rel_file:
            categories["tax"].append(ref)
        elif "property_sales" in rel_file:
            categories["sales"].append(ref)
        elif "property_layout" in rel_file:
            categories["layout"].append(ref)
        elif "property_flood_storm_information" in rel_file:
            all_relationships["property_has_flood_storm_information"] = ref

    for k in categories:
        if categories[k]:
            key = f"{k}_has_property" if k != "tax" and k != "sales" and k != "layout" else f"property_has_{k}"
            all_relationships[key] = categories[k]

    county_data["relationships"] = {k: v for k, v in all_relationships.items() if v is not None}
    return county_data

def build_relationship_files(folder_path: str) -> tuple[List[str], List[str]]:
    relationship_files, errors = [], []
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    prop_files = [f for f in json_files if f.startswith('property')]
    if not prop_files:
        msg = "❌ No property.json file found"
        logger.error(msg)
        return [], [msg]
    property_file = prop_files[0]

    def create_rel(from_file, to_file, rel_name):
        path = os.path.join(folder_path, rel_name)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({"from": {"/": f"./{from_file}"}, "to": {"/": f"./{to_file}"}}, f, indent=2, ensure_ascii=False)
        logger.info(f"     📝 Created {rel_name}")
        relationship_files.append(rel_name)

    categories = ["person", "company", "address", "lot", "tax", "sales", "layout", "flood_storm_information"]
    for cat in categories:
        for file in [f for f in json_files if f.startswith(cat)]:
            suffix = f"_{file.split('_')[-1].replace('.json','')}" if '_' in file else ''
            if cat in ["person", "company"]:
                rel_name = f"relationship_{file.replace('.json','')}_property.json"
                create_rel(file, property_file, rel_name)
            else:
                rel_name = f"relationship_property_{cat}{suffix}.json"
                create_rel(property_file, file, rel_name)

    return relationship_files, errors

# === Main Submission Preparation + Validation ===
def main(data_dir_name: str = "data"):
    data_dir = os.path.join(BASE_DIR, data_dir_name)
    submit_dir = os.path.join(BASE_DIR, "submit")
    upload_results_path = os.path.join(BASE_DIR, "upload-results.csv")
    seed_csv_path = os.path.join(BASE_DIR, "seed.csv")

    if os.path.exists(submit_dir):
        shutil.rmtree(submit_dir)
    os.makedirs(submit_dir, exist_ok=True)

    if not os.path.exists(data_dir):
        logger.error("Data directory not found")
        return

    folder_mapping = {}
    if os.path.exists(upload_results_path):
        df = pd.read_csv(upload_results_path)
        for _, row in df.iterrows():
            parts = row['filePath'].split('/')
            if 'output' in parts:
                i = parts.index('output')
                if i+1 < len(parts):
                    folder_mapping[parts[i+1]] = row['propertyCid']

    seed_data = {}
    if os.path.exists(seed_csv_path):
        df = pd.read_csv(seed_csv_path)
        for _, row in df.iterrows():
            seed_data[str(row['parcel_id'])] = {
                'http_request': row['http_request'],
                'source_identifier': row['source_identifier']
            }

    all_relationship_errors = []
    for folder in os.listdir(data_dir):
        src = os.path.join(data_dir, folder)
        if not os.path.isdir(src): continue

        target = folder_mapping.get(folder, folder)
        dst = os.path.join(submit_dir, target)
        shutil.copytree(src, dst)

        if folder in seed_data:
            for file in os.listdir(dst):
                if file.endswith('.json'):
                    path = os.path.join(dst, file)
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        if 'source_http_request' in data:
                            data['source_http_request'] = unescape_http_request(seed_data[folder]['http_request'])
                            data['request_identifier'] = str(seed_data[folder]['source_identifier'])
                            with open(path, 'w', encoding='utf-8') as f:
                                json.dump(data, f, indent=2, ensure_ascii=False)
                    except Exception as e:
                        logger.error(f"Error updating {path}: {e}")

        rel_files, rel_errors = build_relationship_files(dst)
        if rel_errors:
            all_relationship_errors.extend([f"{folder}: {e}" for e in rel_errors])
        else:
            group = create_county_data_group(rel_files)
            with open(os.path.join(dst, f"{COUNTY_DATA_GROUP_CID}.json"), 'w', encoding='utf-8') as f:
                json.dump(group, f, indent=2, ensure_ascii=False)

    if all_relationship_errors:
        logger.error("Relationship errors:")
        for err in all_relationship_errors:
            logger.error(err)
        return

    # === Run CLI Validator ===
    try:
        subprocess.run(["node", "--version"], check=True)
        subprocess.run(["npm", "--version"], check=True)
    except Exception as e:
        logger.error(f"Missing node/npm: {e}")
        return

    try:
        result = subprocess.run(
            ["npx", "-y", "@elephant-xyz/cli", "validate-and-upload", "submit", "--dry-run", "--output-csv", "results.csv"],
            cwd=BASE_DIR, capture_output=True, text=True, timeout=300
        )
    except subprocess.TimeoutExpired:
        logger.error("CLI validator timed out")
        return

    errors_path = os.path.join(BASE_DIR, "submit_errors.csv")
    if os.path.exists(errors_path):
        try:
            df = pd.read_csv(errors_path)
            if not df.empty:
                messages = sorted(set(f"{row['error_path'].split('/')[-1]} {row['error_message']}" for _, row in df.iterrows()))
                logger.warning("CLI Validation Errors:")
                for m in messages:
                    logger.warning(f"  - {m}")
                return
        except Exception as e:
            logger.error(f"Failed to read submit_errors.csv: {e}")
            return

    logger.info("✅ CLI validation passed with no errors")

if __name__ == "__main__":
    main()
