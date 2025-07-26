#!/usr/bin/env python3

import os
import sys
import shutil
import subprocess
import pandas as pd
import json
import hashlib
import logging
from typing import Dict, Any, List

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get base directory (script directory)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def build_relationship_files(folder_path: str) -> tuple[list[str], list[str]]:
    """
    Build relationship files based on discovered files in the folder
    Returns: (relationship_files, errors)
    """
    relationship_files = []
    errors = []

    # Get all JSON files in the folder
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    sales_person_relations = [f for f in json_files if f.startswith('relationship_sales') and 'person' in f]
    relationship_files.extend(sales_person_relations)
    sales_company_relations = [f for f in json_files if f.startswith('relationship_sales') and 'company' in f]
    relationship_files.extend(sales_company_relations)
    # Categorize files
    person_files = [f for f in json_files if f.startswith('person')]
    company_files = [f for f in json_files if f.startswith('company')]
    property_files = [f for f in json_files if f.startswith('property')]
    address_files = [f for f in json_files if f.startswith('address')]
    lot_files = [f for f in json_files if f.startswith('lot')]
    tax_files = [f for f in json_files if f.startswith('tax')]
    sales_files = [f for f in json_files if f.startswith('sales')]
    layout_files = [f for f in json_files if f.startswith('layout')]
    flood_files = [f for f in json_files if f.startswith('flood_storm_information')]
    structure_files = [f for f in json_files if f.startswith('structure')]
    utility_files = [f for f in json_files if f.startswith('utility')]

    # Ensure we have property.json as the main reference
    if not property_files:
        error_msg = "❌ No property.json file found - you must create one"
        logger.error(error_msg)
        errors.append(error_msg)
        return relationship_files, errors

    property_file = property_files[0]  # Should be property.json

    # Build person/company to property relationships
    for person_file in person_files:
        rel_filename = f"relationship_{person_file.replace('.json', '')}_property.json"
        rel_path = os.path.join(folder_path, rel_filename)

        relationship_data = {
            "from": {"/": f"./{person_file}"},
            "to": {"/": f"./{property_file}"}
        }

        with open(rel_path, 'w', encoding='utf-8') as f:
            json.dump(relationship_data, f, indent=2, ensure_ascii=False)

        relationship_files.append(rel_filename)
        logger.info(f"     📝 Created {rel_filename}")

    for company_file in company_files:
        rel_filename = f"relationship_{company_file.replace('.json', '')}_property.json"
        rel_path = os.path.join(folder_path, rel_filename)

        relationship_data = {
            "from": {"/": f"./{company_file}"},
            "to": {"/": f"./{property_file}"}
        }

        with open(rel_path, 'w', encoding='utf-8') as f:
            json.dump(relationship_data, f, indent=2, ensure_ascii=False)

        relationship_files.append(rel_filename)
        logger.info(f"     📝 Created {rel_filename}")

    # Build property to other entity relationships
    relationship_mappings = [
        (address_files, "address"),
        (lot_files, "lot"),
        (tax_files, "tax"),
        (sales_files, "sales"),
        (layout_files, "layout"),
        (flood_files, "flood_storm_information"),
        (structure_files, "structure"),
        (utility_files, "utility")
    ]

    for files_list, entity_type in relationship_mappings:
        for file in files_list:
            # Extract number suffix if present (e.g., tax_1.json -> _1)
            base_name = file.replace('.json', '')
            if base_name.startswith(entity_type):
                if len(base_name) > len(entity_type):
                    suffix = base_name[len(entity_type):]  # Get everything after the entity type
                else:
                    suffix = ''  # Just the entity type, no suffix
            else:
                suffix = ''

            rel_filename = f"relationship_property_{entity_type}{suffix}.json"
            rel_path = os.path.join(folder_path, rel_filename)

            relationship_data = {
                "from": {"/": f"./{property_file}"},
                "to": {"/": f"./{file}"}
            }

            with open(rel_path, 'w', encoding='utf-8') as f:
                json.dump(relationship_data, f, indent=2, ensure_ascii=False)

            relationship_files.append(rel_filename)
            logger.info(f"     📝 Created {rel_filename}")

    return relationship_files, errors


def create_county_data_group(relationship_files: list[str]) -> dict[str, any]:
    """
    Create the county data group structure based on relationship files
    """
    county_data = {
        "label": "County",
        "relationships": {}
    }

    # Initialize all possible relationships as null
    all_relationships = {
        "person_has_property": None,
        "company_has_property": None,
        "property_has_address": None,
        "property_has_lot": None,
        "property_has_tax": None,
        "property_has_sales_history": None,
        "property_has_layout": None,
        "property_has_flood_storm_information": None,
        "property_has_file": None,
        "property_has_structure": None,
        "property_has_utility": None,
        "sales_history_has_person": None,
        "sales_history_has_company": None,
    }

    # Categorize relationship files
    person_relationships = []
    company_relationships = []
    tax_relationships = []
    sales_relationships = []
    layout_relationships = []
    sales_person_relationships = []
    sales_company_relationships = []

    for rel_file in relationship_files:
        ipld_ref = {"/": f"./{rel_file}"}

        if "person" in rel_file and "property" in rel_file:
            person_relationships.append(ipld_ref)
        elif "company" in rel_file and "property" in rel_file:
            company_relationships.append(ipld_ref)
        elif "property_address" in rel_file:
            all_relationships["property_has_address"] = ipld_ref
        elif "property_lot" in rel_file:
            all_relationships["property_has_lot"] = ipld_ref
        elif "property_tax" in rel_file:
            tax_relationships.append(ipld_ref)
        elif "property_sales" in rel_file:
            sales_relationships.append(ipld_ref)
        elif "property_layout" in rel_file:
            layout_relationships.append(ipld_ref)
        elif "property_flood_storm_information" in rel_file:
            all_relationships["property_has_flood_storm_information"] = ipld_ref
        elif "property_utility" in rel_file:
            all_relationships["property_has_utility"] = ipld_ref
        elif "property_structure" in rel_file:
            all_relationships["property_has_structure"] = ipld_ref
        elif "relationship_sales" in rel_file and "person" in rel_file:
            sales_person_relationships.append(ipld_ref)
        elif "relationship_sales_company" in rel_file and "company" in rel_file:
            sales_company_relationships.append(ipld_ref)

    # Set array relationships
    if person_relationships:
        all_relationships["person_has_property"] = person_relationships
    if company_relationships:
        all_relationships["company_has_property"] = company_relationships
    if tax_relationships:
        all_relationships["property_has_tax"] = tax_relationships
    if sales_relationships:
        all_relationships["property_has_sales_history"] = sales_relationships
    if layout_relationships:
        all_relationships["property_has_layout"] = layout_relationships
    if sales_person_relationships:
        all_relationships["sales_history_has_person"] = sales_person_relationships
    if sales_company_relationships:
        all_relationships["sales_history_has_company"] = sales_company_relationships

    # Only include non-null relationships in the final output
    county_data["relationships"] = {k: v for k, v in all_relationships.items()}

    return county_data


def main():
    """
    Run the CLI validation command and return results
    Returns: (success: bool, error_details: str, error_hash: str)
    """
    data_dir = "data"
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]

    try:
        logger.info("📁 Creating submit directory and copying data with proper naming...")

        # Define directories
        upload_results_path = os.path.join(BASE_DIR, "upload-results.csv")
        data_dir = os.path.join(BASE_DIR, data_dir)
        submit_dir = os.path.join(BASE_DIR, "submit")
        seed_csv_path = os.path.join(BASE_DIR, "seed.csv")

        # Create/clean submit directory
        if os.path.exists(submit_dir):
            shutil.rmtree(submit_dir)
            logger.info("🗑️ Cleaned existing submit directory")

        os.makedirs(submit_dir, exist_ok=True)
        logger.info(f"📁 Created submit directory: {submit_dir}")

        if not os.path.exists(data_dir):
            logger.error("❌ Data directory not found")
            return False, "Data directory not found", ""

        # Read the uploadresults.csv file for mapping
        folder_mapping = {}
        if os.path.exists(upload_results_path):
            df = pd.read_csv(upload_results_path)
            logger.info(f"📊 Found {len(df)} entries in uploadresults.csv")

            # Create mapping from old folder names to new names (propertyCid)
            for _, row in df.iterrows():
                file_path = row['filePath']
                property_cid = row['propertyCid']

                path_parts = file_path.split('/')
                output_index = -1

                # Find the "output" directory in the path
                for i, part in enumerate(path_parts):
                    if part == "output":
                        output_index = i
                        break

                if output_index != -1 and output_index + 1 < len(path_parts):
                    old_folder_name = path_parts[output_index + 1]
                    if old_folder_name not in folder_mapping:
                        folder_mapping[old_folder_name] = property_cid
                        logger.info(f"   📋 Mapping: {old_folder_name} -> {property_cid}")

            logger.info(f"✅ Created mapping for {len(folder_mapping)} unique folders")
        else:
            logger.warning("⚠️ upload-results.csv not found, using original folder names")

        # Read seed.csv and create mapping
        logger.info("Reading seed.csv for JSON updates...")
        seed_data = {}
        seed_csv_path = os.path.join(BASE_DIR, "seed.csv")

        if os.path.exists(seed_csv_path):
            seed_df = pd.read_csv(seed_csv_path)
            logger.info(f"📊 Found {len(seed_df)} entries in seed.csv")

            # Create mapping from parcel_id (original folder name) to http_request and source_identifier
            for _, row in seed_df.iterrows():
                parcel_id = str(row['parcel_id'])
                method = row.get('method')
                url = row.get('url')
                multiValueQueryString = row.get('multiValueQueryString')
                seed_data[parcel_id] = {
                    "source_http_request": {
                        "method": method,
                        "url": url,
                        "multiValueQueryString": json.loads(multiValueQueryString) if multiValueQueryString else None,
                    },
                    'source_identifier': row['source_identifier']
                }

            logger.info(f"✅ Created seed mapping for {len(seed_data)} parcel IDs")
        else:
            logger.warning("⚠️ seed.csv not found, skipping JSON updates")
            seed_data = {}

        # Copy data to submit directory with proper naming and build relationships
        copied_count = 0
        county_data_group_cid = "bafkreigsqoofbrni7fye3dtsjuvtwv4nmmdzrppvblhzlsq3xpucn5daeq"

        # NEW: Collect all relationship building errors
        all_relationship_errors = []

        for folder_name in os.listdir(data_dir):
            src_folder_path = os.path.join(data_dir, folder_name)

            if os.path.isdir(src_folder_path):
                # Determine target folder name
                target_folder_name = folder_mapping.get(folder_name, folder_name)
                dst_folder_path = os.path.join(submit_dir, target_folder_name)

                # Copy the entire folder
                shutil.copytree(src_folder_path, dst_folder_path)
                logger.info(f"   📂 Copied folder: {folder_name} -> {target_folder_name}")
                copied_count += 1

                # Update JSON files with seed data (folder_name is the original parcel_id)
                if folder_name in seed_data:
                    updated_files_count = 0
                    for file_name in os.listdir(dst_folder_path):
                        if file_name.endswith('.json'):
                            json_file_path = os.path.join(dst_folder_path, file_name)

                            if "relation" in file_name.lower():
                                logger.info(f"   🔗 Skipping relationship file: {file_name}")
                                continue

                            try:
                                # Read JSON file
                                with open(json_file_path, 'r', encoding='utf-8') as f:
                                    json_data = json.load(f)

                                json_data['source_http_request'] = seed_data[folder_name]['source_http_request']
                                json_data['request_identifier'] = str(seed_data[folder_name]['source_identifier'])

                                # Write back to file
                                with open(json_file_path, 'w', encoding='utf-8') as f:
                                    json.dump(json_data, f, indent=2, ensure_ascii=False)

                                updated_files_count += 1

                            except json.JSONDecodeError as e:
                                logger.error(f"   ❌ Error parsing JSON file {json_file_path}: {e}")
                            except Exception as e:
                                logger.error(f"   ❌ Error processing file {json_file_path}: {e}")

                    if updated_files_count > 0:
                        logger.info(
                            f"   🌱 Updated {updated_files_count} JSON files with seed data for parcel {folder_name}")
                else:
                    logger.warning(f"   ⚠️ No seed data found for parcel {folder_name}")

                # Build relationship files dynamically - MODIFIED TO CAPTURE ERRORS
                logger.info(f"   🔗 Building relationship files for {target_folder_name}")
                relationship_files, relationship_errors = build_relationship_files(dst_folder_path)

                # Add any relationship errors to our collection
                if relationship_errors:
                    for error in relationship_errors:
                        all_relationship_errors.append(f"Property {folder_name}: {error}")

                # Only create county data group if no relationship errors
                if not relationship_errors:
                    # Create county data group file with all relationships
                    county_data_group = create_county_data_group(relationship_files)
                    county_file_path = os.path.join(dst_folder_path, f"{county_data_group_cid}.json")

                    with open(county_file_path, 'w', encoding='utf-8') as f:
                        json.dump(county_data_group, f, indent=2, ensure_ascii=False)

                    logger.info(
                        f"   ✅ Created {county_data_group_cid}.json with {len(relationship_files)} relationship files")

        # NEW: Check if we have relationship errors before proceeding
        if all_relationship_errors:
            logger.error("❌ Relationship building errors found - returning early")
            error_details = "Relationship Building Errors Found:\n\n"
            for error in all_relationship_errors:
                error_details += f"Error: {error}\n"
            print(f"ERROR: {error_details}")
            return False, error_details, ""

        logger.info(f"✅ Copied {copied_count} folders and built relationship files")

        # Rest of the function continues as before...
        logger.info("🔍 Running CLI validator: npx @elephant-xyz/cli validate-and-upload submit --dry-run")

        # Check prerequisites before running CLI validator
        logger.info("🔍 Checking CLI validator prerequisites...")

        # Check if node/npm are available
        try:
            node_result = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=10)
            npm_result = subprocess.run(["npm", "--version"], capture_output=True, text=True, timeout=10)
            logger.info(f"Node.js version: {node_result.stdout.strip()}")
            logger.info(f"npm version: {npm_result.stdout.strip()}")
        except Exception as e:
            logger.error(f"❌ Node.js/npm not available: {e}")

        # Check if submit directory exists and has content
        submit_dir = os.path.join(BASE_DIR, "submit")
        if os.path.exists(submit_dir):
            submit_contents = os.listdir(submit_dir)
            logger.info(f"Submit directory contains {len(submit_contents)} items: {submit_contents[:5]}...")
        else:
            logger.error("❌ Submit directory not found!")

        logger.info("🔍 Running CLI validator: npx @elephant-xyz/cli validate-and-upload submit --dry-run")

        try:
            result = subprocess.run(
                ["npx", "-y", "@elephant-xyz/cli@1.12.0", "validate-and-upload", "submit", "--dry-run", "--output-csv",
                 "results.csv"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=300  # Keep original 5 minute timeout
            )
        except subprocess.TimeoutExpired as e:
            logger.error("❌ CLI validator timed out after 5 minutes")
            logger.error("This usually indicates:")
            logger.error("  1. Network issues downloading @elephant-xyz/cli")
            logger.error("  2. CLI tool hanging on invalid input")
            logger.error("  3. Large submit directory taking too long to process")
            raise

        # Check for submit_errors.csv file regardless of exit code
        submit_errors_path = os.path.join(BASE_DIR, "submit_errors.csv")

        if os.path.exists(submit_errors_path):
            # Read the CSV file to check for actual errors
            try:
                df = pd.read_csv(submit_errors_path)
                if len(df) > 0:
                    # There are validation errors
                    logger.warning(f"❌ CLI validation found {len(df)} errors in submit_errors.csv")

                    # Get unique error messages and extract field names from error paths
                    unique_errors = set()
                    for _, row in df.iterrows():
                        error_message = row['error_message']
                        error_path = row['error_path']

                        # Extract field name from the error path (last part after the last '/')
                        field_name = error_path.split('/')[-1]

                        # Create formatted error with field name
                        if field_name.startswith('property_has_'):
                            # Extract the type after 'property_has_'
                            info_type = field_name.replace('property_has_', '')
                            formatted_error = f"{info_type.capitalize()} information are missing"
                        else:
                            # Create formatted error with field name for other errors
                            formatted_error = f"{field_name} {error_message}"
                        unique_errors.add(formatted_error)

                    # Format the errors for the generator
                    error_details = "CLI Validation Errors Found:\n\n"

                    for error in sorted(unique_errors):
                        error_details += f"Error: {error}\n"

                    print(f"ERROR: {error_details}")
                    return False, error_details, ""
                else:
                    logger.info("✅ CLI validation passed - no errors in submit_errors.csv")
                    print("SUCCESS: CLI validation passed")
                    return True, "", ""
            except Exception as e:
                logger.error(f"Error reading submit_errors.csv: {e}")
                error_details = f"Could not read submit_errors.csv: {e}"
                error_hash = hashlib.md5(error_details.encode()).hexdigest()
                print(f"ERROR: {error_details}")
                return False, error_details, error_hash
        else:
            # No submit_errors.csv file means no errors (hopefully)
            if result.returncode == 0:
                logger.info("✅ CLI validation passed - no submit_errors.csv file found")
                print("SUCCESS: CLI validation passed")
                return True, "", ""
            else:
                logger.warning("❌ CLI validation failed")
                error_output = f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
                error_hash = hashlib.md5(error_output.encode()).hexdigest()
                print(f"ERROR: {error_output}")
                return False, error_output, error_hash

    except subprocess.TimeoutExpired:
        error_msg = "CLI validation timed out after 5 minutes"
        logger.error(error_msg)
        error_hash = hashlib.md5(error_msg.encode()).hexdigest()
        print(f"ERROR: {error_msg}")
        return False, error_msg, error_hash
    except Exception as e:
        error_msg = f"CLI validation error: {str(e)}"
        logger.error(error_msg)
        error_hash = hashlib.md5(error_msg.encode()).hexdigest()
        print(f"ERROR: {error_msg}")
        return False, error_msg, error_hash


if __name__ == "__main__":
    success, error_details, error_hash = main()
    if success:
        sys.exit(0)
    else:
        sys.exit(1)
