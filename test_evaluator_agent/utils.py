import re
import csv
import logging
import os
import time
import zipfile
import shutil
import json
import sys
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.abspath(".")
LOCAL_DIR = os.path.dirname(__file__)


LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

log_file_path = os.path.join(LOGS_DIR, f"workflow_{int(time.time())}.log")

file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.CRITICAL)  # Only show critical messages


logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

logger = logging.getLogger(__name__)


def create_output_zip(output_name: str = "transformed_output.zip") -> bool:
    """Create output ZIP file from processed data"""
    import zipfile

    output_zip_path = os.path.join(BASE_DIR, output_name)
    submit_dir = os.path.join(BASE_DIR, "data")

    if not os.path.exists(submit_dir):
        print("ERROR: No data directory found to zip")
        return False

    try:
        with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
            # Walk through data directory and add all files
            for root, dirs, files in os.walk(submit_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create archive path relative to data directory
                    archive_path = os.path.relpath(file_path, submit_dir)
                    zip_ref.write(file_path, archive_path)
                    logger.info(f"Added to ZIP: {archive_path}")

        # Count files in the created ZIP
        with zipfile.ZipFile(output_zip_path, "r") as zip_ref:
            file_count = len(zip_ref.namelist())

        print_status(f"Created output ZIP: {output_name} with {file_count} files")
        logger.info(f"✅ Created output ZIP: {output_zip_path}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to create output ZIP: {e}")
        logger.error(f"Failed to create output ZIP: {e}")
        return False


def cleanup_owners_directory():
    """Clean up the owners and data directories at the start of workflow"""
    directories_to_cleanup = [
        ("owners", os.path.join(BASE_DIR, "owners")),
        ("data", os.path.join(BASE_DIR, "data")),
    ]

    for dir_name, dir_path in directories_to_cleanup:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path)
                logger.info(f"🗑️ Cleaned up existing {dir_name} directory: {dir_path}")
                print_status(f"Cleaned up existing {dir_name} directory")
            except Exception as e:
                logger.warning(f"⚠️ Could not clean up {dir_name} directory: {e}")
                print_status(f"Warning: Could not clean up {dir_name} directory: {e}")
        else:
            logger.info(
                f"📁 {dir_name.capitalize()} directory does not exist, no cleanup needed"
            )

        # Create fresh directory
        try:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"📁 Created fresh {dir_name} directory: {dir_path}")
        except Exception as e:
            logger.error(f"❌ Could not create {dir_name} directory: {e}")
            raise


def import_county_scripts():
    """Import scripts directly from counties directory using county_jurisdiction from unnormalized_address.json"""
    import importlib.util
    import sys

    # Read county name from unnormalized_address.json
    seed_csv_path = os.path.join(BASE_DIR, "unnormalized_address.json")

    if os.path.exists(seed_csv_path):
        try:
            # Read as JSON since it's actually unnormalized_address.json content
            with open(seed_csv_path, "r", encoding="utf-8") as f:
                address_data = json.load(f)

            if "county_jurisdiction" in address_data:
                county_name = str(address_data["county_jurisdiction"]).strip()
                logger.info(f"📍 Found county_jurisdiction: {county_name}")
            else:
                logger.error(
                    "❌ 'county_jurisdiction' field not found in unnormalized_address.json"
                )
                return None

        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing unnormalized_address.json as JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error reading unnormalized_address.json: {e}")
            return None
    else:
        logger.error("❌ unnormalized_address.json not found")
        return None

    if not county_name:
        logger.error("❌ Could not determine county name from county_jurisdiction")
        return None

    # Try multiple variations of the county name
    county_variations = [
        county_name.lower(),  # lowercase
        county_name,  # original case
        county_name.title(),  # Title Case
        county_name.upper(),  # UPPERCASE
        county_name.replace(" ", ""),  # No spaces
        county_name.lower().replace(" ", ""),  # Lowercase no spaces
    ]

    # Add special cases for known counties
    if "miami" in county_name.lower() and "dade" in county_name.lower():
        county_variations.append("MiamiDade")
    if "palm" in county_name.lower() and "beach" in county_name.lower():
        county_variations.append("palm beach")

    # Required scripts
    required_scripts = [
        "owner_processor",
        "structure_extractor",
        "utility_extractor",
        "layout_extractor",
        "data_extractor",
    ]

    # Try to find the county directory
    counties_base = os.path.join(LOCAL_DIR, "counties")

    for variation in county_variations:
        county_path = os.path.join(counties_base, variation)

        if os.path.exists(county_path) and os.path.isdir(county_path):
            logger.info(f"✅ Found county directory: {county_path}")

            # Import all required scripts as modules
            modules = {}
            missing_scripts = []

            for script_name in required_scripts:
                script_path = os.path.join(county_path, f"{script_name}.py")

                if os.path.exists(script_path):
                    try:
                        # Create module spec
                        spec = importlib.util.spec_from_file_location(
                            f"county_{script_name}", script_path
                        )

                        if spec and spec.loader:
                            # Create and load module
                            module = importlib.util.module_from_spec(spec)
                            sys.modules[f"county_{script_name}"] = module
                            spec.loader.exec_module(module)
                            modules[script_name] = module
                            logger.info(f"📄 Imported: {script_name}.py")
                        else:
                            logger.error(
                                f"❌ Could not create spec for {script_name}.py"
                            )
                            missing_scripts.append(script_name)
                    except Exception as e:
                        logger.error(f"❌ Error importing {script_name}.py: {e}")
                        missing_scripts.append(script_name)
                else:
                    logger.warning(f"⚠️ Script not found: {script_path}")
                    missing_scripts.append(script_name)

            if missing_scripts:
                logger.error(
                    f"❌ Missing required scripts: {', '.join(missing_scripts)}"
                )
                return None

            logger.info(
                f"✅ Successfully imported {len(modules)} scripts from {variation}/ directory"
            )
            return modules

    # If we've tried all variations and none worked
    logger.error(
        f"❌ Could not find county directory for any variation of '{county_name}'"
    )
    logger.error(
        f"❌ Tried paths under {counties_base}: {', '.join(county_variations)}"
    )
    return None


def download_scripts_from_github():
    """Compatibility wrapper - now imports scripts locally instead of downloading"""
    modules = import_county_scripts()
    return modules is not None


def print_running(node_name):
    """Print running status"""
    print(f"🔄 RUNNING: {node_name}")
    logger.info(f"RUNNING: {node_name}")


def print_status(message):
    """Print status messages to terminal only"""
    print(f"STATUS: {message}")
    logger.info(f"STATUS: {message}")  # Also log to file


def print_completed(node_name, success=True):
    """Print completion status"""
    status = "✅ COMPLETED" if success else "❌ FAILED"
    print(f"{status}: {node_name}")
    logger.info(f"COMPLETED: {node_name} - Success: {success}")


def is_empty_value(value):
    """Check if a value is empty, None, or whitespace"""
    if value is None:
        return True
    if isinstance(value, str):
        return len(value.strip()) == 0
    return False


def ensure_directory(path):
    """Create directory if it doesn't exist"""
    os.makedirs(path, exist_ok=True)


def extract_query_params_and_base_url(url):
    """Extract base URL (including hash-routing path) and query parameters.
       Parses both regular ?query and ?query inside the fragment (after #)."""
    if not url or is_empty_value(url):
        return None, None

    try:
        from urllib.parse import urlparse, parse_qs

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        # Collect params from the standard query part
        params = {}
        if parsed.query:
            for k, v in parse_qs(parsed.query, keep_blank_values=True).items():
                params[k] = v[0] if len(v) == 1 else v

        # Include fragment in base_url; also parse ?query that might be inside the fragment
        if parsed.fragment:
            frag_path, _, frag_query = parsed.fragment.partition('?')
            # Keep the path-like part of the fragment in the base URL
            base_url = f"{base_url}#{frag_path}" if frag_path else f"{base_url}#"

            if frag_query:
                for k, v in parse_qs(frag_query, keep_blank_values=True).items():
                    if k in params:
                        # Merge with existing values
                        existing = params[k] if isinstance(params[k], list) else [params[k]]
                        merged = existing + v
                        params[k] = merged if len(merged) > 1 else merged[0]
                    else:
                        params[k] = v[0] if len(v) == 1 else v

        return base_url, params

    except Exception as e:
        logger.error(f"Error parsing URL {url}: {e}")
        return url, {}


def create_parcel_folder(
    parcel_id, address, method, url, county, headers=None, multi_value_query_string=None, json_body=None
):
    """Create folder name based on parcel_id"""
    # Create folder name based on parcel_id
    clean_parcel_id = re.sub(r"[^\w\-_]", "_", str(parcel_id))
    folder_name = f"output/{clean_parcel_id}"
    ensure_directory(folder_name + "/")

    # Extract base URL only (ignore URL query params, always use CSV multiValueQueryString)
    base_url, _ = extract_query_params_and_base_url(url)

    # Always use multiValueQueryString from CSV (not from URL)
    multi_value_query = multi_value_query_string if multi_value_query_string else {}

    # Create unnormalized_address.json
    unnormalized_address_data = {
        "full_address": address if not is_empty_value(address) else None,
        "source_http_request": {
            "method": method if not is_empty_value(method) else None,
            "url": base_url if not is_empty_value(base_url) else None,
            "multiValueQueryString": multi_value_query,
        },
        "county_jurisdiction": county if not is_empty_value(county) else None,
        "request_identifier": parcel_id if not is_empty_value(parcel_id) else None,
    }

    if headers and not is_empty_value(headers):
        unnormalized_address_data["source_http_request"]["headers"] = headers
    if json_body and not is_empty_value(json_body):
        unnormalized_address_data["source_http_request"]["json"] = json_body

    # Create property_seed.json
    property_seed_data = {
        "parcel_id": parcel_id if not is_empty_value(parcel_id) else None,
        "source_http_request": unnormalized_address_data["source_http_request"].copy(),
        "request_identifier": unnormalized_address_data["request_identifier"],
    }

    # Create relationship_property_to_address.json
    relationship_data = {
        "from": {"/": "./property_seed.json"},
        "to": {"/": "./unnormalized_address.json"},
    }

    # Create root schema
    root_schema = {
        "label": "Seed",
        "relationships": {
            "property_seed": {"/": "./relationship_property_to_address.json"}
        },
    }

    # Write all JSON files
    files_to_create = [
        (f"{folder_name}/unnormalized_address.json", unnormalized_address_data),
        (f"{folder_name}/property_seed.json", property_seed_data),
        (f"{folder_name}/relationship_property_to_address.json", relationship_data),
        (f"{folder_name}/seed_data_group.json", root_schema),
    ]

    for filename, data_obj in files_to_create:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data_obj, f, indent=2, ensure_ascii=False)

    return folder_name, unnormalized_address_data, property_seed_data


def process_csv_to_seed_folders(csv_file_path):
    """Process CSV file and create seed folders for each row"""
    if not os.path.exists(csv_file_path):
        logger.error(f"CSV file not found: {csv_file_path}")
        return False

    # Clean up output directory
    output_dir = os.path.join(BASE_DIR, "output")
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    try:
        created_folders = []

        with open(csv_file_path, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)  # Read all rows into memory

            # Validate CSV has exactly one row
            if len(rows) == 0:
                logger.error("❌ CSV file is empty - no data rows found")
                print_status("ERROR: CSV file is empty")
                return False
            elif len(rows) > 1:
                logger.error(
                    f"❌ CSV file contains {len(rows)} rows - only 1 row (1 property) is allowed for seed processing"
                )
                print_status(
                    f"ERROR: CSV contains {len(rows)} rows, only 1 property allowed"
                )
                return False

            logger.info(f"✅ CSV validation passed - found exactly 1 row")
            print_status("CSV validation passed - processing 1 property")

            # Process the single row
            for row_num, row in enumerate(rows, 1):
                try:
                    # Extract required fields from CSV row
                    parcel_id = row.get("parcel_id", "").strip()
                    address = row.get("address", "").strip()
                    method = row.get("method", "GET").strip()
                    url = row.get("url", "").strip()
                    county = row.get("county", "").strip()
                    headers = (
                        row.get("headers", "").strip() if row.get("headers") else None
                    )
                    multi_value_query_string_str = row.get(
                        "multiValueQueryString", ""
                    ).strip()
                    json_body = row.get("json", "").strip() if row.get("json") else None

                    # Parse multiValueQueryString from CSV if provided
                    multi_value_query_string = None
                    if multi_value_query_string_str and not is_empty_value(
                        multi_value_query_string_str
                    ):
                        try:
                            multi_value_query_string = json.loads(
                                multi_value_query_string_str
                            )
                            logger.info(
                                f"✅ Parsed multiValueQueryString from CSV: {multi_value_query_string}"
                            )
                        except json.JSONDecodeError as e:
                            logger.warning(
                                f"Row {row_num}: Invalid multiValueQueryString JSON format: {e}"
                            )
                            logger.warning(f"Raw value: {multi_value_query_string_str}")

                    # Validate required data
                    if is_empty_value(parcel_id):
                        logger.warning(
                            f"Row {row_num}: parcel_id is required but not provided, skipping"
                        )
                        continue

                    # Parse headers if provided (assuming JSON string)
                    parsed_headers = None
                    if headers:
                        try:
                            parsed_headers = json.loads(headers)
                        except json.JSONDecodeError:
                            logger.warning(
                                f"Row {row_num}: Invalid headers JSON format, ignoring headers"
                            )
                    parsed_json_body = None
                    if json_body:
                        try:
                            parsed_json_body = json.loads(json_body)
                        except json.JSONDecodeError:
                            logger.warning(f"Row {row_num}: Invalid json JSON format, ignoring json")

                    # Create parcel folder and files
                    folder_name, address_data, property_data = create_parcel_folder(
                        parcel_id,
                        address,
                        method,
                        url,
                        county,
                        parsed_headers,
                        multi_value_query_string,
                        parsed_json_body,
                    )

                    created_folders.append(folder_name)
                    logger.info(f"✅ Created seed files for parcel ID {parcel_id}")

                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    continue

        logger.info(
            f"✅ Successfully processed CSV and created {len(created_folders)} seed folders"
        )
        return len(created_folders) > 0

    except Exception as e:
        logger.error(f"❌ Error processing CSV file: {e}")
        return False


def create_seed_output_zip(output_name: str = "seed_output.zip") -> bool:
    """Create output ZIP file from seed folders"""
    output_zip_path = os.path.join(BASE_DIR, output_name)
    output_dir = os.path.join(BASE_DIR, "output")

    if not os.path.exists(output_dir):
        logger.error("No output directory found to zip")
        return False

    try:
        with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
            # Walk through output directory and add all files
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create archive path relative to output directory
                    archive_path = os.path.relpath(file_path, output_dir)
                    zip_ref.write(file_path, archive_path)
                    logger.info(f"Added to ZIP: {archive_path}")

        # Count files in the created ZIP
        with zipfile.ZipFile(output_zip_path, "r") as zip_ref:
            file_count = len(zip_ref.namelist())

        print_status(f"Created seed output ZIP: {output_name} with {file_count} files")
        logger.info(f"✅ Created seed output ZIP: {output_zip_path}")
        return True

    except Exception as e:
        logger.error(f"Failed to create seed output ZIP: {e}")
        return False

