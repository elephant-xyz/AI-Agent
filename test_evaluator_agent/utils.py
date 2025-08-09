import re
import csv
import logging
import os
import time
import zipfile
import shutil
import json
import sys
import requests
import backoff
import git
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.abspath(".")

LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

log_file_path = os.path.join(LOGS_DIR, f"workflow_{int(time.time())}.log")

file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.CRITICAL)  # Only show critical messages


logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)

def create_output_zip(output_name: str = "transformed_output.zip") -> bool:
    """Create output ZIP file from processed data"""
    import zipfile

    output_zip_path = os.path.join(BASE_DIR, output_name)
    data_dir = os.path.join(BASE_DIR, "data")

    if not os.path.exists(data_dir):
        print("ERROR: No data directory found to zip")
        return False

    try:
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
            # Walk through data directory and add all files
            for root, dirs, files in os.walk(data_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create archive path relative to data directory
                    archive_path = os.path.relpath(file_path, data_dir)
                    zip_ref.write(file_path, archive_path)
                    logger.info(f"Added to ZIP: {archive_path}")

        # Count files in the created ZIP
        with zipfile.ZipFile(output_zip_path, 'r') as zip_ref:
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
        ("data", os.path.join(BASE_DIR, "data"))
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
            logger.info(f"📁 {dir_name.capitalize()} directory does not exist, no cleanup needed")

        # Create fresh directory
        try:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"📁 Created fresh {dir_name} directory: {dir_path}")
        except Exception as e:
            logger.error(f"❌ Could not create {dir_name} directory: {e}")
            raise

@backoff.on_exception(
    backoff.expo,
    (git.GitCommandError, ConnectionError, TimeoutError),
    max_tries=3,
    max_time=300,  # 5 minutes total
    on_backoff=lambda details: logger.warning(
        f"🔄 Git clone failed, retrying in {details['wait']:.1f}s (attempt {details['tries']})"),
    on_giveup=lambda details: logger.error(f"💥 Git clone failed after {details['tries']} attempts")
)
def download_scripts_from_github():
    """Download scripts from GitHub using county_jurisdiction from unnormalized_address.json"""

    # Read county name from unnormalized_address.json (which is saved as seed.csv)
    seed_csv_path = os.path.join(BASE_DIR, "unnormalized_address.json")  # This is actually the unnormalized_address.json content

    if os.path.exists(seed_csv_path):
        try:
            # Read as JSON since it's actually unnormalized_address.json content
            with open(seed_csv_path, 'r', encoding='utf-8') as f:
                address_data = json.load(f)

            if 'county_jurisdiction' in address_data:
                county_name = str(address_data['county_jurisdiction']).strip()
                logger.info(f"📍 Found county_jurisdiction: {county_name}")
            else:
                logger.error("❌ 'county_jurisdiction' field not found in unnormalized_address.json")
                return False

        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parsing unnormalized_address.json as JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error reading unnormalized_address.json: {e}")
            return False
    else:
        logger.error("❌ unnormalized_address.json not found (saved as seed.csv)")
        return False

    if not county_name:
        logger.error("❌ Could not determine county name from county_jurisdiction")
        return False

    # Try multiple variations of the county name
    county_variations = [
        county_name.lower(),  # lowercase
        county_name,  # original case
        county_name.title(),  # Title Case
        county_name.upper()  # UPPERCASE
    ]

    try:
        for variation in county_variations:
            # GitHub API URL for the counties directory
            api_url = f"https://api.github.com/repos/elephant-xyz/AI-Agent/contents/counties/{variation}"

            logger.info(f"🔄 Trying county variation: '{variation}' - {api_url}")

            response = requests.get(api_url, timeout=30)

            if response.status_code == 404:
                logger.warning(f"⚠️ County directory '{variation}' not found, trying next variation...")
                continue
            elif response.status_code != 200:
                logger.warning(
                    f"⚠️ GitHub API request failed for '{variation}': {response.status_code}, trying next variation...")
                continue

            # If we get here, we found a valid directory
            logger.info(f"✅ Found county directory: '{variation}'")

            files_data = response.json()
            local_scripts_dir = os.path.join(BASE_DIR, "scripts")
            os.makedirs(local_scripts_dir, exist_ok=True)

            copied_files = []

            for file_info in files_data:
                if file_info['name'].endswith('.py') and file_info['type'] == 'file':
                    # Download the file content
                    file_response = requests.get(file_info['download_url'], timeout=30)
                    if file_response.status_code == 200:
                        file_path = os.path.join(local_scripts_dir, file_info['name'])
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(file_response.text)
                        copied_files.append(file_info['name'])
                        logger.info(f"📄 Downloaded: {file_info['name']}")

            if copied_files:
                logger.info(f"✅ Downloaded {len(copied_files)} scripts from {variation}/ directory")
                return True
            else:
                logger.warning(f"⚠️ No Python scripts found in {variation}/ directory")
                # Continue to try other variations even if this one has no scripts

        # If we've tried all variations and none worked
        logger.error(f"❌ Could not find county directory for any variation of '{county_name}'")
        logger.error(f"❌ Tried: {', '.join(county_variations)}")
        return False

    except Exception as e:
        logger.error(f"❌ Error using GitHub API: {e}")
        return False

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
    """Extract base URL and query parameters separately"""
    if not url or is_empty_value(url):
        return None, None

    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        if parsed.query:
            query_dict = parse_qs(parsed.query)
            # Convert single-item lists to single values, keep multi-item as lists
            multi_value_query = {}
            for key, values in query_dict.items():
                if len(values) == 1:
                    multi_value_query[key] = values[0]
                else:
                    multi_value_query[key] = values
            return base_url, multi_value_query
        else:
            return base_url, {}
    except Exception as e:
        logger.error(f"Error parsing URL {url}: {e}")
        return url, {}


def create_parcel_folder(parcel_id, address, method, url, county, headers=None, multi_value_query_string=None):
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
            "multiValueQueryString": multi_value_query
        },
        "county_jurisdiction": county if not is_empty_value(county) else None,
        "request_identifier": parcel_id if not is_empty_value(parcel_id) else None,
    }

    if headers and not is_empty_value(headers):
        unnormalized_address_data["source_http_request"]["headers"] = headers

    # Create property_seed.json
    property_seed_data = {
        "parcel_id": parcel_id if not is_empty_value(parcel_id) else None,
        "source_http_request": {
            "method": method if not is_empty_value(method) else None,
            "url": base_url if not is_empty_value(base_url) else None,
            "multiValueQueryString": multi_value_query
        },
        "request_identifier": parcel_id if not is_empty_value(parcel_id) else None,
    }

    if headers and not is_empty_value(headers):
        property_seed_data["source_http_request"]["headers"] = headers

    # Create relationship_property_to_address.json
    relationship_data = {
        "from": {"/": "./property_seed.json"},
        "to": {"/": "./unnormalized_address.json"}
    }

    # Create root schema
    root_schema = {
        "label": "Seed",
        "relationships": {"property_seed": {"/": "./relationship_property_to_address.json"}},
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

        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)  # Read all rows into memory

            # Validate CSV has exactly one row
            if len(rows) == 0:
                logger.error("❌ CSV file is empty - no data rows found")
                print_status("ERROR: CSV file is empty")
                return False
            elif len(rows) > 1:
                logger.error(
                    f"❌ CSV file contains {len(rows)} rows - only 1 row (1 property) is allowed for seed processing")
                print_status(f"ERROR: CSV contains {len(rows)} rows, only 1 property allowed")
                return False

            logger.info(f"✅ CSV validation passed - found exactly 1 row")
            print_status("CSV validation passed - processing 1 property")

            # Process the single row
            for row_num, row in enumerate(rows, 1):
                try:
                    # Extract required fields from CSV row
                    parcel_id = row.get('parcel_id', '').strip()
                    address = row.get('address', '').strip()
                    method = row.get('method', 'GET').strip()
                    url = row.get('url', '').strip()
                    county = row.get('county', '').strip()
                    headers = row.get('headers', '').strip() if row.get('headers') else None
                    multi_value_query_string_str = row.get('multiValueQueryString', '').strip()

                    # Parse multiValueQueryString from CSV if provided
                    multi_value_query_string = None
                    if multi_value_query_string_str and not is_empty_value(multi_value_query_string_str):
                        try:
                            multi_value_query_string = json.loads(multi_value_query_string_str)
                            logger.info(f"✅ Parsed multiValueQueryString from CSV: {multi_value_query_string}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"Row {row_num}: Invalid multiValueQueryString JSON format: {e}")
                            logger.warning(f"Raw value: {multi_value_query_string_str}")

                    # Validate required data
                    if is_empty_value(parcel_id):
                        logger.warning(f"Row {row_num}: parcel_id is required but not provided, skipping")
                        continue

                    # Parse headers if provided (assuming JSON string)
                    parsed_headers = None
                    if headers:
                        try:
                            parsed_headers = json.loads(headers)
                        except json.JSONDecodeError:
                            logger.warning(f"Row {row_num}: Invalid headers JSON format, ignoring headers")

                    # Create parcel folder and files
                    folder_name, address_data, property_data = create_parcel_folder(
                        parcel_id, address, method, url, county, parsed_headers, multi_value_query_string
                    )

                    created_folders.append(folder_name)
                    logger.info(f"✅ Created seed files for parcel ID {parcel_id}")

                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    continue

        logger.info(f"✅ Successfully processed CSV and created {len(created_folders)} seed folders")
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
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
            # Walk through output directory and add all files
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create archive path relative to output directory
                    archive_path = os.path.relpath(file_path, output_dir)
                    zip_ref.write(file_path, archive_path)
                    logger.info(f"Added to ZIP: {archive_path}")

        # Count files in the created ZIP
        with zipfile.ZipFile(output_zip_path, 'r') as zip_ref:
            file_count = len(zip_ref.namelist())

        print_status(f"Created seed output ZIP: {output_name} with {file_count} files")
        logger.info(f"✅ Created seed output ZIP: {output_zip_path}")
        return True

    except Exception as e:
        logger.error(f"Failed to create seed output ZIP: {e}")
        return False