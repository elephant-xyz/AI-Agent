import logging
import os
import time
import json
import sys

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
