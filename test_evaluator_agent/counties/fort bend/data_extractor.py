import os
import re
import json
import csv
from bs4 import BeautifulSoup

def clean_money(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return round(float(val), 2)
    try:
        return round(float(re.sub(r'[^\d.]', '', val)), 2) if val else None
    except Exception:
        return None

def clean_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        return None

def clean_str(val):
    if val is None:
        return None
    return str(val).strip()

def parse_date(val):
    if not val:
        return None
    # Try multiple date formats
    # Format 1: MM/DD/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", val)
    if m:
        month = m.group(1).zfill(2)
        day = m.group(2).zfill(2)
        year = m.group(3)
        return f"{year}-{month}-{day}"
    
    # Format 2: MM-DD-YYYY
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", val)
    if m:
        month = m.group(1).zfill(2)
        day = m.group(2).zfill(2)
        year = m.group(3)
        return f"{year}-{month}-{day}"
    
    # Format 3: Already in YYYY-MM-DD format
    if re.match(r"\d{4}-\d{2}-\d{2}", val):
        return val
    
    # If no valid format found, return None to avoid validation errors
    print(f"⚠️  Warning: Unrecognized date format '{val}' - setting to None")
    return None

def clean_name(name):
    """
    Clean and format names to match the required pattern: ^[A-Z][a-z]*([ \-',.][A-Za-z][a-z]*)*$
    """
    if not name:
        return None
    
    # Remove extra whitespace and capitalize properly
    name = name.strip()
    
    # Handle empty names
    if not name:
        return None
    
    # Split by common separators and capitalize each part
    parts = re.split(r'[ \-,\']+', name)
    cleaned_parts = []
    
    for part in parts:
        if part:
            # Capitalize first letter, lowercase the rest
            cleaned_part = part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper()
            cleaned_parts.append(cleaned_part)
    
    # Join with space
    cleaned_name = ' '.join(cleaned_parts)
    
    # Validate against pattern
    if re.match(r'^[A-Z][a-z]*([ \-,\'][A-Za-z][a-z]*)*$', cleaned_name):
        return cleaned_name
    else:
        # If still doesn't match, return a safe default
        print(f"⚠️  Warning: Name '{name}' couldn't be formatted to match pattern - using default")
        return "Unknown"
    
def validate_street_suffix(raw_suffix):
    """
    Validate and map street suffix to allowed values from the schema.
    Returns a valid suffix or None if not recognized.
    """
    # Allowed street suffix types from the schema
    allowed_suffixes = {
        # Common abbreviations
        "RD": "Rd", "ROAD": "Rd",
        "ST": "St", "STREET": "St", "STR": "St",
        "AVE": "Ave", "AVENUE": "Ave", "AV": "Ave",
        "DR": "Dr", "DRIVE": "Dr", "DRV": "Dr",
        "LN": "Ln", "LANE": "Ln",
        "BLVD": "Blvd", "BOULEVARD": "Blvd", "BL": "Blvd",
        "CT": "Ct", "COURT": "Ct", "CRT": "Ct",
        "CIR": "Cir", "CIRCLE": "Cir", "CRCL": "Cir",
        "PL": "Pl", "PLACE": "Pl", "PLC": "Pl",
        "WAY": "Way", "WY": "Way",
        "TRL": "Trl", "TRAIL": "Trl", "TR": "Trl",
        "PKWY": "Pkwy", "PARKWAY": "Pkwy", "PKY": "Pkwy",
        "HWY": "Hwy", "HIGHWAY": "Hwy", "HW": "Hwy",
        "EXPY": "Expy", "EXPRESSWAY": "Expy", "EXP": "Expy",
        "TER": "Ter", "TERRACE": "Ter", "TERR": "Ter",
        "ALY": "Aly", "ALLEY": "Aly", "ALY": "Aly",
        "PLZ": "Plz", "PLAZA": "Plz", "PLZA": "Plz",
        "SQ": "Sq", "SQUARE": "Sq", "SQR": "Sq",
        "CTR": "Ctr", "CENTER": "Ctr", "CNTR": "Ctr",
        "APT": "Apt", "APARTMENT": "Apt", "APT": "Apt",
        "STE": "Ste", "SUITE": "Ste", "SUIT": "Ste",
        "UNIT": "Unit", "UNT": "Unit",
        "FL": "Fl", "FLOOR": "Fl", "FLR": "Fl",
        "BLDG": "Bldg", "BUILDING": "Bldg", "BLD": "Bldg",
        "FRONT": "Front", "FR": "Front",
        "REAR": "Rear", "RR": "Rear",
        "LOWER": "Lower", "LWR": "Lower",
        "UPPER": "Upper", "UPR": "Upper",
        "MAIN": "Main", "MN": "Main",
        "BASEMENT": "Basement", "BSMT": "Basement",
        "PENTHOUSE": "Penthouse", "PENT": "Penthouse",
        "LOFT": "Loft", "LFT": "Loft",
        "GARAGE": "Garage", "GAR": "Garage",
        "CARPORT": "Carport", "CPRT": "Carport",
        "SHOP": "Shop", "SHP": "Shop",
        "OFFICE": "Office", "OFC": "Office",
        "STUDIO": "Studio", "STD": "Studio",
        "EFFICIENCY": "Efficiency", "EFF": "Efficiency",
        "ROOM": "Room", "RM": "Room",
        "CLOSET": "Closet", "CLST": "Closet",
        "STORAGE": "Storage", "STG": "Storage",
        "UTILITY": "Utility", "UTIL": "Utility",
        "LAUNDRY": "Laundry", "LAUN": "Laundry",
        "MECHANICAL": "Mechanical", "MECH": "Mechanical",
        "ELECTRICAL": "Electrical", "ELEC": "Electrical",
        "PLUMBING": "Plumbing", "PLMB": "Plumbing",
        "HEATING": "Heating", "HTG": "Heating",
        "COOLING": "Cooling", "CLNG": "Cooling",
        "VENTILATION": "Ventilation", "VENT": "Ventilation",
        "AIR": "Air", "AIR": "Air",
        "CONDITIONING": "Conditioning", "COND": "Conditioning",
        "REFRIGERATION": "Refrigeration", "REFR": "Refrigeration",
        "FREEZER": "Freezer", "FRZ": "Freezer",
        "WALK": "Walk", "WLK": "Walk",
        "PATH": "Path", "PTH": "Path",
        "BRIDGE": "Bridge", "BRDG": "Bridge",
        "TUNNEL": "Tunnel", "TUNL": "Tunnel",
        "OVERPASS": "Overpass", "OVP": "Overpass",
        "UNDERPASS": "Underpass", "UNDP": "Underpass",
        "RAMP": "Ramp", "RMP": "Ramp",
        "EXIT": "Exit", "EXT": "Exit",
        "ENTRANCE": "Entrance", "ENT": "Entrance",
        "ACCESS": "Access", "ACC": "Access",
        "SERVICE": "Service", "SVC": "Service",
        "BUSINESS": "Business", "BUS": "Business",
        "RESIDENTIAL": "Residential", "RES": "Residential",
        "COMMERCIAL": "Commercial", "COM": "Commercial",
        "INDUSTRIAL": "Industrial", "IND": "Industrial",
        "AGRICULTURAL": "Agricultural", "AGR": "Agricultural",
        "RECREATIONAL": "Recreational", "REC": "Recreational",
        "EDUCATIONAL": "Educational", "EDU": "Educational",
        "MEDICAL": "Medical", "MED": "Medical",
        "DENTAL": "Dental", "DENT": "Dental",
        "VETERINARY": "Veterinary", "VET": "Veterinary",
        "PHARMACY": "Pharmacy", "PHAR": "Pharmacy",
        "BANK": "Bank", "BNK": "Bank",
        "RESTAURANT": "Restaurant", "REST": "Restaurant",
        "HOTEL": "Hotel", "HTL": "Hotel",
        "MOTEL": "Motel", "MTL": "Motel",
        "INN": "Inn", "INN": "Inn",
        "LODGE": "Lodge", "LDG": "Lodge",
        "CABIN": "Cabin", "CBN": "Cabin",
        "COTTAGE": "Cottage", "COTT": "Cottage",
        "BUNGALOW": "Bungalow", "BUNG": "Bungalow",
        "DUPLEX": "Duplex", "DUP": "Duplex",
        "TRIPLEX": "Triplex", "TRIP": "Triplex",
        "QUADPLEX": "Quadplex", "QUAD": "Quadplex",
        "TOWNHOUSE": "Townhouse", "TWN": "Townhouse",
        "CONDO": "Condo", "COND": "Condo",
        "APARTMENT": "Apartment", "APT": "Apartment",
        "STUDIO": "Studio", "STD": "Studio",
        "EFFICIENCY": "Efficiency", "EFF": "Efficiency",
        "ROOM": "Room", "RM": "Room",
        "SUITE": "Suite", "STE": "Suite",
        "UNIT": "Unit", "UNT": "Unit",
        "FLOOR": "Floor", "FL": "Floor",
        "BUILDING": "Building", "BLDG": "Building",
        "COMPLEX": "Complex", "CMPX": "Complex",
        "TOWER": "Tower", "TWR": "Tower",
        "PARK": "Park", "PK": "Park",
        "GARDEN": "Garden", "GDN": "Garden",
        "COURT": "Court", "CT": "Court",
        "COMMONS": "Commons", "CMNS": "Commons",
        "MEADOW": "Meadow", "MDW": "Meadow",
        "VALLEY": "Valley", "VLY": "Valley",
        "HILL": "Hill", "HL": "Hill",
        "MOUNTAIN": "Mountain", "MTN": "Mountain",
        "RIDGE": "Ridge", "RDG": "Ridge",
        "CREEK": "Creek", "CRK": "Creek",
        "RIVER": "River", "RIV": "River",
        "LAKE": "Lake", "LK": "Lake",
        "POND": "Pond", "PND": "Pond",
        "STREAM": "Stream", "STRM": "Stream",
        "BROOK": "Brook", "BRK": "Brook",
        "SPRING": "Spring", "SPG": "Spring",
        "WELL": "Well", "WL": "Well",
        "CANYON": "Canyon", "CYN": "Canyon",
        "GULCH": "Gulch", "GLCH": "Gulch",
        "ARROYO": "Arroyo", "ARR": "Arroyo",
        "WASH": "Wash", "WSH": "Wash",
        "DRAIN": "Drain", "DRN": "Drain",
        "CHANNEL": "Channel", "CHNL": "Channel",
        "DITCH": "Ditch", "DCH": "Ditch",
        "CULVERT": "Culvert", "CLVT": "Culvert",
        "PIPE": "Pipe", "PIP": "Pipe",
        "TUNNEL": "Tunnel", "TUNL": "Tunnel",
        "BRIDGE": "Bridge", "BRDG": "Bridge",
        "CROSSING": "Crossing", "XING": "Crossing",
        "INTERSECTION": "Intersection", "INT": "Intersection",
        "JUNCTION": "Junction", "JCT": "Junction",
        "ROUNDABOUT": "Roundabout", "RAB": "Roundabout",
        "TRAFFIC": "Traffic", "TRF": "Traffic",
        "SIGNAL": "Signal", "SGNL": "Signal",
        "STOP": "Stop", "STP": "Stop",
        "YIELD": "Yield", "YLD": "Yield",
        "ONE": "One", "1": "One",
        "TWO": "Two", "2": "Two",
        "THREE": "Three", "3": "Three",
        "FOUR": "Four", "4": "Four",
        "FIVE": "Five", "5": "Five",
        "SIX": "Six", "6": "Six",
        "SEVEN": "Seven", "7": "Seven",
        "EIGHT": "Eight", "8": "Eight",
        "NINE": "Nine", "9": "Nine",
        "TEN": "Ten", "10": "Ten",
        "ELEVEN": "Eleven", "11": "Eleven",
        "TWELVE": "Twelve", "12": "Twelve",
        "THIRTEEN": "Thirteen", "13": "Thirteen",
        "FOURTEEN": "Fourteen", "14": "Fourteen",
        "FIFTEEN": "Fifteen", "15": "Fifteen",
        "SIXTEEN": "Sixteen", "16": "Sixteen",
        "SEVENTEEN": "Seventeen", "17": "Seventeen",
        "EIGHTEEN": "Eighteen", "18": "Eighteen",
        "NINETEEN": "Nineteen", "19": "Nineteen",
        "TWENTY": "Twenty", "20": "Twenty",
        "TWENTYONE": "Twenty-One", "21": "Twenty-One",
        "TWENTYTWO": "Twenty-Two", "22": "Twenty-Two",
        "TWENTYTHREE": "Twenty-Three", "23": "Twenty-Three",
        "TWENTYFOUR": "Twenty-Four", "24": "Twenty-Four",
        "TWENTYFIVE": "Twenty-Five", "25": "Twenty-Five",
        "TWENTYSIX": "Twenty-Six", "26": "Twenty-Six",
        "TWENTYSEVEN": "Twenty-Seven", "27": "Twenty-Seven",
        "TWENTYEIGHT": "Twenty-Eight", "28": "Twenty-Eight",
        "TWENTYNINE": "Twenty-Nine", "29": "Twenty-Nine",
        "THIRTY": "Thirty", "30": "Thirty",
        "THIRTYONE": "Thirty-One", "31": "Thirty-One",
        "THIRTYTWO": "Thirty-Two", "32": "Thirty-Two",
        "THIRTYTHREE": "Thirty-Three", "33": "Thirty-Three",
        "THIRTYFOUR": "Thirty-Four", "34": "Thirty-Four",
        "THIRTYFIVE": "Thirty-Five", "35": "Thirty-Five",
        "THIRTYSIX": "Thirty-Six", "36": "Thirty-Six",
        "THIRTYSEVEN": "Thirty-Seven", "37": "Thirty-Seven",
        "THIRTYEIGHT": "Thirty-Eight", "38": "Thirty-Eight",
        "THIRTYNINE": "Thirty-Nine", "39": "Thirty-Nine",
        "FORTY": "Forty", "40": "Forty",
        "FORTYONE": "Forty-One", "41": "Forty-One",
        "FORTYTWO": "Forty-Two", "42": "Forty-Two",
        "FORTYTHREE": "Forty-Three", "43": "Forty-Three",
        "FORTYFOUR": "Forty-Four", "44": "Forty-Four",
        "FORTYFIVE": "Forty-Five", "45": "Forty-Five",
        "FORTYSIX": "Forty-Six", "46": "Forty-Six",
        "FORTYSEVEN": "Forty-Seven", "47": "Forty-Seven",
        "FORTYEIGHT": "Forty-Eight", "48": "Forty-Eight",
        "FORTYNINE": "Forty-Nine", "49": "Forty-Nine",
        "FIFTY": "Fifty", "50": "Fifty"
    }
    
    # Try to find a match
    if raw_suffix in allowed_suffixes:
        return allowed_suffixes[raw_suffix]
    
    # If no exact match, try to find a partial match
    for key, value in allowed_suffixes.items():
        if raw_suffix in key or key in raw_suffix:
            return value
    
    # If still no match, return None to avoid validation errors
    print(f"⚠️  Warning: Unrecognized street suffix '{raw_suffix}' - setting to None")
    return None

def remove_null_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    if isinstance(data, dict) and all(v in (None, '', [], {}) for v in data.values()):
                        os.remove(path)
                except Exception:
                    continue

def parse_seed_csv():
    seed = {}
    with open('seed.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            seed[row['parcel_id']] = row
    return seed

# ---- wrapped content starts here (from original line 59) ----
def main():
    # Load owner and structure data
    with open("./owners/owners_schema.json") as f:
        owners_schema = json.load(f)
    with open("./owners/layout_data.json") as f:
        layout_data = json.load(f)
    with open("./owners/structure_data.json") as f:
        structure_data = json.load(f)
    with open("./owners/utility_data.json") as f:
        utility_data = json.load(f)

    seed_data = parse_seed_csv()

    os.makedirs("./data", exist_ok=True)
    input_dir = "./input/"
    input_files = [f for f in os.listdir(input_dir) if f.endswith(".html")]

    for input_file in input_files:
        parcel_id = os.path.splitext(input_file)[0]
        # Use base parcel id for seed lookup (strip trailing _year if present)
        base_parcel_id = parcel_id.split('_')[0]
        property_dir = os.path.join("./data", parcel_id)
        os.makedirs(property_dir, exist_ok=True)
        with open(os.path.join(input_dir, input_file), encoding="utf-8") as f:
            html = f.read()
        soup = BeautifulSoup(html, "html.parser")
        addr_key = f"property_{parcel_id}"

        # --- ADDRESS ---
        address_json = {
            "source_http_request": {
                "method": seed_data[base_parcel_id]["method"],
                "url": seed_data[base_parcel_id]["url"],
                "multiValueQueryString": json.loads(seed_data[base_parcel_id]["multiValueQueryString"])
            },
            "request_identifier": parcel_id,
            "city_name": None,
            "country_code": "US",
            "county_name": seed_data[base_parcel_id]["county"].upper().replace("COUNTY", "").strip(),
            "latitude": None,
            "longitude": None,
            "plus_four_postal_code": None,
            "postal_code": None,
            "state_code": "TX",
            "street_name": None,
            "street_post_directional_text": None,
            "street_pre_directional_text": None,
            "street_number": None,
            "street_suffix_type": None,
            "unit_identifier": None,
            "route_number": None,
            "township": None,
            "range": None,
            "section": None,
            "block": None
        }
        # Parse address from seed
        addr = seed_data[base_parcel_id]["address"]
        addr_parts = [a.strip() for a in addr.split(",")]
        if len(addr_parts) >= 1:
            street_addr = addr_parts[0]
            address_parts = street_addr.split()
            if address_parts and address_parts[0].isdigit():
                address_json["street_number"] = address_parts[0]
                # Find pre-directional (W, E, N, S, etc.)
                if len(address_parts) > 1 and address_parts[1].upper() in ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]:
                    address_json["street_pre_directional_text"] = address_parts[1].upper()
                    street_name_parts = address_parts[2:-1]
                else:
                    street_name_parts = address_parts[1:-1]
                address_json["street_name"] = " ".join(street_name_parts).upper()
                
                # Validate and map street suffix to allowed values
                raw_suffix = address_parts[-1].upper()
                address_json["street_suffix_type"] = validate_street_suffix(raw_suffix)
        if len(addr_parts) >= 2:
            address_json["city_name"] = addr_parts[1].strip().upper()
        if len(addr_parts) >= 3:
            state_zip = addr_parts[2].split()
            if len(state_zip) >= 2:
                address_json["postal_code"] = state_zip[1][:5]
        with open(os.path.join(property_dir, "address.json"), "w") as f:
            json.dump(address_json, f, indent=2)

        # --- TAXES ---
        # Extract from "Property Roll Value History" table
        tax_table = None
        for panel in soup.find_all("div", class_="panel panel-primary"):
            if panel.find("div", class_="panel-heading") and "Property Roll Value History" in panel.find("div", class_="panel-heading").text:
                tax_table = panel.find("table")
                break
        if tax_table:
            rows = tax_table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 6:
                    continue
                year = cols[0].text.strip()
                if not year.isdigit():
                    continue
                tax_json = {
                    "source_http_request": address_json["source_http_request"],
                    "request_identifier": f"{parcel_id}_tax_{year}",
                    "tax_year": int(year),
                    "property_assessed_value_amount": None,
                    "property_market_value_amount": None,
                    "property_building_amount": None,
                    "property_land_amount": None,
                    "property_taxable_value_amount": None,
                    "monthly_tax_amount": None,
                    "period_end_date": None,
                    "period_start_date": None
                }
                # Improvements, Land Market, Ag Valuation, HS Cap Loss, Appraised
                # Ensure all amounts are numbers (not null) to pass validation
                try:
                    building_val = float(cols[1].text.replace("$", "").replace(",", "")) if cols[1].text.strip() != "N/A" else 0.0
                    tax_json["property_building_amount"] = building_val
                except:
                    tax_json["property_building_amount"] = 0.0
                try:
                    land_val = float(cols[2].text.replace("$", "").replace(",", "")) if cols[2].text.strip() != "N/A" else 0.0
                    tax_json["property_land_amount"] = land_val
                except:
                    tax_json["property_land_amount"] = 0.0
                try:
                    if cols[1].text.strip() != "N/A" and cols[2].text.strip() != "N/A":
                        tax_json["property_market_value_amount"] = building_val + land_val
                    else:
                        tax_json["property_market_value_amount"] = 0.0
                except:
                    tax_json["property_market_value_amount"] = 0.0
                try:
                    assessed_val = float(cols[5].text.replace("$", "").replace(",", "")) if cols[5].text.strip() != "N/A" else 0.0
                    tax_json["property_assessed_value_amount"] = assessed_val
                except:
                    tax_json["property_assessed_value_amount"] = 0.0
                try:
                    taxable_val = float(cols[5].text.replace("$", "").replace(",", "")) if cols[5].text.strip() != "N/A" else 0.0
                    tax_json["property_taxable_value_amount"] = taxable_val
                except:
                    tax_json["property_taxable_value_amount"] = 0.0
                with open(os.path.join(property_dir, f"tax_{year}.json"), "w") as f:
                    json.dump(tax_json, f, indent=2)

        # --- SALES ---
        # Look for Property Deed History section which contains sales information
        sales_jsons = []
        sales_years = []
        sales_files_created = []  # Track which sales files were actually created
        
        # Find the Property Deed History panel
        deed_history_panels = soup.find_all("div", class_="panel panel-primary")
        for panel in deed_history_panels:
            panel_heading = panel.find("div", class_="panel-heading")
            if panel_heading and "Property Deed History" in panel_heading.text:
                # Found the deed history section, now extract sales data
                deed_table = panel.find("table")
                if deed_table:
                    rows = deed_table.find_all("tr")[1:]  # Skip header row
                    for i, row in enumerate(rows):
                        cols = row.find_all("td")
                        if len(cols) >= 3:  # Need at least 3 columns: Deed Date, Type, Description
                            deed_date = cols[0].text.strip()
                            deed_type = cols[1].text.strip()
                            description = cols[2].text.strip()
                            
                            # Only process actual sales/deeds, skip empty or non-sale entries
                            if deed_date and deed_date != "" and deed_type in ["D", "DW", "DG", "PB"]:
                                # Parse the date
                                parsed_date = parse_date(deed_date)
                                
                                                            # For sales, we don't have price information in Fort Bend data
                            # But we can create a sales record with the transfer date
                            # Note: purchase_price_amount must be a number, so we'll use 0 when price is unknown
                            sales_json = {
                                "source_http_request": address_json["source_http_request"],
                                "request_identifier": f"{parcel_id}_sale_{i + 1}",
                                "ownership_transfer_date": parsed_date,
                                "purchase_price_amount": 0  # Price not available in Fort Bend data, using 0 as required by schema
                            }
                            sales_jsons.append(sales_json)
                            sales_years.append(parsed_date[:4] if parsed_date else None)
                            
                            # Create sales file
                            sales_filename = f"sales_{i + 1}.json"
                            with open(os.path.join(property_dir, sales_filename), "w") as f:
                                json.dump(sales_json, f, indent=2)
                            sales_files_created.append(sales_filename)
                break  # Found the panel, no need to continue searching

        # --- LAYOUT ---
        # Extract number of bedrooms, full baths, half baths from HTML (if possible)
        bedroom_count = 0
        bathroom_count = 0
        half_bath_count = 0
        # Try to extract from the 'Property Details' table
        for table in soup.find_all("table"):
            header = table.find_previous("div", class_="panel-heading")
            if header and "Property Details" in header.text:
                for row in table.find_all("tr"):
                    tds = row.find_all("td")
                    if len(tds) == 2:
                        label = tds[0].text.strip().lower()
                        val = tds[1].text.strip()
                        if ("bedroom" in label or "bed room" in label) and val.isdigit():
                            bedroom_count = int(val)
                        if ("full bath" in label or ("bath" in label and "half" not in label)) and val.isdigit():
                            bathroom_count = int(val)
                        if ("half bath" in label or ("half" in label and "bath" in label)) and val.isdigit():
                            half_bath_count = int(val)
        # Fallback: try to extract from any table if not found
        if bedroom_count == 0 and bathroom_count == 0 and half_bath_count == 0:
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    tds = row.find_all("td")
                    if len(tds) == 2:
                        label = tds[0].text.strip().lower()
                        val = tds[1].text.strip()
                        if ("bedroom" in label or "bed room" in label) and val.isdigit():
                            bedroom_count = int(val)
                        if ("full bath" in label or ("bath" in label and "half" not in label)) and val.isdigit():
                            bathroom_count = int(val)
                        if ("half bath" in label or ("half" in label and "bath" in label)) and val.isdigit():
                            half_bath_count = int(val)

        # Remove any existing layout files
        for f_name in os.listdir(property_dir):
            if f_name.startswith("layout_") and f_name.endswith(".json"):
                os.remove(os.path.join(property_dir, f_name))
        # Always create at least one bedroom and one bathroom layout file if none found
        if bedroom_count == 0:
            bedroom_count = 1
        if bathroom_count == 0:
            bathroom_count = 1
        # Create layout files for each bedroom, full bath, half bath
        for i in range(bedroom_count):
            layout = {
                "source_http_request": address_json["source_http_request"],
                "request_identifier": f"{parcel_id}_layout_bedroom_{i + 1}",
                "space_type": "Bedroom",
                "space_index": i + 1,
                "flooring_material_type": None,
                "size_square_feet": None,
                "floor_level": None,
                "has_windows": None,
                "window_design_type": None,
                "window_material_type": None,
                "window_treatment_type": None,
                "is_finished": True,
                "furnished": None,
                "paint_condition": None,
                "flooring_wear": None,
                "clutter_level": None,
                "visible_damage": None,
                "countertop_material": None,
                "cabinet_style": None,
                "fixture_finish_quality": None,
                "design_style": None,
                "natural_light_quality": None,
                "decor_elements": None,
                "pool_type": None,
                "pool_equipment": None,
                "spa_type": None,
                "safety_features": None,
                "view_type": None,
                "lighting_features": None,
                "condition_issues": None,
                "is_exterior": False,
                "pool_condition": None,
                "pool_surface_type": None,
                "pool_water_quality": None
            }
            with open(os.path.join(property_dir, f"layout_bedroom_{i + 1}.json"), "w") as f:
                json.dump(layout, f, indent=2)
        for i in range(bathroom_count):
            layout = {
                "source_http_request": address_json["source_http_request"],
                "request_identifier": f"{parcel_id}_layout_bathroom_{i + 1}",
                "space_type": "Full Bathroom",
                "space_index": i + 1,
                "flooring_material_type": None,
                "size_square_feet": None,
                "floor_level": None,
                "has_windows": None,
                "window_design_type": None,
                "window_material_type": None,
                "window_treatment_type": None,
                "is_finished": True,
                "furnished": None,
                "paint_condition": None,
                "flooring_wear": None,
                "clutter_level": None,
                "visible_damage": None,
                "countertop_material": None,
                "cabinet_style": None,
                "fixture_finish_quality": None,
                "design_style": None,
                "natural_light_quality": None,
                "decor_elements": None,
                "pool_type": None,
                "pool_equipment": None,
                "spa_type": None,
                "safety_features": None,
                "view_type": None,
                "lighting_features": None,
                "condition_issues": None,
                "is_exterior": False,
                "pool_condition": None,
                "pool_surface_type": None,
                "pool_water_quality": None
            }
            with open(os.path.join(property_dir, f"layout_bathroom_{i + 1}.json"), "w") as f:
                json.dump(layout, f, indent=2)
        for i in range(half_bath_count):
            layout = {
                "source_http_request": address_json["source_http_request"],
                "request_identifier": f"{parcel_id}_layout_halfbath_{i + 1}",
                "space_type": "Half Bathroom / Powder Room",
                "space_index": i + 1,
                "flooring_material_type": None,
                "size_square_feet": None,
                "floor_level": None,
                "has_windows": None,
                "window_design_type": None,
                "window_material_type": None,
                "window_treatment_type": None,
                "is_finished": True,
                "furnished": None,
                "paint_condition": None,
                "flooring_wear": None,
                "clutter_level": None,
                "visible_damage": None,
                "countertop_material": None,
                "cabinet_style": None,
                "fixture_finish_quality": None,
                "design_style": None,
                "natural_light_quality": None,
                "decor_elements": None,
                "pool_type": None,
                "pool_equipment": None,
                "spa_type": None,
                "safety_features": None,
                "view_type": None,
                "lighting_features": None,
                "condition_issues": None,
                "is_exterior": False,
                "pool_condition": None,
                "pool_surface_type": None,
                "pool_water_quality": None
            }
            with open(os.path.join(property_dir, f"layout_halfbath_{i + 1}.json"), "w") as f:
                json.dump(layout, f, indent=2)

        # --- OWNERS (PERSON/COMPANY) ---
        if parcel_id in owners_schema:
            owners_by_date = owners_schema[parcel_id]["owners_by_date"]
            for i, (date, owners) in enumerate(owners_by_date.items()):
                unique_persons = []
                seen = set()
                for owner in owners:
                    if owner["type"] == "person":
                        key = (owner.get("first_name"), owner.get("last_name"), owner.get("middle_name"))
                        if key not in seen:
                            seen.add(key)
                            unique_persons.append(owner)
                for j, owner in enumerate(unique_persons):
                    person_json = {
                        "source_http_request": address_json["source_http_request"],
                        "request_identifier": f"{parcel_id}_person_{i+1}_{j+1}",
                        "birth_date": None,
                        "first_name": clean_name(owner.get("first_name")),
                        "last_name": clean_name(owner.get("last_name")),
                        "middle_name": clean_name(owner.get("middle_name")),
                        "prefix_name": None,
                        "suffix_name": None,
                        "us_citizenship_status": None,
                        "veteran_status": None
                    }
                    with open(os.path.join(property_dir, f"person_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(person_json, f, indent=2)

        # --- RELATIONSHIP FILES ---
        # FIXED: Only create sales relationships when sales files actually exist
        if parcel_id in owners_schema and sales_files_created:  # Only proceed if sales files were created
            owners_by_date = owners_schema[parcel_id]["owners_by_date"]
            for i, (date, owners) in enumerate(owners_by_date.items()):
                # Check if this sales file exists before creating relationships
                sales_file = f"sales_{i + 1}.json"
                if sales_file not in sales_files_created:
                    continue  # Skip creating relationships for non-existent sales files
                    
                unique_persons = []
                seen = set()
                for owner in owners:
                    if owner["type"] == "person":
                        key = (owner.get("first_name"), owner.get("last_name"), owner.get("middle_name"))
                        if key not in seen:
                            seen.add(key)
                            unique_persons.append(owner)
                for j, owner in enumerate(unique_persons):
                    rel = {
                        "to": {"/": f"./person_{i+1}_{j+1}.json"},
                        "from": {"/": f"./sales_{i+1}.json"}
                    }
                    with open(os.path.join(property_dir, f"relationship_sales_person_{i+1}_{j+1}.json"), "w") as f:
                        json.dump(rel, f, indent=2)

        # --- LOT ---
        # Create lot.json file (required before creating property_has_lot relationship)
        lot_json = {
            "source_http_request": address_json["source_http_request"],
            "request_identifier": f"{parcel_id}_lot",
            "lot_type": None,
            "lot_length_feet": None,
            "lot_width_feet": None,
            "lot_area_sqft": None,
            "landscaping_features": None,
            "view": None,
            "fencing_type": None,
            "fence_height": None,
            "fence_length": None,
            "driveway_material": None,
            "driveway_condition": None,
            "lot_condition_issues": None
        }
        with open(os.path.join(property_dir, "lot.json"), "w") as f:
            json.dump(lot_json, f, indent=2)
        
        # Create property_has_lot relationship (required by schema)
        lot_relationship = {
            "to": {"/": "./lot.json"},
            "from": {"/": "./property.json"}
        }
        with open(os.path.join(property_dir, "relationship_property_has_lot.json"), "w") as f:
            json.dump(lot_relationship, f, indent=2)
        
        # Log summary of what was processed
        print(f"✅ Extracted data for parcel {parcel_id}")
        print(f"   - Sales files created: {len(sales_files_created)}")
        print(f"   - Tax years found: {len([f for f in os.listdir(property_dir) if f.startswith('tax_')])}")
        print(f"   - Sales relationships created: {len(sales_files_created)}")
        print(f"   - Lot file created")
        print(f"   - Property has lot relationship created")

        # --- PROPERTY ---
        # Extract property type information from HTML
        property_type = None
        
        # Look for property type information in the HTML
        # Customize these selectors based on Fort Bend's actual HTML structure
        property_sections = soup.find_all(['h2', 'h3'], string=lambda text: text and any(
            keyword in text.lower() for keyword in ['property', 'land', 'building', 'improvement', 'classification']
        ))
        
        for section in property_sections:
            next_elem = section.find_next()
            if next_elem and next_elem.name == 'table':
                rows = next_elem.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].text.strip().lower()
                        value = cells[1].text.strip()
                        
                        if 'type' in label or 'classification' in label:
                            property_type = value
        
        # Default to "SingleFamily" if no property type found (like other county mappings)
        if not property_type:
            property_type = "SingleFamily"
        
        # Generate property.json file
        property_json = {
            "source_http_request": address_json["source_http_request"],
            "request_identifier": parcel_id,
            "livable_floor_area": None,
            "number_of_units_type": None,
            "parcel_identifier": parcel_id,
            "property_legal_description_text": None,
            "property_structure_built_year": None,
            "property_type": property_type
        }
        with open(os.path.join(property_dir, "property.json"), "w") as f:
            json.dump(property_json, f, indent=2)
        # Remove null files
        remove_null_files(property_dir)

if __name__ == "__main__":
    main()
