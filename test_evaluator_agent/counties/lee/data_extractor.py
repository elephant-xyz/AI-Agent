import os
import re
import json
import csv
from bs4 import BeautifulSoup
from urllib.parse import quote



# Helper functions for address parsing
def parse_date_to_iso(date_string):
    """Convert various date formats to ISO YYYY-MM-DD format"""
    if not date_string or not date_string.strip():
        return None

    date_string = date_string.strip()

    # Common date patterns to try
    date_patterns = [
        '%m/%d/%Y',  # MM/DD/YYYY (03/24/2025)
        '%m-%d-%Y',  # MM-DD-YYYY
        '%Y-%m-%d',  # YYYY-MM-DD (already ISO)
        '%Y/%m/%d',  # YYYY/MM/DD
        '%d/%m/%Y',  # DD/MM/YYYY
        '%d-%m-%Y',  # DD-MM-YYYY
        '%B %d, %Y',  # March 24, 2025
        '%b %d, %Y',  # Mar 24, 2025
        '%d %B %Y',  # 24 March 2025
        '%d %b %Y',  # 24 Mar 2025
    ]

    import datetime

    for pattern in date_patterns:
        try:
            parsed_date = datetime.datetime.strptime(date_string, pattern)
            return parsed_date.strftime('%Y-%m-%d')  # Convert to ISO format
        except ValueError:
            continue

    # If no pattern matches, try to extract date components with regex
    import re

    # Try to find MM/DD/YYYY pattern anywhere in the string
    match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_string)
    if match:
        month, day, year = match.groups()
        try:
            parsed_date = datetime.datetime(int(year), int(month), int(day))
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Try to find YYYY-MM-DD pattern
    match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_string)
    if match:
        year, month, day = match.groups()
        try:
            parsed_date = datetime.datetime(int(year), int(month), int(day))
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # If all else fails, return None
    return None

def parse_address(address):
    """Parse address string into components with improved directional and suffix handling"""
    result = {
        'street_number': None,
        'street_name': None,
        'unit_identifier': None,
        'plus_four_postal_code': None,
        'street_post_directional_text': None,
        'street_pre_directional_text': None,
        'street_suffix_type': None,
        'city_name': None,
        'state_code': None,
        'postal_code': None,
        'country_code': 'US',
        'county_name': None,
        'route_number': None
    }

    # Define mappings based on schema
    directional_mappings = {
        'NORTH': 'N', 'SOUTH': 'S', 'EAST': 'E', 'WEST': 'W',
        'NORTHEAST': 'NE', 'NORTHWEST': 'NW', 'SOUTHEAST': 'SE', 'SOUTHWEST': 'SW',
        'N': 'N', 'S': 'S', 'E': 'E', 'W': 'W',
        'NE': 'NE', 'NW': 'NW', 'SE': 'SE', 'SW': 'SW'
    }

    # Schema-compliant suffix mappings (same as before)
    suffix_mappings = {
        'STREET': 'St', 'ST': 'St',
        'AVENUE': 'Ave', 'AVE': 'Ave',
        'BOULEVARD': 'Blvd', 'BLVD': 'Blvd',
        'ROAD': 'Rd', 'RD': 'Rd',
        'LANE': 'Ln', 'LN': 'Ln',
        'DRIVE': 'Dr', 'DR': 'Dr',
        'COURT': 'Ct', 'CT': 'Ct',
        'PLACE': 'Pl', 'PL': 'Pl',
        'TERRACE': 'Ter', 'TER': 'Ter',
        'CIRCLE': 'Cir', 'CIR': 'Cir',
        'WAY': 'Way', 'WAY': 'Way',
        'PARKWAY': 'Pkwy', 'PKWY': 'Pkwy',
        'PLAZA': 'Plz', 'PLZ': 'Plz',
        'TRAIL': 'Trl', 'TRL': 'Trl',
        'BEND': 'Bnd', 'BND': 'Bnd',
        'LOOP': 'Loop', 'LOOP': 'Loop',
        'CRESCENT': 'Cres', 'CRES': 'Cres',
        'MANOR': 'Mnr', 'MNR': 'Mnr',
        'SQUARE': 'Sq', 'SQ': 'Sq',
        'CROSSING': 'Xing', 'XING': 'Xing',
        'PATH': 'Path', 'PATH': 'Path',
        'RUN': 'Run', 'RUN': 'Run',
        'WALK': 'Walk', 'WALK': 'Walk',
        'ROW': 'Row', 'ROW': 'Row',
        'ALLEY': 'Aly', 'ALY': 'Aly',
        'BEACH': 'Bch', 'BCH': 'Bch',
        'BRIDGE': 'Br', 'BRG': 'Br',
        'BROOK': 'Brk', 'BRK': 'Brk',
        'BROOKS': 'Brks', 'BRKS': 'Brks',
        'BUG': 'Bg', 'BG': 'Bg',
        'BUGS': 'Bgs', 'BGS': 'Bgs',
        'CLUB': 'Clb', 'CLB': 'Clb',
        'CLIFF': 'Clf', 'CLF': 'Clf',
        'CLIFFS': 'Clfs', 'CLFS': 'Clfs',
        'COMMON': 'Cmn', 'CMN': 'Cmn',
        'COMMONS': 'Cmns', 'CMNS': 'Cmns',
        'CORNER': 'Cor', 'COR': 'Cor',
        'CORNERS': 'Cors', 'CORS': 'Cors',
        'CREEK': 'Crk', 'CRK': 'Crk',
        'COURSE': 'Crse', 'CRSE': 'Crse',
        'CREST': 'Crst', 'CRST': 'Crst',
        'CAUSEWAY': 'Cswy', 'CSWY': 'Cswy',
        'COVE': 'Cv', 'CV': 'Cv',
        'CANYON': 'Cyn', 'CYN': 'Cyn',
        'DALE': 'Dl', 'DL': 'Dl',
        'DAM': 'Dm', 'DM': 'Dm',
        'DRIVES': 'Drs', 'DRS': 'Drs',
        'DIVIDE': 'Dv', 'DV': 'Dv',
        'ESTATE': 'Est', 'EST': 'Est',
        'ESTATES': 'Ests', 'ESTS': 'Ests',
        'EXPRESSWAY': 'Expy', 'EXPY': 'Expy',
        'EXTENSION': 'Ext', 'EXT': 'Ext',
        'EXTENSIONS': 'Exts', 'EXTS': 'Exts',
        'FALL': 'Fall', 'FALL': 'Fall',
        'FALLS': 'Fls', 'FLS': 'Fls',
        'FLAT': 'Flt', 'FLT': 'Flt',
        'FLATS': 'Flts', 'FLTS': 'Flts',
        'FORD': 'Frd', 'FRD': 'Frd',
        'FORDS': 'Frds', 'FRDS': 'Frds',
        'FORGE': 'Frg', 'FRG': 'Frg',
        'FORGES': 'Frgs', 'FRGS': 'Frgs',
        'FORK': 'Frk', 'FRK': 'Frk',
        'FORKS': 'Frks', 'FRKS': 'Frks',
        'FOREST': 'Frst', 'FRST': 'Frst',
        'FREEWAY': 'Fwy', 'FWY': 'Fwy',
        'FIELD': 'Fld', 'FLD': 'Fld',
        'FIELDS': 'Flds', 'FLDS': 'Flds',
        'GARDEN': 'Gdn', 'GDN': 'Gdn',
        'GARDENS': 'Gdns', 'GDNS': 'Gdns',
        'GLEN': 'Gln', 'GLN': 'Gln',
        'GLENS': 'Glns', 'GLNS': 'Glns',
        'GREEN': 'Grn', 'GRN': 'Grn',
        'GREENS': 'Grns', 'GRNS': 'Grns',
        'GROVE': 'Grv', 'GRV': 'Grv',
        'GROVES': 'Grvs', 'GRVS': 'Grvs',
        'GATEWAY': 'Gtwy', 'GTWY': 'Gtwy',
        'HARBOR': 'Hbr', 'HBR': 'Hbr',
        'HARBORS': 'Hbrs', 'HBRS': 'Hbrs',
        'HILL': 'Hl', 'HL': 'Hl',
        'HILLS': 'Hls', 'HLS': 'Hls',
        'HOLLOW': 'Holw', 'HOLW': 'Holw',
        'HEIGHTS': 'Hts', 'HTS': 'Hts',
        'HAVEN': 'Hvn', 'HVN': 'Hvn',
        'HIGHWAY': 'Hwy', 'HWY': 'Hwy',
        'INLET': 'Inlt', 'INLT': 'Inlt',
        'ISLAND': 'Is', 'IS': 'Is',
        'ISLANDS': 'Iss', 'ISS': 'Iss',
        'ISLE': 'Isle', 'ISLE': 'Isle',
        'JUNCTION': 'Jct', 'JCT': 'Jct',
        'JUNCTIONS': 'Jcts', 'JCTS': 'Jcts',
        'KNOLL': 'Knl', 'KNL': 'Knl',
        'KNOLLS': 'Knls', 'KNLS': 'Knls',
        'LOCK': 'Lck', 'LCK': 'Lck',
        'LOCKS': 'Lcks', 'LCKS': 'Lcks',
        'LODGE': 'Ldg', 'LDG': 'Ldg',
        'LIGHT': 'Lgt', 'LGT': 'Lgt',
        'LIGHTS': 'Lgts', 'LGTS': 'Lgts',
        'LAKE': 'Lk', 'LK': 'Lk',
        'LAKES': 'Lks', 'LKS': 'Lks',
        'LANDING': 'Lndg', 'LNDG': 'Lndg',
        'MALL': 'Mall', 'MALL': 'Mall',
        'MEADOW': 'Mdw', 'MDW': 'Mdw',
        'MEADOWS': 'Mdws', 'MDWS': 'Mdws',
        'MEWS': 'Mews', 'MEWS': 'Mews',
        'MILL': 'Ml', 'ML': 'Ml',
        'MILLS': 'Mls', 'MLS': 'Mls',
        'MANORS': 'Mnrs', 'MNRS': 'Mnrs',
        'MOUNT': 'Mt', 'MT': 'Mt',
        'MOUNTAIN': 'Mtn', 'MTN': 'Mtn',
        'MOUNTAINS': 'Mtns', 'MTNS': 'Mtns',
        'OVERPASS': 'Opas', 'OPAS': 'Opas',
        'ORCHARD': 'Orch', 'ORCH': 'Orch',
        'OVAL': 'Oval', 'OVAL': 'Oval',
        'PARK': 'Park', 'PARK': 'Park',
        'PASS': 'Pass', 'PASS': 'Pass',
        'PIKE': 'Pike', 'PIKE': 'Pike',
        'PLAIN': 'Pln', 'PLN': 'Pln',
        'PLAINS': 'Plns', 'PLNS': 'Plns',
        'PINE': 'Pne', 'PNE': 'Pne',
        'PINES': 'Pnes', 'PNES': 'Pnes',
        'PRAIRIE': 'Pr', 'PR': 'Pr',
        'PORT': 'Prt', 'PRT': 'Prt',
        'PORTS': 'Prts', 'PRTS': 'Prts',
        'PASSAGE': 'Psge', 'PSGE': 'Psge',
        'POINT': 'Pt', 'PT': 'Pt',
        'POINTS': 'Pts', 'PTS': 'Pts',
        'RADIAL': 'Radl', 'RADL': 'Radl',
        'RAMP': 'Ramp', 'RAMP': 'Ramp',
        'RIDGE': 'Rdg', 'RDG': 'Rdg',
        'RIDGES': 'Rdgs', 'RDGS': 'Rdgs',
        'ROADS': 'Rds', 'RDS': 'Rds',
        'REST': 'Rst', 'REST': 'Rst',
        'RANCH': 'Rnch', 'RNCH': 'Rnch',
        'RAPID': 'Rpd', 'RPD': 'Rpd',
        'RAPIDS': 'Rpds', 'RPDS': 'Rpds',
        'ROUTE': 'Rte', 'RTE': 'Rte',
        'SHOAL': 'Shl', 'SHL': 'Shl',
        'SHOALS': 'Shls', 'SHLS': 'Shls',
        'SHORE': 'Shr', 'SHR': 'Shr',
        'SHORES': 'Shrs', 'SHRS': 'Shrs',
        'SKYWAY': 'Skwy', 'SKWY': 'Skwy',
        'SUMMIT': 'Smt', 'SMT': 'Smt',
        'SPRING': 'Spg', 'SPG': 'Spg',
        'SPRINGS': 'Spgs', 'SPGS': 'Spgs',
        'SPUR': 'Spur', 'SPUR': 'Spur',
        'SQUARES': 'Sqs', 'SQS': 'Sqs',
        'STATION': 'Sta', 'STA': 'Sta',
        'STRAVENUE': 'Stra', 'STRA': 'Stra',
        'STREAM': 'Strm', 'STRM': 'Strm',
        'STREETS': 'Sts', 'STS': 'Sts',
        'THROUGHWAY': 'Trwy', 'TRWY': 'Trwy',
        'TRACE': 'Trce', 'TRCE': 'Trce',
        'TRAFFICWAY': 'Trfy', 'TRFY': 'Trfy',
        'TRAILER': 'Trlr', 'TRLR': 'Trlr',
        'TUNNEL': 'Tunl', 'TUNL': 'Tunl',
        'UNION': 'Un', 'UN': 'Un',
        'UNIONS': 'Uns', 'UNS': 'Uns',
        'UNDERPASS': 'Upas', 'UPAS': 'Upas',
        'VIA': 'Via', 'VIA': 'Via',
        'VIEW': 'Vw', 'VIEW': 'Vw',
        'VIEWS': 'Vws', 'VIEWS': 'Vws',
        'VILLAGE': 'Vlg', 'VLG': 'Vlg',
        'VILLAGES': 'Vlgs', 'VLGS': 'Vlgs',
        'VALLEY': 'Vl', 'VLY': 'Vl',
        'VALLEYS': 'Vlys', 'VLYS': 'Vlys',
        'WAYS': 'Ways', 'WAYS': 'Ways',
        'WELL': 'Wl', 'WL': 'Wl',
        'WELLS': 'Wls', 'WLS': 'Wls',
        'CROSSROAD': 'Xrd', 'XRD': 'Xrd',
        'CROSSROADS': 'Xrds', 'XRDS': 'Xrds'
    }

    parts = address.split(',')
    if len(parts) >= 3:
        street = parts[0].strip()
        city = parts[1].strip()
        state_zip = parts[2].strip()

        result['city_name'] = city.upper()

        # Parse state and zip
        state_zip_parts = state_zip.split()
        if len(state_zip_parts) >= 2:
            result['state_code'] = state_zip_parts[0]
            zip_code = state_zip_parts[1]
            result['postal_code'] = zip_code[:5]
            if len(zip_code) > 5:
                result['plus_four_postal_code'] = zip_code[6:] if zip_code[5] == '-' else zip_code[5:]

        # SMARTER STREET PARSING
        street_parts = street.split()
        if street_parts:
            result['street_number'] = street_parts[0]
            remaining_parts = street_parts[1:]

            if not remaining_parts:
                return result

            # Create a list to mark each part: 'PRE_DIR', 'SUFFIX', 'POST_DIR', 'STREET'
            part_types = ['STREET'] * len(remaining_parts)

            # Mark all suffixes
            for i, part in enumerate(remaining_parts):
                if part.upper() in suffix_mappings:
                    part_types[i] = 'SUFFIX'

            # Mark all directionals
            for i, part in enumerate(remaining_parts):
                if part.upper() in directional_mappings:
                    part_types[i] = 'DIRECTIONAL'

            # Now determine which directionals are PRE vs POST
            # Strategy: Find the MAIN suffix (rightmost suffix)
            main_suffix_idx = None
            for i in range(len(remaining_parts) - 1, -1, -1):
                if part_types[i] == 'SUFFIX':
                    main_suffix_idx = i
                    break

            # Directionals before any suffix are PRE
            # Directionals after the main suffix are POST
            pre_directional = None
            post_directional = None
            suffix = None

            for i, part in enumerate(remaining_parts):
                part_upper = part.upper()

                if part_types[i] == 'SUFFIX' and i == main_suffix_idx:
                    suffix = suffix_mappings[part_upper]

                elif part_types[i] == 'DIRECTIONAL':
                    if main_suffix_idx is not None:
                        if i < main_suffix_idx and pre_directional is None:
                            # First directional before suffix is pre-directional
                            pre_directional = directional_mappings[part_upper]
                        elif i > main_suffix_idx and post_directional is None:
                            # First directional after suffix is post-directional
                            post_directional = directional_mappings[part_upper]
                    else:
                        # No suffix found, assume first directional is pre
                        if pre_directional is None:
                            pre_directional = directional_mappings[part_upper]

            # Extract street name and route number (everything that's not pre-directional, suffix, or post-directional)
            street_name_parts = []
            route_number = None

            for i, part in enumerate(remaining_parts):
                part_upper = part.upper()

                # Skip if it's the pre-directional we selected
                if pre_directional and part_upper in directional_mappings and directional_mappings[
                    part_upper] == pre_directional:
                    # Only skip the FIRST occurrence of this directional
                    if not any(
                            directional_mappings.get(remaining_parts[j].upper()) == pre_directional for j in range(i)):
                        continue

                # Skip if it's the main suffix
                if i == main_suffix_idx:
                    continue

                # Skip if it's the post-directional we selected
                if post_directional and part_upper in directional_mappings and directional_mappings[
                    part_upper] == post_directional:
                    # Only skip if it's after the main suffix
                    if main_suffix_idx is not None and i > main_suffix_idx:
                        continue

                # Check if this part is a route number (digit that appears after route-related keywords)
                if part.isdigit() and len(street_name_parts) > 0:
                    # Check if previous parts contain route-related keywords
                    previous_text = ' '.join(street_name_parts).upper()
                    route_keywords = ['HWY', 'HIGHWAY', 'ROUTE', 'RT', 'RTE', 'STATE', 'US', 'SR', 'CR', 'COUNTY',
                                      'ROAD']

                    if any(keyword in previous_text for keyword in route_keywords):
                        route_number = part
                        continue  # Don't add to street name

                # This part is part of the street name
                street_name_parts.append(part)

            # Set results
            result['street_pre_directional_text'] = pre_directional
            result['street_post_directional_text'] = post_directional
            result['street_suffix_type'] = suffix
            result['street_name'] = ' '.join(street_name_parts) if street_name_parts else None
            result['route_number'] = route_number

    return result


def extract_location_data_from_html(html_content):
    """Extract location data (township, range, section, etc.) from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    location_data = {
        'township': None,
        'range': None,
        'section': None,
        'block': None,
        'latitude': None,
        'longitude': None,
        'route_number': None
    }

    # Look for the appraisalDetailsLocation table
    location_table = soup.find('table', {'class': 'appraisalDetailsLocation'})
    if location_table:
        rows = location_table.find_all('tr')

        # Process the table to extract location data
        for i, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])

            # First data row contains Township, Range, Section, Block, Lot
            if i == 1 and len(cells) >= 3:  # Data row
                if cells[0].get_text(strip=True):  # Township
                    location_data['township'] = cells[0].get_text(strip=True)
                if cells[1].get_text(strip=True):  # Range
                    location_data['range'] = cells[1].get_text(strip=True)
                if cells[2].get_text(strip=True):  # Section
                    location_data['section'] = cells[2].get_text(strip=True)
                if len(cells) > 3 and cells[3].get_text(strip=True):  # Block
                    location_data['block'] = cells[3].get_text(strip=True)

            # Third data row contains Municipality, Latitude, Longitude
            elif i == 3 and len(cells) >= 3:  # Second data row
                if len(cells) > 1 and cells[1].get_text(strip=True):  # Latitude
                    lat_text = cells[1].get_text(strip=True)
                    try:
                        location_data['latitude'] = float(lat_text)
                    except (ValueError, TypeError):
                        pass
                if len(cells) > 2 and cells[2].get_text(strip=True):  # Longitude
                    lon_text = cells[2].get_text(strip=True)
                    try:
                        location_data['longitude'] = float(lon_text)
                    except (ValueError, TypeError):
                        pass

    return location_data


def extract_lot_information_from_html(html_content):
    """Extract lot information from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')

    lot_info = {
        'lot_type': None,
        'lot_length_feet': None,
        'lot_width_feet': None,
        'lot_area_sqft': None,
        'landscaping_features': None,
        'view': None,
        'fencing_type': None,
        'fence_height': None,
        'fence_length': None,
        'driveway_material': None,
        'driveway_condition': None,
        'lot_condition_issues': None
    }

    # Extract land features (fencing, etc.)
    for section in soup.find_all('div', {'id': ['PropertyDetailsCurrent', 'PropertyDetails']}):
        for table in section.find_all('table', class_='appraisalAttributes'):
            # Look for Land Features section
            land_features_found = False
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 1:
                    cell_text = cells[0].get_text(strip=True).lower()
                    if 'land features' in cell_text:
                        land_features_found = True
                        continue

                    # If we're in the land features section
                    if land_features_found and len(cells) >= 3:
                        # Skip header rows
                        if 'description' in cell_text or 'year added' in cell_text:
                            continue

                        # Empty row usually indicates end of section
                        if not cell_text:
                            break

                        description = cells[0].get_text(strip=True).lower()
                        year_added = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                        units = cells[2].get_text(strip=True) if len(cells) > 2 else ""

                        # Extract fencing information
                        if 'fence' in description:
                            # Determine fence type
                            if 'wood' in description or 'stockade' in description:
                                lot_info['fencing_type'] = 'Wood'
                                if 'stockade' in description:
                                    lot_info['fencing_type'] = 'Stockade'
                            elif 'chain' in description or 'link' in description:
                                lot_info['fencing_type'] = 'ChainLink'
                            elif 'vinyl' in description:
                                lot_info['fencing_type'] = 'Vinyl'
                            elif 'aluminum' in description:
                                lot_info['fencing_type'] = 'Aluminum'
                            elif 'iron' in description or 'wrought' in description:
                                lot_info['fencing_type'] = 'WroughtIron'
                            elif 'privacy' in description:
                                lot_info['fencing_type'] = 'Privacy'
                            elif 'picket' in description:
                                lot_info['fencing_type'] = 'Picket'
                            elif 'composite' in description:
                                lot_info['fencing_type'] = 'Composite'

                            # Extract fence length from units (e.g., "390" linear feet)
                            if units and units.isdigit():
                                fence_length = int(units)
                                # Map to schema enum values
                                if fence_length <= 30:
                                    lot_info['fence_length'] = '25ft'
                                elif fence_length <= 60:
                                    lot_info['fence_length'] = '50ft'
                                elif fence_length <= 87:
                                    lot_info['fence_length'] = '75ft'
                                elif fence_length <= 125:
                                    lot_info['fence_length'] = '100ft'
                                elif fence_length <= 175:
                                    lot_info['fence_length'] = '150ft'
                                elif fence_length <= 250:
                                    lot_info['fence_length'] = '200ft'
                                elif fence_length <= 400:
                                    lot_info['fence_length'] = '300ft'
                                elif fence_length <= 750:
                                    lot_info['fence_length'] = '500ft'
                                else:
                                    lot_info['fence_length'] = '1000ft'

                        # Extract driveway information
                        elif 'driveway' in description or 'drive' in description:
                            if 'concrete' in description:
                                lot_info['driveway_material'] = 'Concrete'
                            elif 'asphalt' in description:
                                lot_info['driveway_material'] = 'Asphalt'
                            elif 'paver' in description:
                                lot_info['driveway_material'] = 'Pavers'
                            elif 'gravel' in description:
                                lot_info['driveway_material'] = 'Gravel'

                        # Look for landscaping features
                        elif any(keyword in description for keyword in ['tree', 'garden', 'lawn', 'landscape']):
                            if 'mature' in description or 'oak' in description or 'palm' in description:
                                lot_info['landscaping_features'] = 'MatureTrees'
                            elif 'garden' in description:
                                lot_info['landscaping_features'] = 'ManicuredGarden'
                            elif 'lawn' in description or 'grass' in description:
                                lot_info['landscaping_features'] = 'Lawn'

    # Extract lot area from Land Tracts (if available)
    # Look for land tract with area information
    for section in soup.find_all('div', {'id': ['PropertyDetailsCurrent', 'PropertyDetails']}):
        for table in section.find_all('table', class_='appraisalAttributes'):
            land_tracts_found = False
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 1:
                    cell_text = cells[0].get_text(strip=True).lower()
                    if 'land tracts' in cell_text:
                        land_tracts_found = True
                        continue

                    # If we found land tracts, look for area information
                    if land_tracts_found and len(cells) >= 4:
                        # Skip headers
                        if any(header in cell_text for header in ['use code', 'description', 'number', 'unit']):
                            continue

                        if not cell_text:
                            break

                        # Extract number of units and unit of measure
                        number_of_units = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                        unit_of_measure = cells[3].get_text(strip=True).lower() if len(cells) > 3 else ""

                        # Convert to square feet if possible
                        try:
                            units = float(number_of_units)
                            if 'acre' in unit_of_measure:
                                # Convert acres to square feet (1 acre = 43,560 sq ft)
                                lot_info['lot_area_sqft'] = int(units * 43560)
                            elif 'sq ft' in unit_of_measure or 'square feet' in unit_of_measure:
                                lot_info['lot_area_sqft'] = int(units)
                            elif unit_of_measure == 'lot' and units == 1.0:
                                # Single lot - we might need additional info to determine size
                                # For now, leave as None since we don't know the lot size
                                pass
                        except (ValueError, TypeError):
                            pass

    # Determine lot type based on area
    if lot_info['lot_area_sqft']:
        # 1/4 acre = 10,890 sq ft
        if lot_info['lot_area_sqft'] <= 10890:
            lot_info['lot_type'] = 'LessThanOrEqualToOneQuarterAcre'
        else:
            lot_info['lot_type'] = 'GreaterThanOneQuarterAcre'

    # Default assumptions (could be enhanced with more HTML analysis)
    if not lot_info['lot_type']:
        lot_info['lot_type'] = 'LessThanOrEqualToOneQuarterAcre'  # Most residential lots

    # Set default fence height if we found fencing
    if lot_info['fencing_type'] and not lot_info['fence_height']:
        lot_info['fence_height'] = '6ft'  # Common residential fence height

    return lot_info

def extract_property_information_from_html(html_content):
    """Extract property information from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')

    property_info = {
        'livable_floor_area': None,
        'number_of_units_type': None,
        'property_legal_description_text': None,
        'property_structure_built_year': None,
        'property_type': None
    }

    # Extract livable floor area (Gross Living Area)
    for table in soup.find_all('table', class_='appraisalDetails'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                header = cells[0].get_text(strip=True).lower()
                if 'gross living area' in header:
                    area_text = cells[1].get_text(strip=True)
                    # Remove commas and extract numeric value
                    area_clean = re.sub(r'[^\d]', '', area_text)
                    if area_clean and len(area_clean) >= 2:
                        property_info['livable_floor_area'] = area_clean

    # Extract property legal description
    legal_desc_section = soup.find('div', class_='textPanel')
    if legal_desc_section:
        legal_text = legal_desc_section.get_text(strip=True)
        if legal_text and len(legal_text) > 5:  # Basic validation
            property_info['property_legal_description_text'] = legal_text

    # Extract year built from building details
    for table in soup.find_all('table', class_='appraisalAttributes'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                header = cells[0].get_text(strip=True).lower()
                if 'year built' in header:
                    year_text = cells[1].get_text(strip=True)
                    year_match = re.search(r'(\d{4})', year_text)
                    if year_match:
                        property_info['property_structure_built_year'] = int(year_match.group(1))
                        break

    # If not found in building details, check appraisalDetails table
    if not property_info['property_structure_built_year']:
        for table in soup.find_all('table', class_='appraisalDetails'):
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True).lower()
                    if '1st year building' in header or 'year built' in header:
                        year_text = cells[1].get_text(strip=True)
                        year_match = re.search(r'(\d{4})', year_text)
                        if year_match:
                            property_info['property_structure_built_year'] = int(year_match.group(1))
                            break

    # Extract living units and model type from building characteristics
    living_units = None
    model_type = None
    raw_model_type = None

    for table in soup.find_all('table', class_='appraisalAttributes'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                header = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                if 'living units' in header:
                    try:
                        living_units = int(value)
                    except ValueError:
                        pass
                elif 'model type' in header:
                    raw_model_type = value  # Keep original
                    model_type = value.lower()

    # Extract Use Code Description from Land Tracts table as fallback
    use_code_description = None
    raw_use_code_description = None

    # Look for Land Tracts table in both current and historical property details
    for section in soup.find_all('div', {'id': ['PropertyDetailsCurrent', 'PropertyDetails']}):
        for table in section.find_all('table', class_='appraisalAttributes'):
            # Look for Land Tracts section
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 1 and 'land tracts' in cells[0].get_text(strip=True).lower():
                    # Found Land Tracts section, look for Use Code Description header
                    header_row = None
                    data_rows = []

                    # Get all rows after this one
                    current_row = row
                    while current_row:
                        current_row = current_row.find_next_sibling('tr')
                        if current_row:
                            cells = current_row.find_all(['td', 'th'])
                            if not cells:
                                break
                            # Check if this is a header row
                            if any('use code description' in cell.get_text(strip=True).lower() for cell in cells):
                                header_row = current_row
                            elif header_row and cells:
                                # This is a data row after the header
                                data_rows.append(current_row)
                            elif len(data_rows) > 0 and not any(cell.get_text(strip=True) for cell in cells):
                                # Empty row, end of section
                                break

                    # Extract use code description from data rows
                    if header_row and data_rows:
                        header_cells = header_row.find_all(['td', 'th'])
                        headers = [cell.get_text(strip=True).lower() for cell in header_cells]

                        # Find Use Code Description column index
                        desc_col_index = None
                        for i, header in enumerate(headers):
                            if 'use code description' in header:
                                desc_col_index = i
                                break

                        # Extract from first data row
                        if desc_col_index is not None and data_rows:
                            first_data_row = data_rows[0]
                            data_cells = first_data_row.find_all(['td', 'th'])
                            if desc_col_index < len(data_cells):
                                raw_use_code_description = data_cells[desc_col_index].get_text(strip=True)
                                use_code_description = raw_use_code_description.lower()
                                break

            if use_code_description:
                break
        if use_code_description:
            break

    # Map living units to number_of_units_type
    if living_units:
        if living_units == 1:
            property_info['number_of_units_type'] = 'One'
        elif living_units == 2:
            property_info['number_of_units_type'] = 'Two'
        elif living_units == 3:
            property_info['number_of_units_type'] = 'Three'
        elif living_units == 4:
            property_info['number_of_units_type'] = 'Four'
        elif 1 <= living_units <= 4:
            property_info['number_of_units_type'] = 'OneToFour'
        elif 2 <= living_units <= 4:
            property_info['number_of_units_type'] = 'TwoToFour'

    # PROPERTY TYPE EXTRACTION - Priority: Model Type first, then Use Code Description
    matched_type = None
    raw_source_value = None

    def try_map_property_type(type_text, raw_value, living_units=None):
        """Try to map a property type description to schema enum"""
        if not type_text:
            return None, None

        matched = None
        type_text = type_text.lower()

        # IMPORTANT: Check for "vacant" first
        if 'vacant' in type_text:
            matched = 'VacantLand'

        # Direct keyword matching for all schema values
        elif any(keyword in type_text for keyword in ['single family', 'single-family']):
            matched = 'SingleFamily'
        elif 'duplex' in type_text or '2 unit' in type_text or 'two unit' in type_text:
            if living_units == 2:
                matched = 'Duplex'
            else:
                matched = '2Units'
        elif 'triplex' in type_text or '3 unit' in type_text or 'three unit' in type_text:
            matched = '3Units'
        elif 'fourplex' in type_text or '4 unit' in type_text or 'four unit' in type_text:
            matched = '4Units'
        elif any(keyword in type_text for keyword in ['townhouse', 'town house', 'townhome']):
            matched = 'Townhouse'
        elif 'condominium' in type_text or 'condo' in type_text:
            if 'detached' in type_text:
                matched = 'DetachedCondominium'
            elif 'non warrantable' in type_text or 'nonwarrantable' in type_text:
                matched = 'NonWarrantableCondo'
            else:
                matched = 'Condominium'
        elif 'cooperative' in type_text or 'co-op' in type_text:
            matched = 'Cooperative'
        elif 'manufactured' in type_text or 'mobile' in type_text or 'trailer' in type_text:
            if any(keyword in type_text for keyword in ['multi', 'double', 'triple', 'wide']):
                matched = 'ManufacturedHousingMultiWide'
            elif 'single wide' in type_text:
                matched = 'ManufacturedHousingSingleWide'
            else:
                matched = 'ManufacturedHousing'
        elif 'modular' in type_text:
            matched = 'Modular'
        elif any(keyword in type_text for keyword in ['pud', 'planned unit', 'planned development']):
            matched = 'Pud'
        elif 'timeshare' in type_text or 'time share' in type_text:
            matched = 'Timeshare'
        elif any(keyword in type_text for keyword in ['multiple family', 'multi family', 'apartment', 'multi-family']):
            matched = 'MultipleFamily'
        elif any(keyword in type_text for keyword in ['two to four', '2 to 4', '2-4']):
            matched = 'TwoToFourFamily'

        return matched, raw_value


    # Try Model Type first (Priority 1)
    if model_type:
        matched_type, raw_source_value = try_map_property_type(model_type, raw_model_type)

    # If no match from Model Type, try Use Code Description (Priority 2)
    if not matched_type and use_code_description:
        matched_type, raw_source_value = try_map_property_type(use_code_description, raw_use_code_description)

    # If still no match, use living units as fallback (Priority 3)
    if not matched_type and living_units:
        if living_units == 1:
            matched_type = 'SingleFamily'
        elif living_units == 2:
            matched_type = 'Duplex'
        elif living_units == 3:
            matched_type = '3Units'
        elif living_units == 4:
            matched_type = '4Units'
        elif 2 <= living_units <= 4:
            matched_type = 'TwoToFourFamily'
        elif living_units > 4:
            matched_type = 'MultipleFamily'

    # Set the final property type
    if matched_type:
        property_info['property_type'] = matched_type
    elif raw_source_value:
        # Return the raw value from the county website for schema filtering
        property_info['property_type'] = raw_source_value

    if property_info.get("property_type") is None:
        section_titles = soup.find_all("div", class_="sectionSubTitle")
        for title in section_titles:
            raw_text = title.get_text(strip=True).lower()
            print(raw_text)
            if "condominium" in raw_text:
                property_info["property_type"] = "Condominium"
                break
            elif "townhouse" in raw_text:
                property_info["property_type"] = "Townhouse"
                break
            elif "single family" in raw_text or "single-family" in raw_text:
                property_info["property_type"] = "SingleFamily"
                break

    return property_info

def format_name(name):
    """Format name to match the required regex pattern"""
    if not name:
        return None

    # Clean the name: remove extra spaces and convert to string
    name = str(name).strip()
    if not name:
        return None

    # Split by common separators and process each part
    parts = re.split(r'([ \-\',.]+)', name)
    formatted_parts = []

    for part in parts:
        if re.match(r'^[ \-\',.]+$', part):
            # Keep separators as-is
            formatted_parts.append(part)
        elif part.strip():
            # Format word: capitalize first letter, lowercase the rest
            formatted_parts.append(part.strip().capitalize())

    result = ''.join(formatted_parts)

    # Validate against the regex pattern
    if re.match(r"^[A-Z][a-z]*([ \-',.][A-Za-z][a-z]*)*$", result):
        return result
    else:
        return None

def load_property_seed_json():
    """Load property seed JSON data"""
    try:
        with open('property_seed.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Warning: property_seed.json not found")
        return {}

def load_unnormalized_address_json():
    """Load unnormalized address JSON data"""
    try:
        with open('unnormalized_address.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Warning: unnormalized_address.json not found")
        return {}


def load_json(path):
    """Load JSON file"""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: {path} not found")
        return {}


def ensure_dir(path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(path):
        os.makedirs(path)


def parse_float(val):
    """Parse string to float, handling currency symbols and commas"""
    if not val:
        return 0.0
    try:
        # Remove currency symbols, commas, and whitespace
        clean_val = re.sub(r'[$,\s]', '', str(val))
        return float(clean_val) if clean_val else 0.0
    except (ValueError, TypeError):
        return 0.0


def parse_int(val):
    """Parse string to int, extracting only digits"""
    if not val:
        return None
    try:
        digits_only = re.sub(r'[^0-9]', '', str(val))
        return int(digits_only) if digits_only else None
    except (ValueError, TypeError):
        return None


def extract_flood_storm_information_from_html(html_content):
    """Extract flood and storm information from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')

    flood_info = {
        'community_id': None,
        'effective_date': None,
        'evacuation_zone': None,
        'fema_search_url': None,
        'flood_insurance_required': None,
        'flood_zone': None,
        'map_version': None,
        'panel_number': None,
        'request_identifier': None,
        'source_http_request': None
    }

    # Look for the flood information section
    elevation_section = soup.find('div', {'id': 'ElevationDetails'})
    if not elevation_section:
        return flood_info

    # Find the table with flood information
    flood_table = elevation_section.find('table', class_='detailsTable')
    if not flood_table:
        return flood_info

    rows = flood_table.find_all('tr')
    if len(rows) >= 3:  # Should have header rows and data row
        # Get data from the last row (contains the actual flood data)
        data_row = rows[-1]
        cells = data_row.find_all('td')

        if len(cells) >= 5:
            flood_info['community_id'] = cells[0].get_text(strip=True) or None
            flood_info['panel_number'] = cells[1].get_text(strip=True) or None
            flood_info['map_version'] = cells[2].get_text(strip=True) or None
            effective_date_text = cells[3].get_text(strip=True) or None
            flood_info['effective_date'] = parse_date_to_iso(effective_date_text)
            flood_info['evacuation_zone'] = cells[4].get_text(strip=True) or None

    # Look for FEMA search URL - check for any links to FEMA in the flood section
    fema_link = elevation_section.find('a', href=lambda x: x and 'fema.gov' in x)
    if fema_link:
        raw_url = fema_link.get('href')
        # URL encode spaces as %20
        flood_info['fema_search_url'] = quote(raw_url, safe=':/?#[]@!$&\'()*+,;=')

    # Determine if flood insurance is required based on evacuation zone
    # Zones A, AE, AH, AO, AR, A99, V, VE typically require flood insurance
    evacuation_zone = flood_info['evacuation_zone']
    if evacuation_zone:
        high_risk_zones = ['A', 'AE', 'AH', 'AO', 'AR', 'A99', 'V', 'VE']
        flood_info['flood_insurance_required'] = evacuation_zone in high_risk_zones

    return flood_info
def extract_sales_and_taxes_from_html(html_content):
    """Extract sales and tax data from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    sales_list = []
    tax_list = []

    # SALES DATA EXTRACTION
    sales_table = None
    for table in soup.find_all('table'):
        table_text = table.get_text().lower()
        if 'sale' in table_text and 'date' in table_text:
            sales_table = table
            break

    if sales_table:
        headers = []
        header_row = sales_table.find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

        for row in sales_table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            sale = {}
            for i, cell in enumerate(cells):
                header = headers[i] if i < len(headers) else f'col{i}'
                sale[header] = cell.get_text(strip=True)

            # Map to schema fields
            price_field = sale.get('Sale Price') or sale.get('Price') or sale.get('Amount') or '0'
            price_val = parse_float(price_field)

            date_field = (sale.get('Date') or
                          sale.get('Sale Date') or
                          sale.get('Transfer Date') or
                          None)
            iso_date = parse_date_to_iso(date_field)
            sales_list.append({
                'ownership_transfer_date': iso_date,
                'purchase_price_amount': price_val,
                'request_identifier': None,
                'source_http_request': None
            })

    # TAX DATA EXTRACTION
    # Look for valueGrid table or any table with tax-related data
    value_grid = soup.find('table', {'id': 'valueGrid'})
    if not value_grid:
        # Look for tables containing tax-related keywords
        for table in soup.find_all('table'):
            table_text = table.get_text().lower()
            if any(keyword in table_text for keyword in ['tax', 'assessed', 'taxable', 'value']):
                value_grid = table
                break

    if value_grid:
        rows = value_grid.find_all('tr')
        if rows:
            headers = [th.get_text(strip=True) for th in rows[0].find_all(['th', 'td'])]

            for row in rows[1:]:  # Skip header row
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                tax = {}
                for i, cell in enumerate(cells):
                    header = headers[i] if i < len(headers) else f'col{i}'
                    tax[header] = cell.get_text(strip=True)

                # Map to schema fields
                tax_list.append({
                    'tax_year': parse_int(tax.get('Tax Year') or tax.get('Year')),
                    'property_assessed_value_amount': parse_float(
                        tax.get('Capped Assessed') or
                        tax.get('Assessed Value') or
                        tax.get('Assessed')
                    ),
                    'property_market_value_amount': parse_float(
                        tax.get('Just') or
                        tax.get('Market Value') or
                        tax.get('Just Value')
                    ),
                    'property_building_amount': parse_float(tax.get('Building')),
                    'property_land_amount': parse_float(tax.get('Land')),
                    'property_taxable_value_amount': parse_float(tax.get('Taxable')),
                    'monthly_tax_amount': None,
                    'period_start_date': None,
                    'period_end_date': None,
                    'request_identifier': None,
                    'source_http_request': None
                })

    return sales_list, tax_list


def extract_owners_for_sales(parcel_id, owners_schema):
    """Extract owner information for a parcel"""
    key = f'property_{parcel_id}'
    if key in owners_schema and 'owners_by_date' in owners_schema[key]:
        return owners_schema[key]['owners_by_date'].get('current', [])
    return []


def main():
    """Main processing function"""
    input_dir = './input/'
    data_dir = './data/'

    # Ensure directories exist
    ensure_dir(data_dir)

    # Load data files
    print("Loading data files...")
    property_seed = load_property_seed_json()
    address_data = load_unnormalized_address_json()
    owners_schema = load_json('./owners/owners_schema.json')
    layout_data = load_json('./owners/layout_data.json')
    structure_data = load_json('./owners/structure_data.json')
    utility_data = load_json('./owners/utility_data.json')

    # Check if input directory exists
    if not os.path.exists(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist")
        return

    # Process each HTML file
    html_files = [f for f in os.listdir(input_dir) if f.endswith('.html')]

    if not html_files:
        print(f"No HTML files found in '{input_dir}'")
        return

    print(f"Found {len(html_files)} HTML files to process")

    for filename in html_files:
        parcel_id = filename.replace('.html', '')
        print(f'Processing {parcel_id}')

        # Create output directory for this parcel
        out_dir = os.path.join(data_dir, parcel_id)
        ensure_dir(out_dir)

        # Read HTML file
        html_path = os.path.join(input_dir, filename)
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
        except Exception as e:
            print(f"Error reading {html_path}: {e}")
            continue

        # Get seed data for this parcel
        # Get property seed data for this parcel
        request_identifier = property_seed.get("parcel_id")
        source_http_request = property_seed.get("source_http_request", {
            "method": "GET",
            "url": ""
        })

        try:
            property_data = extract_property_information_from_html(html_content)

            property_json = {
                "source_http_request": source_http_request,
                "request_identifier": request_identifier,
                "livable_floor_area": property_data.get('livable_floor_area'),
                "number_of_units_type": property_data.get('number_of_units_type'),
                "parcel_identifier": parcel_id,
                "property_legal_description_text": property_data.get('property_legal_description_text'),
                "property_structure_built_year": property_data.get('property_structure_built_year'),
                "property_type": property_data.get('property_type')
            }

        except Exception as e:
            print(f"Error extracting property information for {parcel_id}: {e}")
            # Fallback to basic property.json
            property_json = {
                "source_http_request": source_http_request,
                "request_identifier": request_identifier,
                "livable_floor_area": None,
                "number_of_units_type": None,
                "parcel_identifier": parcel_id,
                "property_legal_description_text": None,
                "property_structure_built_year": None,
                "property_type": None
            }
        
        if property_json.get("property_type") is None:
            raise ValueError(f"property_type is not identified for {parcel_id}")

        with open(os.path.join(out_dir, 'property.json'), 'w', encoding='utf-8') as f:
                json.dump(property_json, f, indent=2)

        # ADDRESS.JSON
        address_json = {
            "source_http_request": source_http_request,
            "request_identifier": request_identifier,
            "city_name": None,
            "country_code": "US",
            "county_name": address_data.get("county_jurisdiction"),
            "latitude": None,
            "longitude": None,
            "plus_four_postal_code": None,
            "postal_code": None,
            "state_code": None,
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

        # Parse address from seed CSV if available
        if address_data.get('full_address'):
            addr_parts = parse_address(address_data['full_address'])
            address_json.update(addr_parts)

        # Extract location data (township, range, section, block, lat/lng) from HTML
        try:
            location_data = extract_location_data_from_html(html_content)
            address_json.update(location_data)
        except Exception as e:
            print(f"Error extracting location data for {parcel_id}: {e}")

        with open(os.path.join(out_dir, 'address.json'), 'w', encoding='utf-8') as f:
            json.dump(address_json, f, indent=2)

        # LOT INFORMATION
        try:
            lot_data = extract_lot_information_from_html(html_content)

            lot_json = {
                "source_http_request": source_http_request,
                "request_identifier": request_identifier,
                "lot_type": lot_data.get('lot_type'),
                "lot_length_feet": lot_data.get('lot_length_feet'),
                "lot_width_feet": lot_data.get('lot_width_feet'),
                "lot_area_sqft": lot_data.get('lot_area_sqft'),
                "landscaping_features": lot_data.get('landscaping_features'),
                "view": lot_data.get('view'),
                "fencing_type": lot_data.get('fencing_type'),
                "fence_height": lot_data.get('fence_height'),
                "fence_length": lot_data.get('fence_length'),
                "driveway_material": lot_data.get('driveway_material'),
                "driveway_condition": lot_data.get('driveway_condition'),
                "lot_condition_issues": lot_data.get('lot_condition_issues')
            }

            with open(os.path.join(out_dir, 'lot.json'), 'w', encoding='utf-8') as f:
                json.dump(lot_json, f, indent=2)

        except Exception as e:
            print(f"Error extracting lot information for {parcel_id}: {e}")

        # SALES AND TAX DATA
        try:
            sales_list, tax_list = extract_sales_and_taxes_from_html(html_content)

            # Save sales data
            for i, sale in enumerate(sales_list, 1):
                sale['source_http_request'] = source_http_request
                sale['request_identifier'] = request_identifier
                with open(os.path.join(out_dir, f'sales_{i}.json'), 'w', encoding='utf-8') as f:
                    json.dump(sale, f, indent=2)

            # Save tax data
            for i, tax in enumerate(tax_list, 1):
                tax['source_http_request'] = source_http_request
                tax['request_identifier'] = request_identifier
                with open(os.path.join(out_dir, f'tax_{i}.json'), 'w', encoding='utf-8') as f:
                    json.dump(tax, f, indent=2)

        except Exception as e:
            print(f"Error extracting sales/tax data for {parcel_id}: {e}")
        # FLOOD STORM INFORMATION
        try:
            flood_info = extract_flood_storm_information_from_html(html_content)
            flood_info['source_http_request'] = source_http_request
            flood_info['request_identifier'] = request_identifier

            with open(os.path.join(out_dir, 'flood_storm_information.json'), 'w', encoding='utf-8') as f:
                json.dump(flood_info, f, indent=2)

        except Exception as e:
                print(f"Error extracting flood storm information for {parcel_id}: {e}")

        # OWNERS
        try:
            owners = extract_owners_for_sales(parcel_id, owners_schema)
            for i, owner in enumerate(owners, 1):
                if owner.get('type') == 'person':
                    person_obj = {
                        "source_http_request": source_http_request,
                        "request_identifier": request_identifier,
                        "birth_date": None,
                        "first_name": format_name(owner.get("first_name")),
                        "last_name": format_name(owner.get("last_name")),
                        "middle_name": format_name(owner.get("middle_name")),
                        "prefix_name": None,
                        "suffix_name": None,
                        "us_citizenship_status": None,
                        "veteran_status": None
                    }
                    with open(os.path.join(out_dir, f'person_{i}.json'), 'w', encoding='utf-8') as f:
                        json.dump(person_obj, f, indent=2)

                elif owner.get('type') == 'company':
                    company_obj = {
                        "source_http_request": source_http_request,
                        "request_identifier": request_identifier,
                        "name": owner.get("name")
                    }
                    with open(os.path.join(out_dir, f'company_{i}.json'), 'w', encoding='utf-8') as f:
                        json.dump(company_obj, f, indent=2)

        except Exception as e:
            print(f"Error processing owners for {parcel_id}: {e}")

        # LAYOUTS
        try:
            layouts = layout_data.get(f'property_{parcel_id}', {}).get('layouts', [])
            for i, layout in enumerate(layouts, 1):
                # Update source_http_request to use the correct one from property_seed.json
                layout['source_http_request'] = source_http_request
                with open(os.path.join(out_dir, f'layout_{i}.json'), 'w', encoding='utf-8') as f:
                    json.dump(layout, f, indent=2)
        except Exception as e:
            print(f"Error processing layouts for {parcel_id}: {e}")

        # STRUCTURE
        try:
            structure_key = f'property_{parcel_id}'
            if structure_key in structure_data:
                structure_obj = structure_data[structure_key]
                # Update source_http_request to use the correct one from property_seed.json
                structure_obj['source_http_request'] = source_http_request
                with open(os.path.join(out_dir, 'structure.json'), 'w', encoding='utf-8') as f:
                    json.dump(structure_obj, f, indent=2)
        except Exception as e:
            print(f"Error processing structure for {parcel_id}: {e}")

        # UTILITY
        try:
            utility_key = f'property_{parcel_id}'
            if utility_key in utility_data:
                utility_obj = utility_data[utility_key]
                # Update source_http_request to use the correct one from property_seed.json
                utility_obj['source_http_request'] = source_http_request
                with open(os.path.join(out_dir, 'utility.json'), 'w', encoding='utf-8') as f:
                    json.dump(utility_obj, f, indent=2)
        except Exception as e:
            print(f"Error processing utility for {parcel_id}: {e}")

    print(f"Processing complete! Output saved to '{data_dir}'")


if __name__ == '__main__':
    main()