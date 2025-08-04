
import json
import sys
import argparse
import requests
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
import os
from collections import OrderedDict
import shutil


@dataclass
class IPFSFile:
    """Represents a file found in IPFS structure"""
    name: str
    url: str
    content: str = None
    type: str = 'unknown'
    parsed_content: Any = None


@dataclass
class FieldDiff:
    """Represents a specific field difference"""
    field_path: str
    hash1_value: Any
    hash2_value: Any
    hash3_value: Any


@dataclass
class Difference:
    """Represents a difference between files"""
    name: str
    position: int
    files: List[Optional[IPFSFile]]
    contents: List[Optional[str]]
    type: str  # 'missing' or 'different' or 'order_mismatch'
    field_diffs: List[FieldDiff] = None  # Only the different fields


class IPFSDiffTool:
    def __init__(self, max_workers=10):
        self.gateways = [
            "https://ipfs.io/ipfs/",
            "https://gateway.pinata.cloud/ipfs/",
            "https://dweb.link/ipfs/",
            "https://cloudflare-ipfs.com/ipfs/"
        ]
        self.timeout = 10
        self.max_workers = max_workers
        self.print_lock = threading.Lock()  # For thread-safe printing

    def clean_reports_directory(self, output_dir: str):
        """Delete the existing reports directory if it exists"""
        if os.path.exists(output_dir):
            try:
                shutil.rmtree(output_dir)
                print(f"🗑️  Cleaned existing reports directory: {output_dir}")
            except Exception as e:
                print(f"⚠️  Warning: Could not delete existing reports directory: {e}")
        else:
            print(f"📁 Reports directory doesn't exist yet: {output_dir}")

    def safe_print(self, message: str):
        """Thread-safe printing"""
        with self.print_lock:
            print(message)

    def fetch_with_fallback(self, cid: str, show_progress: bool = True) -> Any:
        """Try to fetch IPFS content from multiple gateways with fallback"""
        for gateway in self.gateways:
            try:
                url = urljoin(gateway, cid)
                if show_progress:
                    self.safe_print(f"  Trying gateway: {gateway}")
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200 and response.text.strip():
                    try:
                        return response.json()
                    except json.JSONDecodeError:
                        return response.text  # Return as text if not JSON
            except Exception as e:
                if show_progress:
                    self.safe_print(f"  Gateway failed: {e}")
                continue

        return None

    def fetch_relationship_data(self, rel_cid: str, rel_name: str, index: int = None) -> Optional[Dict]:
        """Fetch a single relationship's data - designed for parallel execution"""
        try:
            rel_data = self.fetch_with_fallback(rel_cid, show_progress=False)
            if rel_data is None:
                self.safe_print(
                    f"Warning: Could not fetch {rel_name}{'[' + str(index) + ']' if index is not None else ''} from any gateway: {rel_cid}")
                return None
            return {'rel_name': rel_name, 'index': index, 'rel_cid': rel_cid, 'data': rel_data}
        except Exception as e:
            self.safe_print(
                f"Warning: Could not fetch relationship data for {rel_name}{'[' + str(index) + ']' if index is not None else ''}: {e}")
            return None

    def collect_data_ipfs_links(self, data_cid: str) -> OrderedDict[str, str]:
        """
        Collect IPFS links for County data structure - preserving order for position-based comparison
        """
        try:
            print(f"Fetching seed data from CID: {data_cid}")
            seed_data = self.fetch_with_fallback(data_cid)
            if seed_data is None:
                print(f"Error: Could not fetch seed data from any gateway for CID: {data_cid}")
                return OrderedDict()
        except Exception as e:
            print(f"Error fetching seed data: {e}")
            return OrderedDict()

        entity_links = OrderedDict()  # For actual entities - preserves order
        relationship_links = OrderedDict()  # For relationship objects
        url_to_name = {}  # Track which URLs we've already seen and their preferred names

        # Get relationships from the County data
        relationships = seed_data.get("relationships", {})
        print(f"Found {len(relationships)} relationships")

        # Collect all relationship CIDs that need to be fetched
        fetch_tasks = []

        # Process required single-value relationships in consistent order
        single_rels = [
            "property_has_address", "property_has_lot", "property_has_structure",
            "property_has_utility", "property_has_flood_storm_information"
        ]

        for rel_name in single_rels:
            if rel_name in relationships and relationships[rel_name]:
                rel_cid = None
                if isinstance(relationships[rel_name], dict) and "/" in relationships[rel_name]:
                    rel_cid = relationships[rel_name]["/"]
                elif isinstance(relationships[rel_name], str):
                    rel_cid = relationships[rel_name]

                if rel_cid:
                    # Add order priority to ensure consistent processing
                    order_priority = single_rels.index(rel_name)
                    fetch_tasks.append((rel_cid, rel_name, None, order_priority))

        # Process array relationships in consistent order
        array_rels = [
            "company_has_property", "person_has_property", "property_has_file",
            "property_has_layout", "property_has_tax", "property_has_sales_history",
            "sales_history_has_company", "sales_history_has_person"
        ]

        for rel_name in array_rels:
            if rel_name in relationships and relationships[rel_name]:
                rel_array = relationships[rel_name]
                if isinstance(rel_array, list):
                    print(f"Processing array relationship: {rel_name} ({len(rel_array)} items)")
                    for i, rel_item in enumerate(rel_array):
                        rel_cid = None
                        if isinstance(rel_item, dict) and "/" in rel_item:
                            rel_cid = rel_item["/"]
                        elif isinstance(rel_item, str):
                            rel_cid = rel_item

                        if rel_cid:
                            # Add order priority to ensure consistent processing
                            order_priority = len(single_rels) + array_rels.index(rel_name)
                            fetch_tasks.append((rel_cid, rel_name, i, order_priority))

        # Sort fetch tasks by order priority and index to ensure consistent processing
        fetch_tasks.sort(key=lambda x: (x[3], x[2] if x[2] is not None else -1))

        # Fetch all relationship data in parallel
        print(f"Fetching {len(fetch_tasks)} relationships in parallel...")
        relationship_results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all fetch tasks
            future_to_task = {
                executor.submit(self.fetch_relationship_data, rel_cid, rel_name, index): (rel_cid, rel_name, index,
                                                                                          order_priority)
                for rel_cid, rel_name, index, order_priority in fetch_tasks
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_task):
                result = future.result()
                if result:
                    # Add order priority to result for sorting
                    task_info = future_to_task[future]
                    result['order_priority'] = task_info[3]
                    relationship_results.append(result)
                completed += 1
                print(f"  Progress: {completed}/{len(fetch_tasks)} relationships fetched")

        # Sort results to maintain consistent order across different hashes
        relationship_results.sort(key=lambda x: (
            x['order_priority'],
            x['index'] if x['index'] is not None else -1
        ))

        # Process the fetched relationship data in sorted order
        print("Processing fetched relationship data in consistent order...")
        for result in relationship_results:
            rel_name = result['rel_name']
            index = result['index']
            rel_cid = result['rel_cid']
            rel_data = result['data']

            try:
                if "to" in rel_data and "/" in rel_data["to"]:
                    to_cid = rel_data["to"]["/"]
                    # Extract x and y from x_has_y pattern
                    parts = rel_name.split("_has_")
                    x = parts[0] if len(parts) > 0 else "unknown"
                    y = parts[1] if len(parts) > 1 else "unknown"

                    # Handle 'from' URL
                    if "from" in rel_data and "/" in rel_data["from"]:
                        from_cid = rel_data['from']['/']
                        from_url = f"https://ipfs.io/ipfs/{from_cid}"
                        if from_url not in url_to_name:
                            # Create consistent naming that preserves order
                            if index is not None:
                                x_key = f"{x}_{index + 1:03d}"  # Zero-padded for sorting
                            else:
                                x_key = x
                            url_to_name[from_url] = x_key
                            entity_links[x_key] = from_url

                    # Handle 'to' URL
                    to_url = f"https://ipfs.io/ipfs/{to_cid}"
                    if to_url not in url_to_name:
                        # Create consistent naming that preserves order
                        if index is not None:
                            y_key = f"{y}_{index + 1:03d}"  # Zero-padded for sorting
                        else:
                            y_key = y
                        url_to_name[to_url] = y_key
                        entity_links[y_key] = to_url

            except Exception as e:
                print(f"Error processing relationship {rel_name}: {e}")

        # Return ONLY entity links (the deepest files), not relationship links
        print(f"Collected {len(entity_links)} entity links (deepest files) in consistent order")

        # Debug: Print the order of collected entities
        print("Entity order:")
        for i, (name, url) in enumerate(entity_links.items()):
            print(f"  Position {i:02d}: {name}")

        return entity_links

    def fetch_content_for_link(self, name: str, url: str) -> Optional[IPFSFile]:
        """Fetch content for a single IPFS link - designed for parallel execution"""
        try:
            # Extract CID from URL
            cid = url.split("/ipfs/")[-1]
            content = self.fetch_with_fallback(cid, show_progress=False)

            if content is not None:
                if isinstance(content, dict):
                    content_str = json.dumps(content, indent=2, sort_keys=True)
                    parsed_content = content
                else:
                    content_str = str(content)
                    try:
                        parsed_content = json.loads(content_str)
                    except:
                        parsed_content = content_str

                return IPFSFile(
                    name=name,
                    url=url,
                    content=content_str,
                    type='entity',
                    parsed_content=parsed_content
                )
            else:
                self.safe_print(f"Failed to fetch content for {name}")
                return None
        except Exception as e:
            self.safe_print(f"Error fetching {name}: {e}")
            return None

    def fetch_all_links_content(self, links: OrderedDict[str, str]) -> List[IPFSFile]:
        """Fetch content from all collected IPFS links in parallel, preserving order"""
        print(f"Fetching content for {len(links)} links in parallel...")

        # Convert to list to preserve order
        ordered_items = list(links.items())
        files = [None] * len(ordered_items)  # Pre-allocate to preserve order

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all fetch tasks with their index
            future_to_index = {
                executor.submit(self.fetch_content_for_link, name, url): idx
                for idx, (name, url) in enumerate(ordered_items)
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                result = future.result()
                if result:
                    files[idx] = result
                completed += 1
                print(f"  Progress: {completed}/{len(ordered_items)} files fetched")

        # Filter out None results while preserving order
        return [f for f in files if f is not None]

    def find_different_fields(self, obj1: Any, obj2: Any, obj3: Any, path: str = "") -> List[FieldDiff]:
        """Find only the fields that are different between the three objects"""
        differences = []

        # Handle None cases
        if obj1 is None and obj2 is None and obj3 is None:
            return differences

        # If any are not dicts, compare as values
        if not all(isinstance(obj, dict) for obj in [obj1, obj2, obj3] if obj is not None):
            if not (obj1 == obj2 == obj3):
                differences.append(FieldDiff(
                    field_path=path or "root",
                    hash1_value=obj1,
                    hash2_value=obj2,
                    hash3_value=obj3
                ))
            return differences

        # Get all possible keys from all three objects
        all_keys = set()
        for obj in [obj1, obj2, obj3]:
            if isinstance(obj, dict):
                all_keys.update(obj.keys())

        # Compare each key
        for key in sorted(all_keys):
            current_path = f"{path}.{key}" if path else key

            val1 = obj1.get(key) if obj1 else None
            val2 = obj2.get(key) if obj2 else None
            val3 = obj3.get(key) if obj3 else None

            # Skip if all values are the same
            if val1 == val2 == val3:
                continue

            # If they're all dicts, recurse
            if all(isinstance(val, dict) for val in [val1, val2, val3] if val is not None):
                differences.extend(self.find_different_fields(val1, val2, val3, current_path))
            elif all(isinstance(val, list) for val in [val1, val2, val3] if val is not None):
                # Handle arrays
                max_len = max(len(val1) if val1 else 0, len(val2) if val2 else 0, len(val3) if val3 else 0)
                for i in range(max_len):
                    item1 = val1[i] if val1 and i < len(val1) else None
                    item2 = val2[i] if val2 and i < len(val2) else None
                    item3 = val3[i] if val3 and i < len(val3) else None

                    if not (item1 == item2 == item3):
                        differences.extend(self.find_different_fields(item1, item2, item3, f"{current_path}[{i}]"))
            else:
                # Different values - add this field
                differences.append(FieldDiff(
                    field_path=current_path,
                    hash1_value=val1,
                    hash2_value=val2,
                    hash3_value=val3
                ))

        return differences

    def create_minimal_json_diff(self, field_diffs: List[FieldDiff], num_hashes: int = 3) -> Dict[str, str]:
        """Create minimal JSON representations showing ONLY the different fields"""
        if not field_diffs:
            result = {}
            for i in range(num_hashes):
                result[f"hash_{i + 1}_content"] = "{}"
            return result

        # Build minimal objects with ONLY different fields (no common fields)
        objects = [{} for _ in range(num_hashes)]

        for field_diff in field_diffs:
            # Only include fields that are actually different between hashes
            values = [field_diff.hash1_value, field_diff.hash2_value, field_diff.hash3_value][:num_hashes]

            # Skip if all values are the same (shouldn't happen, but safety check)
            unique_values = set(str(v) for v in values if v is not None)
            if len(unique_values) <= 1 and not any(v is None for v in values):
                continue

            # Parse the field path and create nested structure
            parts = field_diff.field_path.split('.')

            # Create nested structure for each hash, but ONLY for different values
            for i, (obj, value) in enumerate(zip(objects, values)):
                # Only add this field if it's different from at least one other hash
                other_values = [v for j, v in enumerate(values) if j != i]
                if value not in other_values or any(v is None for v in values):
                    current = obj
                    for part in parts[:-1]:
                        # Handle array indices like "field[0]"
                        if '[' in part and ']' in part:
                            key = part.split('[')[0]
                            index = int(part.split('[')[1].split(']')[0])
                            if key not in current:
                                current[key] = []
                            while len(current[key]) <= index:
                                current[key].append({})
                            current = current[key][index]
                        else:
                            if part not in current:
                                current[part] = {}
                            current = current[part]

                    # Set the final value
                    final_key = parts[-1]
                    if '[' in final_key and ']' in final_key:
                        key = final_key.split('[')[0]
                        index = int(final_key.split('[')[1].split(']')[0])
                        if key not in current:
                            current[key] = []
                        while len(current[key]) <= index:
                            current[key].append(None)
                        current[key][index] = value
                    else:
                        current[final_key] = value

        # Convert to JSON strings
        result = {}
        for i in range(num_hashes):
            result[f"hash_{i + 1}_content"] = json.dumps(objects[i], indent=2)

        return result

    def compare_files_by_position(self, files1: List[IPFSFile], files2: List[IPFSFile], files3: List[IPFSFile]) -> List[
        Difference]:
        """Compare files by position instead of name"""
        max_len = max(len(files1), len(files2), len(files3))
        differences = []

        print(f"Comparing {max_len} positions...")

        for position in range(max_len):
            file1 = files1[position] if position < len(files1) else None
            file2 = files2[position] if position < len(files2) else None
            file3 = files3[position] if position < len(files3) else None

            # Determine name for this position
            names = [f.name for f in [file1, file2, file3] if f is not None]
            position_name = f"pos_{position:02d}" + (f"_{names[0]}" if names else "_empty")

            contents = [
                file1.content if file1 else None,
                file2.content if file2 else None,
                file3.content if file3 else None
            ]

            # Check for differences
            if file1 is None or file2 is None or file3 is None:
                # Missing file at this position
                differences.append(Difference(
                    name=position_name,
                    position=position,
                    files=[file1, file2, file3],
                    contents=contents,
                    type='missing'
                ))
            elif not all(f.content == file1.content for f in [file1, file2, file3]):
                # Content is different - find only different fields
                field_diffs = self.find_different_fields(
                    file1.parsed_content if file1 else None,
                    file2.parsed_content if file2 else None,
                    file3.parsed_content if file3 else None
                )

                if field_diffs:  # Only create difference if there are actual field differences
                    differences.append(Difference(
                        name=position_name,
                        position=position,
                        files=[file1, file2, file3],
                        contents=contents,
                        type='different',
                        field_diffs=field_diffs
                    ))

        return differences

    def compare_two_files_by_position(self, files1: List[IPFSFile], files2: List[IPFSFile]) -> List[Difference]:
        """Compare files between two hashes by position"""
        max_len = max(len(files1), len(files2))
        differences = []

        print(f"Comparing {max_len} positions (2-way)...")

        for position in range(max_len):
            file1 = files1[position] if position < len(files1) else None
            file2 = files2[position] if position < len(files2) else None

            # Determine name for this position
            names = [f.name for f in [file1, file2] if f is not None]
            position_name = f"pos_{position:02d}" + (f"_{names[0]}" if names else "_empty")

            contents = [
                file1.content if file1 else None,
                file2.content if file2 else None
            ]

            # Check for differences
            if file1 is None or file2 is None:
                # Missing file at this position
                differences.append(Difference(
                    name=position_name,
                    position=position,
                    files=[file1, file2],
                    contents=contents,
                    type='missing'
                ))
            elif file1.content != file2.content:
                # Content is different - find only different fields
                field_diffs = self.find_different_fields(
                    file1.parsed_content if file1 else None,
                    file2.parsed_content if file2 else None,
                    None  # Dummy third value for 2-way comparison
                )

                if field_diffs:  # Only create difference if there are actual field differences
                    differences.append(Difference(
                        name=position_name,
                        position=position,
                        files=[file1, file2],
                        contents=contents,
                        type='different',
                        field_diffs=field_diffs
                    ))

        return differences

    def generate_diff_report(self, results: Dict, output_dir: str = None):
        """Generate comprehensive difference reports as files"""
        if not results:
            return

        # Use provided output_dir or default
        if output_dir is None:
            output_dir = getattr(self, 'output_dir', 'ipfs_diff_reports')

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        all_files = results['all_files']
        all_links = results['all_links']
        hashes = results['hashes']
        three_way_differences = results['differences']

        # Generate main report
        main_report_path = os.path.join(output_dir, f"ipfs_position_diff_report_{timestamp}.md")
        self.generate_main_report(results, main_report_path)

        # Generate three-way comparison with minimal diff
        three_way_path = os.path.join(output_dir, f"three_way_position_comparison_{timestamp}.json")
        self.generate_three_way_json_minimal(three_way_differences, hashes, three_way_path)

        # Generate pairwise comparisons by position
        pairwise_reports = []
        pairs = [(0, 1), (0, 2), (1, 2)]
        pair_names = ["hash1_vs_hash2", "hash1_vs_hash3", "hash2_vs_hash3"]

        for (i, j), pair_name in zip(pairs, pair_names):
            # Compare two specific hashes by position
            pairwise_diff = self.compare_two_files_by_position(all_files[i], all_files[j])

            # Generate pairwise JSON with minimal diff
            pairwise_json_path = os.path.join(output_dir, f"{pair_name}_position_{timestamp}.json")
            self.generate_pairwise_json_minimal(pairwise_diff, hashes[i], hashes[j], pairwise_json_path)

            pairwise_reports.append({
                'comparison': f"Hash {i + 1} vs Hash {j + 1}",
                'differences': len(pairwise_diff),
                'json_file': pairwise_json_path
            })

        # Generate summary
        summary_path = os.path.join(output_dir, f"summary_position_{timestamp}.txt")
        self.generate_summary(results, pairwise_reports, summary_path)

        print(f"\n📁 Reports generated in: {output_dir}/")
        print(f"   📋 Main report: {os.path.basename(main_report_path)}")
        print(f"   📊 Summary: {os.path.basename(summary_path)}")
        print(f"   🔍 Three-way JSON: {os.path.basename(three_way_path)}")
        for report in pairwise_reports:
            print(f"   📝 {report['comparison']}: {os.path.basename(report['json_file'])}")

        return {
            'output_dir': output_dir,
            'main_report': main_report_path,
            'summary': summary_path,
            'three_way_json': three_way_path,
            'pairwise_reports': pairwise_reports
        }

    def generate_main_report(self, results: Dict, output_path: str):
        """Generate the main comprehensive report"""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# IPFS Hash Position-Based Difference Analysis Report (Minimal Diff)\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            hashes = results['hashes']
            all_files = results['all_files']
            differences = results['differences']
            timing = results.get('timing', {})

            f.write("## Hash Overview\n\n")
            for i, hash_val in enumerate(hashes):
                f.write(f"**Hash {i + 1}:** `{hash_val}`\n")
                f.write(f"- Files found: {len(all_files[i])}\n\n")

            if timing:
                f.write("## Performance\n\n")
                f.write(f"- Processing time: {timing.get('processing_time', 0):.2f}s\n")
                f.write(f"- Content fetching time: {timing.get('fetching_time', 0):.2f}s\n")
                f.write(f"- **Total time: {timing.get('total_time', 0):.2f}s**\n\n")

            # Show position comparison table
            f.write("## Position Comparison\n\n")
            max_files = max(len(files) for files in all_files)
            f.write("| Pos | Hash 1 | Hash 2 | Hash 3 |\n")
            f.write("|-----|--------|--------|---------|\n")
            for i in range(max_files):
                names = []
                for files in all_files:
                    if i < len(files):
                        names.append(files[i].name)
                    else:
                        names.append("❌ MISSING")
                f.write(f"| {i:02d} | {names[0]} | {names[1]} | {names[2]} |\n")
            f.write("\n")

            f.write("## Position-Based Comparison Summary\n\n")
            f.write(f"**Total differences found:** {len(differences)}\n\n")

            if differences:
                missing_count = sum(1 for d in differences if d.type == 'missing')
                different_count = sum(1 for d in differences if d.type == 'different')

                f.write(f"- Missing files: {missing_count}\n")
                f.write(f"- Different content: {different_count}\n\n")

                f.write("## Detailed Differences (Only Different Fields)\n\n")
                for i, diff in enumerate(differences, 1):
                    f.write(f"### {i}. {diff.name} (Position {diff.position:02d})\n\n")
                    f.write(f"**Type:** {diff.type}\n\n")

                    if diff.type == 'different' and diff.field_diffs:
                        f.write(f"**Different fields ({len(diff.field_diffs)}):**\n\n")
                        for field_diff in diff.field_diffs:
                            f.write(f"- **{field_diff.field_path}:**\n")
                            f.write(f"  - Hash 1: `{field_diff.hash1_value}`\n")
                            f.write(f"  - Hash 2: `{field_diff.hash2_value}`\n")
                            f.write(f"  - Hash 3: `{field_diff.hash3_value}`\n\n")

                        # Show minimal JSON diff
                        minimal_diff = self.create_minimal_json_diff(diff.field_diffs)
                        f.write("**Minimal JSON Diff:**\n\n")
                        for j, (key, content) in enumerate(minimal_diff.items()):
                            f.write(f"**{key}:**\n")
                            f.write(f"```json\n{content}\n```\n\n")
                    else:
                        # For missing files, show file names at this position
                        f.write("**Files at this position:**\n")
                        for j, file_obj in enumerate(diff.files):
                            f.write(f"- Hash {j + 1}: {file_obj.name if file_obj else '❌ MISSING'}\n")
                        f.write("\n")
            else:
                f.write("✅ No differences found! All hashes have identical content at all positions.\n\n")

    def generate_three_way_json_minimal(self, differences: List[Difference], hashes: List[str], output_path: str):
        """Generate three-way comparison as JSON with minimal diff"""
        data = {
            "generated": datetime.now().isoformat(),
            "hashes": hashes,
            "total_differences": len(differences),
            "differences": []
        }

        for diff in differences:
            diff_data = {
                "name": diff.name,
                "position": diff.position,
                "type": diff.type
            }

            if diff.type == 'different' and diff.field_diffs:
                # Show only different fields
                minimal_diff = self.create_minimal_json_diff(diff.field_diffs)
                diff_data.update(minimal_diff)

                # Also include field-by-field breakdown
                diff_data["field_differences"] = [
                    {
                        "field_path": field_diff.field_path,
                        "hash_1_value": field_diff.hash1_value,
                        "hash_2_value": field_diff.hash2_value,
                        "hash_3_value": field_diff.hash3_value
                    }
                    for field_diff in diff.field_diffs
                ]
            else:
                # For missing files, show file info
                diff_data["files"] = [
                    {"name": f.name, "url": f.url} if f else None
                    for f in diff.files
                ]

            data["differences"].append(diff_data)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def generate_pairwise_json_minimal(self, differences: List[Difference], hash1: str, hash2: str, output_path: str):
        """Generate pairwise comparison as JSON with minimal diff"""
        data = {
            "generated": datetime.now().isoformat(),
            "hash_1": hash1,
            "hash_2": hash2,
            "total_differences": len(differences),
            "differences": []
        }

        for diff in differences:
            diff_data = {
                "name": diff.name,
                "position": diff.position,
                "type": diff.type
            }

            if diff.type == 'different' and diff.field_diffs:
                # Show only different fields (for 2-way comparison, create objects from field diffs)
                minimal_diff = self.create_minimal_json_diff(diff.field_diffs, num_hashes=2)
                diff_data["hash_1_content"] = minimal_diff.get("hash_1_content", "{}")
                diff_data["hash_2_content"] = minimal_diff.get("hash_2_content", "{}")
            else:
                # For missing files, show file info
                diff_data["files"] = [
                    {"name": f.name, "url": f.url} if f else None
                    for f in diff.files
                ]

            data["differences"].append(diff_data)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def generate_summary(self, results: Dict, pairwise_reports: List[Dict], output_path: str):
        """Generate executive summary"""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("IPFS HASH POSITION-BASED DIFFERENCE ANALYSIS - EXECUTIVE SUMMARY (MINIMAL DIFF)\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            hashes = results['hashes']
            all_files = results['all_files']
            differences = results['differences']
            timing = results.get('timing', {})

            f.write("HASH INFORMATION:\n")
            f.write("-" * 20 + "\n")
            for i, hash_val in enumerate(hashes):
                f.write(f"Hash {i + 1}: {hash_val[:50]}...\n")
                f.write(f"  Files: {len(all_files[i])}\n")
            f.write("\n")

            if timing:
                f.write("PERFORMANCE:\n")
                f.write("-" * 20 + "\n")
                f.write(f"Total processing time: {timing.get('total_time', 0):.2f} seconds\n\n")

            f.write("POSITION-BASED COMPARISON:\n")
            f.write("-" * 25 + "\n")
            f.write(f"Total differences: {len(differences)}\n")
            if differences:
                missing = sum(1 for d in differences if d.type == 'missing')
                different = sum(1 for d in differences if d.type == 'different')
                total_fields = sum(len(d.field_diffs) if d.field_diffs else 0 for d in differences)
                f.write(f"  - Missing files: {missing}\n")
                f.write(f"  - Different content: {different}\n")
                f.write(f"  - Different fields: {total_fields}\n")
            f.write("\n")

            f.write("PAIRWISE COMPARISONS:\n")
            f.write("-" * 20 + "\n")
            for report in pairwise_reports:
                f.write(f"{report['comparison']}: {report['differences']} differences\n")
            f.write("\n")

            if differences:
                f.write("TOP DIFFERENCES BY POSITION:\n")
                f.write("-" * 30 + "\n")
                for i, diff in enumerate(differences[:10], 1):
                    f.write(f"{i}. Position {diff.position:02d}: {diff.name} ({diff.type})\n")
                    if diff.field_diffs:
                        f.write(f"   Fields: {', '.join([fd.field_path for fd in diff.field_diffs[:3]])}\n")
                if len(differences) > 10:
                    f.write(f"... and {len(differences) - 10} more positions\n")
            else:
                f.write("RESULT: All hashes are identical at all positions!\n")

    def analyze_differences(self, hash1: str, hash2: str, hash3: str) -> Dict:
        """Main analysis function with parallel processing of all 3 hashes"""
        # Clean reports directory at the beginning
        output_dir = getattr(self, 'output_dir', 'ipfs_diff_reports')
        self.clean_reports_directory(output_dir)

        print("🔍 Starting IPFS analysis (position-based comparison)...")
        print(f"Hash 1: {hash1}")
        print(f"Hash 2: {hash2}")
        print(f"Hash 3: {hash3}")
        print()

        # Process all 3 hashes in parallel
        print("📡 Processing all 3 hashes in parallel...")
        all_links = [None, None, None]  # Preserve order

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=3) as executor:  # One thread per hash
            # Submit all hash processing tasks
            future_to_index = {
                executor.submit(self.collect_data_ipfs_links, hash_val.strip()): i
                for i, hash_val in enumerate([hash1, hash2, hash3])
            }

            # Collect results as they complete
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    links = future.result()
                    all_links[index] = links
                    print(f"Hash {index + 1}: Found {len(links)} links")
                except Exception as e:
                    print(f"❌ Failed to process hash {index + 1}: {e}")
                    return None

        processing_time = time.time() - start_time
        print(f"⚡ All hashes processed in {processing_time:.2f} seconds")

        # Fetch content from all links in parallel
        print("\n📂 Fetching content from all IPFS links in parallel...")
        all_files = [None, None, None]  # Preserve order

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=3) as executor:  # One thread per hash's content fetching
            # Submit all content fetching tasks
            future_to_index = {
                executor.submit(self.fetch_all_links_content, links): i
                for i, links in enumerate(all_links)
            }

            # Collect results as they complete
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    files = future.result()
                    all_files[index] = files
                    print(f"Hash {index + 1}: Successfully fetched {len(files)} files")
                except Exception as e:
                    print(f"❌ Failed to fetch content for hash {index + 1}: {e}")
                    return None

        fetching_time = time.time() - start_time
        print(f"⚡ All content fetched in {fetching_time:.2f} seconds")

        # Compare files by position and generate reports
        print("\n🔍 Comparing files by position (minimal diff mode)...")
        differences = self.compare_files_by_position(all_files[0], all_files[1], all_files[2])

        results = {
            'all_links': all_links,
            'all_files': all_files,
            'differences': differences,
            'hashes': [hash1, hash2, hash3],
            'timing': {
                'processing_time': processing_time,
                'fetching_time': fetching_time,
                'total_time': processing_time + fetching_time
            }
        }

        # Generate comprehensive reports
        print("\n📝 Generating position-based minimal difference reports...")
        report_info = self.generate_diff_report(results)
        results['report_info'] = report_info

        return results

    def print_results(self, results: Dict):
        """Print analysis results"""
        if not results:
            return

        differences = results['differences']
        all_files = results['all_files']
        all_links = results['all_links']
        hashes = results['hashes']

        print(f"\n✅ Analysis Complete!")
        print(f"⚡ Total time: {results.get('timing', {}).get('total_time', 0):.2f} seconds")
        print(f"Found {len(differences)} differences across {[len(f) for f in all_files]} files respectively.")
        print("=" * 80)

        # Show position comparison table
        print(f"\n📋 POSITION COMPARISON:")
        print("-" * 80)
        max_files = max(len(files) for files in all_files)
        print(f"{'Pos':<4} | {'Hash 1':<25} | {'Hash 2':<25} | {'Hash 3':<25}")
        print("-" * 80)
        for i in range(max_files):
            names = []
            for files in all_files:
                if i < len(files):
                    name = files[i].name[:23] + ".." if len(files[i].name) > 25 else files[i].name
                    names.append(name)
                else:
                    names.append("❌ MISSING")
            print(f"{i:02d}   | {names[0]:<25} | {names[1]:<25} | {names[2]:<25}")

        if differences:
            print(f"\n🔍 DIFFERENCES FOUND (POSITION-BASED MINIMAL DIFF):")
            print("-" * 60)

            for i, diff in enumerate(differences, 1):
                print(f"\n{i}. Position {diff.position:02d}: {diff.name}")
                print(f"   Type: {diff.type}")

                if diff.type == 'different' and diff.field_diffs:
                    print(f"   Different fields ({len(diff.field_diffs)}):")
                    for field_diff in diff.field_diffs[:3]:  # Show first 3 fields
                        print(f"     • {field_diff.field_path}:")
                        print(f"       Hash 1: {field_diff.hash1_value}")
                        print(f"       Hash 2: {field_diff.hash2_value}")
                        print(f"       Hash 3: {field_diff.hash3_value}")
                    if len(diff.field_diffs) > 3:
                        print(f"     ... and {len(diff.field_diffs) - 3} more fields")
                elif diff.type == 'missing':
                    print(f"   Files at this position:")
                    for j, file_obj in enumerate(diff.files):
                        print(f"     Hash {j + 1}: {file_obj.name if file_obj else '❌ MISSING'}")
        else:
            print("✅ No differences found! All hashes have identical content at all positions.")


def main():
    parser = argparse.ArgumentParser(
        description='Compare 3 IPFS hashes to find position-based differences (minimal diff mode)')
    parser.add_argument('hashes', nargs='*', help='Three IPFS hashes to compare')
    parser.add_argument('--interactive', '-i', action='store_true',
                        help='Run in interactive mode')
    parser.add_argument('--workers', '-w', type=int, default=10,
                        help='Number of worker threads for parallel processing (default: 10)')
    parser.add_argument('--output', '-o', type=str, default='ipfs_diff_reports',
                        help='Output directory for reports (default: ipfs_diff_reports)')

    args = parser.parse_args()

    tool = IPFSDiffTool(max_workers=args.workers)

    if args.interactive or len(args.hashes) != 3:
        print("🔗 IPFS Hash Position-Based Diff Tool (Minimal Diff Edition)")
        print("=" * 60)

        if len(args.hashes) != 3 and not args.interactive:
            print("❌ Please provide exactly 3 IPFS hashes or use --interactive mode")
            print("Usage: python ipfs_diff.py <hash1> <hash2> <hash3>")
            print("   or: python ipfs_diff.py --interactive")
            print("Options: --workers N (set number of parallel workers)")
            sys.exit(1)

        print("Please enter 3 IPFS hashes to compare:")
        hashes = []
        for i in range(3):
            while True:
                hash_val = input(f"Hash {i + 1}: ").strip()
                if hash_val:
                    hashes.append(hash_val)
                    break
                print("Please enter a valid hash.")
    else:
        hashes = args.hashes

    print(f"🚀 Using {args.workers} worker threads for parallel processing")
    print(f"📁 Reports will be saved to: {args.output}/")

    try:
        # Set the output directory for the tool
        tool.output_dir = args.output
        results = tool.analyze_differences(hashes[0], hashes[1], hashes[2])
        if results:
            tool.print_results(results)
    except KeyboardInterrupt:
        print("\n\n❌ Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()