import os
import csv
import re
from bs4 import BeautifulSoup

def extract_strap(html_content):
    # Match anything that looks like a STRAP with letters/numbers, dashes, and dots
    match = re.search(r'STRAP:\s*([\w\.\-]+)', html_content)
    if match:
        raw_strap = match.group(1)
        cleaned_strap = raw_strap.replace('-', '').replace('.', '')
        return cleaned_strap
    return None

def process_html_folder(folder_path, output_csv='strap_output.csv'):
    results = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".html"):
            filepath = os.path.join(folder_path, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                soup = BeautifulSoup(content, 'html.parser')
                text = soup.get_text()
                strap_id = extract_strap(text)
                if strap_id:
                    folio_id = filename.replace('.html', '')
                    results.append({'folio_id': folio_id, 'parcel_id': strap_id})

    # Write results to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['folio_id', 'parcel_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"CSV saved to {output_csv}")

# Example usage:
process_html_folder('html')
