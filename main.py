#!/usr/bin/env python3
"""
prototype_assessor.py  —  v0.2 (NetRONLINE edition)

Implements **Steps 1‑3** of your agentic workflow using NETR Online’s public-records
routing pages instead of a generic web search.

1. **Resolve assessor / property‑appraiser URL** for a given *County, State* using
   https://publicrecords.netronline.com directory pages.
2. **Download** the county directory HTML (`county_directory.html`).
3. **Open** the linked assessor site and save its landing page (`landing.html`).

The script remains self‑contained (Playwright + BeautifulSoup only) and forms a
clean hand‑off to the forthcoming selector‑generation step.

Usage example:
```bash
pip install playwright beautifulsoup4
playwright install              # first‑time browser download
python prototype_assessor.py "Volusia County, FL"
```
Outputs:
  • `county_directory.html` – NETR directory page for the county
  • `landing.html` – raw HTML of the assessor / appraiser homepage

Extend easily by parsing the assessor page and generating selectors.
"""

import asyncio
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BASE_NETR = "https://publicrecords.netronline.com"
COUNTY_URL = BASE_NETR + "/state/{state}/county/{county}"

# --- helpers -----------------------------------------------------------------

STATES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}
# make reverse lookup of full state names → abbreviation
STATE_TO_ABBR = {name.lower(): abbr for abbr, name in STATES.items()}


def parse_location(raw: str):
    """Return (county_slug, STATE_ABBR) from an input like 'Volusia County, FL'."""
    parts = [p.strip() for p in re.split(r",|\n", raw) if p.strip()]
    if len(parts) == 2:
        county_raw, state_raw = parts
    else:
        raise ValueError("Input must be in the form 'County, ST' or 'County, State'.")

    # drop trailing 'county'
    county_clean = re.sub(r"county$", "", county_raw, flags=re.I).strip()
    county_slug = county_clean.lower().replace(" ", "_")

    state_raw = state_raw.strip()
    if len(state_raw) == 2:  # assume abbreviation
        state_abbr = state_raw.upper()
    else:
        state_abbr = STATE_TO_ABBR.get(state_raw.lower())
        if not state_abbr:
            raise ValueError(f"Unrecognised state name: {state_raw}")
    return county_slug, state_abbr


async def save_html(content: str, filename: Path):
    filename.write_text(content, encoding="utf-8")
    print(f"[+] Saved {filename}")


async def resolve_assessor_url(page, county_slug: str, state_abbr: str):
    """Return assessor / appraiser URL from NETR directory, or None."""
    url = COUNTY_URL.format(state=state_abbr, county=county_slug)
    print(f"[*] Opening NETR directory: {url}")
    await page.goto(url, timeout=60000, wait_until="domcontentloaded")
    county_html = await page.content()
    await save_html(county_html, Path("county_directory.html"))

    soup = BeautifulSoup(county_html, "html.parser")
    for row in soup.find_all("tr"):
        row_text = row.get_text(" ", strip=True).lower()
        if any(kw in row_text for kw in ("property appraiser", "assessor")):
            link = row.find("a", string=re.compile(r"go to data online", re.I))
            if link and link.get("href"):
                return link["href"]
    # fallback: first link labelled "Go to Data Online"
    link = soup.find("a", string=re.compile(r"go to data online", re.I))
    return link["href"] if link else None


async def open_assessor_home(page, assessor_url: str):
    print(f"[*] Opening assessor site: {assessor_url}")
    try:
        await page.goto(assessor_url, timeout=60000)
    except PlaywrightTimeoutError:
        print("[!] Timeout loading assessor site; saving partial HTML anyway…")
    html = await page.content()
    await save_html(html, Path("landing.html"))


async def main(raw_location: str):
    county_slug, state_abbr = parse_location(raw_location)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        assessor_url: str = await resolve_assessor_url(page, county_slug, state_abbr)
        if assessor_url:
            print(f"[+] Resolved assessor URL: {assessor_url}")
            await open_assessor_home(page, assessor_url)
        else:
            print("[!] Could not locate assessor link on NETR directory page.")

        await browser.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python prototype_assessor.py "<County, ST>"')
        sys.exit(1)

    try:
        asyncio.run(main(sys.argv[1]))
    except Exception as exc:
        print(f"[ERROR] {exc}")
