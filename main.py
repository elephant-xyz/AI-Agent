#!/usr/bin/env python3
"""
prototype_assessor.py  —  v0.3 (LLM‑driven NETR edition)

**What changed?**
* Uses a **LangChain LLM call** to analyse the NETR directory HTML and decide which
  link to follow next, instead of hard‑coded BeautifulSoup rules.
* Still performs **Steps 1‑3** of your workflow:
    1. Load the county page on publicrecords.netronline.com.
    2. **Submit the raw HTML to the LLM** (GPT‑4o by default) along with a
       system prompt: *“Return the single best URL for the property assessor”*.
    3. Visit that URL, save `landing.html`, ready for the extraction agent.

You can swap in any LLM supported by LangChain (OpenAI, Anthropic, Azure, etc.)
by setting the `MODEL_NAME` env‑var or CLI flag.

---
Usage example:
```bash
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate
pip install playwright beautifulsoup4 langchain-openai tiktoken
playwright install                                  # one‑time browser download
export OPENAI_API_KEY="sk‑…"                        # or set via Azure / Anthropic vars
python prototype_assessor.py "Volusia County, FL"
```
Outputs:
  • `county_directory.html` – NETR directory page for the county
  • `landing.html`          – raw HTML of the assessor / appraiser homepage

The LLM sees **only** the NETR HTML and the system prompt; it replies with the
absolute URL (nothing else). This makes the module a drop‑in for richer
multi‑step agents later.
"""

import asyncio
import os
import re
import sys
from pathlib import Path
from textwrap import shorten
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup  # still handy for link normalisation
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

# --- LangChain ---------------------------------------------------------------
try:
    from langchain.schema import HumanMessage, SystemMessage
    from langchain_openai import ChatOpenAI  # pip install langchain-openai
except ImportError as exc:  # graceful hint if deps missing
    raise SystemExit("Missing langchain‑openai; run `pip install langchain-openai`.") from exc

MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4.1")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))

# --- constants --------------------------------------------------------------
BASE_NETR = "https://publicrecords.netronline.com"
COUNTY_URL = BASE_NETR + "/state/{state}/county/{county}"

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
STATE_TO_ABBR = {name.lower(): abbr for abbr, name in STATES.items()}

# --- helper functions --------------------------------------------------------


def parse_location(raw: str):
    """Return (county_slug, STATE_ABBR). Accepts 'Volusia County, FL' or 'Volusia, Florida'."""
    parts = [p.strip() for p in re.split(r",|\n", raw) if p.strip()]
    if len(parts) != 2:
        raise ValueError("Input must be in the form 'County, ST' or 'County, State'.")
    county_raw, state_raw = parts

    county_clean = re.sub(r"county$", "", county_raw, flags=re.I).strip()
    county_slug = county_clean.lower().replace(" ", "_")

    state_abbr = state_raw.upper() if len(state_raw) == 2 else STATE_TO_ABBR.get(state_raw.lower())
    if not state_abbr:
        raise ValueError(f"Unrecognised state: {state_raw}")
    return county_slug, state_abbr


async def save_html(text: str, path: Path):
    path.write_text(text, encoding="utf-8")
    print(f"[+] Saved {path}")


async def ask_llm_for_assessor_url(html: str, county_slug: str, state_abbr: str) -> str | None:
    """Give the NETR HTML to the LLM and ask which link is the assessor/appraiser site."""
    llm = ChatOpenAI(model=MODEL_NAME, temperature=TEMPERATURE)

    # HTML can be big; trim to keep the most relevant part (link table usually < 10 KB)
    excerpt = shorten(html, width=16000, placeholder="\n…[truncated]…")

    system_msg = SystemMessage(
        content=(
            "You are a web‑scraping planner. The user will give you raw HTML of a county page on publicrecords.netronline.com. "
            "Your task: return **ONLY** the single absolute URL (starting with http) that leads to the online Property Appraiser, Assessor, or Parcel Information site for that county. "
            "If multiple links look valid, choose the most authoritative one. If none found, return the word NONE."
        )
    )
    human_msg = HumanMessage(
        content=f"HTML for county {county_slug.title()} (state {state_abbr}):\n```html\n{excerpt}\n```\n\nYour answer:"
    )

    # Run the (blocking) call in a thread so we don't block the asyncio event loop
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(None, lambda: llm.invoke([system_msg, human_msg]))
    url = response.content.strip()

    # validate a URL was returned
    if url.lower() == "none":
        return None
    if not urlparse(url).scheme:
        # maybe relative path; prepend base
        url = urljoin(BASE_NETR, url)
    return url


async def main(raw_location: str):
    county_slug, state_abbr = parse_location(raw_location)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        county_url = COUNTY_URL.format(state=state_abbr, county=county_slug)
        print(f"[*] Loading NETR directory: {county_url}")
        await page.goto(county_url, timeout=60000, wait_until="domcontentloaded")
        county_html = await page.content()
        await save_html(county_html, Path("county_directory.html"))

        assessor_url = await ask_llm_for_assessor_url(county_html, county_slug, state_abbr)
        if not assessor_url:
            print("[!] LLM could not find an assessor link. Exiting.")
            await browser.close()
            return

        print(f"[+] LLM‑selected assessor URL: {assessor_url}")
        try:
            await page.goto(assessor_url, timeout=60000)
        except PlaywrightTimeoutError:
            print("[!] Timeout loading assessor site; saving partial HTML…")
        landing = await page.content()
        await save_html(landing, Path("landing.html"))

        await browser.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python prototype_assessor.py "<County, ST>"')
        sys.exit(1)

    try:
        asyncio.run(main(sys.argv[1]))
    except KeyboardInterrupt:
        print("[!] Interrupted by user.")
