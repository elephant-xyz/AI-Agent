# Prototype Assessor (v1.0) — Playwright‑MCP edition

> **Install dependencies with uv:**
> ```bash
> uv venv && source .venv/bin/activate
> uv pip install -r requirements.txt
> ```
>
> **⚠️ Known Issues:**
> - The tool is not stable and may suffer from Playwright errors or unexpected crashes.
> - It does not work properly when an anti-ad blocker modal pops up on the target site.
> - Please expect instability and manual intervention in some cases.

This repository contains **`prototype_assessor.py`**, a LangChain‑powered agent that
navigates U.S. county assessor websites to obtain a sample parcel page. All
browser automation is delegated to the [**Playwright‑MCP** server](https://github.com/microsoft/playwright-mcp), so the codebase stays lightweight and portable.

---

## 1  Prerequisites

| Component    | Purpose                                      | Install command                           |
| ------------ | -------------------------------------------- | ----------------------------------------- |

| Python ≥ 3.9 | Run the client script + LangChain            |  `pyenv install 3.11 && pyenv local 3.11` |
| OpenAI key   | Any GPT‑3.5/4o model (or swap provider)      |  `export OPENAI_API_KEY=sk‑…`             |

> **Why MCP?** The server exposes a standard JSON‑RPC toolset (`openUrl`,
> `click`, `evaluate`, `getAccessibilityTree`) that any LLM client can reuse. No
> Playwright code ships in the Python client.

---

## 2  Quick start

```bash
# ❶ Launch the Playwright‑MCP server (Chromium headless on port 8931)
$ npx @playwright/mcp@latest --port 8931 --no-sandbox

# ❷ Create a virtualenv and install deps
$ python -m venv .venv && source .venv/bin/activate
$ pip install mcp langchain-openai tiktoken beautifulsoup4

# ❸ Run the prototype
$ export OPENAI_API_KEY="sk‑…"
$ python prototype_assessor.py "Volusia County, FL"
```

Generated files:

- `county_directory.html` — NETR directory page
- `landing.html` — assessor homepage
- `nav_step_#.html` — each navigation step (debugging)
- `property_sample.html` — **parcel detail page** when the LLM returns `done`

---

## 3  Configuration

| Variable         | Default                     | Description                                 |
| ---------------- | --------------------------- | ------------------------------------------- |
| `MCP_SERVER_URL` | `http://localhost:8171/sse` | SSE endpoint of the browser MCP server      |
| `MODEL_NAME`     | `gpt-4.1`               | Any model supported by `langchain-openai`   |
| `TEMPERATURE`    |  `0`                        | LLM temperature                             |
| `MAX_STEPS`      |  `10`                       | Hard cap on back‑and‑forth navigation loops |

---

## 4  How it works

1. **Resolve assessor site**  — Opens the NETR directory, grabs HTML via
   `browser.evaluate("document.documentElement.outerHTML")`, and asks the LLM to
   output one absolute URL.
2. **Interactive loop** (≤10 iterations)

   - Fetches an accessibility snapshot with `browser.getAccessibilityTree`.
   - Builds a short list of clickable names (role ∈ `{link, button}`).
   - Sends current URL + clickable list to the LLM.
   - Executes either `browser.openUrl` or `browser.click`.

3. On `{action:"done"}` the current page HTML is saved as
   `property_sample.html`.

---

## 5  Extending the POC

- **Bulk scraping** — Replace the 10‑step loop with a dedicated search‑page
  crawler and call this script per parcel URL.
- **MCP tool chaining** — Combine this browser server with an **S3 MCP server**
  to store extracted JSON rows, or a **PDF MCP server** to fetch deeds.
- **Proxy rotation** — Use the `--config` flag of Playwright‑MCP to supply
  proxy credentials system‑wide.

---

## 6  Resources

- Playwright‑MCP GitHub  — [https://github.com/microsoft/playwright-mcp](https://github.com/microsoft/playwright-mcp)
- MCP Python SDK  — [https://pypi.org/project/mcp](https://pypi.org/project/mcp)
- LangChain MCP Adapters  — [https://github.com/langchain-ai/langchain-mcp-adapters](https://github.com/langchain-ai/langchain-mcp-adapters)
