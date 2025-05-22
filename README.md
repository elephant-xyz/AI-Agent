# Prototype Assessor (v1.0) — Playwright‑MCP edition

> **Install dependencies with uv:**
> ```bash
> uv venv && source .venv/bin/activate
> uv pip install -r requirements.txt
> ```

This repository contains **`main.py`**, a LangChain‑powered agent that
navigates google to get the county appraiser site and then the properties details using the parcel id. All
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

##  Quick start

```bash
# ❶ Launch the Playwright‑MCP server (Chromium headless on port 8931)
$ npx @playwright/mcp@latest --port 8931 --no-sandbox

# ❷ Create a virtualenv and install deps
$ python -m venv .venv && source .venv/bin/activate
$ pip install mcp langchain-openai tiktoken beautifulsoup4

# ❸ Run the prototype
$ export OPENAI_API_KEY="sk‑…"
$ python main.py "19005, CORBINA COURT, ESTERO, LEE COUNTY, FL, 33928, 244626L10400J5520"
```
---

##  Configuration

| Variable         | Default                     | Description                                 |
| ---------------- | --------------------------- | ------------------------------------------- |
| `MCP_SERVER_URL` | `http://localhost:8171/sse` | SSE endpoint of the browser MCP server      |
| `MODEL_NAME`     | `gpt-4.1`               | Any model supported by `langchain-openai`   |
| `TEMPERATURE`    |  `0`                        | LLM temperature                             |
| `MAX_STEPS`      |  `10`                       | Hard cap on back‑and‑forth navigation loops |

---


##  Resources

- Playwright‑MCP GitHub  — [https://github.com/microsoft/playwright-mcp](https://github.com/microsoft/playwright-mcp)
- MCP Python SDK  — [https://pypi.org/project/mcp](https://pypi.org/project/mcp)
- LangChain MCP Adapters  — [https://github.com/langchain-ai/langchain-mcp-adapters](https://github.com/langchain-ai/langchain-mcp-adapters)
