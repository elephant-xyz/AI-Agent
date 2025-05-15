import asyncio
import os
import sys

from langchain.agents import AgentType, initialize_agent
from langchain_mcp_adapters.client import (
    MultiServerMCPClient,
    SSEConnection,
    DEFAULT_SSE_READ_TIMEOUT,
    StdioConnection,
    DEFAULT_ENCODING,
    DEFAULT_ENCODING_ERROR_HANDLER,
)
from langchain_openai import ChatOpenAI

MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4.1")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))
MCP_SERVER: str = os.getenv("MCP_SERVER_URL", "http://localhost:8171/sse")
MAX_STEPS = 10

BASE_NETR = "https://publicrecords.netronline.com"
COUNTY_URL = BASE_NETR + "/state/{state}/county/{county}"

# ---------------------------------------------------------------------------


async def main(location: str):
    """Run an agent that finds a sample parcel page and saves its HTML."""

    # ------------------------------------------------------------------
    # 1) Connect to the Playwright‑MCP server and load *all* its tools.
    # ------------------------------------------------------------------

    server_cfg = {
        "browser": SSEConnection(
            url=MCP_SERVER,
            transport="sse",
            timeout=180.0,
            session_kwargs=None,
            headers=None,
            sse_read_timeout=DEFAULT_SSE_READ_TIMEOUT,
        ),
        "filesytem": StdioConnection(
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", "./data"],
            env=None,
            cwd=".",
            encoding=DEFAULT_ENCODING,
            encoding_error_handler=DEFAULT_ENCODING_ERROR_HANDLER,
            session_kwargs={},
        ),
        # "text-edit": {"transport": "stdio", "command": "uvx", "args": ["mcp-text-editor"]},
    }
    async with MultiServerMCPClient(server_cfg) as mcp_client:
        tools = mcp_client.get_tools()

        # --------------------------------------------------------------
        # 3) Compose agent with LLM + all browser tools + save tool.
        # --------------------------------------------------------------
        llm = ChatOpenAI(model=MODEL_NAME, temperature=TEMPERATURE, top_p=0.2)

        system_prompt = (
            "You are an autonomous web‑navigation agent.\n"
            "Goal: Your ultimate goal is to gather information about a property and save it to a file"
            "Step 1: starting from the NETR Online directory for the given county, find URL to it's Property Appraiser and visit this website",
            "Step 2: access the Property Appraiser website and open page of any property",
            "Step 3: creatae a file that will represent this property. Use property address as file name. Use available tools to save JSON data to the file",
            "DO NOT FINISH till you save the actual data to the file",
            "Make sure to scroll to the HTML elements before cliking them",
            "You may freely use any browser.* tool you need.",
            "You may freely use any text-edit.* tool you need.",
            "PROCEED WITH USING YOUR TOOLS TILL YOU REACH THE RESULT",
            "DO NOT INTERUPT YOUR EXECUTION TILL YOU REACH THE RESULT",
        )
        agent = initialize_agent(
            tools,
            llm,
            agent=AgentType.OPENAI_FUNCTIONS,
            max_iterations=MAX_STEPS,
            verbose=True,
        )

        # --------------------------------------------------------------
        # 5) Kick off the task by giving the county name.
        # --------------------------------------------------------------
        state_name = location.split(",")[-1].strip()
        county_name = location.split(",")[0].lower().replace(" county", "", True).strip().lower().replace(" ", "_")
        start_url = COUNTY_URL.format(
            state=state_name,
            county=county_name,
        )
        user_msg = f"Start from {start_url} and achieve the goal for the {county_name} county."
        user_msg = "/n".join(system_prompt) + "/n" + user_msg
        await agent.arun(user_msg)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python main.py "<County, ST>"')
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
