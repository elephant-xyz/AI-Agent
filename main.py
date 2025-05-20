import asyncio
import os
import sys
import json
import logging
import uuid

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
from langchain_core.tools.base import ToolException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4.1")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))
MCP_SERVER: str = os.getenv("MCP_SERVER_URL", "http://localhost:8171/sse")
MAX_STEPS = 30  # Increased to allow for handling ad blocks and more complex navigation

# Define the data directory for file storage
DATA_DIR = "./data"

# Define the schema template
SCHEMA_TEMPLATE = {
    "people": [
        {
            "@id": "",
            "@type": "person",
            "first_name": "",
            "last_name": "",
            "suffix_name": "",
            "raw_name": "",
            "has_communication_method": ""
        }
    ],
    "communications": [
        {
            "@id": "",
            "@type": "communication",
            "has_mailing_address": ""
        }
    ],
    "ownerships": [
        {
            "@id": "",
            "@type": "ownership",
            "owned_by": "",
            "owned_property": "",
            "date_acquired": ""
        }
    ],
    "property_valuations": [
        {
            "@id": "",
            "@type": "property_valuation",
            "valuation_date": "",
            "actual_value": 0.0,
            "valuation_method_type": "SalesTransaction"
        }
    ],
    "properties": [
        {
            "@id": "",
            "@type": "property",
            "parcel_identifier": "",
            "property_condition_description": "",
            "property_taxable_value_amount": 0.0,
            "property_assessed_value_amount": 0.0,
            "property_exemption_amount": 0.0,
            "lot": "",
            "section": "",
            "block": "",
            "township": "",
            "range": "",
            "has_address": "",
            "property_type": "",
            "has_photos": [],
            "full_bathroom_count": 0,
            "bedroom_count": 0,
            "property_structure_built_year": 0
        }
    ],
    "documents": [
        {
            "@id": "",
            "@type": "document",
            "document_identifier": "",
            "property_image_url": ""
        }
    ],
    "addresses": [
        {
            "@id": "",
            "@type": "address",
            "address_line_1": "",
            "city_name": "",
            "state_code": "",
            "postal_code": "",
            "country_name": ""
        }
    ],
    "sales_transactions": [
        {
            "@id": "",
            "@type": "sales_transaction",
            "sales_date": "",
            "sales_transaction_amount": 0.0,
            "has_document": ""
        }
    ],
    "relationships": [
        {
            "@id": "",
            "@type": "finance_relation",
            "has_property": "",
            "has_sales_transactions": "",
            "has_property_valuation": "",
            "has_ownership": []
        }
    ]
}


# Helper function to generate IDs
def generate_id():
    """Generate a unique ID in the format used in the schema."""
    # Format: 01 + 20 hex characters
    return f"01{uuid.uuid4().hex.upper()[:20]}"


# ---------------------------------------------------------------------------

async def main(location: str):
    """Run an agent that finds property data using the specified schema."""

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Using data directory: {os.path.abspath(DATA_DIR)}")

    # Configure browser with increased timeouts
    server_cfg = {
        "browser": SSEConnection(
            url=MCP_SERVER,
            transport="sse",
            timeout=600.0,  # Increased timeout to 10 minutes
            session_kwargs=None,
            headers=None,
            sse_read_timeout=DEFAULT_SSE_READ_TIMEOUT,
        ),
        "filesystem": StdioConnection(
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", DATA_DIR],
            env=None,
            cwd=".",
            encoding=DEFAULT_ENCODING,
            encoding_error_handler=DEFAULT_ENCODING_ERROR_HANDLER,
            session_kwargs={},
        ),
    }

    logger.info(f"Connecting to MCP server at {MCP_SERVER}")
    async with MultiServerMCPClient(server_cfg) as mcp_client:
        tools = mcp_client.get_tools()
        logger.info(f"Connected to MCP server, loaded {len(tools)} tools")

        llm = ChatOpenAI(model=MODEL_NAME, temperature=TEMPERATURE, top_p=0.2)

        # Serialize the schema to include in the prompt
        schema_json = json.dumps(SCHEMA_TEMPLATE, indent=2)

        # Parse location
        state_name = location.split(",")[-3].strip()
        county_name = location.split(",")[-4].lower().replace(" county", "", True).strip()
        parcel_id = location.split(",")[-1].strip()
        county_display = county_name.title()

        # Comprehensive system prompt with detailed instructions for browser interaction
        system_prompt = f"""You are an autonomous web-navigation agent specializing in property data extraction.

    GOAL: Extract property information with address {location} from {county_display} County, {state_name} and structure it according to the schema below.

    IMPORTANT - IMPROVED NAVIGATION INSTRUCTIONS:

    1. On the property appraiser website:
       - First check for and handle any popup modals, cookie notices, or disclaimers
       - Look for "Property Search" or "Search Records" functions
       - Format the parcel ID {parcel_id} according to the county's requirements and search for the property
       
    IMPORTANT NAVIGATION BEHAVIOR RULES:

        - Before interacting with any page element (like buttons or input fields), wait until the element is fully visible and clickable on the screen. Do not attempt to interact with elements immediately after a page loads—pause and confirm the element is actually present.
        
        - If you click a link or button that causes the page to change (e.g., form submission, new search results, or redirection), wait until the new page finishes loading before doing anything else. Never run commands or read data while the page is still loading.
        
        - Always wait at least 3 seconds after a navigation before you interact with the new page. If possible, scroll the page first to ensure all parts are loaded.
        
        - Do not try to read or extract data from the page unless the relevant section or element is visible. Only proceed with reading or interacting once the page clearly shows the content you need.
        
        - If something goes wrong during navigation or an interaction fails (e.g., a button doesn’t click properly or the page errors), wait a few seconds and try again. If a dropdown or popup blocks a click, refresh the page or choose an alternative action (like pressing Enter instead of clicking).
        
        - Always scroll the page to reveal elements before clicking or extracting them. Avoid clicking invisible or off-screen elements.
        
        - Do not evaluate or extract any content until the page you’re on is fully loaded and stable. If the page navigates while you're trying to extract information, restart the extraction once the new page is ready.
        
        - Be patient: if the site is slow, give it time to respond before continuing.
        
        These rules are **critical** to avoid errors and make sure you reach the correct page and extract the full property information.
    IMPORTANT: If you click a button or link that causes the page to load new content or navigate, always follow the click with an explicit wait.
    
    NEVER evaluate page content immediately after clicking a button that causes navigation.
    Always wait until the new content has fully loaded.


    3. Extract ALL available data about the property, prioritizing fields in the schema below, create a file that will represent this property. Use property address as file name. Use available tools to save JSON data to the file

    SCHEMA TO USE (MAP ALL EXTRACTED DATA TO THIS STRUCTURE):
    {schema_json}
    
    "DO NOT FINISH till you save the actual data to the file",
    "Make sure to scroll to the HTML elements before clicking them",
    "You may freely use any browser.* tool you need.",
    "You may freely use any text-edit.* tool you need.",
    "PROCEED WITH USING YOUR TOOLS TILL YOU REACH THE RESULT",
    "DO NOT INTERRUPT YOUR EXECUTION TILL YOU REACH THE RESULT",
    DO NOT FINISH until you have successfully extracted and saved a properly structured JSON file to the "./data" directory.
    
    if you failed to get the property with parcel id, TRY AGAIN with parcel id in the same session if still fail try with address in the same session
    """

        # Attempt multiple strategies while keeping the Google search approach
        try:

            # Create an agent with higher max iterations to allow for more complex workflows
            agent = initialize_agent(
                tools,
                llm,
                agent=AgentType.OPENAI_FUNCTIONS,
                max_iterations=MAX_STEPS,
                verbose=True,
            )

            # Adjust strategy based on attempt number
            # First attempt: Try Google search with Enter key
            user_msg = f"""
                I need you to extract property information for parcel ID {parcel_id} from {county_display} County, {state_name}.

                IMPORTANT: Start with Google.com but use a different approach:

                GOOGLE SEARCH ALTERNATIVE:
                1. Navigate to Google.com
                2. Type "{county_display} County Property Appraiser" in the search field
                3. When autocomplete suggestions appear, click on the FIRST SUGGESTION instead of the search button
                4. On the results page, click the official website

                Then proceed with property search and extraction.
                """

            logger.info(f"Starting property data extraction for {county_display} County, {state_name}")
            try:
                await agent.arun(system_prompt + "\n\n" + user_msg)
                logger.info("Agent completed successfully")
            except ToolException as e:
                raise

        except Exception as e:

            # Provide detailed error information
            if "modal state" in str(e).lower():
                logger.error("The agent encountered a modal dialog that needs to be handled.")
            elif "timeout" in str(e).lower():
                logger.error("A timeout occurred. The website might be slow or blocking automation.")
                if "search</b>" in str(e).lower() and "intercepts pointer events" in str(e).lower():
                    logger.error(
                        "Google search dropdown is intercepting clicks. Try using Enter key or selecting from dropdown.")
            elif "access denied" in str(e).lower() or "outside allowed directories" in str(e).lower():
                logger.error("File access error: The agent tried to save to an unauthorized location")
            elif "execution context was destroyed" in str(e).lower() or "navigation" in str(e).lower():
                logger.error(f"Navigation error: The page navigated before the operation completed with error{e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python main.py "<County, ST>"')
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
