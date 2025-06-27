import asyncio
import os
import sys
import json
import logging
import random
import requests
from typing import Dict, Any, Optional, List, TypedDict

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import InMemorySaver
from langchain.chat_models import init_chat_model
from langchain_mcp_adapters.client import (
    MultiServerMCPClient,
    StdioConnection,
)
from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4.1")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))

# Define directories
BASE_DIR = os.path.abspath(".")
HTML_DIR = os.path.join(BASE_DIR, "html")
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")

# IPFS CIDs for schemas
SCHEMA_CIDS = {
    "owners.json": "QmZwvc8KCe2sB4dTRSQFby2B2jwgRgkhz4SJXeag8TMk1p",
    "sales.json": "QmNMbHmav4gmWFEPD7NYFLJAbdL5Rn4fpDVNCu3NymYMKf",
    "structure.json": "QmYPv8WBK7jCobu9B5Mau4AFuRXWtF6wkUJ3FmKJKr7568",
    "taxes.json": "QmREoEjuc7ZxUCKJ26x6Ec7VGmkgNYxfcrDZmaRqv2c2zJ",
    "location.json": "QmXqM7Ek6VR6yxJ66ndpk4yay9jo4BtKVcVmr38r41G4J4"
}


class WorkflowState(TypedDict):
    """State shared between nodes"""
    html_files: List[str]
    html_files_count: int
    schemas: Dict[str, Any]
    stub_files: Dict[str, Any]
    extraction_complete: bool
    address_matching_complete: bool
    validation_errors: List[str]
    processed_properties: List[str]
    current_node: str
    tools: List[Any]
    model: Any
    retry_count: int
    max_retries: int
    all_files_processed: bool
    all_addresses_matched: bool


def fetch_schema_from_ipfs(cid):
    """Fetch schema from IPFS using the provided CID."""
    gateways = [
        "https://ipfs.io/ipfs/",
        "https://gateway.pinata.cloud/ipfs/",
        "https://cloudflare-ipfs.com/ipfs/",
        "https://dweb.link/ipfs/",
        "https://ipfs.infura.io/ipfs/"
    ]

    for gateway in gateways:
        try:
            url = f"{gateway}{cid}"
            logger.info(f"Trying to fetch {cid} from {gateway}")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Error fetching from {gateway}: {e}")
            continue

    logger.error(f"Failed to fetch schema from IPFS CID {cid} from all gateways")
    return None


def load_schemas_from_ipfs():
    """Load all schemas from IPFS."""
    schemas = {}
    stub_files = {}

    for filename, cid in SCHEMA_CIDS.items():
        logger.info(f"Fetching schema for {filename} from IPFS...")
        schema = fetch_schema_from_ipfs(cid)
        if schema:
            schemas[filename] = schema
            stub_files[filename] = create_stub_from_schema(schema)
            logger.info(f"Successfully loaded schema for {filename}")
        else:
            logger.error(f"Failed to load schema for {filename}")
            return None, None

    return schemas, stub_files


def create_stub_from_schema(schema):
    """Create a stub structure from a JSON schema."""

    def create_stub_recursive(properties):
        stub = {}
        for key, value in properties.items():
            if value.get('type') == 'object':
                if 'properties' in value:
                    stub[key] = create_stub_recursive(value['properties'])
                else:
                    stub[key] = {}
            elif value.get('type') == 'array':
                if 'items' in value and value['items'].get('type') == 'object':
                    if 'properties' in value['items']:
                        stub[key] = [create_stub_recursive(value['items']['properties'])]
                    else:
                        stub[key] = [{}]
                else:
                    stub[key] = []
            else:
                stub[key] = None
        return stub

    if 'properties' in schema:
        return create_stub_recursive(schema['properties'])
    return {}


def check_extraction_complete(state: WorkflowState) -> bool:
    """Check if all files have been processed and moved to processed folder"""
    data_dir = os.path.join(BASE_DIR, "data")

    # Otherwise check data
    if os.path.exists(data_dir):
        processed_count = len([d for d in os.listdir(data_dir)
                               if os.path.isdir(os.path.join(data_dir, d))])
        logger.info(f"Processed {processed_count} out of {state['html_files_count']} properties in data directory")
        return processed_count >= state['html_files_count']

    return False


def check_address_matching_complete(state: WorkflowState) -> bool:
    """Check if all addresses have been matched or moved to error"""
    processed_dir = os.path.join(BASE_DIR, "processed")

    processed_count = 0
    error_count = 0

    if os.path.exists(processed_dir):
        processed_count = len([d for d in os.listdir(processed_dir)
                               if os.path.isdir(os.path.join(processed_dir, d))])

    total_handled = processed_count + error_count
    logger.info(f"Address matching: {processed_count} matched, {error_count} errors, "
                f"total: {total_handled}/{state['html_files_count']}")

    return total_handled >= state['html_files_count']


async def extraction_and_validation_node(state: WorkflowState) -> WorkflowState:
    """Node 1: Handle data extraction and validation (Steps 1-3)"""
    logger.info(f"=== Starting Node 1: Data Extraction & Validation (Attempt {state['retry_count'] + 1}) ===")
    if state['current_node'] != 'extraction':
        state['retry_count'] = 0
    state['current_node'] = 'extraction'
    # Check if already complete
    if check_extraction_complete(state):
        logger.info("Extraction already complete, skipping to next node")
        state['extraction_complete'] = True
        state['all_files_processed'] = True
        return state

    # Create the extraction and validation prompt
    system_prompt = f"""
    You are an HTML data extraction agent handling STEP 1 of the workflow.

    Current Status:
    - HTML files to process: {state['html_files_count']}
    - Schemas loaded: {len(state['schemas'])} schemas
    - This is attempt {state['retry_count'] + 1} of {state['max_retries']}
    - Schemas: {state['schemas']}
    YOUR TASK Only 1 step

    Before doing the Steps:
    - Select 5 sample HTML files for analysis the structure from `html` folder, have a full unsderstanding for each division and section of the HTML files.
    - Understand how can you get the data for each field in the schema from the HTML files.
    Step 1:
        A. Generate a universal extraction script: `scripts/html_data_extractor.py` that Map html data to the schemas, and save the extracted data as JSON files in the `data` folder.
        B. EXECUTE the script using the code execution tool
        C. the extracted schemas for each property should be add at `data/html_file_name/all 6 json files`
        D. Compare 50 samples of extracted data with the corresponding html to make sure if ALL data is extracted successfully
            - location.json schema: {state['schemas']['location.json']}
            - taxes.json schema: {state['schemas']['taxes.json']}
            - structure.json schema: {state['schemas']['structure.json']}
            - sales.json schema: {state['schemas']['sales.json']}
            - owners.json schema: {state['schemas']['owners.json']}
            IMPORTANT: make sure non of these folders are empty, and all the data was extracted successfully.
            IMPORTANT: All extracted jsons should be validated against their schema
        E. IMPORTANT: If NOT all data is extracted successfully, modify the `html_data_extractor.py` script to fix the extraction logic and re-execute the script.
    

    CRITICAL: You MUST process ALL {state['html_files_count']} HTML files.
    Check the processed folder and ensure all files are there before finishing.

    Available schemas:
    {json.dumps(list(state['schemas'].keys()), indent=2)}

    IMPORTANT: Focus ONLY on extraction and validation. Do NOT handle address matching yet.
    """

    # Create agent for this node
    checkpointer = InMemorySaver()
    agent = create_react_agent(
        model=state['model'],
        tools=state['tools'],
        prompt=system_prompt,
        checkpointer=checkpointer
    )

    config = {
        "configurable": {"thread_id": f"extraction-node-{state['retry_count']}"},
        "recursion_limit": 200
    }

    initial_message = {
        "messages": [{
            "role": "user",
            "content": f"Execute steps 1: Extract and validata data from ALL {state['html_files_count']} HTML files until all are in data folder. Do not stop until all files are processed."
        }]
    }

    try:
        # Execute the agent
        # await agent.ainvoke(initial_message, config)
        async for event in agent.astream_events(initial_message, config, version="v1"):
            kind = event["event"]
            if kind == "on_chain_start":
                print(f"\n🔗 Starting: {event['name']}")
            elif kind == "on_chain_end":
                print(f"✅ Finished: {event['name']}")
            elif kind == "on_tool_start":
                print(f"\n🔧 Calling tool: {event['name']} with inputs: {event['data'].get('input')}")
            elif kind == "on_tool_end":
                print(f"✅ Tool result: {event['data'].get('output')}")
            elif kind == "on_llm_start":
                print(f"\n🧠 LLM thinking...")
            elif kind == "on_llm_end":
                print(f"💭 LLM response: {event['data'].get('output')}")

        # Check if goal was achieved
        if check_extraction_complete(state):
            state['extraction_complete'] = True
            state['all_files_processed'] = True
            logger.info("✅ Node 1 completed successfully - all files processed")
        else:
            state['all_files_processed'] = False
            logger.warning("⚠️ Node 1 did not process all files")

    except Exception as e:
        logger.error(f"Error in extraction node: {e}")
        state['all_files_processed'] = False
        state['validation_errors'].append(str(e))

    return state


async def address_matching_node(state: WorkflowState) -> WorkflowState:
    """Node 3: Handle address matching and final processing (Steps 4-6)"""
    if state['current_node'] != 'address_matching':
        state['retry_count'] = 0
    state['current_node'] = 'address_matching'
    logger.info(f"=== Starting Node 2: Address Matching & Processing (Attempt {state['retry_count'] + 1}) ===")

    # Check if already complete
    if check_address_matching_complete(state):
        logger.info("Address matching already complete")
        state['address_matching_complete'] = True
        state['all_addresses_matched'] = True
        return state

    # Create the address matching prompt
    system_prompt = f"""
    You are an address matching specialist handling the final phase of property data processing.

    🔄 CURRENT STATUS:
        ✅ Data extraction: COMPLETE - All {state['html_files_count']} properties are in ./data/ directory
        🎯 Current task: Match addresses and move properties to ./processed/
        🔁 Attempt: {state['retry_count'] + 1} of {state['max_retries']}

    📂 FILE STRUCTURE:
        • ./data/[property_id]/location.json - Properties needing address matching
        • ./possible_addresses/[property_id].json - Address candidates for each property
        • ./html/[property_id].html - Source HTML for address extraction
        • ./processed/[property_id]/ - Final destination after successful matching

    🎯 YOUR MISSION: Process ALL properties until ./data/ is empty

    PHASE 1: ANALYZE THE DATA STRUCTURE
        1. Examine 3-5 samples from possible_addresses/ to understand:
           - JSON format and field structure
           - How many address candidates each property has
           - Address format variations

        2. Check corresponding HTML files to understand:
           - Where addresses appear in the HTML
           - How to extract: street number, street name, unit, city, zip

        3. Plan your matching strategy before coding

    PHASE 2: BUILD THE MATCHING SCRIPT
        Create: scripts/address_extraction.py

        The script must:
        A. For each property in ./data/:
           - Extract property_id from folder name
           - Load possible_addresses/[property_id].json (candidate addresses)
           - Parse address from html/[property_id].html
           - Extract: street_number, street_name, unit, city, zip, directionals, suffix_type

        B. Find the best address match:
           - Compare HTML address with candidates in possible_addresses/
           - Use exact matching first, then fuzzy matching if needed
           - Focus on: street number + street name + unit (if applicable)

        C. When match found:
           - Update location.json with matched address data using schema: {state['schemas']['location.json']}
           - PRESERVE existing fields like: latitude, longitude, range, section, township
           - Move entire folder: ./data/[property_id]/ → ./processed/[property_id]/

        D. Handle failures:
           - Log properties that couldn't be matched
           - Continue processing other properties

    PHASE 3: EXECUTE AND ITERATE
        1. Run the script on ALL properties
        2. Track progress: "Processed X of {state['html_files_count']} properties"
        3. For failed matches:
           - Analyze why matching failed
           - Improve the script logic
           - Re-run on failed cases
        4. Repeat until ./data/ directory is completely empty

    SUCCESS CRITERIA:
        ✅ ./data/ directory is empty (all folders moved to ./processed/)
        ✅ All location.json files have proper address data
        ✅ No data loss (preserve existing non-address fields)

    EXECUTION RULES:
        • Process systematically, one property at a time
        • Report progress regularly
        • Handle errors gracefully and continue
        • Don't stop until ALL properties are processed
        • Focus ONLY on address matching (extraction is already done)

    START with data analysis, then build the script, then execute until complete.
    """

    # Create agent for this node
    checkpointer = InMemorySaver()
    agent = create_react_agent(
        model=state['model'],
        tools=state['tools'],
        prompt=system_prompt,
        checkpointer=checkpointer
    )

    config = {
        "configurable": {"thread_id": f"address-node-{state['retry_count']}"},
        "recursion_limit": 100
    }

    initial_message = {
        "messages": [{
            "role": "user",
            "content": f"""
    Execute the address matching mission for all {state['html_files_count']} properties.

    GOAL: Move every property folder from ./data/ to ./processed/ with accurate address matching.

    Begin with analyzing the data structure, then create the matching script, then process all properties systematically.

    Report your progress at each phase. Do not stop until ./data/ directory is completely empty.
            """
        }]
    }

    try:
        # Execute the agent
        async for event in agent.astream_events(initial_message, config, version="v1"):
            kind = event["event"]
            if kind == "on_chain_start":
                print(f"\n🔗 Starting: {event['name']}")
            elif kind == "on_chain_end":
                print(f"✅ Finished: {event['name']}")
            elif kind == "on_tool_start":
                print(f"\n🔧 Calling tool: {event['name']} with inputs: {event['data'].get('input')}")
            elif kind == "on_tool_end":
                print(f"✅ Tool result: {event['data'].get('output')}")
            elif kind == "on_llm_start":
                print(f"\n🧠 LLM thinking...")
            elif kind == "on_llm_end":
                print(f"💭 LLM response: {event['data'].get('output')}")

        # Check if goal was achieved
        if check_address_matching_complete(state):
            state['address_matching_complete'] = True
            state['all_addresses_matched'] = True
            logger.info("✅ Node 2 completed successfully - all addresses handled")
        else:
            state['all_addresses_matched'] = False
            logger.warning("⚠️ Node 2 did not handle all addresses")

    except Exception as e:
        logger.error(f"Error in address matching node: {e}")
        state['all_addresses_matched'] = False
        state['validation_errors'].append(str(e))

    return state


def should_retry_extraction(state: WorkflowState) -> str:
    """Determine if extraction node should retry"""
    if state['all_files_processed']:
        state['retry_count'] = 0  # Reset for next node
        return "address_matching"
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying extraction node (attempt {state['retry_count']}/{state['max_retries']})")
        return "extraction"
    else:
        logger.error("Max retries reached for extraction node")
        return "address_matching"


def should_retry_address_matching(state: WorkflowState) -> str:
    """Determine if address matching node should retry"""
    if state['all_addresses_matched']:
        return "end"
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying address matching node (attempt {state['retry_count']}/{state['max_retries']})")
        return "address_matching"
    else:
        logger.error("Max retries reached for address matching node")
        return "end"


async def run_two_node_workflow():
    """Main function to run the two-node workflow with retry logic"""

    # Load schemas from IPFS
    logger.info("Loading schemas from IPFS...")
    schemas, stub_files = load_schemas_from_ipfs()

    if not schemas or not stub_files:
        logger.error("Failed to load schemas from IPFS")
        return

    # Get HTML files
    html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('.html')] if os.path.exists(HTML_DIR) else []

    if not html_files:
        logger.error(f"No HTML files found in {HTML_DIR}")
        return

    html_files_count = len(html_files)
    logger.info(f"Found {html_files_count} HTML files to process")

    # Configure MCP client
    current_dir = os.path.abspath(".")
    server_cfg = {
        "filesystem": StdioConnection(
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", current_dir],
            env=None,
            cwd=current_dir,
            encoding="utf-8",
            encoding_error_handler="ignore",
            session_kwargs={},
        ),
        "code_executor": StdioConnection(
            transport="stdio",
            command="node",
            args=["mcp_code_executor/build/index.js"],
            env={
                "CODE_STORAGE_DIR": current_dir,
                "ENV_TYPE": "venv",
                "VENV_PATH": os.path.join(current_dir, ".venv")
            },
            cwd=current_dir,
            encoding="utf-8",
            encoding_error_handler="ignore",
            session_kwargs={},
        )
    }

    # Initialize MCP client and tools
    logger.info("Connecting to MCP filesystem server")
    mcp_client = MultiServerMCPClient(server_cfg)
    tools = await mcp_client.get_tools()
    logger.info(f"Connected to MCP server, loaded {len(tools)} tools")

    # Initialize model
    model = init_chat_model(
        MODEL_NAME,
        temperature=TEMPERATURE
    )

    # model = ChatGoogleGenerativeAI(
    #     model="models/gemini-pro",  # Pay attention to this line
    #     temperature=TEMPERATURE,
    #     convert_system_message_to_human=True
    # )

    # Initialize workflow state
    initial_state = WorkflowState(
        html_files=html_files,
        html_files_count=html_files_count,
        schemas=schemas,
        stub_files=stub_files,
        extraction_complete=False,
        address_matching_complete=False,
        validation_errors=[],
        processed_properties=[],
        current_node="extraction",
        tools=tools,
        model=model,
        retry_count=0,
        max_retries=3,
        all_files_processed=False,
        all_addresses_matched=False
    )

    # Create the workflow graph
    workflow = StateGraph(WorkflowState)

    # Add nodes
    workflow.add_node("extraction", extraction_and_validation_node)
    workflow.add_node("address_matching", address_matching_node)

    # Add conditional edges with retry logic
    workflow.add_conditional_edges(
        "extraction",
        should_retry_extraction,
        {
            "extraction": "extraction",  # Retry same node
            "address_matching": "address_matching"  # Move to next
        }
    )

    workflow.add_conditional_edges(
        "address_matching",
        should_retry_address_matching,
        {
            "address_matching": "address_matching",  # Retry same node
            "end": END
        }
    )

    # Set entry point
    workflow.set_entry_point("extraction")

    # Compile the graph
    app = workflow.compile()

    # Run the workflow
    try:
        logger.info("Starting two-node workflow execution with retry logic")
        final_state = await app.ainvoke(initial_state)

        # Log final status
        if final_state['all_files_processed'] and final_state['all_addresses_matched']:
            logger.info("✅ Workflow completed successfully - all tasks completed")
        else:
            logger.warning("⚠️ Workflow completed with incomplete tasks")
            if not final_state['all_files_processed']:
                logger.warning("- Not all files were processed")
            if not final_state['all_addresses_matched']:
                logger.warning("- Not all addresses were matched")

    except Exception as e:
        logger.error(f"Workflow error: {e}")
        raise


async def main():
    """Main entry point"""
    try:
        await run_two_node_workflow()
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
