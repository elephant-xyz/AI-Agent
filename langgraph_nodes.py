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
# Remove the init_chat_model import since we'll use Gemini directly
# from langchain.chat_models import init_chat_model
from langchain_mcp_adapters.client import (
    MultiServerMCPClient,
    StdioConnection,
)
from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI
# Method 1: Suppress warnings using logging configuration (Recommended)
import logging
import warnings

# Add this at the top of your script after the logging configuration
logging.getLogger("langchain_google_genai._function_utils").setLevel(logging.ERROR)

# Or alternatively, you can filter specific warning messages
warnings.filterwarnings("ignore", message=".*Key.*is not supported in schema.*")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Update model configuration for Gemini
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.5-pro-preview-05-06") # or "gemini-1.5-flash" for faster responses
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))
# Add Google API key
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")  # Make sure to set this environment variable

# Define directories
BASE_DIR = os.path.abspath(".")
HTML_DIR = os.path.join(BASE_DIR, "html")
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")

# IPFS CIDs for schemas
SCHEMA_CIDS = {
    "owners.json": "QmTTTSdvryWnLyqL5L7omMzCVHeo76a2p2PBdpZ31rgSsd",
    "sales.json": "QmPr4G8Jvz5UqEWz7QLZPWr3fpMgxC7MQS5twR7j5qzzyu",
    "structure.json": "QmPU13UJkfXabQAnsPxFKsVspizA8tw4cjKCyisq46ZY8e",
    "taxes.json": "QmPVamV7C9V7PbHquQa5MzQ8UyAT214TpsZkjhLo6LLxcG",
    "location.json": "Qmevin8Lnpt9i7N8K75Mw6auEzZWirsrYWn9jAeKu2soEs"
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
    schema_validated_dir = os.path.join(BASE_DIR, "data")

    # Otherwise check Schema_validated
    if os.path.exists(schema_validated_dir):
        processed_count = len([d for d in os.listdir(schema_validated_dir)
                               if os.path.isdir(os.path.join(schema_validated_dir, d))])
        logger.info(f"Processed {processed_count} out of {state['html_files_count']} properties in Schema_validated")
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
        C. Generate a universal extraction script: `scripts/html_data_extractor.py` that Map html data to the schemas, and save the extracted data as JSON files in the `data` folder.
        D. EXECUTE the script using the code execution tool
        E. the extracted schemas for each property should be add at `data/property_id/all 6 json files`
        E. Compare 50 samples of extracted data with the corresponding html to make sure if ALL data is extracted successfully
            - location.json schema: {state['schemas']['location.json']}
            - taxes.json schema: {state['schemas']['taxes.json']}
            - structure.json schema: {state['schemas']['structure.json']}
            - sales.json schema: {state['schemas']['sales.json']}
            - owners.json schema: {state['schemas']['owners.json']}
        f. If NOT all data is extracted successfully, modify the `html_data_extractor.py` script to fix the extraction logic and re-run it.

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
            "content": f"Execute steps 1-3: validata data from ALL {state['html_files_count']} HTML files until all are in Schema_validated folder. Do not stop until all files are processed."
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
    logger.info(f"=== Starting Node 2: Address Matching & Processing (Attempt {state['retry_count'] + 1}) ===")

    # Check if already complete
    if check_address_matching_complete(state):
        logger.info("Address matching already complete")
        state['address_matching_complete'] = True
        state['all_addresses_matched'] = True
        return state

    # Create the address matching prompt
    system_prompt = f"""
    You are handling STEPS 5-6 of the workflow - address matching and final processing.

    🔄 Current Status:
        ✅ All property JSON files have been validated and are located in the ./data/ directory.
        ⚠️ Now it's time to match property addresses using data from the lee_County_addresses/ folder and finalize processing.   
        🔁 Attempt: {state['retry_count'] + 1} of {state['max_retries']}

    YOUR TASK (Steps 5-6):

    STEP 5: 
        1- Understand the data:
            - Browse a few samples from lee_County_addresses/. Each file contains a list of all possible addresses for a property, look for samples that have multiple addresses like the list is more than 1 addresss, and their crossponding html understand them well.
        2-For each location.json inside data/the_property_id/:
            - Read the folder name to determine the property_id.
            - Load the corresponding file lee_County_addresses/the_property_id.json, which contains a list of possible address candidates (in structured JSON format).
            - Extract the reference address from the corresponding HTML file under html/the_property_id.html and find what is the the Street Number/ Street Name/Unit/City/Zip of the property.
            - Compare the HTML address with the list of addresses in lee_County_addresses/the_property_id.json to find the best match.
            - You may use fuzzy matching or rule-based comparison (e.g., match on number, street, and unit).
            - Make sure you have collected the correct Unit ID for the property
        3- Create a script:
            - Name it: scripts/address_extraction.py
            - make a plan before staring writing the address matching script.
            - Purpose:
                - first read the html file , get the property address
                - second read the json file, scan all the address and find the crossponding json object
                - third double check the address you are giving me, does exist in html by matching the address you read from the property_detail 
                - you should be able to extract from street_name: sequence_number, street_name, street_post_directional_text, street_pre_directional_text, street_suffix_type
                - Compare the address extracted from HTML with the list of possible addresses.
                - Determine the best match and make sure you have the correct unit number from the json file that matches the one in html file 
                - If a match is found:
                    - Map the matched address fields into the structure defined by the location.json schema:{state['schemas']['location.json']}
                    - Preserve any pre-existing fields in location.json that are not part of the address, e.g.:latitude, longitude, range, section, township
                    - Replace the current location.json with the new one, ensuring all required fields from the schema are present.
                    - Move the entire folder (data/the_property_id/) to processed/the_property_id/.
        4- For properties where a match was not found:
            - Investigate the failure.
            - Analyze for the failed ones that still in data directory:
                - The address structure in HTML
                - The format of candidate addresses
            - Update and improve the logic in scripts/Address_extraction.py.
            - Re-run the script and reattempt address matching for failed cases.
        5- keep iterating untill all properties are handled and moved to processed/the_property_id/ folder.
         
    CRITICAL: You MUST handle ALL {state['html_files_count']} properties.

    IMPORTANT: All extraction is already done. Focus ONLY on address matching.
    IMPORTANT: Keep doing until no files in data and all in processed
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
            "content": f"use your skills to find what is the possible address for each property, Do not stop until all are handled."
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
            logger.info("✅ Node 3 completed successfully - all addresses handled")
        else:
            state['all_addresses_matched'] = False
            logger.warning("⚠️ Node 3 did not handle all addresses")

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
        state['retry_count'] = 0  # Reset for next node even on failure
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

    # Initialize Gemini model
    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY environment variable is not set!")
        logger.error("Please set your Google API key: export GOOGLE_API_KEY='your-api-key-here'")
        return

    model = ChatGoogleGenerativeAI(
        model=MODEL_NAME,
        temperature=TEMPERATURE,
        google_api_key=GOOGLE_API_KEY,
        convert_system_message_to_human=True,  # This is important for Gemini
    )

    logger.info(f"Initialized Gemini model: {MODEL_NAME}")

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
        logger.info("Starting two-node workflow execution with retry logic using Gemini")
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