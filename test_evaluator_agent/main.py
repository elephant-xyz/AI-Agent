import asyncio
import os
import sys
import json
import logging
import requests
import time
import hashlib
import shutil
import subprocess
from typing import Dict, Any, List, TypedDict, Set
import pandas as pd

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import InMemorySaver
from langchain.chat_models import init_chat_model
from langchain_mcp_adapters.client import (
    MultiServerMCPClient,
    StdioConnection,
)
from langgraph.prebuilt import create_react_agent

logger = logging.getLogger(__name__)

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
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
INPUT_DIR = os.path.join(BASE_DIR, "input")
#
# # IPFS CIDs for schemas
SCHEMA_CIDS = {
    "person.json": "bafkreiglumouewubfiwflhxgainthalfjpafv3pkce2apao26igtou4u5m",
    "company.json": "bafkreid5ohgd6cwgazimlnb3k4lafch7exv3rkyacwzswpds6mapdyx3qe",
    "property.json": "bafkreig7xi5bdnzi6j2ckljwzw5nfgn7mehxtia6okyckfxhrtqlmaifbq",
    "address.json": "bafkreie24md3coljlgdyfdrydb6ajul3zxkvmd7gvvp65bcp3pcclb2tne",
    "tax.json": "bafkreiaqd72dpmmtmfazbl64ccbwctmtyl4xdsijogm4p7wcnq6pnkklb4",
    "lot.json": "bafkreigclcied2zfs4exxtuyjeu2t3oxakzwpt2lehtidd5sctlpdmqebq",
    "sales.json": "bafkreidcudd5plas3el2iks6x224lzf3f2pi662zxaw3rx3kfdyoeaf4lq",
    "layout.json": "bafkreigdotg455zgmtn7rmxcdjoaiiihetzffydkqjeac7d373wp2yyqla",
    "flood_storm_information.json": "bafkreib5sckeesr35igexfsql3cnw7umlfmb4pl64mn22yfkpd56l2njiq",
}


class WorkflowState(TypedDict):
    """State shared between nodes"""
    input_files: List[str]
    input_files_count: int
    schemas: Dict[str, Any]
    stub_files: Dict[str, Any]
    extraction_complete: bool
    address_matching_complete: bool
    owner_analysis_complete: bool
    validation_errors: List[str]
    processed_properties: List[str]
    current_node: str
    tools: List[Any]
    model: Any
    retry_count: int
    max_retries: int
    all_files_processed: bool
    all_addresses_matched: bool
    error_history: List[str]  # Track recent errors
    consecutive_same_errors: int  # Count of same errors in a row
    last_error_hash: str  # Hash of last error for comparison
    generation_restart_count: int  # Track how many times we've restarted
    max_generation_restarts: int  # Limit restarts to prevent infinite loops
    agent_timeout_seconds: int  # Timeout for agent operations
    last_agent_activity: float


def update_agent_activity(state: WorkflowState):
    """Update the last agent activity timestamp"""
    state['last_agent_activity'] = time.time()


def is_agent_frozen(state: WorkflowState) -> bool:
    """Check if agent has been inactive for too long"""
    if 'last_agent_activity' not in state or state['last_agent_activity'] == 0:
        return False

    elapsed = time.time() - state['last_agent_activity']
    return elapsed > state['agent_timeout_seconds']


def should_restart_due_to_timeout(state: WorkflowState) -> bool:
    """Check if we should restart due to agent timeout"""
    return (is_agent_frozen(state) and
            state['generation_restart_count'] < state['max_generation_restarts'])


class OwnerAnalysisAgent:
    """Agent responsible for extracting and analyzing owner names from input files"""

    def __init__(self, state: WorkflowState, model, tools):
        self.state = state
        self.model = model
        self.tools = tools
        self.max_conversation_turns = 10
        self.checkpointer = InMemorySaver()  # Own independent checkpointer
        self.thread_id = "owner-analysis-independent-1"  # Own independent thread

    async def run_owner_analysis(self) -> WorkflowState:
        """Run owner analysis and schema generation"""

        logger.info("🔄 Starting Owner Analysis Agent")
        logger.info(f"💬 Analyzing owners from {self.state['input_files_count']} input files")

        # Create owner analysis agent
        owner_agent = await self._create_owner_analysis_agent()

        conversation_turn = 0
        analysis_complete = False

        # Start the analysis
        logger.info("🤖 Owner Analysis Agent starts...")

        try:
            await self._agent_speak(
                agent=owner_agent,
                agent_name="OWNER_ANALYZER",
                turn=1,
                user_instruction="""Start by analyzing all owner names from input files and generate owner schema.

CRITICAL ISSUE: Previously, the extraction resulted in all null owner names. This means the extraction script didn't understand the input file structure.

YOU MUST:
1. FIRST examine the input files to understand their exact structure
2. Look for where ownerName1 and ownerName2 are located (could be nested in JSON or embedded in HTML)
3. Create extraction script that correctly finds and extracts the owner data
4. VERIFY that extracted data contains actual names, not nulls

REQUIRED OUTPUT FILES:
1. owners/owners_extracted.json (raw extracted owner data - must contain actual names, not nulls)
2. owners/owners_schema.json (analyzed and structured owner data)

DO NOT just talk about creating them - ACTUALLY CREATE THEM using your tools.
Execute your scripts and verify the files contain real owner data before finishing."""
            )

            # Check if files were actually created
            owners_schema_path = os.path.join(BASE_DIR, "owners", "owners_schema.json")
            owners_extracted_path = os.path.join(BASE_DIR, "owners", "owners_extracted.json")

            if os.path.exists(owners_schema_path) and os.path.exists(owners_extracted_path):
                analysis_complete = True
                logger.info("📁 Verified: Both required files were created successfully")
            else:
                analysis_complete = False
                missing = []
                if not os.path.exists(owners_schema_path):
                    missing.append("owners_schema.json")
                if not os.path.exists(owners_extracted_path):
                    missing.append("owners_extracted.json")
                logger.error(f"❌ Missing required files: {', '.join(missing)}")

        except Exception as e:
            logger.error(f"Error in owner analysis: {e}")
            analysis_complete = False

        if analysis_complete:
            logger.info("✅ Owner Analysis completed successfully")
            self.state['owner_analysis_complete'] = True
        else:
            logger.warning("⚠️ Owner Analysis completed with issues")
            self.state['owner_analysis_complete'] = False

        return self.state

    async def _create_owner_analysis_agent(self):
        """Create Owner Analysis agent"""

        owner_analysis_prompt = f"""
        You are the OWNER ANALYSIS SPECIALIST responsible for extracting and analyzing owner names from property input files.

        🎯 YOUR MISSION: 
        1. Extract ALL owner names from input files
        2. Analyze and categorize them (Person vs Company)
        3. Generate proper schema structure for each type

        📂 INPUT STRUCTURE:
        - Input files are located in ./input/ directory ({self.state['input_files_count']} files)
        - Files can be .html or .json format
        - understand the structure of owner names and how they are stored
        - Some properties have only ownerName1, others have both ownerName1 and ownerName2
        - YOU MUST EXAMINE the actual input files to understand their structure first
        - Owner data might be nested within JSON objects or embedded in HTML

        🔧 YOUR TASKS:

        PHASE 0: UNDERSTAND INPUT STRUCTURE (CRITICAL)
        1. First, examine 3-5 sample input files to understand:
           - Are they JSON or HTML format?
           - What is the exact structure?
           - Where exactly are ownerName1 and ownerName2 located?
           - Are they at root level or nested inside objects?
           - Print sample data to understand the format

        PHASE 1: EXTRACTION
        1. CREATE a script: scripts/owner_extractor.py
        2. The script must:
           - Read ALL files from ./input/ directory
           - Handle both JSON and HTML input formats correctly
           - Extract ownerName1 and ownerName2 from each file (find the correct path/location)
           - Handle nested JSON structures if needed
           - Parse HTML content if input files are HTML
           - Save extracted data to: owners/owners_extracted.json
           - ENSURE owner names are NOT null - if extraction fails, debug and fix
        3. Execute the extraction script and verify it extracts actual owner names

        PHASE 2: ANALYSIS & CATEGORIZATION
        1. CREATE a script: scripts/owner_analyzer.py  
        2. The script must:
           - Read owners/owners_extracted.json
           - Analyze each owner name to determine if it's a Person or Company
           - Parse person names into: first_name, last_name, middle_name
           - Identify company names (look for: Inc, LLC, Ltd, Foundation, Alliance, Solutions, Services, etc.)
           - Generate structured data following person/company schemas

        🏢 COMPANY DETECTION RULES:
        Detect companies by these indicators:
        - Legal suffixes: Inc, LLC, Ltd, Corp, Co
        - Nonprofits: Foundation, Alliance, Rescue, Mission
        - Services: Solutions, Services, Systems, Council
        - Military/Emergency: Veterans, First Responders, Heroes
        - Organizations: Initiative, Association, Group


        📋 OUTPUT STRUCTURE:
        Generate: owners/owners_schema.json with this structure:
        ```json
        {{
          "property_[id]": {{
            "owners": [
              {{
                "type": "person",
                "first_name": "Jason",
                "last_name": "Tomaszewski",
                "middle_name": null
              }},
              {{
                "type": "person", 
                "first_name": "Miryam",
                "last_name": "Greene-Tomaszewski",
                "middle_name": null
              }}
            ]
          }},
          "property_[id2]": {{
            "owners": [
              {{
                "type": "person",
                "first_name": "Thomas",
                "last_name": "Walker", 
                "middle_name": "W"
              }}
            ]
          }},
          "property_[id3]": {{
            "owners": [
              {{
                "type": "company",
                "name": "First Responders Foundation"
              }}
            ]
          }}
        }}
        ```

        ⚠️ CRITICAL RULES:
        - FIRST examine input file structure before writing extraction code
        - Process ALL {self.state['input_files_count']} input files
        - Handle missing ownerName2 gracefully (can be empty/null, but ownerName1 should exist)
        - If all owner names are null, the extraction script has a bug - FIX IT
        - Properly detect company vs person names
        - Convert names to proper case (not ALL CAPS)
        - Handle special characters and hyphens correctly
        - Generate clean, structured output, not include & in person name
        - YOU MUST CREATE BOTH FILES: owners_extracted.json AND owners_schema.json
        - DO NOT FINISH until both files exist and contain actual owner data (not all nulls)

        🚀 START: Begin by examining input file structure, then create extraction script, then analysis script, then execute both and VERIFY files contain real data.
        """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=owner_analysis_prompt,
            checkpointer=self.checkpointer  # Use independent checkpointer
        )

    async def _agent_speak(self, agent, agent_name: str, turn: int, user_instruction: str) -> str:
        """Have an agent speak in the conversation"""

        update_agent_activity(self.state)

        logger.info(f"     🗣️ {agent_name} speaking (Turn {turn})...")

        config = {
            "configurable": {"thread_id": self.thread_id},  # Use independent thread
            "recursion_limit": 100
        }

        messages = [{
            "role": "user",
            "content": user_instruction
        }]

        agent_response = ""
        tool_calls_made = []

        try:
            timeout_seconds = self.state['agent_timeout_seconds']
            last_activity = time.time()

            async def check_inactivity():
                nonlocal last_activity
                while True:
                    await asyncio.sleep(10)
                    current_time = time.time()
                    if current_time - last_activity > timeout_seconds:
                        logger.error(f"     ⏰ {agent_name} INACTIVITY TIMEOUT after {timeout_seconds} seconds")
                        raise asyncio.TimeoutError(f"{agent_name} inactive for {timeout_seconds} seconds")

            inactivity_task = asyncio.create_task(check_inactivity())

            try:
                async for event in agent.astream_events({"messages": messages}, config, version="v1"):
                    last_activity = time.time()
                    update_agent_activity(self.state)

                    kind = event["event"]

                    if kind == "on_tool_start":
                        tool_name = event['name']
                        tool_output = event['data'].get('output', '')
                        logger.info(f"       🔧 {agent_name} using tool: {tool_name}")
                        logger.info(f"       📝 Tool input: {str(tool_output)[:150]}...")
                        tool_calls_made.append(tool_name)

                    elif kind == "on_tool_end":
                        tool_name = event['name']
                        tool_output = event['data'].get('output', '')
                        success_indicator = "✅" if "error" not in str(tool_output).lower() else "❌"
                        logger.info(f"       {success_indicator} {agent_name} tool {tool_name} completed")

                    elif kind == "on_chain_end":
                        output = event['data'].get('output', '')
                        if isinstance(output, dict) and 'messages' in output:
                            last_message = output['messages'][-1] if output['messages'] else None
                            if last_message and hasattr(last_message, 'content'):
                                agent_response = last_message.content

            finally:
                inactivity_task.cancel()
                try:
                    await inactivity_task
                except asyncio.CancelledError:
                    pass

            logger.info(f"     ✅ {agent_name} finished speaking")
            return agent_response or f"{agent_name} completed turn {turn}"

        except asyncio.TimeoutError:
            logger.error(f"     ⏰ {agent_name} INACTIVITY TIMEOUT")
            raise Exception(f"{agent_name} timed out - restarting")
        except Exception as e:
            logger.error(f"     ❌ {agent_name} error: {str(e)}")
            return f"{agent_name} error on turn {turn}: {str(e)}"


class AddressMatchingGeneratorEvaluatorPair:
    """Generator and Evaluator for address matching node with validation"""

    def __init__(self, state: WorkflowState, model, tools, schemas: Dict[str, Any]):
        self.state = state
        self.model = model
        self.tools = tools
        self.schemas = schemas
        self.max_conversation_turns = 15  # Fewer turns than extraction
        self.shared_checkpointer = InMemorySaver()  # Shared between all agents
        self.shared_thread_id = "address-matching-conversation-1"  # Same thread for all
        self.consecutive_script_failures = 0
        self.max_script_failures = 3

    async def _restart_generation_process(self) -> WorkflowState:
        """Restart the generation process with a fresh thread"""
        logger.info("🔄 RESTARTING ADDRESS MATCHING PROCESS - Creating new thread and agents")

        cleanup_directories()
        self.state['last_agent_activity'] = 0
        self.state['generation_restart_count'] += 1

        # Create new thread ID for fresh start
        self.shared_thread_id = f"address-matching-conversation-restart-{self.state['generation_restart_count']}"
        self.shared_checkpointer = InMemorySaver()

        # Reset conversation state
        self.max_conversation_turns = 15

        logger.info(f"🆕 Starting fresh address matching attempt #{self.state['generation_restart_count']}")
        logger.info(f"🆕 New thread ID: {self.shared_thread_id}")

        return await self.run_feedback_loop()

    async def run_feedback_loop(self) -> WorkflowState:
        """Run Address Matching Generator + Evaluator CONVERSATION (No CLI validation)"""

        logger.info("🔄 Starting Address Matching Generator + Evaluator CONVERSATION")
        logger.info(f"💬 Using shared thread: {self.shared_thread_id}")
        logger.info(f"🎭 Two agents: Generator, Evaluator")

        # Create agents
        generator_agent = await self._create_address_generator_agent()
        evaluator_agent = await self._create_address_evaluator_agent()

        conversation_turn = 0
        evaluator_accepted = False

        # GENERATOR STARTS: Create initial script
        logger.info("🤖 Address Generator starts the conversation...")

        await self._agent_speak(
            agent=generator_agent,
            agent_name="ADDRESS_GENERATOR",
            turn=1,
            user_instruction="Start by creating the address matching script and processing all input files"
        )

        # Continue conversation until evaluator accepts or max turns
        while conversation_turn < self.max_conversation_turns:
            conversation_turn += 1

            if hasattr(self, 'force_restart_now') and self.force_restart_now:
                logger.warning("🔄 Address matching script failure restart triggered - restarting now")
                self.force_restart_now = False
                return await self._restart_generation_process()

            if should_restart_due_to_timeout(self.state):
                logger.warning("⏰ Agent timeout detected - restarting address matching process")
                return await self._restart_generation_process()

            logger.info(f"💬 Address Matching Turn {conversation_turn}/{self.max_conversation_turns}")

            # EVALUATOR RESPONDS
            logger.info("📊 Address Evaluator reviews Generator's work...")
            try:
                evaluator_message = await self._agent_speak(
                    agent=evaluator_agent,
                    agent_name="ADDRESS_EVALUATOR",
                    turn=conversation_turn,
                    user_instruction="Review and evaluate the Address Generator's matching work and validate address completeness by comparing with sample input files. Check if addresses_mapping.json is properly created with correct address components.",
                )
                # Add debug logging here:
                logger.info(f"🔍 DEBUG: Full evaluator response:")
                logger.info(f"📝 {evaluator_message}")

            except Exception as e:
                if "timed out" in str(e):
                    logger.warning("⏰ Agent timeout during ADDRESS_EVALUATOR - restarting")
                    return await self._restart_generation_process()
                raise

            evaluator_accepted = "STATUS: ACCEPTED" in evaluator_message
            logger.info(f"📊 Address Evaluator decision: {'ACCEPTED' if evaluator_accepted else 'NEEDS FIXES'}")

            # Check if evaluator accepted
            if evaluator_accepted:
                logger.info("✅ Address matching conversation completed successfully - Evaluator approved!")
                self.state['address_matching_complete'] = True
                self.state['all_addresses_matched'] = True
                break

            # GENERATOR RESPONDS: Sees feedback from evaluator and fixes issues
            logger.info("🤖 Address Generator responds to evaluator's feedback...")

            await self._agent_speak(
                agent=generator_agent,
                agent_name="ADDRESS_GENERATOR",
                turn=conversation_turn + 1,
                user_instruction=f"""IMMEDIATE ACTION REQUIRED,
                SILENTLY Fix the address matching issues found by the evaluator immediately. Work silently, Don't reply to them, just use your tools to update the address_extraction.py script to Fix these specific issues:\n\n{evaluator_message}

                DONOT REPLY FIX SILENTLY,

                YOU MUST:
                    1. Use the filesystem tools to read/modify the address extraction script
                    2. You MUST understand all the root causes of the errors to update address_extraction.py script to fix the address matching errors
                    3. Fix ALL the specific errors mentioned above
                    4. Test your changes by running the script and make sure you eliminated these errors
                    5. MAKE sure you have eliminated the errors before Quitting

                DO NOT just acknowledge - TAKE ACTION NOW with tools to fix these issues.""")

            logger.info(f"🔄 Address matching turn {conversation_turn} complete, continuing conversation...")

        # Conversation ended
        final_status = "ACCEPTED" if evaluator_accepted else "PARTIAL"

        if final_status != "ACCEPTED":
            logger.warning(
                f"⚠️ Address matching conversation ended without full success after {self.max_conversation_turns} turns")
            logger.warning(f"Evaluator: {'✅' if evaluator_accepted else '❌'}")
            self.state['all_addresses_matched'] = False

        logger.info(f"💬 Address matching conversation completed with status: {final_status}")
        return self.state

    async def _create_address_generator_agent(self):
        """Create Address Generator agent"""
        generator_prompt = f"""
            You are an address matching specialist handling the final phase of property data processing, you job is to create address_extraction.py script after understading your full task:

            🔄 CURRENT STATUS:
                🔁 Attempt: {self.state['retry_count'] + 1} of {self.state['max_retries']}

            🎯 YOUR MISSION: Process ALL properties in ./input/ by writing a script that matches addresses,  scripts/address_extraction.py.

            PHASE 1: ANALYZE THE DATA STRUCTURE
                1. Examine 3-5 samples from possible_addresses/ to understand:
                   - JSON format and field structure
                   - How many address candidates each property has
                   - Address format variations

                2. Check corresponding input files for these 3-5 samples to understand:
                   - Where addresses appear in the input
                   - How to extract: street number, street name, unit, city, zip

                3. Plan your matching strategy before coding

            PHASE 2: BUILD THE MATCHING SCRIPT
                Create: scripts/address_extraction.py

                The script must:
                   - process all property in ./input/
                   - Extract property_parcel_id from input file name
                   - Load possible_addresses/[property_parcel_id].json (candidate addresses)
                   - Parse address from input/[property_parcel_id] files
                   - Extract all address attributes inside ./schemas/address.json following the address schema with a validation function to validate the address against the schema
                   - with a validation function to validate the address against the schema , if not valid, fix the script and rerun
                   - correctly extract plus_four_postal_code
                   - get the county name from seed.csv file

                B. Find the best address match:
                   - Compare input files address with candidates in possible_addresses/
                   - Use exact matching first, then fuzzy matching if needed
                   - Focus on: street number + street name + unit (if applicable)


                C. When match found:
                   - Update addresses_mapping.json with matched address data using schema: ./schemas/address.json 

            📋 OUTPUT STRUCTURE:
            Generate: owners/addresses_mapping.json with this structure: following the address schema: {self.state['schemas']['address.json']}
            ```json
            {{
              "property_[id]": {{
                "address": 
                  {{
                      "city_name": "MARGATE",
                      "postal_code": "33063",
                      "state_code": "FL",
                      "street_name": "18",
                      "street_number": "6955",
                      "street_post_directional_text": null,
                      "street_pre_directional_text": "NW",
                      "street_suffix_type": "ST",
                      "unit_identifier": null,
                      "latitude": 1234,
                      "longitude": 1234,
                      ...
                  }}

              }},

              "property_[id2]": {{
                "address": 
                  {{
                      "city_name": "MARGATE",
                      "postal_code": "33063",
                      "state_code": "FL",
                      "street_name": "24 COURT",
                      "street_number": "6731",
                      "street_post_directional_text": null,
                      "street_pre_directional_text": "NW",
                      "street_suffix_type": "ST",
                      "unit_identifier": null,
                      "latitude": 1234,
                      "longitude": 1234,
                  }}
              }}
            }}
            ```

            SUCCESS CRITERIA:
                ✅ All addresses_mapping.json files have proper address data with N number of properties in input/ dir

            START with data analysis, then build the script, then execute until complete.
            """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=generator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _create_address_evaluator_agent(self):
        """Create Address Evaluator agent"""
        evaluator_prompt = f"""
        You are the ADDRESS EVALUATOR in a multi-agent address matching pipeline.

        🎯 YOUR TASK:
        You are responsible for **address matching completeness and accuracy validation ONLY**. You must validate that the addresses_mapping.json file contains properly matched addresses from the input files.

        🔍 YOUR VALIDATION FLOW:

        STEP 1: CREATE VALIDATION SCRIPT
        First, create a validation script: scripts/address_validation.py that:
        - Reads owners/addresses_mapping.json
        - Validates each address against the address schema: {self.state['schemas']['address.json']}
        - Checks for required fields, data types, and format compliance
        - Reports validation errors, missing data, or schema violations
        - Prints detailed validation results

        STEP 2: EXECUTE VALIDATION SCRIPT
        Run the validation script and analyze the results, ALL ADDRESSES MUST BE VALID AGAINST THE SCHEMA.

        STEP 3: MANUAL VALIDATION
        For a **sample of 3–5 properties**:
        1. **Check if owners/addresses_mapping.json exists** and has content
        2. you MUST **Verify address components** are properly extracted, everysingle attribute must be verified that it exists:
          Checklist of address components to verify:
           - street_number
           - street_name 
           - unit_identifier
           - city_name
           - postal_code
           - state_code
           - street_pre_directional_text
           - street_post_directional_text
           - street_suffix_type
           - latitude/longitude if available
        3. **Compare with input files** to ensure addresses match the source data
        4. **Verify matching logic** worked correctly by checking a few examples

        ✅ VALIDATION CHECKLIST:
        1. addresses_mapping.json file exists in owners/ directory
        2. File contains address data for all properties in input/ directory
        3. Address components are properly extracted and not null/empty where data exists
        4. Addresses match the format specified in the address schema
        5. No placeholder or TODO values in the address data
        6. Address matching logic correctly identified the best match from possible_addresses/
        7. Schema validation script runs successfully and reports no errors
        8.  ALL ADDRESSES MUST BE VALID AGAINST THE SCHEMA.

        📝 RESPONSE FORMAT:
        Start your response with one of the following:
        STATUS: ACCEPTED
        or
        STATUS: REJECTED

        Then explain only what is necessary:
        - Missing addresses_mapping.json file: <Only if missing>
        - Validation script results: <Include the output from address_validation.py>
        - Incomplete address data: <Only if incomplete>
        - Address component issues: <Only if found>
        - Schema compliance issues: <Only if found>
        - Matching logic problems: <Only if found>

        🗣️ CONVERSATION RULES:
        - You must FIRST create and run the validation script
        - Then perform manual validation on sample data
        - Only care about address matching completeness and accuracy
        - Be strict. Accept only if ALL criteria are clearly met
        - Always include the validation script output in your response

        🚀 START: Create validation script, run it, then check address matching completeness and return your evaluation.
        """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=evaluator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _agent_speak(self, agent, agent_name: str, turn: int, user_instruction: str) -> str:
        """Have an agent speak in the conversation - they see all previous messages"""
        # Copy the exact same implementation from ExtractionGeneratorEvaluatorPair._agent_speak
        # This is identical to the extraction version

        update_agent_activity(self.state)
        logger.info(f"     🗣️ {agent_name} speaking (Turn {turn})...")
        logger.info(f"     👀 {agent_name} using shared checkpointer memory")

        config = {
            "configurable": {"thread_id": self.shared_thread_id},
            "recursion_limit": 100
        }

        messages = [{
            "role": "user",
            "content": user_instruction
        }]

        logger.info(f"     📖 {agent_name} using checkpointer memory...")

        agent_response = ""
        tool_calls_made = []

        try:
            timeout_seconds = self.state['agent_timeout_seconds']
            last_activity = time.time()

            async def check_inactivity():
                nonlocal last_activity
                while True:
                    await asyncio.sleep(10)
                    current_time = time.time()
                    if current_time - last_activity > timeout_seconds:
                        logger.error(f"     ⏰ {agent_name} INACTIVITY TIMEOUT after {timeout_seconds} seconds")
                        raise asyncio.TimeoutError(f"{agent_name} inactive for {timeout_seconds} seconds")

            inactivity_task = asyncio.create_task(check_inactivity())

            try:
                async for event in agent.astream_events({"messages": messages}, config, version="v1"):
                    last_activity = time.time()
                    update_agent_activity(self.state)

                    kind = event["event"]

                    if kind == "on_chain_start":
                        logger.info(f"       🔗 {agent_name} chain starting: {event.get('name', 'unknown')}")

                    elif kind == "on_llm_start":
                        logger.info(f"       🧠 {agent_name} thinking...")

                    elif kind == "on_llm_end":
                        llm_output = event['data'].get('output', '')
                        if hasattr(llm_output, 'content'):
                            content = llm_output.content[:200] + "..." if len(
                                llm_output.content) > 200 else llm_output.content
                            logger.info(f"       💭 {agent_name} decided: {content}")

                    elif kind == "on_tool_start":
                        tool_name = event['name']
                        tool_input = event['data'].get('input', {})
                        logger.info(f"       🔧 {agent_name} using tool: {tool_name}")
                        logger.info(f"       📝 Tool input: {str(tool_input)[:150]}...")
                        tool_calls_made.append(tool_name)

                    elif kind == "on_tool_end":
                        tool_name = event['name']
                        tool_output = event['data'].get('output', '')
                        success_indicator = "✅" if "error" not in str(tool_output).lower() else "❌"
                        if tool_name == "execute_code_file":
                            if "error" in str(tool_output).lower():
                                self.consecutive_script_failures += 1
                                logger.warning(
                                    f"       ❌ Script execution failed ({self.consecutive_script_failures}/{self.max_script_failures})")

                                if self.consecutive_script_failures >= self.max_script_failures:
                                    logger.error(
                                        f"       🔄 Script failed {self.max_script_failures} times - triggering restart")
                                    self.force_restart_now = True
                                    return "RESTART_TRIGGERED"
                            else:
                                self.consecutive_script_failures = 0
                                logger.info(f"       ✅ Script executed successfully - reset failure counter")

                        logger.info(f"       {success_indicator} {agent_name} tool {tool_name} completed")
                        logger.info(f"       📤 Result: {str(tool_output)[:100]}...")

                    elif kind == "on_chain_end":
                        chain_name = event.get('name', 'unknown')
                        output = event['data'].get('output', '')
                        logger.info(f"       🎯 {agent_name} chain completed: {chain_name}")

                        if isinstance(output, dict) and 'messages' in output:
                            last_message = output['messages'][-1] if output['messages'] else None
                            if last_message and hasattr(last_message, 'content'):
                                agent_response = last_message.content
                        elif hasattr(output, 'content'):
                            agent_response = output.content
                        elif isinstance(output, str):
                            agent_response = output

            finally:
                inactivity_task.cancel()
                try:
                    await inactivity_task
                except asyncio.CancelledError:
                    pass

            logger.info(f"     ✅ {agent_name} finished speaking")
            logger.info(f"     🔧 Tools used: {', '.join(tool_calls_made) if tool_calls_made else 'None'}")
            logger.info(f"     📄 Response length: {len(agent_response)} characters")

            return agent_response or f"{agent_name} completed turn {turn} (no response captured)"

        except asyncio.TimeoutError:
            logger.error(f"     ⏰ {agent_name} INACTIVITY TIMEOUT after {timeout_seconds} seconds")
            logger.error(f"     🔄 This will trigger a restart of the address matching process")
            cleanup_directories()
            raise Exception(
                f"{agent_name} timed out after {timeout_seconds} seconds of inactivity - restarting address matching")
        except Exception as e:
            logger.error(f"     ❌ {agent_name} error: {str(e)}")
            return f"{agent_name} error on turn {turn}: {str(e)}"


class ExtractionGeneratorEvaluatorPair:
    """Generator and TWO Evaluators for extraction node with schema and data validation"""

    def __init__(self, state: WorkflowState, model, tools, schemas: Dict[str, Any]):
        self.state = state
        self.model = model
        self.tools = tools
        self.schemas = schemas
        self.max_conversation_turns = 20  # More turns for 3 agents
        self.shared_checkpointer = InMemorySaver()  # Shared between all agents
        self.shared_thread_id = "extraction-conversation-1"  # Same thread for all
        self.consecutive_script_failures = 0
        self.max_script_failures = 3

    def canonicalize_cli_errors(self, cli_errors: str) -> str:
        """Simple canonicalization: extract file paths and normalize them"""

        # Parse the CLI errors to extract file paths
        error_files = set()

        # Check if submit_errors.csv exists (the actual CLI error format)
        submit_errors_path = os.path.join(BASE_DIR, "submit_errors.csv")

        if os.path.exists(submit_errors_path):
            try:
                df = pd.read_csv(submit_errors_path)
                for _, row in df.iterrows():
                    file_path = row['file_path']
                    # Extract just the property folder name from the path
                    # e.g., "submit/property_123/property.json" -> "property_123"
                    parts = file_path.split('/')
                    if len(parts) >= 2:
                        property_folder = parts[-2]  # Get the folder name
                        error_files.add(property_folder)
            except Exception as e:
                logger.warning(f"Could not parse submit_errors.csv: {e}")

        # If no CSV, try to extract from error text
        if not error_files:
            lines = cli_errors.split('\n')
            for line in lines:
                if 'File:' in line:
                    file_path = line.replace('File:', '').strip()
                    # Extract property folder from path
                    parts = file_path.split('/')
                    if len(parts) >= 2:
                        property_folder = parts[-2]
                        error_files.add(property_folder)

        # Sort the property folders for consistent ordering
        canonical_errors = sorted(list(error_files))

        # Create simple canonical string
        canonical_string = json.dumps(canonical_errors, sort_keys=True)

        # Return hash
        return hashlib.md5(canonical_string.encode()).hexdigest()

    def _should_restart_generation(self, current_error_details: str) -> bool:
        """Check if we should restart based on canonicalized file paths"""

        # Get canonical hash of current errors
        current_error_hash = self.canonicalize_cli_errors(current_error_details)

        # Check if it's the same as last error
        if self.state['last_error_hash'] == current_error_hash:
            self.state['consecutive_same_errors'] += 1
        else:
            self.state['consecutive_same_errors'] = 1
            self.state['last_error_hash'] = current_error_hash

        # Log for debugging
        logger.info(f"🔍 Canonical error hash: {current_error_hash}")
        logger.info(f"🔢 Consecutive same errors: {self.state['consecutive_same_errors']}")

        # Restart after 3 consecutive same errors
        if (self.state['consecutive_same_errors'] > 4 and
                self.state['generation_restart_count'] < self.state['max_generation_restarts']):
            logger.warning(f"🔄 Same file path errors detected {self.state['consecutive_same_errors']} times")
            return True

        return False

    def _update_error_tracking(self, error_hash: str, error_details: str):
        """Update error tracking state"""
        if self.state['last_error_hash'] == error_hash:
            self.state['consecutive_same_errors'] += 1
        else:
            self.state['consecutive_same_errors'] = 1
            self.state['last_error_hash'] = error_hash

        # Keep history of recent errors (last 10)
        self.state['error_history'].append(error_details)
        if len(self.state['error_history']) > 10:
            self.state['error_history'] = self.state['error_history'][-10:]

    async def _restart_generation_process(self) -> WorkflowState:
        """Restart the generation process with a fresh thread"""
        logger.info("🔄 RESTARTING GENERATION PROCESS - Creating new thread and agents")

        cleanup_directories()
        self.state['last_agent_activity'] = 0
        # Increment restart counter
        self.state['generation_restart_count'] += 1

        # Create new thread ID for fresh start
        self.shared_thread_id = f"extraction-conversation-restart-{self.state['generation_restart_count']}"
        self.shared_checkpointer = InMemorySaver()  # Fresh checkpointer

        # Reset conversation state
        self.max_conversation_turns = 20  # Reset turn counter

        # Reset error tracking for new attempt
        self.state['consecutive_same_errors'] = 0
        self.state['last_error_hash'] = ""

        logger.info(f"🆕 Starting fresh generation attempt #{self.state['generation_restart_count']}")
        logger.info(f"🆕 New thread ID: {self.shared_thread_id}")

        # Start the conversation loop again from the beginning
        return await self.run_feedback_loop()

    async def run_feedback_loop(self) -> WorkflowState:
        """Run FOUR AGENTS: Generator + Schema Evaluator + Data Evaluator + CLI Validator"""

        logger.info("🔄 Starting Generator + Schema Evaluator + Data Evaluator + CLI Validator CONVERSATION")
        logger.info(f"💬 Using shared thread: {self.shared_thread_id}")
        logger.info(f"🎭 Four agents: Generator, Schema Evaluator, Data Evaluator, CLI Validator")

        # Create THREE separate LLM agents
        generator_agent = await self._create_generator_agent()
        # schema_evaluator_agent = await self._create_schema_evaluator_agent()
        data_evaluator_agent = await self._create_data_evaluator_agent()

        conversation_turn = 0
        data_accepted = False
        cli_accepted = False

        # GENERATOR STARTS: Create initial script
        logger.info("🤖 Generator starts the conversation...")

        await self._agent_speak(
            agent=generator_agent,
            agent_name="GENERATOR",
            turn=1,
            user_instruction="Start by creating the extraction script and processing all input files, Make sure to extract all sales-taxes-layouts data  "
        )

        # Continue conversation until all evaluators accept or max turns
        while conversation_turn < self.max_conversation_turns:
            conversation_turn += 1

            if hasattr(self, 'force_restart_now') and self.force_restart_now:
                logger.warning("🔄 Script failure restart triggered - restarting now")
                self.force_restart_now = False  # Reset flag
                return await self._restart_generation_process()

            if should_restart_due_to_timeout(self.state):
                logger.warning("⏰ Agent timeout detected - restarting generation process")
                return await self._restart_generation_process()

            logger.info(f"💬 Conversation Turn {conversation_turn}/{self.max_conversation_turns}")

            # DATA EVALUATOR RESPONDS
            logger.info("📊 Data Evaluator reviews Generator's work...")
            try:
                # Your existing agent calls with timeout protection
                data_message = await self._agent_speak(
                    agent=data_evaluator_agent,
                    agent_name="DATA_EVALUATOR",
                    turn=conversation_turn,
                    user_instruction="Review and evaluate the Generator's extraction work all over Again even if you already accepted it in previous run and validate data completeness by comparing with sample input files and make sure validation points are met, pick 3 different samples to compare, if you already accepted, check again for any new issues that might have been introduced by the generator",
                )
            except Exception as e:
                if "timed out" in str(e):
                    logger.warning("⏰ Agent timeout during DATA_EVALUATOR - restarting")
                    return await self._restart_generation_process()
                raise

            data_accepted = "STATUS: ACCEPTED" in data_message
            logger.info(f"📊 Data Evaluator decision: {'ACCEPTED' if data_accepted else 'NEEDS FIXES'}")

            # CLI VALIDATOR RUNS (non-LLM function)
            logger.info("⚡ CLI Validator running validation...")
            cli_success, cli_errors, _ = run_cli_validator("data")  # Note: 3 return values now

            if cli_success:
                cli_message = "STATUS: ACCEPTED - CLI validation passed successfully"
                cli_accepted = True
                self.state['consecutive_same_errors'] = 0
                self.state['last_error_hash'] = ""
                logger.info("✅ CLI Validator decision: ACCEPTED")
            else:
                cli_message = f"STATUS: REJECTED - CLI validation failed with errors:\n{cli_errors}"
                cli_accepted = False

                # Check if we should restart due to repeated file path errors
                if self._should_restart_generation(cli_errors):
                    logger.warning("🔄 Same file path errors detected 3 times - restarting generation process")
                    return await self._restart_generation_process()

                logger.info("❌ CLI Validator decision: NEEDS FIXES")

            # Check if ALL validators accepted
            if data_accepted and cli_accepted:
                logger.info("✅ Conversation completed successfully - ALL validators approved!")
                self.state['extraction_complete'] = True
                self.state['all_files_processed'] = True
                break

            # GENERATOR RESPONDS: Sees feedback from ALL validators and fixes issues
            logger.info("🤖 Generator responds to all validators' feedback...")

            feedback_summary = ""
            # if not schema_accepted:
            #     feedback_summary += f"Schema Evaluator feedback: {schema_message}\n\n"
            if not data_accepted:
                feedback_summary += f"Data Evaluator feedback: {data_message}\n\n"
            if not cli_accepted:
                feedback_summary += f"CLI Validator feedback: {cli_message}\n\n"

            await self._agent_speak(
                agent=generator_agent,
                agent_name="GENERATOR",
                turn=conversation_turn + 1,
                user_instruction=f"""IMMEDIATE ACTION REQUIRED,"
                SILENTLY Fix the issues found by ALL validators immediately. Work silently, Don't reply to them, just use your tools to update the data_extraction.py script to Fix these specific issues:\n\n{feedback_summary},
                DONOT REPLY FIX SILENTLY,

                YOU MUST:
                    1. Use the filesystem tools to read/modify the extraction script
                    2. You MUST understand all the root causes of the errors to update data_extraction.py script to fix th extraction errors
                    3. Fix ALL the specific errors mentioned above
                    4. run the script 

                DO NOT just acknowledge - TAKE ACTION NOW with tools to fix these issues.""")

            logger.info(f"🔄 Turn {conversation_turn} complete, continuing conversation...")

        # Conversation ended
        final_status = "ACCEPTED" if (data_accepted and cli_accepted) else "PARTIAL"

        if final_status != "ACCEPTED":
            logger.warning(f"⚠️ Conversation ended without full success after {self.max_conversation_turns} turns")
            logger.warning(
                f"Schema: Data: {'✅' if data_accepted else '❌'}, CLI: {'✅' if cli_accepted else '❌'}")
            self.state['all_files_processed'] = False

        logger.info(f"💬 Conversation completed with status: {final_status}")
        return self.state

    async def _create_generator_agent(self):
        """Create Generator agent with YOUR EXACT PROMPT"""

        generator_prompt = f"""
            You are the GENERATOR for input data extraction in a conversation with an EVALUATORS.

            🎯 YOUR MISSION: Process ALL {self.state['input_files_count']} files from ./input/ folder 

            📂 REQUIRED OUTPUT STRUCTURE: this output should be generated through a data_extraction.py script
            if any file don't have data, DO NOT create it, example: if input data don't have flood_storm_information don't create this file
            ./data/[property_parcel_id]/property.json
            ./data/[property_parcel_id]/address.json use owners/addresses_mapping.json in address extraction along with the input file
            ./data/[property_parcel_id]/lot.json
            ./data/[property_parcel_id]/sales_1.json 
            ./data/[property_parcel_id]/tax_1.json
            ./data/[property_parcel_id]/layout_1.json (It represents number of space_type inside the property) you need to know what space_type you have from the schema and apply them  if found in the property data, Layouts for ALL (bedrooms, full and half bathrooms) must be extracted into distinct layout objects. E.g., 2 beds, 1 full, 1 half bath = 4 layout files.)
            ./data/[property_parcel_id]/flood_storm_information.json
            ./data/[property_parcel_id]/person.json   or ./data/[property_parcel_id]/company.json extract person/company data from owners/owners_schema.json file
            and if multiple persons you should have 
            ./data/[property_parcel_id]/person_1.json
            ./data/[property_parcel_id]/person_2.json
            same for company, if there is multiple persons or companies, create multiple files with suffixes.

            ⚠️ Generator should detect and extract address components and set them correctly in address class:
                - street_number
                - street_name
                - unit_identifier
                - plus_four_postal_code
                - street_post_directional_text
                - street_pre_directional_text
                - street_suffix_type

            📋 SCHEMAS TO FOLLOW:
            All schemas are available in the ./schemas/ directory. Read each schema file to understand the required structure, you MUST follow the exact structure provided in the schemas.

            🔧 YOUR TASKS:
            1. READ all schema files from ./schemas/ directory to understand data structures
            2. Analyze input structure (examine 3-5 sample files)
            3. Analyze the owners data from owners/owners_schema.json to understand how to extract person/company data
            4. Analyze the address structure in the owners/addresses_mapping.json to use in extraction process
            4. Generate a universal extraction script: `scripts/data_extractor.py` that Map input data to the schemas, and save the extracted data as JSON files in the `data` folder.
            5. The script MUST be execeutable and you MUST NOT QUITE until the script is executed successfully with No errors
            6. Execute the script to process ALL input files
            7. MAKE SURE script is executable and can be run without errors
            8. data could have Either persons or company, but not both, if persons is present then company should be null and vice versa

            ⚠️ CRITICAL RULES:
            - Process 10 input file in ./input/ directory
            - Follow the exact schema structure provided
            - Handle missing data gracefully (use null/empty values)

            🗣️ CONVERSATION RULES:
            - You are having a conversation with TWO VALIDATORS: Data Evaluator, and CLI Validator
            - When ANY validator gives you feedback, read it carefully and work in silent to fix the issues
            - Fix the specific issues they mention in silence

            🚀 START WORKING: Begin with input analysis, then create/execute the extraction script.
            """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=generator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _create_data_evaluator_agent(self):
        """Create Data Evaluator agent - validates data completeness"""

        data_evaluator_prompt = f"""
            You are the DATA EVALUATOR in a multi-agent data extraction pipeline rseponsible for VALIDATION CHECKLIST execution. You work alongside:

            - The GENERATOR: generates the extraction script.
            - The SCHEMA EVALUATOR: validates JSON schema compliance (this is NOT your job).

            🎯 YOUR TASK:
            You are responsible for **data completeness and accuracy validation ONLY**. You must validate that the extracted data in `./data/` matches exactly the content present in the original raw files in `./input/`.

            You must do **side-by-side comparisons** between the original input files and the extracted output files for 3 properties as a sample.

            Do NOT rely solely on output content.
            Instead, for each field/value in the extracted JSON, find and verify it exists in the raw input file for 3 properties as a sample.

            ---

            🔍 YOUR VALIDATION FLOW:

            For a **sample of 3–5 properties**:
            1. **Locate the extracted files in `./data/`** and their **corresponding input sources in `./input/`**.
            2. For each extracted value in data/[property_parcel_id]/**:
               - **Find the matching information in the original input**.
               - **Reject** if any value cannot be verified from the input.

            ✅ VALIDATION CHECKLIST: you must ensure the following criteria are met one by one everytime:
            1. Full data coverage: All information present in the input file must be extracted in county_data_group.json.
            2. Layouts for ALL (bedrooms, full and half bathrooms) must be extracted into distinct layout objects. E.g., 2 beds, 1.5 bathroom = 4 layout files (2 beds, 1 full bathroom, 1 half bathroom).
            3. Layout Must represent exactly the number of space_type inside the property NO MORE, if there is 2 bedrooms, 1.5 bathroom, then you should have 4 layout files ONLY.
            4. Tax history must include ALL years from the input. No years should be missing starting from 2025
            5. Sales history must include ALL years from the input for this PARCEL ID ONLY. No years should be missing, DO NOT INCLUDE SALES OF SUBDIVISON.
            6. MAKE SURE you are getting sales for this parcel ID ONLY, do not include sales of subdivision.
            7. All essential property details must be present (e.g., parcel ID, zoning, square footage, etc.).
            8. Layouts, persons, and sales must be correctly numbered (e.g., layout_1, layout_2, person_1, etc.).
            9. No TODO placeholders allowed in the extraction script. Request fixes immediately.
            10. json files that have all data as null or empty should be removed from the data folder.
            11. Address is correctly extracted with all components:
                - street_number
                - street_name
                - unit_identifier
                - plus_four_postal_code
                - street_post_directional_text
                - street_pre_directional_text
                - street_suffix_type

            📝 RESPONSE FORMAT:
            Start your response with one of the following:
            STATUS: ACCEPTED
            or
            STATUS: REJECTED

            Then explain only what is necessary:
            - Data completeness issues: <Only if incomplete>
            - Missing tax/sales years: <Only if found>
            - Incomplete person/company classification: <Only if found>
            - Missing layout information: <Only if found>
            - Script issues or TODOs: <Only if found>

            🗣️ CONVERSATION RULES:
            - You only care about data completeness and extraction accuracy.
            - You do NOT evaluate schema compliance (that’s the SCHEMA EVALUATOR’s job).
            - You must reference the GENERATOR’s extracted outputs when pointing out problems.
            - Be strict. Accept only if ALL criteria are clearly met.
            - reply only with the STATUS and the issues found, do not reply with anything else, just reply with the issues found, if any.

            🚀 START: Check data completeness and return your evaluation.
            """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=data_evaluator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _agent_speak(self, agent, agent_name: str, turn: int, user_instruction: str) -> str:
        """Have an agent speak in the conversation - they see all previous messages"""

        # Update activity at start
        update_agent_activity(self.state)

        logger.info(f"     🗣️ {agent_name} speaking (Turn {turn})...")
        logger.info(f"     👀 {agent_name} using shared checkpointer memory")

        config = {
            "configurable": {"thread_id": self.shared_thread_id},  # SAME THREAD!
            "recursion_limit": 100
        }

        # Agent sees the FULL conversation history + current instruction
        messages = [{
            "role": "user",
            "content": user_instruction
        }]

        logger.info(f"     📖 {agent_name} using checkpointer memory...")

        agent_response = ""
        tool_calls_made = []

        try:
            # Get timeout from state
            timeout_seconds = self.state['agent_timeout_seconds']

            # Track activity for inactivity timeout
            last_activity = time.time()

            async def check_inactivity():
                """Check for inactivity timeout in background"""
                nonlocal last_activity
                while True:
                    await asyncio.sleep(10)  # Check every 10 seconds
                    current_time = time.time()
                    if current_time - last_activity > timeout_seconds:
                        logger.error(f"     ⏰ {agent_name} INACTIVITY TIMEOUT after {timeout_seconds} seconds")
                        raise asyncio.TimeoutError(f"{agent_name} inactive for {timeout_seconds} seconds")

            # Start the inactivity checker
            inactivity_task = asyncio.create_task(check_inactivity())

            try:
                async for event in agent.astream_events({"messages": messages}, config, version="v1"):
                    # Update activity timestamp on each event
                    last_activity = time.time()
                    update_agent_activity(self.state)

                    kind = event["event"]

                    if kind == "on_chain_start":
                        logger.info(f"       🔗 {agent_name} chain starting: {event.get('name', 'unknown')}")

                    elif kind == "on_llm_start":
                        logger.info(f"       🧠 {agent_name} thinking...")

                    elif kind == "on_llm_end":
                        llm_output = event['data'].get('output', '')
                        if hasattr(llm_output, 'content'):
                            content = llm_output.content[:200] + "..." if len(
                                llm_output.content) > 200 else llm_output.content
                            logger.info(f"       💭 {agent_name} decided: {content}")

                    elif kind == "on_tool_start":
                        tool_name = event['name']
                        tool_input = event['data'].get('input', {})
                        logger.info(f"       🔧 {agent_name} using tool: {tool_name}")
                        logger.info(f"       📝 Tool input: {str(tool_input)[:150]}...")
                        tool_calls_made.append(tool_name)

                    elif kind == "on_tool_end":
                        tool_name = event['name']
                        tool_output = event['data'].get('output', '')
                        success_indicator = "✅" if "error" not in str(tool_output).lower() else "❌"
                        if tool_name == "execute_code_file":
                            if "error" in str(tool_output).lower():
                                self.consecutive_script_failures += 1
                                logger.warning(
                                    f"       ❌ Script execution failed ({self.consecutive_script_failures}/{self.max_script_failures})")

                                # TRIGGER RESTART IF TOO MANY FAILURES
                                if self.consecutive_script_failures >= self.max_script_failures:
                                    logger.error(
                                        f"       🔄 Script failed {self.max_script_failures} times - triggering restart")
                                    # Set flag to trigger restart
                                    self.force_restart_now = True
                                    return "RESTART_TRIGGERED"  # Return early to trigger restart
                            else:
                                # Reset counter on success
                                self.consecutive_script_failures = 0
                                logger.info(f"       ✅ Script executed successfully - reset failure counter")

                        logger.info(f"       {success_indicator} {agent_name} tool {tool_name} completed")
                        logger.info(f"       📤 Result: {str(tool_output)[:100]}...")

                    elif kind == "on_chain_end":
                        chain_name = event.get('name', 'unknown')
                        output = event['data'].get('output', '')
                        logger.info(f"       🎯 {agent_name} chain completed: {chain_name}")

                        # Capture the agent's response
                        if isinstance(output, dict) and 'messages' in output:
                            last_message = output['messages'][-1] if output['messages'] else None
                            if last_message and hasattr(last_message, 'content'):
                                agent_response = last_message.content
                        elif hasattr(output, 'content'):
                            agent_response = output.content
                        elif isinstance(output, str):
                            agent_response = output

            finally:
                # Cancel the inactivity checker
                inactivity_task.cancel()
                try:
                    await inactivity_task
                except asyncio.CancelledError:
                    pass

            logger.info(f"     ✅ {agent_name} finished speaking")
            logger.info(f"     🔧 Tools used: {', '.join(tool_calls_made) if tool_calls_made else 'None'}")
            logger.info(f"     📄 Response length: {len(agent_response)} characters")
            logger.info(f"     🗨️ {agent_name} said: {agent_response}...")

            return agent_response or f"{agent_name} completed turn {turn} (no response captured)"

        except asyncio.TimeoutError:
            logger.error(f"     ⏰ {agent_name} INACTIVITY TIMEOUT after {timeout_seconds} seconds")
            logger.error(f"     🔄 This will trigger a restart of the generation process")
            cleanup_directories()
            raise Exception(
                f"{agent_name} timed out after {timeout_seconds} seconds of inactivity - restarting generation")
        except Exception as e:
            logger.error(f"     ❌ {agent_name} error: {str(e)}")
            return f"{agent_name} error on turn {turn}: {str(e)}"


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


def unescape_http_request(http_request):
    """Decode escaped characters in HTTP request strings"""
    if not http_request:
        return None

    # Replace common escape sequences
    unescaped = http_request.replace('\\r\\n', '\r\n')
    unescaped = unescaped.replace('\\r', '\r')
    unescaped = unescaped.replace('\\n', '\n')
    unescaped = unescaped.replace('\\"', '"')
    unescaped = unescaped.replace('\\\\', '\\')

    return unescaped


def run_cli_validator(data_dir: str = "data") -> tuple[bool, str, str]:
    """
    Run the CLI validation command and return results
    Returns: (success: bool, error_details: str, error_hash: str)
    """
    try:
        logger.info("📁 Creating submit directory and copying data with proper naming...")

        # Define directories
        upload_results_path = os.path.join(BASE_DIR, "upload-results.csv")
        data_dir = os.path.join(BASE_DIR, data_dir)
        submit_dir = os.path.join(BASE_DIR, "submit")
        seed_csv_path = os.path.join(BASE_DIR, "seed.csv")

        # Create/clean submit directory
        if os.path.exists(submit_dir):
            shutil.rmtree(submit_dir)
            logger.info("🗑️ Cleaned existing submit directory")

        os.makedirs(submit_dir, exist_ok=True)
        logger.info(f"📁 Created submit directory: {submit_dir}")

        if not os.path.exists(data_dir):
            logger.error("❌ Data directory not found")
            return False, "Data directory not found", ""

        # Read the uploadresults.csv file for mapping
        folder_mapping = {}
        if os.path.exists(upload_results_path):
            df = pd.read_csv(upload_results_path)
            logger.info(f"📊 Found {len(df)} entries in uploadresults.csv")

            # Create mapping from old folder names to new names (propertyCid)
            for _, row in df.iterrows():
                file_path = row['filePath']
                property_cid = row['propertyCid']

                path_parts = file_path.split('/')
                output_index = -1

                # Find the "output" directory in the path
                for i, part in enumerate(path_parts):
                    if part == "output":
                        output_index = i
                        break

                if output_index != -1 and output_index + 1 < len(path_parts):
                    old_folder_name = path_parts[output_index + 1]
                    if old_folder_name not in folder_mapping:
                        folder_mapping[old_folder_name] = property_cid
                        logger.info(f"   📋 Mapping: {old_folder_name} -> {property_cid}")

            logger.info(f"✅ Created mapping for {len(folder_mapping)} unique folders")
        else:
            logger.warning("⚠️ upload-results.csv not found, using original folder names")

        # Read seed.csv and create mapping
        logger.info("Reading seed.csv for JSON updates...")
        seed_data = {}
        seed_csv_path = os.path.join(BASE_DIR, "seed.csv")

        if os.path.exists(seed_csv_path):
            seed_df = pd.read_csv(seed_csv_path)
            logger.info(f"📊 Found {len(seed_df)} entries in seed.csv")

            # Create mapping from parcel_id (original folder name) to http_request and source_identifier
            for _, row in seed_df.iterrows():
                parcel_id = str(row['parcel_id'])
                method = row.get('method')
                url = row.get('url')
                multiValueQueryString = row.get('multiValueQueryString')
                seed_data[parcel_id] = {
                    "source_http_request": {
                        "method": method,
                        "url": url,
                        "multiValueQueryString": json.loads(multiValueQueryString) if multiValueQueryString else None,
                    },
                    'source_identifier': row['source_identifier']
                }

            logger.info(f"✅ Created seed mapping for {len(seed_data)} parcel IDs")
        else:
            logger.warning("⚠️ seed.csv not found, skipping JSON updates")
            seed_data = {}

        # Copy data to submit directory with proper naming and build relationships
        copied_count = 0
        county_data_group_cid = "bafkreihmkn4iptitc4wtapo4upiwfiznxrjejpulousriynkc3g3kmjcge"

        # NEW: Collect all relationship building errors
        all_relationship_errors = []

        for folder_name in os.listdir(data_dir):
            src_folder_path = os.path.join(data_dir, folder_name)

            if os.path.isdir(src_folder_path):
                # Determine target folder name
                target_folder_name = folder_mapping.get(folder_name, folder_name)
                dst_folder_path = os.path.join(submit_dir, target_folder_name)

                # Copy the entire folder
                shutil.copytree(src_folder_path, dst_folder_path)
                logger.info(f"   📂 Copied folder: {folder_name} -> {target_folder_name}")
                copied_count += 1

                # Update JSON files with seed data (folder_name is the original parcel_id)
                if folder_name in seed_data:
                    updated_files_count = 0
                    for file_name in os.listdir(dst_folder_path):
                        if file_name.endswith('.json'):
                            json_file_path = os.path.join(dst_folder_path, file_name)

                            try:
                                # Read JSON file
                                with open(json_file_path, 'r', encoding='utf-8') as f:
                                    json_data = json.load(f)

                                # Check if this JSON has source_http_request field
                                if 'source_http_request' in json_data:
                                    # Update the JSON data with seed information
                                    json_data['source_http_request'] = seed_data[folder_name]['source_http_request']
                                    json_data['request_identifier'] = str(seed_data[folder_name]['source_identifier'])

                                    # Write back to file
                                    with open(json_file_path, 'w', encoding='utf-8') as f:
                                        json.dump(json_data, f, indent=2, ensure_ascii=False)

                                    updated_files_count += 1

                            except json.JSONDecodeError as e:
                                logger.error(f"   ❌ Error parsing JSON file {json_file_path}: {e}")
                            except Exception as e:
                                logger.error(f"   ❌ Error processing file {json_file_path}: {e}")

                    if updated_files_count > 0:
                        logger.info(
                            f"   🌱 Updated {updated_files_count} JSON files with seed data for parcel {folder_name}")
                else:
                    logger.warning(f"   ⚠️ No seed data found for parcel {folder_name}")

                # Build relationship files dynamically - MODIFIED TO CAPTURE ERRORS
                logger.info(f"   🔗 Building relationship files for {target_folder_name}")
                relationship_files, relationship_errors = build_relationship_files(dst_folder_path)

                # Add any relationship errors to our collection
                if relationship_errors:
                    for error in relationship_errors:
                        all_relationship_errors.append(f"Property {folder_name}: {error}")

                # Only create county data group if no relationship errors
                if not relationship_errors:
                    # Create county data group file with all relationships
                    county_data_group = create_county_data_group(relationship_files)
                    county_file_path = os.path.join(dst_folder_path, f"{county_data_group_cid}.json")

                    with open(county_file_path, 'w', encoding='utf-8') as f:
                        json.dump(county_data_group, f, indent=2, ensure_ascii=False)

                    logger.info(
                        f"   ✅ Created {county_data_group_cid}.json with {len(relationship_files)} relationship files")

        # NEW: Check if we have relationship errors before proceeding
        if all_relationship_errors:
            logger.error("❌ Relationship building errors found - returning early")
            error_details = "Relationship Building Errors Found:\n\n"
            for error in all_relationship_errors:
                error_details += f"Error: {error}\n"
            return False, error_details, ""

        logger.info(f"✅ Copied {copied_count} folders and built relationship files")

        # Rest of the function continues as before...
        logger.info("🔍 Running CLI validator: npx @elephant-xyz/cli validate-and-upload submit --dry-run")

        # Check prerequisites before running CLI validator
        logger.info("🔍 Checking CLI validator prerequisites...")

        # Check if node/npm are available
        try:
            node_result = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=10)
            npm_result = subprocess.run(["npm", "--version"], capture_output=True, text=True, timeout=10)
            logger.info(f"Node.js version: {node_result.stdout.strip()}")
            logger.info(f"npm version: {npm_result.stdout.strip()}")
        except Exception as e:
            logger.error(f"❌ Node.js/npm not available: {e}")

        # Check if submit directory exists and has content
        submit_dir = os.path.join(BASE_DIR, "submit")
        if os.path.exists(submit_dir):
            submit_contents = os.listdir(submit_dir)
            logger.info(f"Submit directory contains {len(submit_contents)} items: {submit_contents[:5]}...")
        else:
            logger.error("❌ Submit directory not found!")

        logger.info("🔍 Running CLI validator: npx @elephant-xyz/cli validate-and-upload submit --dry-run")

        try:
            result = subprocess.run(
                ["npx", "-y", "@elephant-xyz/cli", "validate-and-upload", "submit", "--dry-run", "--output-csv",
                 "results.csv"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=300  # Keep original 5 minute timeout
            )
        except subprocess.TimeoutExpired as e:
            logger.error("❌ CLI validator timed out after 5 minutes")
            logger.error("This usually indicates:")
            logger.error("  1. Network issues downloading @elephant-xyz/cli")
            logger.error("  2. CLI tool hanging on invalid input")
            logger.error("  3. Large submit directory taking too long to process")
            raise

        # Check for submit_errors.csv file regardless of exit code
        submit_errors_path = os.path.join(BASE_DIR, "submit_errors.csv")

        if os.path.exists(submit_errors_path):
            # Read the CSV file to check for actual errors
            try:
                df = pd.read_csv(submit_errors_path)
                if len(df) > 0:
                    # There are validation errors
                    logger.warning(f"❌ CLI validation found {len(df)} errors in submit_errors.csv")

                    # Get unique error messages and extract field names from error paths
                    unique_errors = set()
                    for _, row in df.iterrows():
                        error_message = row['error_message']
                        error_path = row['error_path']

                        # Extract field name from the error path (last part after the last '/')
                        field_name = error_path.split('/')[-1]

                        # Create formatted error with field name
                        formatted_error = f"{field_name} {error_message}"
                        unique_errors.add(formatted_error)

                    # Format the errors for the generator
                    error_details = "CLI Validation Errors Found:\n\n"

                    for error in sorted(unique_errors):
                        error_details += f"Error: {error}\n"

                    return False, error_details, ""
                else:
                    logger.info("✅ CLI validation passed - no errors in submit_errors.csv")
                    return True, "", ""
            except Exception as e:
                logger.error(f"Error reading submit_errors.csv: {e}")
                error_details = f"Could not read submit_errors.csv: {e}"
                error_hash = hashlib.md5(error_details.encode()).hexdigest()
                return False, error_details, error_hash
        else:
            # No submit_errors.csv file means no errors (hopefully)
            if result.returncode == 0:
                logger.info("✅ CLI validation passed - no submit_errors.csv file found")
                return True, "", ""
            else:
                logger.warning("❌ CLI validation failed")
                error_output = f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
                error_hash = hashlib.md5(error_output.encode()).hexdigest()
                return False, error_output, error_hash

    except subprocess.TimeoutExpired:
        error_msg = "CLI validation timed out after 5 minutes"
        logger.error(error_msg)
        error_hash = hashlib.md5(error_msg.encode()).hexdigest()
        return False, error_msg, error_hash
    except Exception as e:
        error_msg = f"CLI validation error: {str(e)}"
        logger.error(error_msg)
        error_hash = hashlib.md5(error_msg.encode()).hexdigest()
        return False, error_msg, error_hash


def build_relationship_files(folder_path: str) -> tuple[List[str], List[str]]:
    """
    Build relationship files based on discovered files in the folder
    Returns: (relationship_files, errors)
    """
    relationship_files = []
    errors = []

    # Get all JSON files in the folder
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

    # Categorize files
    person_files = [f for f in json_files if f.startswith('person')]
    company_files = [f for f in json_files if f.startswith('company')]
    property_files = [f for f in json_files if f.startswith('property')]
    address_files = [f for f in json_files if f.startswith('address')]
    lot_files = [f for f in json_files if f.startswith('lot')]
    tax_files = [f for f in json_files if f.startswith('tax')]
    sales_files = [f for f in json_files if f.startswith('sales')]
    layout_files = [f for f in json_files if f.startswith('layout')]
    flood_files = [f for f in json_files if f.startswith('flood_storm_information')]

    # Ensure we have property.json as the main reference
    if not property_files:
        error_msg = "❌ No property.json file found - you must create one"
        logger.error(error_msg)
        errors.append(error_msg)
        return relationship_files, errors

    property_file = property_files[0]  # Should be property.json

    # Build person/company to property relationships
    for person_file in person_files:
        rel_filename = f"relationship_{person_file.replace('.json', '')}_property.json"
        rel_path = os.path.join(folder_path, rel_filename)

        relationship_data = {
            "from": {"/": f"./{person_file}"},
            "to": {"/": f"./{property_file}"}
        }

        with open(rel_path, 'w', encoding='utf-8') as f:
            json.dump(relationship_data, f, indent=2, ensure_ascii=False)

        relationship_files.append(rel_filename)
        logger.info(f"     📝 Created {rel_filename}")

    for company_file in company_files:
        rel_filename = f"relationship_{company_file.replace('.json', '')}_property.json"
        rel_path = os.path.join(folder_path, rel_filename)

        relationship_data = {
            "from": {"/": f"./{company_file}"},
            "to": {"/": f"./{property_file}"}
        }

        with open(rel_path, 'w', encoding='utf-8') as f:
            json.dump(relationship_data, f, indent=2, ensure_ascii=False)

        relationship_files.append(rel_filename)
        logger.info(f"     📝 Created {rel_filename}")

    # Build property to other entity relationships
    relationship_mappings = [
        (address_files, "address"),
        (lot_files, "lot"),
        (tax_files, "tax"),
        (sales_files, "sales"),
        (layout_files, "layout"),
        (flood_files, "flood_storm_information")
    ]

    for files_list, entity_type in relationship_mappings:
        for file in files_list:
            # Extract number suffix if present (e.g., tax_1.json -> _1)
            base_name = file.replace('.json', '')
            if '_' in base_name:
                suffix = '_' + base_name.split('_')[-1]
            else:
                suffix = ''

            rel_filename = f"relationship_property_{entity_type}{suffix}.json"
            rel_path = os.path.join(folder_path, rel_filename)

            relationship_data = {
                "from": {"/": f"./{property_file}"},
                "to": {"/": f"./{file}"}
            }

            with open(rel_path, 'w', encoding='utf-8') as f:
                json.dump(relationship_data, f, indent=2, ensure_ascii=False)

            relationship_files.append(rel_filename)
            logger.info(f"     📝 Created {rel_filename}")

    return relationship_files, errors


def create_county_data_group(relationship_files: List[str]) -> Dict[str, Any]:
    """
    Create the county data group structure based on relationship files
    """
    county_data = {
        "label": "County",
        "relationships": {}
    }

    # Initialize all possible relationships as null
    all_relationships = {
        "person_has_property": None,
        "company_has_property": None,
        "property_has_address": None,
        "property_has_lot": None,
        "property_has_tax": None,
        "property_has_sales_history": None,
        "property_has_layout": None,
        "property_has_flood_storm_information": None,
        "property_has_file": None
    }

    # Categorize relationship files
    person_relationships = []
    company_relationships = []
    tax_relationships = []
    sales_relationships = []
    layout_relationships = []

    for rel_file in relationship_files:
        ipld_ref = {"/": f"./{rel_file}"}

        if "person" in rel_file and "property" in rel_file:
            person_relationships.append(ipld_ref)
        elif "company" in rel_file and "property" in rel_file:
            company_relationships.append(ipld_ref)
        elif "property_address" in rel_file:
            all_relationships["property_has_address"] = ipld_ref
        elif "property_lot" in rel_file:
            all_relationships["property_has_lot"] = ipld_ref
        elif "property_tax" in rel_file:
            tax_relationships.append(ipld_ref)
        elif "property_sales" in rel_file:
            sales_relationships.append(ipld_ref)
        elif "property_layout" in rel_file:
            layout_relationships.append(ipld_ref)
        elif "property_flood_storm_information" in rel_file:
            all_relationships["property_has_flood_storm_information"] = ipld_ref

    # Set array relationships
    if person_relationships:
        all_relationships["person_has_property"] = person_relationships
    if company_relationships:
        all_relationships["company_has_property"] = company_relationships
    if tax_relationships:
        all_relationships["property_has_tax"] = tax_relationships
    if sales_relationships:
        all_relationships["property_has_sales_history"] = sales_relationships
    if layout_relationships:
        all_relationships["property_has_layout"] = layout_relationships

    # Only include non-null relationships in the final output
    county_data["relationships"] = {k: v for k, v in all_relationships.items()}

    return county_data


def load_schemas_from_ipfs(save_to_disk=True):
    """Load all schemas from IPFS and optionally save to local folder."""
    schemas = {}
    stub_files = {}

    # Create schemas directory if it doesn't exist
    if save_to_disk:
        schemas_dir = os.path.join(BASE_DIR, "schemas")
        os.makedirs(schemas_dir, exist_ok=True)

    for filename, cid in SCHEMA_CIDS.items():
        logger.info(f"Fetching schema for {filename} from IPFS...")
        schema = fetch_schema_from_ipfs(cid)
        if schema:
            schemas[filename] = schema
            stub_files[filename] = create_stub_from_schema(schema)

            # Save to local file
            if save_to_disk:
                schema_path = os.path.join(schemas_dir, filename)
                with open(schema_path, 'w') as f:
                    json.dump(schema, f, indent=2)
                logger.info(f"Saved schema to {schema_path}")

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
    """Check if all files have been processed and data extracted"""
    data_dir = os.path.join(BASE_DIR, "data")

    if os.path.exists(data_dir):
        processed_count = len([d for d in os.listdir(data_dir)
                               if os.path.isdir(os.path.join(data_dir, d))])
        logger.info(f"Extracted {processed_count} out of {state['input_files_count']} properties")
        return processed_count >= state['input_files_count']

    return False


def check_address_matching_complete(state: WorkflowState) -> bool:
    """Check if address matching is complete"""
    addresses_mapping_path = os.path.join(BASE_DIR, "owners", "addresses_mapping.json")

    if os.path.exists(addresses_mapping_path):
        logger.info("Address matching complete - addresses_mapping.json exists")
        return True

    logger.info("Address matching not complete - addresses_mapping.json not found")
    return False


async def owner_analysis_node(state: WorkflowState) -> WorkflowState:
    """Node 1.5: Handle owner name analysis and schema generation"""
    if state['current_node'] != 'owner_analysis':
        state['retry_count'] = 0
    state['current_node'] = 'owner_analysis'
    logger.info(f"=== Starting Node 1: Owner Analysis (Attempt {state['retry_count'] + 1}) ===")

    # Check if already complete
    owners_schema_path = os.path.join(BASE_DIR, "owners", "owners_schema.json")
    owners_extracted_path = os.path.join(BASE_DIR, "owners", "owners_extracted.json")

    if os.path.exists(owners_schema_path):
        logger.info("Owner analysis already complete - both files exist")
        state['owner_analysis_complete'] = True
        if os.path.exists(owners_extracted_path):
            try:
                os.remove(owners_extracted_path)
                logger.info("🗑️ Cleaned up leftover temporary file: owners_extracted.json")
            except Exception as e:
                logger.warning(f"⚠️ Could not delete leftover owners_extracted.json: {e}")

        return state

    # Create the owner analysis agent
    owner_analysis_agent = OwnerAnalysisAgent(
        state=state,
        model=state['model'],
        tools=state['tools']
    )

    try:
        # Run the owner analysis
        updated_state = await owner_analysis_agent.run_owner_analysis()

        # Update the original state with results
        state.update(updated_state)

        if os.path.exists(owners_schema_path):
            logger.info("✅ Owner Analysis completed successfully - Created: owners_schema.json")
            state['owner_analysis_complete'] = True
        else:
            logger.warning("⚠️ Owner Analysis completed but owners_schema.json not found")
            state['owner_analysis_complete'] = False


    except Exception as e:
        logger.error(f"Error in owner analysis: {e}")
        state['owner_analysis_complete'] = False
        state['validation_errors'].append(str(e))

    return state


async def address_matching_node(state: WorkflowState) -> WorkflowState:
    """Enhanced Node 2: Handle address matching with generator-evaluator pattern"""
    logger.info(
        f"=== Starting Node 2: Address Matching & Validation with Evaluator (Attempt {state['retry_count'] + 1}) ===")

    if state['current_node'] != 'address_matching':
        state['retry_count'] = 0
    state['current_node'] = 'address_matching'

    # Check if already complete
    if check_address_matching_complete(state):
        logger.info("Address matching already complete, skipping to next node")
        state['address_matching_complete'] = True
        state['all_addresses_matched'] = True
        return state

    # Create the generator-evaluator pair
    address_gen_eval_pair = AddressMatchingGeneratorEvaluatorPair(
        state=state,
        model=state['model'],
        tools=state['tools'],
        schemas=state['schemas']
    )

    try:
        # Run the feedback loop
        updated_state = await address_gen_eval_pair.run_feedback_loop()

        # Update the original state with results
        state.update(updated_state)

        if state['all_addresses_matched']:
            logger.info(
                "✅ Address Matching Generator-Evaluator completed successfully - all addresses matched and validated")
        else:
            logger.warning(
                "⚠️ Address Matching Generator-Evaluator completed but not all addresses were properly matched")

    except Exception as e:
        logger.error(f"Error in address matching generator-evaluator pair: {e}")
        state['all_addresses_matched'] = False
        state['address_matching_complete'] = False
        state['validation_errors'].append(str(e))

    return state


async def extraction_and_validation_node(state: WorkflowState) -> WorkflowState:
    """Enhanced Node 1: Handle data extraction with generator-evaluator pattern"""
    logger.info(
        f"=== Starting Node 2: Data Extraction & Validation with Evaluator (Attempt {state['retry_count'] + 1}) ===")

    if state['current_node'] != 'extraction':
        state['retry_count'] = 0
    state['current_node'] = 'extraction'

    # Check if already complete
    if check_extraction_complete(state):
        logger.info("Extraction already complete, skipping to next node")
        state['extraction_complete'] = True
        state['all_files_processed'] = True
        return state

    # Create the generator-evaluator pair
    gen_eval_pair = ExtractionGeneratorEvaluatorPair(
        state=state,
        model=state['model'],
        tools=state['tools'],
        schemas=state['schemas']
    )

    try:
        # Run the feedback loop
        updated_state = await gen_eval_pair.run_feedback_loop()

        # Update the original state with results
        state.update(updated_state)

        if state['all_files_processed']:
            logger.info("✅ Generator-Evaluator completed successfully - all files processed and validated")
        else:
            logger.warning("⚠️ Generator-Evaluator completed but not all files were properly processed")

    except Exception as e:
        logger.error(f"Error in generator-evaluator pair: {e}")
        state['all_files_processed'] = False
        state['validation_errors'].append(str(e))

    return state


def cleanup_directories():
    """Clean up data and submit directories before starting"""
    directories_to_clean = ["scripts"]

    for dir_name in directories_to_clean:
        dir_path = os.path.join(BASE_DIR, dir_name)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            logger.info(f"🗑️ Cleaned up {dir_name} directory")
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"📁 Created fresh {dir_name} directory")


def should_retry_owner_analysis(state: WorkflowState) -> str:
    """Determine if owner analysis node should retry"""

    # ✅ Check for timeout first
    if should_restart_due_to_timeout(state):
        logger.warning("⏰ Owner analysis timeout detected - restarting")
        state['generation_restart_count'] += 1
        state['last_agent_activity'] = time.time()
        return "owner_analysis"  # Restart same node

    # Normal completion check
    if state.get('owner_analysis_complete', False):
        state['retry_count'] = 0  # Reset for next node
        return "address_matching"  # Go to address matching next
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying owner analysis node (attempt {state['retry_count']}/{state['max_retries']})")
        return "owner_analysis"
    else:
        logger.error("Max retries reached for owner analysis node")
        return "address_matching"


def should_retry_extraction(state: WorkflowState) -> str:
    """Determine if extraction node should retry"""
    if state['all_files_processed']:
        return "end"  # Now this goes to end
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying extraction node (attempt {state['retry_count']}/{state['max_retries']})")
        return "extraction"
    else:
        logger.error("Max retries reached for extraction node")
        return "end"


def should_retry_address_matching(state: WorkflowState) -> str:
    """Determine if address matching node should retry"""

    # ✅ Check for timeout first
    if should_restart_due_to_timeout(state):
        logger.warning("⏰ Address matching timeout detected - restarting")
        state['generation_restart_count'] += 1
        state['last_agent_activity'] = time.time()
        return "address_matching"  # Restart same node

    # Normal completion check
    if state.get('address_matching_complete', False):
        state['retry_count'] = 0  # Reset for next node
        return "extraction"  # Go to extraction next
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying address matching node (attempt {state['retry_count']}/{state['max_retries']})")
        return "address_matching"
    else:
        logger.error("Max retries reached for address matching node")
        return "extraction"


async def run_three_node_workflow():
    """Main function to run the two-node workflow with retry logic"""
    cleanup_directories()
    # Load schemas from IPFS
    logger.info("Loading schemas from IPFS and saving to ./schemas/ directory...")
    schemas, stub_files = load_schemas_from_ipfs(save_to_disk=True)

    if not schemas or not stub_files:
        logger.error("Failed to load schemas from IPFS")
        return

    def discover_input_files():
        """Discover all processable files in input folder"""
        if not os.path.exists(INPUT_DIR):
            logger.error(f"Input directory {INPUT_DIR} does not exist")
            return []

        all_files = os.listdir(INPUT_DIR)
        input_files = [f for f in all_files if f.endswith(('.html', '.json'))]

        if input_files:
            # Log what we found
            html_count = len([f for f in input_files if f.endswith('.html')])
            json_count = len([f for f in input_files if f.endswith('.json')])
            logger.info(f"Found {len(input_files)} files: {html_count} HTML, {json_count} JSON")

        return input_files

    # Use it:
    input_files = discover_input_files()
    if not input_files:
        logger.error("No processable files found in input folder")
        return
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
                "ENV_TYPE": "venv-uv",
                "UV_VENV_PATH": os.path.join(current_dir, ".venv")
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

    # Initialize workflow state
    initial_state = WorkflowState(
        input_files=input_files,
        input_files_count=len(input_files),
        schemas=schemas,
        stub_files=stub_files,
        extraction_complete=False,
        address_matching_complete=False,
        owner_analysis_complete=False,
        validation_errors=[],
        processed_properties=[],
        current_node="owner_analysis",
        tools=tools,
        model=model,
        retry_count=0,
        max_retries=3,
        all_files_processed=False,
        all_addresses_matched=False,
        error_history=[],
        consecutive_same_errors=0,
        last_error_hash="",
        generation_restart_count=0,
        max_generation_restarts=2,
        agent_timeout_seconds=300,  # 5 minutes timeout per agent operation
        last_agent_activity=0,
    )

    # Create the workflow graph
    workflow = StateGraph(WorkflowState)

    # Add nodes
    workflow.add_node("owner_analysis", owner_analysis_node)
    workflow.add_node("address_matching", address_matching_node)
    workflow.add_node("extraction", extraction_and_validation_node)

    workflow.add_conditional_edges(
        "owner_analysis",
        should_retry_owner_analysis,
        {
            "owner_analysis": "owner_analysis",  # Retry same node
            "address_matching": "address_matching"  # Move to next
        }
    )

    workflow.add_conditional_edges(
        "address_matching",
        should_retry_address_matching,
        {
            "address_matching": "address_matching",  # Retry same node
            "extraction": "extraction"  # Move to extraction (not end)
        }
    )

    workflow.add_conditional_edges(
        "extraction",
        should_retry_extraction,
        {
            "extraction": "extraction",  # Retry same node
            "end": END  # Now this goes to end
        }
    )

    # Set entry point
    workflow.set_entry_point("owner_analysis")

    # Compile the graph
    app = workflow.compile()

    # Run the workflow
    try:
        logger.info("Starting three-node  workflow execution with retry logic")
        final_state = await app.ainvoke(initial_state)

        # Log final status
        if final_state['owner_analysis_complete'] and final_state['all_addresses_matched'] and final_state[
            'all_files_processed']:
            logger.info("✅ Workflow completed successfully - all tasks completed")
        else:
            logger.warning("⚠️ Workflow completed with incomplete tasks")
            if not final_state['owner_analysis_complete']:
                logger.warning("- Owner analysis was not completed")
            if not final_state['all_addresses_matched']:
                logger.warning("- Not all addresses were matched")
            if not final_state['all_files_processed']:
                logger.warning("- Not all files were processed")

    except Exception as e:
        logger.error(f"Workflow error: {e}")
        raise


async def main():
    """Main entry point"""
    try:
        await run_three_node_workflow()
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)


def run_main():
    """Non-async main entry point for CLI"""
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())