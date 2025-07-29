import asyncio
import os
import sys
import json
import logging
import requests
import backoff
import time
import git
import tempfile
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
from dotenv import load_dotenv

# Try to load .env from multiple locations
for env_path in [".env", os.path.expanduser("~/.env")]:
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        break
else:
    load_dotenv()  # fallback to default behavior

# logger = logging.getLogger(__name__)
#
# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# )

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
    "person.json": "bafkreiajbdqn32mgb3s52xkrvzzwer7oit3gma6bpetzfcmkldxgy5di7m",
    "company.json": "bafkreibnw5zonrappj3prexq7p376njvvawqqzfde222qytsl2jsbst3da",
    "property.json": "bafkreih6x76aedhs7lqjk5uq4zskmfs33agku62b4flpq5s5pa6aek2gga",
    "address.json": "bafkreid5icxhvf6qmmwzok6pnxlgxmqahddbngykwtdaqbcqznjfqh2tve",
    "tax.json": "bafkreibnk4xl6jwgxfeumim6cqpi66ngabzxlxljyhwhuziksbz7buau54",
    "lot.json": "bafkreichj2jpejog35oqwxlbxv2i7mi4vfec5njoreppya3grnukf7rdy4",
    "sales.json": "bafkreicdvzuuymrsyn6wpbo5ossj3q3xcdwiyiniwg7bkmpvbfxtikjv5a",
    "layout.json": "bafkreiegxxnvwnhmrrighkvqikulfi54a7jv7gndnar6zju722wfxzk6xm",
    "flood_storm_information.json": "bafkreidh7s2pk26qtob2iiznkvdb6hqr75weybo5p67erq23e53rsfbnuy",
    "structure.json": "bafkreictnk74jkby6q64d3vm6h57s6vr5x65p2wzubpwvwsjil2254okhi",
    "utility.json": "bafkreib3wrmiwqyi34xdengoyud4aplz5rbsjs6vag4eic4n7ohturx6xq"
}

# Create logs directory
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure logging to file
log_file_path = os.path.join(LOGS_DIR, f"workflow_{int(time.time())}.log")

# Create file handler for detailed logs
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# Create console handler for status messages only
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.CRITICAL)  # Only show critical messages

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)


class WorkflowState(TypedDict):
    """State shared between nodes"""
    input_files: List[str]
    input_files_count: int
    schemas: Dict[str, Any]
    stub_files: Dict[str, Any]
    extraction_complete: bool
    address_matching_complete: bool
    owner_analysis_complete: bool
    structure_extraction_complete: bool
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
    county_data_group_cid: str


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


def print_status(message):
    """Print status messages to terminal only"""
    print(f"STATUS: {message}")
    logger.info(f"STATUS: {message}")  # Also log to file


def print_running(node_name):
    """Print running status"""
    print(f"🔄 RUNNING: {node_name}")
    logger.info(f"RUNNING: {node_name}")


def print_completed(node_name, success=True):
    """Print completion status"""
    status = "✅ COMPLETED" if success else "❌ FAILED"
    print(f"{status}: {node_name}")
    logger.info(f"COMPLETED: {node_name} - Success: {success}")


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
1. Look in inputs files for where ownerName1 and ownerName2 are located (could be nested in JSON or embedded in HTML)
2. Create extraction script that correctly finds and extracts the owner data
3. VERIFY that extracted data contains actual names, not nulls

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

        🔄 MANDATORY SCRIPT-FIRST WORKFLOW:

        STEP 1: CHECK FOR EXISTING SCRIPT (ALWAYS DO THIS FIRST!)
        1. **IMMEDIATELY** check if scripts/owner_processor.py already exists using read_file tool
        2. If the file EXISTS:
           a. **RUN THE EXISTING SCRIPT** using execute_code_file tool
           b. **CHECK OUTPUT** - read owners/owners_schema.json if it was created
           c. **VALIDATION CHECKLIST** 

        3. If output is CORRECT and COMPLETE → **STOP HERE, YOU'RE DONE!**
        4. If output is MISSING or INCORRECT → go to STEP 3 to UPDATE the script
        5. If the file DOES NOT EXIST → go to STEP 3 to CREATE the script

        STEP 2: ANALYSIS & CATEGORIZATION PHASE:
           - PICK 3 samples of the input files to do validation check on it 
           - Compare the owners/owners_schema.json file with the input files
           - Analyze each extracted owner name to determine if it's a Person or Company
           - ensure it contains actual owner names (not nulls/empty)
           - you MUST Analyze EVERY AND EACH extracted owner name in owners/owners_schema.json to determine if it's EXTRACTED CORRECTLY a Person or Company
           - companies identified properly.
           - verify person names parsed into: first_name, last_name, middle_name correctly
           - Generate clean, structured output, not include & in person name
           - If owners is not extracted correctly fix the script and rerun
           - Parse person names into: first_name, last_name, middle_name
           - Identify company names (Inc, LLC, Ltd, Foundation, Alliance, Solutions, etc.)
           - Generate structured data following person/company schemas
           - If data is not correct move to STEP 3

        STEP 3: CREATE/UPDATE UNIFIED SCRIPT (Only if Step 1 failed or file missing)

        Actions:
        1. EXAMINE input file structure first (3-5 samples)
        2. CREATE or UPDATE scripts/owner_processor.py as a SINGLE UNIFIED SCRIPT that:

           A. EXTRACTION PHASE:
           - Handle both JSON and HTML input formats correctly
           - Extract ownerName1 and ownerName2 from each file
           - Make sure to extract all previous owners as well, if available
           - Store extracted data in memory (no need for separate extracted file)
           - ENSURE owner names are NOT null - debug and fix if extraction fails

           B. ANALYSIS & CATEGORIZATION PHASE (same script):
           - Analyze each extracted owner name to determine if it's a Person or Company
           - Parse person names into: first_name, last_name, middle_name
           - Identify company names (Inc, LLC, Ltd, Foundation, Alliance, Solutions, etc.)
           - Generate structured data following person/company schemas
           - Save FINAL output to: owners/owners_schema.json

        3. **TEST THE UPDATED SCRIPT** - run it and validate output

        🏢 COMPANY DETECTION RULES (built into unified script):
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
            "owners_by_date": {{
                "04/29/2024":[
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
                    ],
                "04/07/2022": [
                     {{
                        "type": "company",
                        "name": "First Responders Foundation"
                      }}
                    ],
                }}
            }},
        }}
        ```

        ⚠️ CRITICAL RULES:
        - **NEVER CREATE A NEW SCRIPT WITHOUT FIRST CHECKING FOR EXISTING ONE**
        - **ALWAYS RUN EXISTING SCRIPT FIRST AND CHECK OUTPUT**
        - Use ONE UNIFIED SCRIPT (owner_processor.py) that does both extraction and analysis
        - Only CREATE script if no existing script found
        - Only UPDATE script if existing script produces wrong output
        - Process ALL {self.state['input_files_count']} input files
        - Final output: ONLY owners/owners_schema.json (no intermediate files needed)

        🚨 WORKFLOW ENFORCEMENT:
        - FIRST ACTION: Use read_file tool on scripts/owner_processor.py
        - IF FILE EXISTS: Use execute_code_file tool to run it
        - IF FILE DOESN'T EXIST: Then and only then create it
        - IF OUTPUT IS WRONG: Then and only then update the existing script

        🚀 START: **IMMEDIATELY** check for existing script with read_file tool, run it if exists, validate output, then create/update only if needed.
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
                        logger.info(f"       📤 Result: {str(tool_output)[:100]}...")

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
        try:
            await self._agent_speak(
                agent=generator_agent,
                agent_name="ADDRESS_GENERATOR",
                turn=1,
                user_instruction="Start by creating the address matching script and processing all input files"
            )
        except Exception as e:
            if "timed out" in str(e):
                logger.warning("⏰ Agent timeout during initial ADDRESS_GENERATOR - restarting")
                return await self._restart_generation_process()
            raise

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
                    user_instruction="""
                     Review and evaluate the Address Generator's matching work and validate address completeness by comparing with sample input files.
                     Check if addresses_mapping.json is properly created with correct address components.
                     """,
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
                    2.look at the ./schema/address.json schema and ./input directory to understand the address structure before fixing script
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
        # UPDATED ADDRESS MATCHING GENERATOR PROMPT
        generator_prompt = f"""
        You are an address matching specialist handling address data processing and matching.

        🔄 CURRENT STATUS:
            🔁 Attempt: {self.state['retry_count'] + 1} of {self.state['max_retries']}

        🎯 YOUR MISSION: Process ALL properties in ./input/ by matching addresses using scripts/address_extraction.py.

        🔄 MANDATORY SCRIPT-FIRST WORKFLOW:

        STEP 1: CHECK FOR EXISTING SCRIPT AND RUN IT
        1. **IMMEDIATELY** check if scripts/address_extraction.py already exists using read_file tool
        2. If the file EXISTS:
           - **RUN THE EXISTING SCRIPT** using execute_code_file tool
           - **CHECK OUTPUT** - read owners/addresses_mapping.json if it was created
           - **WAIT FOR EVALUATOR FEEDBACK** - the evaluator will validate your output
        3. If the file DOES NOT EXIST → go to STEP 2 to CREATE the script

        STEP 2: CREATE/UPDATE SCRIPT (Only when needed)
        **EXECUTE THIS ONLY IF:**
        - No scripts/address_extraction.py exists, OR
        - Evaluator found validation errors in your output

        Actions for creating/updating script:
        1. **ANALYZE DATA STRUCTURE** (only if creating/fixing script):
           - Examine 3-5 samples from possible_addresses/ to understand JSON format
           - Check corresponding input files to understand address location
           - Plan matching strategy before coding

        2. **CREATE or UPDATE** scripts/address_extraction.py:
           A. EXTRACTION PHASE:
           - Process all properties in ./input/
           - Extract property_parcel_id from input file name
           - Load possible_addresses/[property_parcel_id].json (candidate addresses)
           - Parse address from input files
           - Extract all address attributes following ./schemas/address.json
           - Get county name from seed.csv file

           B. MATCHING PHASE:
           - Compare input file address with candidates in possible_addresses/
           - Use exact matching first, then fuzzy matching if needed
           - Focus on: street number + street name + unit (if applicable)

           C. OUTPUT PHASE:
           - Save matched address data to: owners/addresses_mapping.json
           - Follow address schema: {self.state['schemas']['address.json']}

        3. **TEST THE SCRIPT** - run it and generate output

        📋 OUTPUT STRUCTURE:
        Generate: owners/addresses_mapping.json following address schema:
        ```json
        {{
          "property_[id]": {{
            "address": {{
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
                "plus_four_postal_code": "1234"
            }}
          }}
        }}
        ```

        ⚠️ CRITICAL RULES:
        - **NEVER CREATE SCRIPT WITHOUT FIRST CHECKING FOR EXISTING ONE**
        - **ALWAYS RUN EXISTING SCRIPT FIRST**
        - Only CREATE script if no existing script found
        - Only UPDATE script if evaluator finds validation errors
        - Process ALL properties in ./input/ directory
        - Follow address schema exactly

        🚨 WORKFLOW ENFORCEMENT:
        - **FIRST ACTION**: Use read_file tool on scripts/address_extraction.py
        - **IF EXISTS**: Use execute_code_file tool to run it
        - **IF DOESN'T EXIST**: Then create it
        - **IF EVALUATOR REJECTS**: Then update the existing script with specific fixes

        🚀 START: **IMMEDIATELY** check for existing script, run it if exists, then wait for evaluator feedback.
        """

        # UPDATED ADDRESS EVALUATOR PROMPT
        evaluator_prompt = f"""
        You are the ADDRESS EVALUATOR in a multi-agent address matching pipeline.

        🎯 YOUR TASK:
        Validate address matching completeness and accuracy by checking the generator's output.

        🔄 MANDATORY VALIDATION WORKFLOW:

        STEP 1: CHECK FOR EXISTING VALIDATION SCRIPT AND RUN IT
        1. **FIRST** check if scripts/address_validation.py already exists using read_file tool
        2. If the file EXISTS:
           - **RUN THE EXISTING VALIDATION SCRIPT** using execute_code_file tool
           - **ANALYZE THE VALIDATION RESULTS** 
           - **PROCEED TO STEP 2 FOR ADDITIONAL MANUAL VALIDATION**
        3. If the file DOES NOT EXIST → create it, run it, then proceed to STEP 2

        STEP 2: MANDATORY MANUAL VALIDATION (NEVER SKIP THIS!)
        **YOU MUST ALWAYS DO THIS VALIDATION:**

        A. **CHECK OUTPUT FILE EXISTS**:
           - Verify owners/addresses_mapping.json exists and has content
           - Count how many properties are included

        B. **PICK 3 SAMPLE PROPERTIES** for detailed validation:
           - Read 3 different input files from ./input/ directory
           - Extract what the addresses should be from these files manually
           - Read the corresponding entries in owners/addresses_mapping.json

        C. **VALIDATE ADDRESS COMPONENTS** (CRITICAL - CHECK EVERY FIELD):
           For each sample property, verify ALL address components exist and are correct:
           - **street_number** (not null/empty where it should exist)
           - **street_name** (not null/empty)
           - **unit_identifier** (null if no unit, correct value if unit exists)
           - **city_name** (not null/empty)
           - **postal_code** (not null/empty, correct format)
           - **state_code** (not null/empty)
           - **street_pre_directional_text** (N, S, E, W if applicable)
           - **street_post_directional_text** (directional after street name if applicable)
           - **street_suffix_type** (ST, AVE, BLVD, etc. if applicable)
           - **plus_four_postal_code** (4-digit extension if available)
           - **latitude/longitude** (if available in source)

        D. **COMPARE WITH SOURCE DATA**:
           - Ensure addresses in output match the actual addresses from input files
           - Check that address matching logic selected the best candidate
           - Verify no placeholder values (like "TODO", "TBD", etc.)

        E. **SCHEMA COMPLIANCE CHECK**:
           - Verify all addresses follow the exact schema format
           - Check data types are correct
           - Ensure required fields are present

        STEP 3: VALIDATION DECISION
        - **If ALL validation checks PASS** → Return "STATUS: ACCEPTED"
        - **If ANY validation check FAILS** → Return "STATUS: REJECTED" with action plan

        **COMMON ISSUES TO CHECK FOR:**
        - Missing addresses_mapping.json file
        - Missing properties (not all input files processed)
        - Null/empty required address components
        - Incorrect address components (wrong street name, number, etc.)
        - Schema violations (wrong data types, missing fields)
        - Placeholder values instead of real data

        STEP 4: CREATE/UPDATE VALIDATION SCRIPT (Only if needed)
        If no validation script exists:
        - CREATE scripts/address_validation.py that:
          - Reads owners/addresses_mapping.json
          - Validates against address schema: {self.state['schemas']['address.json']}
          - Checks for required fields, data types, format compliance
          - Reports detailed validation results

        📝 RESPONSE FORMAT:
        Start with:
        **STATUS: ACCEPTED** or **STATUS: REJECTED**

        If REJECTED, provide specific action plan:
        - List exactly what is wrong
        - Provide step-by-step instructions for the generator to fix issues
        - Be specific about which address components are missing/incorrect

        ✅ VALIDATION CHECKLIST:
        1. addresses_mapping.json exists in owners/ directory
        2. Contains address data for ALL properties in input/ directory  
        3. All address components properly extracted (not null where they should exist)
        4. Addresses match source data in input files
        5. Schema compliance - correct format and data types
        6. No placeholder/TODO values
        7. Address matching selected best candidates from possible_addresses/

        ⚠️ CRITICAL RULES:
        - **STEP 2 MANUAL VALIDATION IS MANDATORY** - never skip it
        - **CHECK EVERY ADDRESS COMPONENT** for 3 sample properties
        - **COMPARE WITH SOURCE INPUT FILES** to verify accuracy
        - Only accept if ALL validation criteria are met
        - Be strict - any missing or incorrect data = REJECTED

        🚨 WORKFLOW ENFORCEMENT:
        1. **FIRST**: Check for existing validation script
        2. **RUN SCRIPT**: If exists, execute it
        3. **MANDATORY**: Do manual validation (compare 3 samples with source)
        4. **DECISION**: Accept only if everything is perfect
        5. **IF REJECTED**: Provide specific action plan for generator

        🚀 START: Check for existing validation script, run it, then ALWAYS do manual validation to catch any errors.
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
        For a **sample of AT MOST 3–5 properties**:
        1. **Check if owners/addresses_mapping.json exists** and has content
        2. do a checksum to make sure all properties address are extracted
        3. you MUST **Verify address components** are properly extracted, everysingle attribute must be verified that it exists:
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
        4. **Compare with input files** to ensure addresses match the source data
        5. **Verify matching logic** worked correctly by checking a few examples

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

        if REJECTED, REPLY ONLY WITH AN ACTION PLAN FOR THE GENERATOR TO DO AS STEPS TO FIX THE ISSUES

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
            raise Exception(
                f"{agent_name} timed out after {timeout_seconds} seconds of inactivity - restarting address matching")
        except Exception as e:
            logger.error(f"     ❌ {agent_name} error: {str(e)}")
            return f"{agent_name} error on turn {turn}: {str(e)}"


class StructureGeneratorEvaluatorPair:
    """Generator and Evaluator for structure extraction node with validation"""

    def __init__(self, state: WorkflowState, model, tools, schemas: Dict[str, Any]):
        self.state = state
        self.model = model
        self.tools = tools
        self.schemas = schemas
        self.max_conversation_turns = 15
        self.shared_checkpointer = InMemorySaver()
        self.shared_thread_id = "structure-conversation-1"
        self.consecutive_script_failures = 0
        self.max_script_failures = 3

    async def _restart_generation_process(self) -> WorkflowState:
        """Restart the generation process with a fresh thread"""
        logger.info("🔄 RESTARTING STRUCTURE EXTRACTION PROCESS - Creating new thread and agents")

        self.state['last_agent_activity'] = 0
        self.state['generation_restart_count'] += 1

        # Create new thread ID for fresh start
        self.shared_thread_id = f"structure-conversation-restart-{self.state['generation_restart_count']}"
        self.shared_checkpointer = InMemorySaver()

        # Reset conversation state
        self.max_conversation_turns = 15

        logger.info(f"🆕 Starting fresh structure extraction attempt #{self.state['generation_restart_count']}")
        logger.info(f"🆕 New thread ID: {self.shared_thread_id}")

        return await self.run_feedback_loop()

    async def run_feedback_loop(self) -> WorkflowState:
        """Run Structure Generator + Evaluator CONVERSATION"""

        logger.info("🔄 Starting Structure Generator + Evaluator CONVERSATION")
        logger.info(f"💬 Using shared thread: {self.shared_thread_id}")
        logger.info(f"🎭 Two agents: Generator, Evaluator")

        # Create agents
        generator_agent = await self._create_structure_generator_agent()
        evaluator_agent = await self._create_structure_evaluator_agent()

        conversation_turn = 0
        evaluator_accepted = False

        # GENERATOR STARTS: Create initial script
        logger.info("🤖 Structure Generator starts the conversation...")
        try:
            await self._agent_speak(
                agent=generator_agent,
                agent_name="STRUCTURE_GENERATOR",
                turn=1,
                user_instruction="Start by creating the structure extraction scripts and processing all input files"
            )
        except Exception as e:
            if "timed out" in str(e):
                logger.warning("⏰ Agent timeout during initial STRUCTURE_GENERATOR - restarting")
                return await self._restart_generation_process()
            raise

        # Continue conversation until evaluator accepts or max turns
        while conversation_turn < self.max_conversation_turns:
            conversation_turn += 1

            if hasattr(self, 'force_restart_now') and self.force_restart_now:
                logger.warning("🔄 Structure extraction script failure restart triggered - restarting now")
                self.force_restart_now = False
                return await self._restart_generation_process()

            if should_restart_due_to_timeout(self.state):
                logger.warning("⏰ Agent timeout detected - restarting structure extraction process")
                return await self._restart_generation_process()

            logger.info(f"💬 Structure Extraction Turn {conversation_turn}/{self.max_conversation_turns}")

            # EVALUATOR RESPONDS
            logger.info("📊 Structure Evaluator reviews Generator's work...")
            try:
                evaluator_message = await self._agent_speak(
                    agent=evaluator_agent,
                    agent_name="STRUCTURE_EVALUATOR",
                    turn=conversation_turn,
                    user_instruction="""
                    Review and evaluate the Structure Generator's extraction work and validate structure/utility/layout data completeness by comparing with sample input files.
                     Check if all three output files are properly created with correct data extraction. Reply only with ACCEPTED or REJECTED with an ACTION PLAN WITH THE ISSUES ONLY if REJECTED for the generator to fix the issues, if any.  

                     """,
                )
                logger.info(f"🔍 DEBUG: Full evaluator response:")
                logger.info(f"📝 {evaluator_message}")

            except Exception as e:
                if "timed out" in str(e):
                    logger.warning("⏰ Agent timeout during STRUCTURE_EVALUATOR - restarting")
                    return await self._restart_generation_process()
                raise

            evaluator_accepted = "STATUS: ACCEPTED" in evaluator_message
            logger.info(f"📊 Structure Evaluator decision: {'ACCEPTED' if evaluator_accepted else 'NEEDS FIXES'}")

            # Check if evaluator accepted
            if evaluator_accepted:
                logger.info("✅ Structure extraction conversation completed successfully - Evaluator approved!")
                self.state['structure_extraction_complete'] = True
                break

            # GENERATOR RESPONDS: Sees feedback from evaluator and fixes issues
            logger.info("🤖 Structure Generator responds to evaluator's feedback...")

            await self._agent_speak(
                agent=generator_agent,
                agent_name="STRUCTURE_GENERATOR",
                turn=conversation_turn + 1,
                user_instruction=f"""IMMEDIATE ACTION REQUIRED,
                SILENTLY Fix the structure extraction issues found by the evaluator immediately. Work silently, Don't reply to them, just use your tools to update the extraction scripts to Fix these specific issues:\n\n{evaluator_message}

                DONOT REPLY FIX SILENTLY,

                YOU MUST:
                    1. Use the filesystem tools to read/modify the structure extraction scripts
                    2. Look at the ./schemas/ directory to understand the required structure
                    3. Look at the ./input directory to understand the data structure
                    4. You MUST understand all the root causes of the errors to fix the extraction scripts
                    5. Fix ALL the specific errors mentioned above
                    6. Test your changes by running the scripts and make sure you eliminated these errors
                    7. MAKE sure you have eliminated the errors before Quitting

                DO NOT just acknowledge - TAKE ACTION NOW with tools to fix these issues.""")

            logger.info(f"🔄 Structure extraction turn {conversation_turn} complete, continuing conversation...")

        # Conversation ended
        final_status = "ACCEPTED" if evaluator_accepted else "PARTIAL"

        if final_status != "ACCEPTED":
            logger.warning(
                f"⚠️ Structure extraction conversation ended without full success after {self.max_conversation_turns} turns")
            logger.warning(f"Evaluator: {'✅' if evaluator_accepted else '❌'}")

        logger.info(f"💬 Structure extraction conversation completed with status: {final_status}")
        return self.state

    async def _create_structure_generator_agent(self):
        """Create Structure Generator agent"""
        generator_prompt = f"""
        You are the STRUCTURE GENERATOR - an expert in home designs and structural design responsible for extracting structure, utility, and layout information.

        🏗️ YOUR EXPERTISE: 
        - Residential and commercial building structures
        - Construction materials and methods
        - Utility systems (electrical, plumbing, HVAC)
        - Room layouts and space planning
        - Building codes and standards

        🎯 YOUR MISSION: 
        Extract structure, utility, and layout information from property input files using three separate scripts.
        And YOU MUST create the validation script, do not ask generator to create it

        📂 INPUT STRUCTURE:
        - Input files are located in ./input/ directory ({self.state['input_files_count']} files)
        - Files can be .html or .json format

        🔄 MANDATORY SCRIPT-FIRST WORKFLOW:

        STEP 1: CHECK FOR EXISTING SCRIPTS AND RUN THEM
        1. **CHECK STRUCTURE SCRIPT**: 
           - Check if scripts/structure_extractor.py exists using read_file tool
           - If EXISTS: Run it using execute_code_file tool
           - Check if owners/structure_data.json was created

        2. **CHECK UTILITY SCRIPT**:
           - Check if scripts/utility_extractor.py exists using read_file tool
           - If EXISTS: Run it using execute_code_file tool
           - Check if owners/utility_data.json was created

        3. **CHECK LAYOUT SCRIPT**:
           - Check if scripts/layout_extractor.py exists using read_file tool
           - If EXISTS: Run it using execute_code_file tool
           - Check if owners/layout_data.json was created

        4. **WAIT FOR EVALUATOR FEEDBACK** - the evaluator will validate your output

        STEP 2: CREATE/UPDATE SCRIPTS (Only when evaluator finds issues)
        **EXECUTE THIS ONLY IF:**
        - Any of the three scripts don't exist, OR
        - Evaluator found validation errors in any output file

        📋 SCHEMAS TO FOLLOW:
        Read the schemas from ./schemas/ directory:
        - structure.json - for building structure information
        - utility.json - for utility systems information  
        - layout.json - for room and space layout information

        Actions:
        1. **READ AND EXAMINE SCHEMAS FIRST** from ./schemas/ directory:
           - What fields are required for structure data
           - What fields are required for utility data
           - What fields are required for layout data
           - What are the allowed enum values for each field
           - What data types are expected

        2. **EXAMINE INPUT FILES** (3-5 samples) to understand:
           - What structural information is available
           - What utility information is available  
           - What layout/room information is available
           - How room counts are presented (e.g., "2 bed, 1.5 bath")

        3. **CREATE/UPDATE SCRIPTS** as needed:

           A. **STRUCTURE SCRIPT** (scripts/structure_extractor.py):
           - Read ALL files from ./input/ directory
           - Handle both JSON and HTML input formats correctly
           - Extract structure-related information (building type, construction materials, etc.)
           - Follow the structure.json schema exactly
           - Use enum values from the schema where applicable
           - Save extracted data to: owners/structure_data.json
           - Format: {{"property_[id]": {{structure data following schema}}}}

           B. **UTILITY SCRIPT** (scripts/utility_extractor.py):
           - Read ALL files from ./input/ directory
           - Extract utility system information (electrical, plumbing, HVAC, etc.)
           - Follow the utility.json schema exactly
           - Use enum values from the schema where applicable
           - Save extracted data to: owners/utility_data.json
           - Format: {{"property_[id]": {{utility data following schema}}}}

           C. **LAYOUT SCRIPT** (scripts/layout_extractor.py):
           - Read ALL files from ./input/ directory
           - Extract room and space layout information
           - Follow the layout.json schema exactly
           - Use enum values from the schema where applicable
           - Identify different room types (bedroom, bathroom, kitchen, etc.)
           - Save extracted data to: owners/layout_data.json
           - Format: {{"property_[id]": {{layout data following schema}}}}

        4. **TEST ALL UPDATED SCRIPTS** - run them and verify output

        🏗️ STRUCTURE DETECTION GUIDELINES:
        Look for information about:
        - Building type (residential, commercial, etc.)
        - Construction materials (wood, brick, concrete, etc.)
        - Foundation type, roof type and materials

        🔧 UTILITY DETECTION GUIDELINES:
        Look for information about:
        - Electrical systems (voltage, panel type, wiring)
        - Plumbing systems (water supply, drainage, fixtures)
        - HVAC systems (heating, cooling, ventilation)
        - Other utilities (gas, internet, cable, etc.)

        🏠 LAYOUT DETECTION GUIDELINES:
        Look for information about:
        - Room types and counts (bedrooms, bathrooms, etc.)
        - Room dimensions and square footage
        - Space relationships and flow
        - Special features (fireplaces, built-ins, etc.)
        - Storage spaces
        IMPORTANT: Layouts for ALL (bedrooms, full and half bathrooms) must be extracted into distinct layout objects. E.g., 2 beds, 1 full, 1 half bath = 4 layout objects.

        📋 OUTPUT STRUCTURE:
        Generate three separate files:
        ```json
        // owners/structure_data.json
        {{
          "property_[id]": {{
            // ... structure fields per schema
          }}
        }}

        // owners/utility_data.json  
        {{
          "property_[id]": {{
            // ... other utility fields per schema
          }}
        }}

        // owners/layout_data.json
        {{
          "property_[id]": {{
            "layouts": [
              // ... space_type and other utility fields per schema
            ]
          }}
        }}
        ```

        ⚠️ CRITICAL RULES:
        - **CHECK EXISTING SCRIPTS FIRST** before creating new ones
        - **RUN EXISTING SCRIPTS** before updating them
        - **FOLLOW SCHEMAS EXACTLY** - use correct field names, data types, enum values
        - Process ALL {self.state['input_files_count']} input files

        🗣️ CONVERSATION RULES:
        - You are having a conversation with the EVALUATOR
        - When the evaluator gives you feedback, read it carefully and work in silence to fix the issues
        - Fix the specific issues they mention without acknowledging

        🚨 WORKFLOW ENFORCEMENT:
        1. **FIRST**: Check all three existing scripts with read_file tool
        2. **IF EXIST**: Run them with execute_code_file tool
        3. **WAIT**: For evaluator feedback
        4. **IF EVALUATOR REJECTS**: Update specific scripts based on feedback

        🚀 START: **IMMEDIATELY** check for existing scripts, run them if they exist, then wait for evaluator feedback.
        """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=generator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _create_structure_evaluator_agent(self):
        """Create Structure Evaluator agent"""
        evaluator_prompt = f"""
        You are the STRUCTURE EVALUATOR understanding that this is **INTELLIGENT DATA MAPPING**, not exact replication.

        🎯 YOUR UNDERSTANDING:
        - Input data varies widely in format and completeness  
        - Your job is to verify reasonable extraction and mapping
        - Schema enums should be mapped to best available matches
        - Missing data results in null values (this is normal and acceptable)

        🔍 REASONABLE VALIDATION:
        Check 2-3 sample properties for:

        1. **Basic Extraction**: Are structure/utility/layout files created?
        2. **Schema Mapping**: Are available data points mapped to appropriate schema fields?
        3. **Logical Consistency**: Do room counts and layouts make sense?
        4. **Coverage**: Are all input files processed?

        ✅ ACCEPT IF:
        - All three output files exist and have reasonable content
        - Available data is mapped to schema appropriately
        - Room/space counts are logically extracted
        - Enum values are reasonable matches
        - Processing covers all input files

        ❌ ONLY REJECT IF:
        - Output files missing entirely
        - Major structural data ignored when clearly present
        - Room counts are completely wrong (e.g., 5 bedrooms extracted as 1)
        - No processing occurred

        🗣️ BE PRACTICAL:
        - Data mapping involves interpretation - accept reasonable choices
        - Null values for unavailable data are normal
        - Focus on major extraction issues, not perfect enum matching
        - Understand that source data quality varies

        RESPONSE: **STATUS: ACCEPTED** or **STATUS: REJECTED** with major issues only.
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
        # (Same timeout handling, activity tracking, etc.)

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
            logger.error(f"     🔄 This will trigger a restart of the structure extraction process")
            raise Exception(
                f"{agent_name} timed out after {timeout_seconds} seconds of inactivity - restarting structure extraction")
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
                    normalized_path = file_path.replace('\\', '/')
                    parts = normalized_path.split('/')
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
                    normalized_path = file_path.replace('\\', '/')
                    parts = normalized_path.split('/')
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
            user_instruction="Start by creating the extraction script and processing all input files, Make sure to extract all sales-taxes-owners data  "
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
                    user_instruction="""
                    you are a restrict reviewer, you have a checklist , you have to make sure every single point in this check list is correct,
                      your job is to Review and evaluate the Generator's extraction work all over Again even if you already accepted it in previous run"
                      validate data completeness by comparing with sample input files and make sure validation points are met, pick AT MOST 3 different samples to compare,
                      DO NOT repeat yourself, if generator persisted in an output makesure you are correct and revalidate yourself
                      if you already accepted in the previous run, check again for any new issues that might have been introduced by the generator
                      if REJECTED, REPLY ONLY WITH AN ACTION PLAN FOR THE GENERATOR TO DO AS STEPS TO FIX THE ISSUES
                     """,
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
            cli_success, cli_errors, _ = run_cli_validator("data", self.state['county_data_group_cid'])

            print(f"🔍 CLI Validator errors: {cli_errors}")
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

            print(f"🔍 Feedback summary for generator:{feedback_summary}")
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
                    3. read the Schema inside ./schemas/ directory to understand the required structure, you MUST follow the exact structure provided in the schemas.
                    4. look AGAIN at the input data to know how you will extract the data OR fix the extraction script
                    4. Fix ALL the specific errors mentioned above
                    5. run the script 

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

            🔄 MANDATORY SCRIPT-FIRST WORKFLOW:

            STEP 1: CHECK FOR EXISTING SCRIPT AND RUN IT
            1. **IMMEDIATELY** check if scripts/data_extractor.py already exists using read_file tool
            2. If the file EXISTS:
               - **RUN THE EXISTING SCRIPT** using execute_code_file tool
               - **CHECK OUTPUT** - verify files were created in ./data/ directory
               - **WAIT FOR EVALUATOR FEEDBACK** - the evaluators will validate your output
            3. If the file DOES NOT EXIST → go to STEP 2 to CREATE the script

            STEP 2: CREATE/UPDATE SCRIPT (Only when needed)
            **EXECUTE THIS ONLY IF:**
            - No scripts/data_extractor.py exists, OR
            - Evaluators found validation errors in your output

            Actions for creating/updating script:
            1. **READ ALL SCHEMAS** from ./schemas/ directory to understand data structures
            2. **ANALYZE INPUT STRUCTURE** (examine 3-5 sample files)
            3. **ANALYZE SUPPORTING DATA**:
               - owners/owners_schema.json (for person/company data)
               - owners/addresses_mapping.json (for address extraction)
               - owners/layout_data.json, owners/structure_data.json, owners/utility_data.json
            4. **CREATE or UPDATE** scripts/data_extractor.py that generates output structure below

            📂 REQUIRED OUTPUT STRUCTURE: this output should be generated through a data_extraction.py script
            if any file don't have data, DO NOT create it, example: if input data don't have flood_storm_information don't create this file
            ./data/[property_parcel_id]/property.json
            ./data/[property_parcel_id]/address.json use owners/addresses_mapping.json in address extraction along with the input file
            ./data/[property_parcel_id]/lot.json
            ./data/[property_parcel_id]/tax_*.json
            ./data/[property_parcel_id]/flood_storm_information.json
            ./data/[property_parcel_id]/sales_*.json

            ./data/[property_parcel_id]/person_*.json   or ./data/[property_parcel_id]/company_*.json  
            for every sales year extract "EVERY" person and company data for all current/previous owners of each property from owners/owners_schema.json file

            use owners/layout_data.json and owners/structure_data.json and owners/utility_data.json to extract the data
            ./data/[property_parcel_id]/structure.json
            ./data/[property_parcel_id]/utility.json
            ./data/[property_parcel_id]/layout_*.json

            🔗 RELATIONSHIP FILES MAPPING:
                Create relationship files with these exact structures:

                relationship_sales_person.json (person → property) or relationship_sales_company.json (company → property):
                to link between the purchase date and the person/company who purchased the property.
                and if two owners at the same time, you should have multiple files with suffixes contain each have
                {{
                    "to": {{
                        "/": "./person_1.json"
                    }},
                    "from": {{
                        "/": "./sales_2.json"
                    }}
                }}

                {{
                    "to": {{
                        "/": "./person_2.json"
                    }},
                    "from": {{
                        "/": "./sales_2.json"
                    }}
                }}
                or if it is a company:
                {{
                    "to": {{
                        "/": "./company_1.json"
                    }},
                    "from": {{
                        "/": "./sales_5.json"
                    }}
                }}

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
            - DO NOT invent or fabricate data, you data MUST come directly from the input source.

            🗣️ CONVERSATION RULES:
            - You are having a conversation with TWO VALIDATORS: Data Evaluator, and CLI Validator
            - When ANY validator gives you feedback, read it carefully and work in silent to fix the issues
            - Fix the specific issues they mention in silence

            🚨 WORKFLOW ENFORCEMENT:
            - **FIRST ACTION**: Use read_file tool on scripts/data_extractor.py
            - **IF EXISTS**: Use execute_code_file tool to run it
            - **IF DOESN'T EXIST**: Then create it
            - **IF EVALUATORS REJECT**: Then update the existing script with specific fixes

            🚀 START: **IMMEDIATELY** check for existing script, run it if exists, then wait for evaluator feedback.
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
            You are the DATA EVALUATOR who **ACTUALLY VALIDATES DATA** by doing the checking yourself.

            🎯 YOUR JOB: 
            **DO THE VALIDATION WORK YOURSELF** - don't tell the generator what to check, YOU check it.

            🔍 YOUR VALIDATION PROCESS:

            STEP 1: **YOU PERSONALLY EXAMINE THE DATA**
            1. **Read AT MOST 3 sample input files** from ./input/ directory yourself
            2. **Read the corresponding output folders** in ./data/ directory yourself  
            3. **Compare them side-by-side** yourself
            4. **Count and verify** the data yourself

            STEP 2: **YOU DO THE ACTUAL CHECKING**
            For each sample property, YOU verify:

            ✅ **Layout Validation (YOU COUNT):**
            - Read the input file and COUNT bedrooms/bathrooms yourself
            - Read the layout_*.json files and COUNT how many exist
            - If input says "2 bed, 1.5 bath" = should be exactly 4 layout files (2 bedroom + 1 full bath + 1 half bath)
            - YOU verify the numbers match

            ✅ **Tax History Validation (YOU CHECK YEARS):**
            - Read the input file and LIST all tax years yourself
            - Read the tax_*.json files and LIST what years exist
            - YOU verify every year is present, no duplicates, no missing years

            ✅ **Owner Validation (YOU VERIFY OWNERS):**
            - Read owners_schema.json yourself and COUNT owners per property
            - Read the person_*.json/company_*.json files and COUNT how many exist  
            - YOU verify each owner has exactly one file

            ✅ **Relationship Validation (YOU CHECK LINKS):**
            - Read the sales_*.json files yourself and COUNT sales
            - Read the relationship_sales_*_person/company_*.json files and COUNT relationships
            - YOU verify each sale has relationship files linking to owners, ONLY and ONLY if owner is present

            ✅ **Address Validation (YOU CHECK COMPONENTS):**
            - Read the input file and EXTRACT the address yourself
            - Read the address.json file and CHECK each component
            - YOU verify street_number, street_name, etc. are properly extracted

            STEP 3: **YOUR DECISION BASED ON YOUR ACTUAL CHECKING**

            🟢 **STATUS: ACCEPTED** IF:
            - YOU verified layout counts match input (your counting, not generator's promise)
            - YOU verified all tax years present (your checking, not generator's promise)  
            - YOU verified all owners extracted (your verification, not generator's promise)
            - YOU verified relationships exist (your checking, not generator's promise)
            - YOU verified addresses extracted (your validation, not generator's promise)

            🔴 **STATUS: REJECTED** IF:
            - YOU found layout count mismatches (specify which property and what you found)
            - YOU found missing tax years (specify which years and which property)
            - YOU found missing owners (specify which owners and which property)
            - YOU found missing relationships (specify which sales lack relationships)
            - YOU found address extraction issues (specify which components are wrong)

            📝 **RESPONSE FORMAT:**

            **IF EVERYTHING IS CORRECT:**
            ```
            **STATUS: ACCEPTED**

            Validation completed successfully. I personally verified:
            - Layout counts match input room counts
            - All tax years from input are present  
            - All owners from schema are extracted
            - All sales have proper relationship files ONLY if owner is present
            - Address components are correctly extracted
            ```

            **IF ISSUES ARE FOUND:**
            ```
            **STATUS: REJECTED**


            ⚠️ **NEVER give "action plans" or "if you find issues" instructions**
            ⚠️ **NEVER say "No action needed" - just ACCEPT if validation passes**
            ⚠️ **ALWAYS REPLY WITH SPECIFIC ISSUES FOUND ONLY IF STATUS: REJECTED ** - not potential future problems

            ⚠️ **CRITICAL RULES:**
            - **YOU do the validation work yourself using tools**
            - **YOU read files yourself and compare data**  
            - **YOU make the accept/reject decision based on your actual findings**
            - **NO giving orders to generator** - you're the one checking
            - **NO "ensure this" or "verify that"** - YOU ensure and verify
            - **NO "action plans" or future instructions** - just ACCEPT or REJECT
            - **NO "no action needed" messages** - if validation passes, just ACCEPT
            - **BE SPECIFIC** about actual problems found, not potential future issues

            🚀 **START:** Use your tools to read sample files and do the validation work yourself.
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
            raise Exception(
                f"{agent_name} timed out after {timeout_seconds} seconds of inactivity - restarting generation")
        except Exception as e:
            logger.error(f"     ❌ {agent_name} error: {str(e)}")
            return f"{agent_name} error on turn {turn}: {str(e)}"


def cleanup_owners_directory():
    """Clean up the owners and data directories at the start of workflow"""
    directories_to_cleanup = [
        ("owners", os.path.join(BASE_DIR, "owners")),
        ("data", os.path.join(BASE_DIR, "data"))
    ]

    for dir_name, dir_path in directories_to_cleanup:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path)
                logger.info(f"🗑️ Cleaned up existing {dir_name} directory: {dir_path}")
                print_status(f"Cleaned up existing {dir_name} directory")
            except Exception as e:
                logger.warning(f"⚠️ Could not clean up {dir_name} directory: {e}")
                print_status(f"Warning: Could not clean up {dir_name} directory: {e}")
        else:
            logger.info(f"📁 {dir_name.capitalize()} directory does not exist, no cleanup needed")

        # Create fresh directory
        try:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"📁 Created fresh {dir_name} directory: {dir_path}")
        except Exception as e:
            logger.error(f"❌ Could not create {dir_name} directory: {e}")
            raise


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
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Error fetching from {gateway}: {e}")
            continue

    logger.error(f"Failed to fetch schema from IPFS CID {cid} from all gateways")
    return None


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, ConnectionError, TimeoutError, json.JSONDecodeError),
    max_tries=3,
    max_time=120,  # 2 minutes total
    on_backoff=lambda details: logger.warning(
        f"🔄 County CID fetch failed, retrying in {details['wait']:.1f}s (attempt {details['tries']})"),
    on_giveup=lambda details: logger.error(f"💥 County CID fetch failed after {details['tries']} attempts")
)
def fetch_county_data_group_cid():
    """Fetch the county data group CID from the schema manifest API"""
    manifest_url = "https://lexicon.elephant.xyz/json-schemas/schema-manifest.json"

    try:
        logger.info(f"🔍 Fetching schema manifest from: {manifest_url}")
        response = requests.get(manifest_url, timeout=30)
        response.raise_for_status()

        manifest_data = response.json()
        logger.info("✅ Successfully fetched schema manifest")

        # Extract County data group CID
        if "County" in manifest_data:
            county_cid = manifest_data["County"]["ipfsCid"]
            logger.info(f"📋 Found County data group CID: {county_cid}")
            return county_cid
        else:
            error_msg = "❌ County entry not found in schema manifest"
            logger.error(error_msg)
            raise ValueError(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"❌ Error fetching schema manifest: {e}"
        logger.error(error_msg)
        raise ConnectionError(error_msg)
    except json.JSONDecodeError as e:
        error_msg = f"❌ Error parsing schema manifest JSON: {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"❌ Unexpected error fetching schema manifest: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def run_cli_validator(data_dir: str = "data", county_data_group_cid: str = None) -> tuple[bool, str, str]:
    """
    Run the CLI validation command and return results
    Returns: (success: bool, error_details: str, error_hash: str)
    """
    if not county_data_group_cid:
        error_msg = "County data group CID not provided to CLI validator"
        logger.error(error_msg)
        error_hash = hashlib.md5(error_msg.encode()).hexdigest()
        return False, error_msg, error_hash

    logger.info(f"🏛️ Using County CID: {county_data_group_cid}")

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

                normalized_path = file_path.replace('\\', '/')
                path_parts = normalized_path.split('/')

                if len(path_parts) >= 2:
                    old_folder_name = path_parts[-2]
                    if old_folder_name not in folder_mapping:
                        folder_mapping[old_folder_name] = property_cid
                        logger.info(f"   📋 Mapping: {old_folder_name} -> {property_cid}")
                else:
                    logger.warning(f"   ⚠️ Invalid path format: {file_path}")
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

                            if "relation" in file_name.lower():
                                logger.info(f"   🔗 Skipping relationship file: {file_name}")
                                continue

                            try:
                                # Read JSON file
                                with open(json_file_path, 'r', encoding='utf-8') as f:
                                    json_data = json.load(f)

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
                ["npx", "-y", "@elephant-xyz/cli@1.12.0", "validate-and-upload", "submit", "--dry-run", "--output-csv",
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
                        normalized_path = error_path.replace('\\', '/')
                        field_name = normalized_path.split('/')[-1]

                        # Create formatted error with field name
                        if field_name.startswith('property_has_'):
                            # Extract the type after 'property_has_'
                            info_type = field_name.replace('property_has_', '')
                            formatted_error = f"{info_type.capitalize()} information are missing"
                        else:
                            # Create formatted error with field name for other errors
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
    sales_person_relations = [f for f in json_files if f.startswith('relationship_sales') and 'person' in f]
    relationship_files.extend(sales_person_relations)
    sales_company_relations = [f for f in json_files if f.startswith('relationship_sales') and 'company' in f]
    relationship_files.extend(sales_company_relations)
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
    structure_files = [f for f in json_files if f.startswith('structure')]
    utility_files = [f for f in json_files if f.startswith('utility')]

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
        (flood_files, "flood_storm_information"),
        (structure_files, "structure"),
        (utility_files, "utility")
    ]

    for files_list, entity_type in relationship_mappings:
        for file in files_list:
            # Extract number suffix if present (e.g., tax_1.json -> _1)
            base_name = file.replace('.json', '')
            if base_name.startswith(entity_type):
                if len(base_name) > len(entity_type):
                    suffix = base_name[len(entity_type):]  # Get everything after the entity type
                else:
                    suffix = ''  # Just the entity type, no suffix
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
        "property_has_file": None,
        "property_has_structure": None,
        "property_has_utility": None,
        "sales_history_has_person": None,
        "sales_history_has_company": None,
    }

    # Categorize relationship files
    person_relationships = []
    company_relationships = []
    tax_relationships = []
    sales_relationships = []
    layout_relationships = []
    sales_person_relationships = []
    sales_company_relationships = []

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
        elif "property_utility" in rel_file:
            all_relationships["property_has_utility"] = ipld_ref
        elif "property_structure" in rel_file:
            all_relationships["property_has_structure"] = ipld_ref
        elif "relationship_sales" in rel_file and "person" in rel_file:
            sales_person_relationships.append(ipld_ref)
        elif "relationship_sales_company" in rel_file and "company" in rel_file:
            sales_company_relationships.append(ipld_ref)

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
    if sales_person_relationships:
        all_relationships["sales_history_has_person"] = sales_person_relationships
    if sales_company_relationships:
        all_relationships["sales_history_has_company"] = sales_company_relationships

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
    print_running("Owner Analysis")

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
    print_running("Address Matching")

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
    print_running("Data Extraction & Validation")

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


async def structure_extraction_node(state: WorkflowState) -> WorkflowState:
    """Enhanced Node 2.5: Handle structure, utility, and layout extraction with validation"""
    logger.info(
        f"=== Starting Node 2.5: Structure Extraction & Validation with Evaluator (Attempt {state['retry_count'] + 1}) ===")

    if state['current_node'] != 'structure_extraction':
        state['retry_count'] = 0
    state['current_node'] = 'structure_extraction'
    print_running("Structure Extraction")

    # Check if already complete
    structure_path = os.path.join(BASE_DIR, "owners", "structure_data.json")
    utility_path = os.path.join(BASE_DIR, "owners", "utility_data.json")
    layout_path = os.path.join(BASE_DIR, "owners", "layout_data.json")

    # Check if any of the files exist (flexible completion check)
    existing_files = []
    if os.path.exists(structure_path):
        existing_files.append("structure_data.json")
    if os.path.exists(utility_path):
        existing_files.append("utility_data.json")
    if os.path.exists(layout_path):
        existing_files.append("layout_data.json")

    if len(existing_files) >= 1:  # At least one file should exist
        logger.info(f"Structure extraction already complete - found: {', '.join(existing_files)}")
        state['structure_extraction_complete'] = True
        return state

    # Create the generator-evaluator pair
    structure_gen_eval_pair = StructureGeneratorEvaluatorPair(
        state=state,
        model=state['model'],
        tools=state['tools'],
        schemas=state['schemas']
    )

    try:
        # Run the feedback loop
        updated_state = await structure_gen_eval_pair.run_feedback_loop()

        # Update the original state with results
        state.update(updated_state)

        if state['structure_extraction_complete']:
            logger.info("✅ Structure Generator-Evaluator completed successfully - data extracted and validated")
        else:
            logger.warning("⚠️ Structure Generator-Evaluator completed but not all data was properly extracted")

    except Exception as e:
        logger.error(f"Error in structure generator-evaluator pair: {e}")
        state['structure_extraction_complete'] = False
        state['validation_errors'].append(str(e))

    return state


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
        return "structure_extraction"  # Go to structure extraction next
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying address matching node (attempt {state['retry_count']}/{state['max_retries']})")
        return "address_matching"
    else:
        logger.error("Max retries reached for address matching node")
        return "structure_extraction"


def should_retry_structure_extraction(state: WorkflowState) -> str:
    """Determine if structure extraction node should retry"""

    # ✅ Check for timeout first
    if should_restart_due_to_timeout(state):
        logger.warning("⏰ Structure extraction timeout detected - restarting")
        state['generation_restart_count'] += 1
        state['last_agent_activity'] = time.time()
        return "structure_extraction"  # Restart same node

    # Normal completion check
    if state.get('structure_extraction_complete', False):
        state['retry_count'] = 0  # Reset for next node
        return "extraction"  # Move to extraction next
    elif state['retry_count'] < state['max_retries']:
        state['retry_count'] += 1
        logger.warning(f"Retrying structure extraction node (attempt {state['retry_count']}/{state['max_retries']})")
        return "structure_extraction"
    else:
        logger.error("Max retries reached for structure extraction node")
        return "extraction"


@backoff.on_exception(
    backoff.expo,
    (git.GitCommandError, ConnectionError, TimeoutError),
    max_tries=3,
    max_time=300,  # 5 minutes total
    on_backoff=lambda details: logger.warning(
        f"🔄 Git clone failed, retrying in {details['wait']:.1f}s (attempt {details['tries']})"),
    on_giveup=lambda details: logger.error(f"💥 Git clone failed after {details['tries']} attempts")
)
def download_scripts_from_github():
    """Download scripts from GitHub repository with retry logic"""
    repo_url = "https://github.com/elephant-xyz/county-data-extraction-scripts"

    logger.info(f"🔄 Checking GitHub repository: {repo_url}")

    # Read county name from seed.csv
    seed_csv_path = os.path.join(BASE_DIR, "seed.csv")
    county_name = None

    if os.path.exists(seed_csv_path):
        try:
            import pandas as pd
            seed_df = pd.read_csv(seed_csv_path)
            if 'County' in seed_df.columns and len(seed_df) > 0:
                county_name = str(seed_df['County'].iloc[0]).strip()
                logger.info(f"📍 Found county: {county_name}")
            else:
                logger.error("❌ No 'County' column found in seed.csv or file is empty")
                return False
        except Exception as e:
            logger.error(f"❌ Error reading seed.csv: {e}")
            return False
    else:
        logger.error("❌ seed.csv file not found")
        return False

    if not county_name:
        logger.error("❌ Could not determine county name")
        return False

    try:
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"📁 Cloning repository to temporary directory...")

            # Clone with shallow clone for speed and timeout settings
            repo = git.Repo.clone_from(
                repo_url,
                temp_dir,
                multi_options=[
                    '--depth=1',  # Shallow clone
                    '--single-branch',
                    '--config http.timeout=60',
                    '--config http.postBuffer=524288000'
                ],
                allow_unsafe_options=True
            )
            logger.info("✅ Repository cloned successfully")

            # Check county-specific directory
            county_dir = os.path.join(temp_dir, county_name)
            logger.info(f"🔍 Looking for scripts in: {county_name}/")

            if os.path.exists(county_dir) and os.path.isdir(county_dir):
                # Copy scripts from county directory
                local_scripts_dir = os.path.join(BASE_DIR, "scripts")
                os.makedirs(local_scripts_dir, exist_ok=True)

                copied_files = []
                for file in os.listdir(county_dir):
                    if file.endswith('.py'):
                        src = os.path.join(county_dir, file)
                        dst = os.path.join(local_scripts_dir, file)
                        shutil.copy2(src, dst)
                        copied_files.append(file)
                        logger.info(f"📄 Copied: {file}")

                if copied_files:
                    logger.info(f"✅ Downloaded {len(copied_files)} scripts from {county_name}/ directory")
                    return True
                else:
                    logger.warning(f"⚠️ No Python scripts found in {county_name}/ directory")
                    return False
            else:
                logger.warning(f"⚠️ No '{county_name}/' directory found in repository")
                return False

    except Exception as e:
        # Re-raise for backoff to handle
        logger.error(f"❌ Error during git clone: {e}")
        raise


async def run_three_node_workflow():
    """Main function to run the two-node workflow with retry logic"""

    logger.info("Fetching County data group CID from schema manifest...")
    try:
        county_data_group_cid = fetch_county_data_group_cid()
        logger.info(f"✅ Successfully retrieved County CID: {county_data_group_cid}")
        print_status(f"County CID retrieved: {county_data_group_cid}")
    except (ConnectionError, ValueError, RuntimeError) as e:
        error_msg = f"Failed to fetch County data group CID: {str(e)}"
        logger.error(error_msg)
        print_status(f"CRITICAL ERROR: {error_msg}")
        raise SystemExit(f"Workflow failed: {error_msg}")

    if not county_data_group_cid:
        error_msg = "County data group CID is empty - exiting"
        logger.error(error_msg)
        print_status(f"CRITICAL ERROR: {error_msg}")
        raise SystemExit(f"Workflow failed: {error_msg}")

    # Load schemas from IPFS
    logger.info("Downloading scripts from GitHub repository...")
    if not download_scripts_from_github():
        logger.error("Failed to download scripts from GitHub repository - exiting")

    logger.info("Loading schemas from IPFS and saving to ./schemas/ directory...")
    schemas, stub_files = load_schemas_from_ipfs(save_to_disk=True)

    if not schemas or not stub_files:
        logger.error("Failed to load schemas from IPFS")
        return

    cleanup_owners_directory()

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
        structure_extraction_complete=False,
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
        county_data_group_cid=county_data_group_cid,
    )

    # Create the workflow graph
    workflow = StateGraph(WorkflowState)

    # Add nodes
    workflow.add_node("owner_analysis", owner_analysis_node)
    workflow.add_node("address_matching", address_matching_node)
    workflow.add_node("structure_extraction", structure_extraction_node)
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
            "structure_extraction": "structure_extraction"
        }
    )

    workflow.add_conditional_edges(
        "structure_extraction",
        should_retry_structure_extraction,
        {
            "structure_extraction": "structure_extraction",  # Retry same node
            "extraction": "extraction"  # Move to extraction
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
            print_status("Workflow completed successfully - all tasks completed")
            logger.info("✅ Workflow completed successfully - all tasks completed")
        else:
            print_status("Workflow completed with incomplete tasks")
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