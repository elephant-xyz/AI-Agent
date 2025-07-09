import asyncio
import os
import sys
import json
import logging
import requests
import shutil
import subprocess
from typing import Dict, Any, List, TypedDict
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
    "person.json": "bafkreiasr6575uesracnkl7ayoru3qkjf43ggrnylkzqfoacnovtbf2j3m",
    "company.json": "bafkreiaeqwyz5ntt6mkhajym2ptfipyf2xmruvyftfazq7ff7e6hrrh6li",
    "property.json": "bafkreifqv4eogkjqval5spoiqey2gvolnactxakzxw2sfv2akiqdlxd4fq",
    "address.json": "bafkreihez5udn5ho7ihkbirsdxbkq6grwu3uorakhksys6ucvokkbfdoru",
    "tax.json": "bafkreic7jtiilcurjfv6q3uz6qyhrbwjsearxqefzwrbibd3plttwqfma4",
    "lot.json": "bafkreibl6rwbx5nyeitdbkvn2l5ygbav42mav3s5q4gpj3tbyntl6p5nny",
    "sales.json": "bafkreialispvbk6p3sxprp5ydmqcqnctc74kk4u4kffkkpnzicicmcgtna",
    "layout.json": "bafkreigypa6mroe77rr63qumofeflllootajf7pniytz5273aw574mhwjm",
    "flood_storm_information.json": "bafkreibqdyph2kduvbu5g45frtwlvhagh45guvcwgaowaf7mn6dgnyxxyi",
    "data_group.json": "bafkreidxxf27myb7d5wtuxblkwadt37jj4oyhfd35j6g7icrz2oyecsnqq"
}


class WorkflowState(TypedDict):
    """State shared between nodes"""
    input_files: List[str]
    input_files_count: int
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

    async def run_feedback_loop(self) -> WorkflowState:
        """Run FOUR AGENTS: Generator + Schema Evaluator + Data Evaluator + CLI Validator"""

        logger.info("🔄 Starting Generator + Schema Evaluator + Data Evaluator + CLI Validator CONVERSATION")
        logger.info(f"💬 Using shared thread: {self.shared_thread_id}")
        logger.info(f"🎭 Four agents: Generator, Schema Evaluator, Data Evaluator, CLI Validator")

        # Create THREE separate LLM agents
        generator_agent = await self._create_generator_agent()
        schema_evaluator_agent = await self._create_schema_evaluator_agent()
        data_evaluator_agent = await self._create_data_evaluator_agent()

        conversation_turn = 0
        schema_accepted = False
        data_accepted = False
        cli_accepted = False

        # GENERATOR STARTS: Create initial script
        logger.info("🤖 Generator starts the conversation...")

        await self._agent_speak(
            agent=generator_agent,
            agent_name="GENERATOR",
            turn=1,
            user_instruction="Start by creating the extraction script and processing all input files"
        )

        # Continue conversation until all evaluators accept or max turns
        while conversation_turn < self.max_conversation_turns:
            conversation_turn += 1

            logger.info(f"💬 Conversation Turn {conversation_turn}/{self.max_conversation_turns}")

            # SCHEMA EVALUATOR RESPONDS
            logger.info("🔍 Schema Evaluator reviews Generator's work...")
            schema_message = await self._agent_speak(
                agent=schema_evaluator_agent,
                agent_name="SCHEMA_EVALUATOR",
                turn=conversation_turn,
                user_instruction="Review the Generator's extraction work and validate schema compliance"
            )

            schema_accepted = "STATUS: ACCEPTED" in schema_message
            logger.info(f"📊 Schema Evaluator decision: {'ACCEPTED' if schema_accepted else 'NEEDS FIXES'}")

            # DATA EVALUATOR RESPONDS
            logger.info("📊 Data Evaluator reviews Generator's work...")
            data_message = await self._agent_speak(
                agent=data_evaluator_agent,
                agent_name="DATA_EVALUATOR",
                turn=conversation_turn,
                user_instruction="Review the Generator's extraction work and validate data completeness by comparing with sample input files and make sure validation points are met"
            )

            data_accepted = "STATUS: ACCEPTED" in data_message
            logger.info(f"📊 Data Evaluator decision: {'ACCEPTED' if data_accepted else 'NEEDS FIXES'}")

            # CLI VALIDATOR RUNS (non-LLM function)
            logger.info("⚡ CLI Validator running validation...")
            cli_success, cli_errors = run_cli_validator("data")

            if cli_success:
                cli_message = "STATUS: ACCEPTED - CLI validation passed successfully"
                cli_accepted = True
                logger.info("✅ CLI Validator decision: ACCEPTED")
            else:
                cli_message = f"STATUS: REJECTED - CLI validation failed with errors:\n{cli_errors}"
                cli_accepted = False
                logger.info("❌ CLI Validator decision: NEEDS FIXES")

            # Check if ALL validators accepted
            if schema_accepted and data_accepted and cli_accepted:
                logger.info("✅ Conversation completed successfully - ALL validators approved!")
                self.state['extraction_complete'] = True
                self.state['all_files_processed'] = True
                break

            # GENERATOR RESPONDS: Sees feedback from ALL validators and fixes issues
            logger.info("🤖 Generator responds to all validators' feedback...")

            feedback_summary = ""
            if not schema_accepted:
                feedback_summary += f"Schema Evaluator feedback: {schema_message}\n\n"
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
                    2. Fix ALL the specific errors mentioned above
                    3. Test your changes by running the script
                    
                DO NOT just acknowledge - TAKE ACTION NOW with tools to fix these issues.""")



            logger.info(f"🔄 Turn {conversation_turn} complete, continuing conversation...")

        # Conversation ended
        final_status = "ACCEPTED" if (schema_accepted and data_accepted and cli_accepted) else "PARTIAL"

        if final_status != "ACCEPTED":
            logger.warning(f"⚠️ Conversation ended without full success after {self.max_conversation_turns} turns")
            logger.warning(
                f"Schema: {'✅' if schema_accepted else '❌'}, Data: {'✅' if data_accepted else '❌'}, CLI: {'✅' if cli_accepted else '❌'}")
            self.state['all_files_processed'] = False

        logger.info(f"💬 Conversation completed with status: {final_status}")
        return self.state

    async def _create_generator_agent(self):
        """Create Generator agent with YOUR EXACT PROMPT"""

        generator_prompt = f"""
            You are the GENERATOR for input data extraction in a conversation with an EVALUATORS.

            🎯 YOUR MISSION: Process ALL {self.state['input_files_count']} files from ./input/ folder 

            📂 REQUIRED OUTPUT STRUCTURE: this output should be generated through a data_extraction.py script
            ./data/[property_parcel_id]/property.json
            ./data/[property_parcel_id]/address.json
            ./data/[property_parcel_id]/lot.json
            ./data/[property_parcel_id]/sales_1.json
            ./data/[property_parcel_id]/tax_1.json
            ./data/[property_parcel_id]/layout_1.json (It represents number of space_type inside the property) you need to know what space_type you have from the schema and apply them  if found in the property data
            ./data/[property_parcel_id]/flood_storm_information.json
            ./data/[property_parcel_id]/person.json   or ./data/[property_parcel_id]/company.json
            you must be able to identify if the owner is a company or a person 
            and if multiple persons you should have 
            ./data/[property_parcel_id]/person_1.json
            ./data/[property_parcel_id]/person_2.json

            if there is multiple sales or layouts, create multiple files with suffixes:
            ./data/[property_parcel_id]/sales_1.json
            ./data/[property_parcel_id]/sales_2.json
            ./data/[property_parcel_id]/layout_1.json
            ./data/[property_parcel_id]/flood_storm_information.json


            🔗 RELATIONSHIP FILES MAPPING:
            Create relationship files with these exact structures:

            relationship_person_property.json (person → property) or relationship_company_property.json (company → property):
            {{
                "from": {{
                    "/": "./person.json"
                }},
                "to": {{
                    "/": "./property.json"
                }}
            }}
            or 
            {{
                "from": {{
                    "/": "./company.json"
                }},
                "to": {{
                    "/": "./property.json"
                }}
            }}

            relationship_property_address.json (property → address):
            {{
                "from": {{
                    "/": "./property.json"
                }},
                "to": {{
                    "/": "./address.json"
                }}
            }}

            and so on , until you create relationship files for :
                relationship_person_property.json (person → property)
                relationship_property_address.json (property → address)
                relationship_property_lot.json (property → lot)
                relationship_property_tax_1.json (property → tax_1)
                relationship_property_tax_2.json (property → tax_2)
                relationship_property_sales_1.json(property → sales_1)
                relationship_property_sales_2.json(property → sales_2)
                relationship_property_layout_1.json (property → layout_1)
                relationship_property_layout_2.json (property → layout_2)
                relationship_property_flood_storm_information.json (property → flood_storm_information)
                and so on ...

            Then create a file named county_data_group.json in IPLD format following the data_group.json schema For each property
            to add all the relationships relative path inside data_group.json For each property, you should have a county_data_group.json file
            as example: 
                {{
                  "label": "County",
                  "relationships": {{
                    "person_has_property": [
                      {{
                        "/": "./relationship_Jason_property.json"
                      }},
                      {{
                        "/": "./relationship_Miryam_property.json"
                      }}
                    ]
                    }} AND SO ON
                }}



            📋 SCHEMAS TO FOLLOW:
            All schemas are available in the ./schemas/ directory. Read each schema file to understand the required structure

            🔧 YOUR TASKS:
            1. READ all schema files from ./schemas/ directory to understand data structures
            2. Analyze input structure (examine 3-5 sample files)
            3. Generate a universal extraction script: `scripts/data_extractor.py` that Map html data to the schemas, and save the extracted data as JSON files in the `data` folder.
            4. Execute the script to process ALL input files
            5. Compare 50 samples of extracted data with the corresponding input file to make sure if ALL data is extracted successfully
            6. IMPORTANT: If NOT all data is extracted successfully, modify the `html_data_extractor.py` script to fix the extraction logic and re-execute the script.

            ⚠️ CRITICAL RULES:
            - Process EVERY input file in ./input/ directory
            - DONOT QUITE until extraction script generates county_data_group.json following schema above with label 
            - Create exactly JSON files per property
            - Follow the exact schema structure provided
            - Handle missing data gracefully (use null/empty values)
            - Log progress and errors clearly

            🗣️ CONVERSATION RULES:
            - You are having a conversation with THREE VALIDATORS: Schema Evaluator, Data Evaluator, and CLI Validator
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

    async def _create_schema_evaluator_agent(self):
        """Create Schema Evaluator agent - validates schema compliance"""

        schema_evaluator_prompt = f"""
        You are the SCHEMA EVALUATOR for input extraction validation in a conversation with a GENERATOR and DATA EVALUATOR.

        🔍 YOUR MISSION: Validate schema compliance and JSON structure

        📋 VALIDATION TASKS:
            1. CREATE A COMPREHENSIVE VALIDATION SCRIPT: scripts/schema_validator.py
            2. The script must:
               - Check that ./data/ contains {self.state['input_files_count']} property folders
               - Verify each property has all required JSON files
               - Validate each JSON file against its schema using jsonschema library
               - Test data format correctness and schema compliance
               - Generate detailed validation report
               - Make sure no missing Attributes inside the JSON files, any missing Attributes should be represented as null

            3. EXECUTE the validation script
            4. ANALYZE the results and provide specific feedback
        📊 SCHEMAS TO VALIDATE AGAINST:
            All schemas are located in ./schemas/ directory. Your validation script should:
            1. Read schema files from ./schemas/ directory
            2. Use jsonschema library to validate each extracted JSON file
            3. Check schema compliance for all data files
            4. Verify JSON structure and data types

        📝 RESPONSE FORMAT:
        Start with: STATUS: ACCEPTED or STATUS: REJECTED

        Then explain:
            - Schema compliance results
            - JSON structure issues
            - Data type validation errors  
            - Specific schema fixes needed


        🗣️ CONVERSATION RULES:
        - You focus ONLY on schema compliance and JSON structure
        - Reference what the GENERATOR did in your responses
        - Give specific feedback about schema violations
        - Don't worry about data completeness - that's the Data Evaluator's job

        🚀 START: Check schema compliance and give your feedback.
        """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=schema_evaluator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _create_data_evaluator_agent(self):
        """Create Data Evaluator agent - validates data completeness"""

        data_evaluator_prompt = f"""
        You are the DATA EVALUATOR for input extraction validation in a conversation with a GENERATOR and SCHEMA EVALUATOR,Your very restricted about your Validation.

        🔍 YOUR MISSION: Validate data completeness and accuracy check the extraction script if there is any TODO ask the generator to fix
        CRITICAL: DONOT ACCEPT IF county_data_group.json file not found in each property
        your job is :
        Take sample of 3-5 properties from ./data/ directory and check if the extraction script is generated,  You should accept the data unless all the following criteria are met :
            1- Your job to make sure all the data has been extracted successfully by Comparing extracted data with original input files
            2- If county_data_group.json file is missing or not in IPLD format or not following data_group.json schema ask generator to fix the script to include for each property
            3- presence of layouts, bedrooms,fullbathrooms and half bathroom, that were extracted for all available space types example 2 bedroom, 1 full batroom and 1 half bathroom results in 4 layout files
            4- All Tax history was extracted for all available years and Ensure no tax years were missed
            5- All sales history was extracted for all available years and Ensure no sales years were missed
            6- Validate all property details are captured
            7- Check that multiple layouts/sales/persons are properly numbered
            8- IF there is any TODO in the extraction script ask the generator to fix the script
            9- All relationships files are being filled correctly in the county_data_group.json file, and missing relationships are being represented as null

        📝 RESPONSE FORMAT:
        Start with: STATUS: ACCEPTED or STATUS: REJECTED

        Then explain:
        - Data completeness results
        - Missing sales/tax years
        - Incomplete person/company data
        - Missing layout information
        - Specific data extraction fixes needed

        🗣️ CONVERSATION RULES:
        - You focus ONLY on data completeness and accuracy
        - Reference what the GENERATOR extracted in your responses
        - Give specific feedback about missing or incomplete data
        - Don't worry about schema compliance - that's the Schema Evaluator's job

        🚀 START: Check data completeness and give your feedback.
        """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=data_evaluator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _agent_speak(self, agent, agent_name: str, turn: int, user_instruction: str) -> str:
        """Have an agent speak in the conversation - they see all previous messages"""

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
            # Stream the agent's speech with full logging
            async for event in agent.astream_events({"messages": messages}, config, version="v1"):
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

                    # Capture the agent's response
                    if isinstance(output, dict) and 'messages' in output:
                        last_message = output['messages'][-1] if output['messages'] else None
                        if last_message and hasattr(last_message, 'content'):
                            agent_response = last_message.content
                    elif hasattr(output, 'content'):
                        agent_response = output.content
                    elif isinstance(output, str):
                        agent_response = output

            logger.info(f"     ✅ {agent_name} finished speaking")
            logger.info(f"     🔧 Tools used: {', '.join(tool_calls_made) if tool_calls_made else 'None'}")
            logger.info(f"     📄 Response length: {len(agent_response)} characters")
            logger.info(f"     🗨️ {agent_name} said: {agent_response}...")

            return agent_response or f"{agent_name} completed turn {turn} (no response captured)"

        except Exception as e:
            logger.error(f"     ❌ {agent_name} error: {str(e)}")
            return f"{agent_name} error on turn {turn}: {str(e)}"


class AddressMatchingGeneratorEvaluatorPair:
    """Generator and Evaluator for address matching node with CLI validation"""

    def __init__(self, state: WorkflowState, model, tools):
        self.state = state
        self.model = model
        self.tools = tools
        self.max_conversation_turns = 15  # Fewer turns since this is simpler than extraction
        self.shared_checkpointer = InMemorySaver()  # Shared between agents
        self.shared_thread_id = "address-matching-conversation-1"  # Same thread for all

    async def run_feedback_loop(self) -> WorkflowState:
        """Run TWO AGENTS: Generator + CLI Validator"""

        logger.info("🔄 Starting Generator + CLI Validator CONVERSATION for Address Matching")
        logger.info(f"💬 Using shared thread: {self.shared_thread_id}")
        logger.info(f"🎭 Two agents: Address Generator, CLI Validator")

        # Create the generator agent
        generator_agent = await self._create_address_generator_agent()

        conversation_turn = 0
        cli_accepted = False

        # GENERATOR STARTS: Create initial address matching script
        logger.info("🤖 Address Generator starts the conversation...")

        await self._agent_speak(
            agent=generator_agent,
            agent_name="ADDRESS_GENERATOR",
            turn=1,
            user_instruction="Start by creating the address matching script and processing all properties"
        )

        # Continue conversation until CLI validator accepts or max turns
        while conversation_turn < self.max_conversation_turns:
            conversation_turn += 1

            logger.info(f"💬 Conversation Turn {conversation_turn}/{self.max_conversation_turns}")

            # CLI VALIDATOR RUNS (non-LLM function)
            logger.info("⚡ CLI Validator running validation...")
            cli_success, cli_errors = run_cli_validator("processed")

            if cli_success:
                cli_message = "STATUS: ACCEPTED - CLI validation passed successfully"
                cli_accepted = True
                logger.info("✅ CLI Validator decision: ACCEPTED")
                break
            else:
                cli_message = f"STATUS: REJECTED - CLI validation failed with errors:\n{cli_errors}"
                cli_accepted = False
                logger.info("❌ CLI Validator decision: NEEDS FIXES")

            # GENERATOR RESPONDS: Sees feedback from CLI validator and fixes issues
            logger.info("🤖 Address Generator responds to CLI validator feedback...")

            await self._agent_speak(
                agent=generator_agent,
                agent_name="ADDRESS_GENERATOR",
                turn=conversation_turn + 1,
                user_instruction=f"update address_extraction.py to fix issues found by CLI validator immediately. Work silently to Fix these specific issues:\n\n{cli_message}"
            )

            logger.info(f"🔄 Turn {conversation_turn} complete, continuing conversation...")

        # Conversation ended
        final_status = "ACCEPTED" if cli_accepted else "PARTIAL"

        if final_status != "ACCEPTED":
            logger.warning(
                f"⚠️ Address matching conversation ended without full success after {self.max_conversation_turns} turns")
            logger.warning(f"CLI: {'✅' if cli_accepted else '❌'}")
            self.state['all_addresses_matched'] = False
        else:
            self.state['address_matching_complete'] = True
            self.state['all_addresses_matched'] = True

        logger.info(f"💬 Address matching conversation completed with status: {final_status}")
        return self.state

    async def _create_address_generator_agent(self):
        """Create Address Generator agent with your exact original prompt"""

        generator_prompt = f"""
        You are an address matching specialist handling the final phase of property data processing.

        🔄 CURRENT STATUS:
            ✅ Data extraction: COMPLETE - All {self.state['input_files_count']} properties are in ./data/ directory
            🎯 Current task: Match addresses and move properties to ./processed/
            🔁 Attempt: {self.state['retry_count'] + 1} of {self.state['max_retries']}

        📂 FILE STRUCTURE:
            • ./data/[property_parcel_id]/address.json - Properties needing address matching
            • ./possible_addresses/[property_parcel_id].json - Address candidates for each property
            • ./input/[property_parcel_id] file - Source input data for address extraction
            • ./processed/[property_parcel_id]/ - Final destination after successful matching

        🎯 YOUR MISSION: Process ALL properties until ./data/ is empty by writing a script that matches addresses,  scripts/address_extraction.py.

        PHASE 1: ANALYZE THE DATA STRUCTURE
            1. Examine 3-5 samples from possible_addresses/ to understand:
               - JSON format and field structure
               - How many address candidates each property has
               - Address format variations

            2. Check corresponding input files to understand:
               - Where addresses appear in the input
               - How to extract: street number, street name, unit, city, zip

            3. Plan your matching strategy before coding

        PHASE 2: BUILD THE MATCHING SCRIPT
            Create: scripts/address_extraction.py

            The script must:
            A. For each property in ./data/:
               - Extract property_parcel_id from folder name
               - Load possible_addresses/[property_parcel_id].json (candidate addresses)
               - Parse address from input/[property_parcel_id] files
               - Extract: street_number, street_name, unit, city, zip, directionals, suffix_type

            B. Find the best address match:
               - Compare input files address with candidates in possible_addresses/
               - Use exact matching first, then fuzzy matching if needed
               - Focus on: street number + street name + unit (if applicable)

            C. When match found:
               - Update address.json with matched address data using schema: {self.state['schemas']['address.json']}
               - PRESERVE existing fields like: latitude, longitude, range, section, township
               - Move entire folder: ./data/[property_parcel_id]/ → ./processed/[property_parcel_id]/

            D. Handle failures:
               - Log properties that couldn't be matched
               - Continue processing other properties

        PHASE 3: EXECUTE AND ITERATE
            1. Run the script on ALL properties
            2. Track progress: "Processed X of {self.state['input_files_count']} properties"
            3. For failed matches:
               - Analyze why matching failed
               - Improve the script logic
               - Re-run on failed cases
            4. Repeat until ./data/ directory is completely empty

        SUCCESS CRITERIA:
            ✅ ./data/ directory is empty (all folders moved to ./processed/)
            ✅ All address.json files have proper address data
            ✅ No data loss (preserve existing non-address fields)

        EXECUTION RULES:
            • Process systematically, one property at a time
            • Report progress regularly
            • Handle errors gracefully and continue
            • Don't stop until ALL properties are processed
            • Focus ONLY on address matching (extraction is already done)

        🗣️ CONVERSATION RULES (ADDED FOR VALIDATION):
            - You are now in a conversation with a CLI VALIDATOR
            - When CLI validator gives you feedback, read it carefully and work silently to fix the issues
            - Fix the specific issues they mention and continue working
            - The CLI validator will check if your address matching is working correctly

        START with data analysis, then build the script, then execute until complete.
        """

        return create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=generator_prompt,
            checkpointer=self.shared_checkpointer
        )

    async def _agent_speak(self, agent, agent_name: str, turn: int, user_instruction: str) -> str:
        """Have an agent speak in the conversation - they see all previous messages"""

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
            # Stream the agent's speech with full logging
            async for event in agent.astream_events({"messages": messages}, config, version="v1"):
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

                    # Capture the agent's response
                    if isinstance(output, dict) and 'messages' in output:
                        last_message = output['messages'][-1] if output['messages'] else None
                        if last_message and hasattr(last_message, 'content'):
                            agent_response = last_message.content
                    elif hasattr(output, 'content'):
                        agent_response = output.content
                    elif isinstance(output, str):
                        agent_response = output

            logger.info(f"     ✅ {agent_name} finished speaking")
            logger.info(f"     🔧 Tools used: {', '.join(tool_calls_made) if tool_calls_made else 'None'}")
            logger.info(f"     📄 Response length: {len(agent_response)} characters")
            logger.info(f"     🗨️ {agent_name} said: {agent_response}...")

            return agent_response or f"{agent_name} completed turn {turn} (no response captured)"

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


def run_cli_validator(data_dir: str = "data") -> tuple[bool, str]:
    """
    Run the CLI validation command and return results
    Returns: (success: bool, error_details: str)
    """
    try:
        logger.info("📁 Creating submit directory and copying data with proper naming...")

        # Define directories
        upload_results_path = os.path.join(BASE_DIR, "upload-results.csv")
        data_dir = os.path.join(BASE_DIR, data_dir)
        submit_dir = os.path.join(BASE_DIR, "submit")

        # Create/clean submit directory
        if os.path.exists(submit_dir):
            shutil.rmtree(submit_dir)
            logger.info("🗑️ Cleaned existing submit directory")

        os.makedirs(submit_dir, exist_ok=True)
        logger.info(f"📁 Created submit directory: {submit_dir}")

        if not os.path.exists(data_dir):
            logger.error("❌ Data directory not found")
            return False, "Data directory not found"

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
            logger.warning("⚠️ uploadresults.csv not found, using original folder names")

        # Copy data to submit directory with proper naming
        copied_count = 0
        renamed_files_count = 0
        county_data_group_cid = "bafkreihnrwrxr4ummdh5tkckzf5v4o7emww7jylpjo2waf4taer3v4vhju"

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

                # Rename county_data_group.json file in the copied folder
                county_file_path = os.path.join(dst_folder_path, "county_data_group.json")
                new_file_path = os.path.join(dst_folder_path, f"{county_data_group_cid}.json")

                if os.path.exists(county_file_path):
                    os.rename(county_file_path, new_file_path)
                    logger.info(
                        f"   ✅ Renamed county_data_group.json -> {county_data_group_cid}.json in {target_folder_name}")
                    renamed_files_count += 1
                else:
                    logger.warning(f"   ⚠️ county_data_group.json not found in {target_folder_name}")

        logger.info(f"✅ Copied {copied_count} folders and renamed {renamed_files_count} county_data_group.json files")

        logger.info("🔍 Running CLI validator: npx @elephant-xyz/cli validate-and-upload submit --dry-run")

        result = subprocess.run(
            ["npx", "@elephant-xyz/cli", "validate-and-upload", "submit", "--dry-run", "--output-csv", "results.csv"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=300
        )

        # Check for submit_errors.csv file regardless of exit code
        submit_errors_path = os.path.join(BASE_DIR, "submit_errors.csv")

        if os.path.exists(submit_errors_path):
            # Read the CSV file to check for actual errors
            try:
                df = pd.read_csv(submit_errors_path)
                if len(df) > 0:
                    # There are validation errors
                    logger.warning(f"❌ CLI validation found {len(df)} errors in submit_errors.csv")

                    # Format the errors for the generator
                    error_details = "CLI Validation Errors Found:\n\n"
                    for _, row in df.iterrows():
                        error_details += f"Property: {row['property_cid']}\n"
                        error_details += f"File: {row['file_path']}\n"
                        error_details += f"Error: {row['error']}\n"
                        error_details += f"Timestamp: {row['timestamp']}\n\n"

                    return False, error_details
                else:
                    logger.info("✅ CLI validation passed - no errors in submit_errors.csv")
                    return True, ""
            except Exception as e:
                logger.error(f"Error reading submit_errors.csv: {e}")
                return False, f"Could not read submit_errors.csv: {e}"
        else:
            # No submit_errors.csv file means no errors (hopefully)
            if result.returncode == 0:
                logger.info("✅ CLI validation passed - no submit_errors.csv file found")
                return True, ""
            else:
                logger.warning("❌ CLI validation failed")
                error_output = f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
                return False, error_output

    except subprocess.TimeoutExpired:
        error_msg = "CLI validation timed out after 5 minutes"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"CLI validation error: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


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
    """Check if all files have been processed and moved to processed folder"""
    data_dir = os.path.join(BASE_DIR, "submit")

    # Otherwise check data
    if os.path.exists(data_dir):
        processed_count = len([d for d in os.listdir(data_dir)
                               if os.path.isdir(os.path.join(data_dir, d))])
        logger.info(f"Processed {processed_count} out of {state['input_files_count']} properties in data directory")
        return processed_count >= state['input_files_count']

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
                f"total: {total_handled}/{state['input_files_count']}")

    return total_handled >= state['input_files_count']


async def extraction_and_validation_node(state: WorkflowState) -> WorkflowState:
    """Enhanced Node 1: Handle data extraction with generator-evaluator pattern"""
    logger.info(
        f"=== Starting Node 1: Data Extraction & Validation with Evaluator (Attempt {state['retry_count'] + 1}) ===")

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


async def address_matching_node(state: WorkflowState) -> WorkflowState:
    """Node 2: Handle address matching with validation feedback loop"""
    if state['current_node'] != 'address_matching':
        state['retry_count'] = 0
    state['current_node'] = 'address_matching'
    logger.info(
        f"=== Starting Node 2: Address Matching & Processing with Validation (Attempt {state['retry_count'] + 1}) ===")

    # Check if already complete
    if check_address_matching_complete(state):
        logger.info("Address matching already complete")
        state['address_matching_complete'] = True
        state['all_addresses_matched'] = True
        return state

    # Create the address matching generator-evaluator pair
    address_gen_eval_pair = AddressMatchingGeneratorEvaluatorPair(
        state=state,
        model=state['model'],
        tools=state['tools']
    )

    try:
        # Run the feedback loop
        updated_state = await address_gen_eval_pair.run_feedback_loop()

        # Update the original state with results
        state.update(updated_state)

        if state['all_addresses_matched']:
            logger.info("✅ Address Generator-Validator completed successfully - all addresses matched and validated")
        else:
            logger.warning("⚠️ Address Generator-Validator completed but not all addresses were properly matched")

    except Exception as e:
        logger.error(f"Error in address matching generator-evaluator pair: {e}")
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

    # Initialize workflow state
    initial_state = WorkflowState(
        input_files=input_files,
        input_files_count=len(input_files),
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

def run_main():
    """Non-async main entry point for CLI"""
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())