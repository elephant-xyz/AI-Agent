# Test Evaluator Agent

An AI-powered data extraction evaluator agent using LangGraph for property data processing.

## Features

- Multi-agent conversation system for data extraction validation
- Schema validation using JSON Schema
- CLI validation integration
- Address matching with fuzzy logic
- Retry mechanism with configurable limits
- IPFS schema fetching and caching

## Installation

You can run this tool directly using `uvx`:

```bash
uvx --from git+https://github.com/elephant-xyz/agentic_ai test-evaluator-agent
```

## Usage

The agent requires specific directory structure and environment variables:


### pre-requisites
```
./possible_addresses/ directory should contain all possible addresses for each property has its own json file with all possible addresses from OpenAddress.
./seed.csv  # Seed CSV file for initial data
./input/  # Input files to process
./upload-results.csv # csv generated from Minting Seed data group, it contains all the Property CIDs.

```

### Agent output directory structure
```
./schemas/        # JSON schemas for validation
./data/          # Intermediate extracted data
./submit/        # containes the final output that will be submitted to the CLI Validator
```

### Environment Variables
- `MODEL_NAME`: AI model to use (default: gpt-4.1)
- `TEMPERATURE`: Model temperature (default: 0)

### Running the Agent

```bash
# Set environment variables
export MODEL_NAME=gpt-4.1
export TEMPERATURE=0

# Run the agent
test-evaluator-agent
```

## How It Works

1. **Owner Extraction Phase**: The agent generates extraction scripts based on input files and schemas and analyze the owner data to determine whether it is a valid person or company.
2. **Address Matching Phase**: Generator matches addresses with candidates, CLI Validator checks final output
3 **Data Extraction and validation Phase**: Generator creates extraction scripts, Schema and Data Evaluators validate output

## Development

This package is designed to be run as a standalone tool with all dependencies managed automatically.
