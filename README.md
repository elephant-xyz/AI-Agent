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
uvx --from git+https://github.com/StaircaseAPI/agentic_ai test-evaluator-agent
```

## Usage

The agent requires specific directory structure and environment variables:

### Directory Structure
```
./input/          # Input files to process
./schemas/        # JSON schemas for validation
./data/          # Intermediate extracted data
./processed/     # Final processed data
./possible_addresses/  # Address candidates
./submit/        # Final submission format
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

1. **Data Extraction Phase**: Generator creates extraction scripts, Schema and Data Evaluators validate output
2. **Address Matching Phase**: Generator matches addresses with candidates, CLI Validator checks final output
3. **Validation**: Multiple validation layers ensure data quality and schema compliance

## Development

This package is designed to be run as a standalone tool with all dependencies managed automatically.