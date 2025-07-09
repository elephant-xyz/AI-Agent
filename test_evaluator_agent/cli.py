#!/usr/bin/env python3
"""CLI entry point for test-evaluator-agent"""

import sys
import asyncio
import argparse
from .main import run_main
from .setup_mcp import setup_mcp_code_executor, check_dependencies

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Test Evaluator Agent")
    parser.add_argument("--setup", action="store_true", help="Setup mcp_code_executor dependency")
    
    args = parser.parse_args()
    
    if args.setup:
        print("Setting up mcp_code_executor...")
        if not check_dependencies():
            sys.exit(1)
        if not setup_mcp_code_executor():
            sys.exit(1)
        print("Setup completed successfully!")
        return
    
    # Check if mcp_code_executor exists, if not, set it up automatically
    from pathlib import Path
    mcp_dir = Path.cwd() / "mcp_code_executor"
    if not mcp_dir.exists():
        print("mcp_code_executor not found, setting up automatically...")
        if not check_dependencies():
            print("Please run with --setup flag or install git and npm manually")
            sys.exit(1)
        if not setup_mcp_code_executor():
            sys.exit(1)
    
    try:
        run_main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()