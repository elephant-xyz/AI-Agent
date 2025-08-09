#!/usr/bin/env python3
"""CLI entry point for test-evaluator-agent"""

import sys
import asyncio
import argparse
from .main import main as main_async
from .setup_mcp import setup_mcp_code_executor, setup_uv_venv, check_dependencies


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Test Evaluator Agent")
    parser.add_argument("--setup", action="store_true", help="Setup UV venv and mcp_code_executor dependencies")
    parser.add_argument("--transform", action="store_true", help="Run in simple mode: download scripts → run scripts → CLI validation (no AI agents)")
    parser.add_argument("--input-zip", type=str,
                        help="Path to ZIP file containing unnormalized_address class, property_seed.json class and data needs to be transformed")
    parser.add_argument("--output-zip", type=str,
                        help="Output ZIP filename (e.g., my_output.zip). If not specified, auto-generates based on input.")


    args = parser.parse_args()

    if args.setup:
        print("Setting up dependencies...")
        if not check_dependencies():
            sys.exit(1)
        if not setup_uv_venv():
            sys.exit(1)
        if not setup_mcp_code_executor():
            sys.exit(1)
        print("Setup completed successfully!")
        return

    # Check if dependencies exist, if not, set them up automatically
    from pathlib import Path
    mcp_dir = Path.cwd() / "mcp_code_executor"
    venv_dir = Path.cwd() / ".venv"

    if not venv_dir.exists() or not mcp_dir.exists():
        print("Dependencies not found, setting up automatically...")
        if not check_dependencies():
            print("Please run with --setup flag or install git, npm, and uv manually")
            sys.exit(1)
        if not venv_dir.exists() and not setup_uv_venv():
            sys.exit(1)
        if not mcp_dir.exists() and not setup_mcp_code_executor():
            sys.exit(1)

    try:
        # Pass the arguments to the main function
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()