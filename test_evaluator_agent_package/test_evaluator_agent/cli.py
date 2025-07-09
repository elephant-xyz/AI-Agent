#!/usr/bin/env python3
"""CLI entry point for test-evaluator-agent"""

import sys
import asyncio
from .main import main as async_main

def main():
    """Main CLI entry point"""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()