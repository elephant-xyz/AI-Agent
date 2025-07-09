#!/usr/bin/env python3
"""
Setup script for mcp_code_executor dependency
"""
import os
import subprocess
import sys
import shutil
from pathlib import Path

def setup_mcp_code_executor():
    """Clone and install mcp_code_executor if not already present"""
    current_dir = Path.cwd()
    mcp_dir = current_dir / "mcp_code_executor"
    
    # Check if mcp_code_executor already exists
    if mcp_dir.exists():
        print("mcp_code_executor already exists, skipping clone...")
        return True
    
    try:
        # Clone the repository
        print("Cloning mcp_code_executor...")
        subprocess.run([
            "git", "clone", 
            "https://github.com/bazinga012/mcp_code_executor.git"
        ], check=True, cwd=current_dir)
        
        # Install npm dependencies
        print("Installing npm dependencies...")
        subprocess.run([
            "npm", "install"
        ], check=True, cwd=mcp_dir)
        
        print("mcp_code_executor setup completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error setting up mcp_code_executor: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def check_dependencies():
    """Check if git and npm are available"""
    try:
        subprocess.run(["git", "--version"], capture_output=True, check=True)
        subprocess.run(["npm", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: git and npm are required but not found in PATH")
        print("Please install git and Node.js/npm before running this package")
        return False

if __name__ == "__main__":
    if not check_dependencies():
        sys.exit(1)
    
    if not setup_mcp_code_executor():
        sys.exit(1)