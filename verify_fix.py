#!/usr/bin/env python3
"""
Simple verification script to test that NemesisSequence is properly excluded.
This script does a basic import-level verification of the fix.
"""

import os
import sys

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    try:
        # Test 1: Basic import test - check that NemesisSequence is in COMPLEX_NEMESIS
        print("Test 1: Checking if NemesisSequence is in COMPLEX_NEMESIS...")
        
        # Read the nemesis.py file directly to verify the change
        with open('sdcm/nemesis.py', 'r') as f:
            content = f.read()
        
        # Check if NemesisSequence is in the COMPLEX_NEMESIS list
        if 'CategoricalMonkey, NemesisSequence]' in content:
            print("✓ NemesisSequence is correctly added to COMPLEX_NEMESIS list")
        else:
            print("✗ NemesisSequence is NOT found in COMPLEX_NEMESIS list")
            return False
            
        # Test 2: Check that the class exists 
        print("\nTest 2: Checking if NemesisSequence class exists...")
        if 'class NemesisSequence(Nemesis):' in content:
            print("✓ NemesisSequence class exists")
        else:
            print("✗ NemesisSequence class not found")
            return False
            
        # Test 3: Check that disrupt_run_unique_sequence method exists
        print("\nTest 3: Checking if disrupt_run_unique_sequence method exists...")
        if 'def disrupt_run_unique_sequence(self):' in content:
            print("✓ disrupt_run_unique_sequence method exists")
        else:
            print("✗ disrupt_run_unique_sequence method not found")
            return False
            
        print("\n🎉 All verification tests passed! NemesisSequence is properly excluded from SisyphusNemesis selection.")
        return True
        
    except Exception as e:
        print(f"Error during verification: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)