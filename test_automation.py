#!/usr/bin/env python3
"""
Test script to validate PythonAnywhere upload functionality.
Run this locally to test your credentials before deploying.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add the scripts directory to the path
sys.path.append('.github/scripts')

try:
    from upload_to_pythonanywhere import PythonAnywhereUploader
except ImportError:
    print("❌ Could not import upload script. Make sure you're running from the project root.")
    sys.exit(1)

def test_upload():
    """Test the upload functionality with a small test file."""
    # Get credentials
    username = input("Enter your PythonAnywhere username: ").strip()
    api_token = input("Enter your PythonAnywhere API token: ").strip()
    
    if not username or not api_token:
        print("❌ Username and API token are required")
        return False
    
    # Create a test file
    test_content = f"Test file created for automation validation\nTimestamp: {os.popen('date').read().strip()}"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(test_content)
        test_file_path = f.name
    
    try:
        # Initialize uploader
        uploader = PythonAnywhereUploader(username, api_token)
        
        # Test directory creation
        test_dir = f"/home/{username}/automation_test"
        print(f"Testing directory creation: {test_dir}")
        if uploader.create_directory(test_dir):
            print("✅ Directory creation test passed")
        else:
            print("❌ Directory creation test failed")
            return False
        
        # Test file upload
        remote_path = f"{test_dir}/test_upload.txt"
        print(f"Testing file upload to: {remote_path}")
        if uploader.upload_file(test_file_path, remote_path):
            print("✅ File upload test passed")
            print(f"✅ Test file uploaded to: {remote_path}")
            print("You can check this file in your PythonAnywhere Files tab")
            return True
        else:
            print("❌ File upload test failed")
            return False
            
    finally:
        # Clean up test file
        os.unlink(test_file_path)
    
    return False

def main():
    """Main test function."""
    print("🧪 PythonAnywhere Upload Test")
    print("=" * 40)
    print("This script will test your PythonAnywhere credentials and upload functionality.")
    print("Make sure you have your API token ready.")
    print()
    
    if test_upload():
        print("\n🎉 All tests passed! Your automation should work correctly.")
        print("\nNext steps:")
        print("1. Set up GitHub secrets (PYTHONANYWHERE_USERNAME and PYTHONANYWHERE_API_TOKEN)")
        print("2. Push your code to GitHub")
        print("3. Test the workflow manually in GitHub Actions")
    else:
        print("\n❌ Tests failed. Please check your credentials and try again.")
        print("\nTroubleshooting:")
        print("1. Verify your PythonAnywhere username is correct")
        print("2. Make sure your API token is valid and not expired")
        print("3. Check your internet connection")

if __name__ == "__main__":
    main()