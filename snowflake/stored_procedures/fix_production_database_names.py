#!/usr/bin/env python3
"""
Fix ALL hardcoded staging database names in production/ directory
Replace with production database names
"""
import os
import re

# Database name mappings
REPLACEMENTS = [
    # Staging -> Production database names
    (r'\bTEST_STAGING\b', 'NOSEY_PROD'),
    (r'\btest_staging\b', 'NOSEY_PROD'),
    (r'\bMETADATA_MASTER_CLEANED_STAGING\b', 'METADATA_MASTER'),

    # UPLOAD_DB -> UPLOAD_DB_PROD (but only in specific contexts)
    # We need to be careful here - only replace UPLOAD_DB when it's not already UPLOAD_DB_PROD
    (r'\bUPLOAD_DB\.PUBLIC\b', 'UPLOAD_DB_PROD.PUBLIC'),
    (r'\bupload_db\.public\b', 'UPLOAD_DB_PROD.public'),

    # Also fix the CREATE statements that use lowercase
    (r'CREATE OR REPLACE PROCEDURE upload_db\.public\.', 'CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.public.'),
    (r'CREATE OR REPLACE FUNCTION upload_db\.public\.', 'CREATE OR REPLACE FUNCTION UPLOAD_DB_PROD.public.'),

    # Fix GRANT statements
    (r'GRANT USAGE ON PROCEDURE upload_db\.public\.', 'GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.public.'),
    (r'GRANT USAGE ON FUNCTION upload_db\.public\.', 'GRANT USAGE ON FUNCTION UPLOAD_DB_PROD.public.'),
]

def fix_file(file_path):
    """Fix database names in a single file"""
    with open(file_path, 'r') as f:
        content = f.read()

    original_content = content

    # Apply all replacements
    for pattern, replacement in REPLACEMENTS:
        content = re.sub(pattern, replacement, content)

    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    return False

def main():
    production_dir = 'production'

    if not os.path.exists(production_dir):
        print(f"Error: {production_dir} directory not found")
        return

    print("=" * 80)
    print("FIXING ALL STAGING DATABASE NAMES IN PRODUCTION/")
    print("=" * 80)
    print()

    fixed_count = 0

    for root, dirs, files in os.walk(production_dir):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                if fix_file(file_path):
                    print(f"✓ Fixed: {file_path}")
                    fixed_count += 1

    print()
    print("=" * 80)
    print(f"FIXED {fixed_count} FILES")
    print("=" * 80)

if __name__ == '__main__':
    main()
