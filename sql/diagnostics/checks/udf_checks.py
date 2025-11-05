"""Check UDF existence and permissions"""

def check_udfs(cursor, env_config):
    """Check if required UDFs exist and have proper permissions"""
    print("=" * 80)
    print("UDF Checks")
    print("=" * 80)
    print()

    upload_db = env_config['UPLOAD_DB']

    # Check for EXTRACT_PRIMARY_TITLE function
    print("Checking EXTRACT_PRIMARY_TITLE function...")
    cursor.execute(f"""
        SHOW USER FUNCTIONS LIKE 'EXTRACT_PRIMARY_TITLE' IN {upload_db}.PUBLIC
    """)

    functions = cursor.fetchall()

    if functions:
        print(f"  ✅ Found EXTRACT_PRIMARY_TITLE in {upload_db}.PUBLIC")

        # Check permissions
        try:
            cursor.execute(f"""
                SHOW GRANTS ON FUNCTION {upload_db}.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR)
            """)
            grants = cursor.fetchall()

            has_webapp_grant = any('WEB_APP' in str(grant) for grant in grants)
            if has_webapp_grant:
                print("  ✅ WEB_APP role has USAGE permission")
            else:
                print("  ❌ WEB_APP role missing USAGE permission")
                print("     💡 Run: GRANT USAGE ON FUNCTION ... TO ROLE WEB_APP")
                return False
        except Exception as e:
            print(f"  ⚠️  Could not check permissions: {e}")
    else:
        print(f"  ❌ EXTRACT_PRIMARY_TITLE NOT FOUND in {upload_db}.PUBLIC")
        print("     💡 Deploy UDFs: python sql/deploy/deploy.py --env staging --only 002_udfs")
        return False

    print()
    return True
