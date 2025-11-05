"""
Database setup script for Data Template Manager

This script creates the viewership_file_formats table in Snowflake.
Run this script if you want to manually create the table or reset it.
"""

import snowflake.connector
import sys

def create_table(conn):
    """Create the viewership_file_formats table"""
    cursor = conn.cursor()

    create_table_sql = """
    CREATE OR REPLACE TABLE  dictionary.public.viewership_file_formats (
        config_id VARCHAR(36) PRIMARY KEY,
        platform VARCHAR(255) NOT NULL,
        partner VARCHAR(255) NOT NULL,
        channel VARCHAR(255) NOT NULL,
        territory VARCHAR(255) NOT NULL,
        column_mappings VARIANT NOT NULL,
        validation_rules VARIANT,
        filename_pattern VARCHAR(500),
        source_columns VARIANT,
        target_table VARCHAR(255),
        created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        updated_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        created_by VARCHAR(255),
      custom_sanitization_procedure VARCHAR(100),
      custom_territory_procedure VARCHAR(100),
      custom_channel_procedure VARCHAR(100),
      custom_date_procedure VARCHAR(100),
      custom_normalizers VARIANT, 
        UNIQUE (platform, partner, channel, territory)
    )
    """

    try:
        print("Creating viewership_file_formats table...")
        cursor.execute(create_table_sql)
        conn.commit()
        print("✓ Table created successfully!")
        return True
    except Exception as e:
        print(f"✗ Error creating table: {str(e)}")
        return False
    finally:
        cursor.close()


def main():
    """Main setup function"""
    print("=" * 60)
    print("Data Template Manager - Database Setup")
    print("=" * 60)
    print()

    # Get Snowflake credentials
    print("Enter your Snowflake credentials:")
    user = input("Username: ")
    password = input("Password: ")
    account = input("Account (e.g., xy12345.us-east-1): ")
    warehouse = input("Warehouse: ")
    database = input("Database: ")
    schema = input("Schema: ")

    print("\nConnecting to Snowflake...")

    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        print("✓ Connected successfully!")

        # Create table
        success = create_table(conn)

        conn.close()

        if success:
            print("\n" + "=" * 60)
            print("Setup completed successfully!")
            print("=" * 60)
            print("\nYou can now run the Streamlit app with: streamlit run app.py")
            sys.exit(0)
        else:
            sys.exit(1)

    except Exception as e:
        print(f"\n✗ Connection failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
