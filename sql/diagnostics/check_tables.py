import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("Checking available tables and columns...")

# Check deals table
print("\n1. Checking dictionary.public.deals...")
try:
    cursor.execute("DESCRIBE TABLE dictionary.public.deals")
    print("   Columns:")
    for row in cursor.fetchall():
        print(f"     - {row[0]} ({row[1]})")
except Exception as e:
    print(f"   Error: {e}")

# Check active_deals table
print("\n2. Checking dictionary.public.active_deals...")
try:
    cursor.execute("DESCRIBE TABLE dictionary.public.active_deals")
    print("   Columns:")
    for row in cursor.fetchall():
        print(f"     - {row[0]} ({row[1]})")
except Exception as e:
    print(f"   Error: {e}")

# Check platform_config table
print("\n3. Checking dictionary.public.platform_config...")
try:
    cursor.execute("DESCRIBE TABLE dictionary.public.platform_config")
    print("   Columns:")
    for row in cursor.fetchall():
        print(f"     - {row[0]} ({row[1]})")
except Exception as e:
    print(f"   Error: {e}")

# Check platforms table (singular)
print("\n4. Checking dictionary.public.platforms...")
try:
    cursor.execute("DESCRIBE TABLE dictionary.public.platforms")
    print("   Columns:")
    for row in cursor.fetchall():
        print(f"     - {row[0]} ({row[1]})")
except Exception as e:
    print(f"   Error: {e}")

# Sample from deals
print("\n5. Sample from dictionary.public.deals...")
try:
    cursor.execute("SELECT * FROM dictionary.public.deals LIMIT 3")
    print(f"   Columns: {[desc[0] for desc in cursor.description]}")
    for row in cursor.fetchall():
        print(f"   {row}")
except Exception as e:
    print(f"   Error: {e}")

cursor.close()
conn.close()
