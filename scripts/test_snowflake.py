import os
import snowflake.connector


conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

try:
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            CURRENT_ACCOUNT(),
            CURRENT_REGION(),
            CURRENT_USER(),
            CURRENT_ROLE(),
            CURRENT_DATABASE(),
            CURRENT_WAREHOUSE()
    """)

    result = cursor.fetchone()

    print("Snowflake connection successful!")
    print("--------------------------------")
    print(f"Account    : {result[0]}")
    print(f"Region     : {result[1]}")
    print(f"User       : {result[2]}")
    print(f"Role       : {result[3]}")
    print(f"Database   : {result[4]}")
    print(f"Warehouse  : {result[5]}")

    cursor.execute("""
        SELECT COUNT(*)
        FROM CLAIMS_DATA_DB.RAW.CLAIMS_RAW
    """)

    raw_count = cursor.fetchone()[0]

    print(f"RAW record count: {raw_count}")
finally:
    cursor.close()
    conn.close()