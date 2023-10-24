import snowflake.connector

def get_snowflake_connection(user, password, account, warehouse, database, schema):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    return conn

def can_select_from_table(conn, role, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE ROLE {role}")
            cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False

def can_create_table(conn, role, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE ROLE {role}")
            cur.execute(f"CREATE TABLE {table_name} (id INT)")
            cur.execute(f"DROP TABLE {table_name}")
        return True
    except Exception:
        return False

if __name__ == "__main__":
    USER = 'your_username'
    PASSWORD = 'your_password'
    ACCOUNT = 'your_account_url'
    WAREHOUSE = 'your_warehouse'
    DATABASE = 'your_database'
    SCHEMA = 'your_schema'
    ROLE_TO_TEST = 'role_name'

    conn = get_snowflake_connection(USER, PASSWORD, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA)

    # Test SELECT permission on a specific table
    print(f"Can {ROLE_TO_TEST} SELECT from MY_TABLE? {can_select_from_table(conn, ROLE_TO_TEST, 'MY_TABLE')}")

    # Test CREATE TABLE permission
    print(f"Can {ROLE_TO_TEST} CREATE TABLE? {can_create_table(conn, ROLE_TO_TEST, 'TEST_TABLE')}")

    conn.close()