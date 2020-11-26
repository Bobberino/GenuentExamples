import pyodbc

# Can Clear the local JobDivaAPDBa and get the metrics from either tablr,
# Get the JobDivaAPI data.  Update the local JobDivaAPI Datbase..

# Set the SQL Server location
remote_sql_server = False


def connect_to_local_server():
    # driver = 'SQL Server Native Client 11.0'
    driver = '{ODBC Driver 17 for SQL Server}'
    server = 'localhost'
    database = 'RPT_JobDivaAPI'

    return pyodbc.connect(
        f"Driver={driver};Server={server};Database={database};Trusted_Connection=yes;")


def connect_to_remote_server():
    server = '10.10.100.86\\SQLREPORTING'
    # When running on SQLReporting - use local
    # server = 'SQLREPORTING\\SQLREPORTING'
    database = 'RPT_JobDivaAPI'
    driver = '{ODBC Driver 17 for SQL Server}'
    username = 'remote'
    with open('../../secret_key.txt', 'r') as f:
        password = f.read().strip()

    return pyodbc.connect(
        f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}')


if remote_sql_server:
    print("\nConnecting to Remote SQL Server")
    conn = connect_to_remote_server()
else:
    print("\nConnecting to Local SQL Server")
    conn = connect_to_local_server()

cursor = conn.cursor()

cmd_prod_executesp = """EXEC SP_PREP_LOAD """
conn.autocommit = True
cursor.execute(cmd_prod_executesp)

cursor.close()
conn.close()
