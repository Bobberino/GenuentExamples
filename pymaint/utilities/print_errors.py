import pyodbc

# Set the SQL Server location
remote_sql_server = False


def connect_to_local_server():
    driver = 'SQL Server Native Client 11.0'
    server = 'localhost'
    database = 'JobDivaAPI'

    conn_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};Trusted_Connection=yes;'
    print("conn_string is: ", conn_string)
    return pyodbc.connect(conn_string)


def connect_to_remote_server():
    server = '10.10.100.86\\SQLREPORTING'
    # When running on SQLReporting - use local
    # server = 'SQLREPORTING\\SQLREPORTING'
    database = 'JobDivaAPI'
    driver = '{ODBC Driver 17 for SQL Server}'
    # driver = 'SQL Server Native Client 11.0'
    username = 'remote'
    password = 'Bethany1'
    # with open('secret_key.txt', 'r') as f:
    #    password = f.read().strip()
    conn_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    print("conn_string is: ", conn_string)
    return pyodbc.connect(conn_string)


try:
    if remote_sql_server:
        print("\nConnecting to Remote SQL Server")
        conn = connect_to_remote_server()
        print("\nConnected to Remote SQL Server")
    else:
        print("\nConnecting to Local SQL Server")
        conn = connect_to_local_server()
        print("\nConnected to Local SQL Server")

    cursor = conn.cursor()

    date = "2020-11-15"
    query = f"""SELECT * FROM JobDivaAPI.API.ERROR_LOG 
            WHERE Cast(error_datettime AS DATE) > '2020-11-15'
            ORDER BY error_datettime"""

    cursor.execute(query)

    while True:
        print("\nFetching next row")
        row = cursor.fetchone()
        if row is None:
            break
        print("Time of Error: ", row[0])
        print("metric: ", row[1])
        print("query: ", row[2])
        print("query val: ", row[3])
        print("error text: ", row[4])
        print("INIT_LOAD: ", row[5])

    cursor.close()

except pyodbc.Error as error:
    print("Failed to read data from table", error)
finally:
    if conn:
        conn.close()
        print("The connection is closed")
