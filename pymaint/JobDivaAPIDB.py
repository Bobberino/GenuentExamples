import datetime
from urllib.parse import quote

import pyodbc
from sqlalchemy import create_engine


class JobDivaAPIDB:
    """description of class"""

    def __init__(self):
        self.server = 'localhost'
        self.database = 'JobDivaAPI'
        self.driver = '{ODBC Driver 17 for SQL Server}'
        # self.driver = '{SQL Server Native Client 11.0}'

        self.cnxn = self.get_connection()
        self.cursor = self.cnxn.cursor()

    def get_connection(self):

        connection_string = f"Driver={self.driver};Server={self.server};" \
                            f"Database={self.database};Trusted_Connection=yes;"
        
        return pyodbc.connect(connection_string)

    def get_metric_info(self, metric):
        # Query metrics table JOBDIVA.API.METRICS to get parameters required to call API
        query_string_report = f"SELECT schema_name, MetricName, UDF, UDF2, UDF3, UDF4, UDF5, pr " \
                              f"FROM JobDivaAPI.API.METRICS " \
                              f"where MetricName ='{metric}'"

        # return parameters required for API Call
        result_report = self.cursor.execute(query_string_report).fetchall()

        # this loop runs once per metric
        for r in result_report:

            # metric_name = table in SQL Database.
            # Remove any spaces in the metric name to allow for proper table name
            metric_name = quote(r.MetricName)
            table_name = metric_name.replace(' ', '').replace('%20', '').replace('/', '').upper()
            schema = quote(r.schema_name)
            table = schema + '.' + table_name

            # create parameter list
            params = []
            if r.UDF != '' or r.UDF is not None:
                udf = r.UDF
                params.append('&Parameters=' + str(udf))
            if r.UDF2 != '' or r.UDF2 is not None:
                udf = r.UDF2
                params.append('&Parameters=' + str(udf))
            if r.UDF3 != '' or r.UDF3 is not None:
                udf = r.UDF3
                params.append('&Parameters=' + str(udf))
            if r.UDF4 != '' or r.UDF4 is not None:
                udf = r.UDF4
                params.append('&Parameters=' + str(udf))
            if r.UDF5 != '' or r.UDF5 is not None:
                udf = r.UDF5
                params.append('&Parameters=' + str(udf))
            pr = r.pr
            # convert list to string for api URL
            params = ''.join(params)

            return metric_name, table_name, schema, table, params, pr



    def update_job_diva_api_db(self, output, table_name, schema, metric_name,
                               jda, tree, from_date, to_date):

        # Get the SQLAlchemy engine for the server for writing the results
        # from the JobDivaAPI

        # Set SQLAlchemy engine

        engine = self.get_engine_to_jobdivaapi()

        try:
            # Write the dataframe to the appropriate table in JobDivaAPI database
            output.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
            print(f"\nTable: {table_name} updated")
        except Exception as e:
            # set current date for error time
            error_time = datetime.datetime.now()
            # create error insert String
            error_val_string = ', '.join(['?'] * 2)
            error_list = []
            error_list.append(metric_name)
            error_list.append(str(e))
            error_string = "INSERT INTO JobDivaAPI.[API].[ERROR_LOG] ([metric], " \
                           "[error_txt])  VALUES (%s)" % error_val_string
            self.cursor.execute(error_string, error_list)

            # if df insert fails, run again pyodbc each line
            error_list = []
            error_list.append(metric_name)
            error_list.append("Ran line by line")
            error_string = "INSERT INTO JobDivaAPI.[API].[ERROR_LOG] ([metric], [error_txt])  " \
                           "VALUES (%s)" % error_val_string
            self.cursor.execute(error_string, error_list)
            jda.parse_api_get_data(tree, output, 1, from_date, to_date)
        finally:
            self.cnxn.commit()  # if auto-commit not set
            # Close Cursor
            self.cursor.commit()
            self.cursor.close()
            # Close connection
            self.cnxn.close()
            # dispose of the sqlalchemy engine
            engine.dispose()
