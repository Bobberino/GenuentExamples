import datetime
import xml.etree.ElementTree as ET
from datetime import timedelta
from urllib.parse import quote

import pandas as pd
import pyodbc
import requests
from sqlalchemy import create_engine

import sys

# ToDo - put output file in its own folder

def difference(api, db):
    list_dif = [i.lower() for i in api + db if i.lower() not in db]
    return list_dif
	

def get_metric_info():
    # Query metrics table JOBDIVA.API.METRICS to get parameters required to call API
    query_string_report = f"SELECT schema_name, MetricName, UDF, UDF2, UDF3, UDF4, UDF5, pr " \
                          f"FROM JobDivaAPI.API.METRICS " \
                          f"where MetricName ='{metric}'"

    # return parameters required for API Call
    result_report = cursor.execute(query_string_report).fetchall()

    # if more than one metric loop through metric
    # this file only runs for one metric
    for r in result_report:

        # MetricName = table in SQL Database. Remove any spaces in the metric name to allow for proper table name
        MetricName = quote(r.MetricName)
        TableName = MetricName.replace(' ', '').replace('%20', '').replace('/', '')
        schema = quote(r.schema_name)
        table = schema + '.' + TableName

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

        return MetricName, TableName, schema, table, params, pr


def parse_api_check_columns():
    try:
        for call in tree.iter('Data'):
            for record in call.iter('Columns'):
                all_columns = []
                for val in record.iter('Column'):
                    all_columns.append(val.text if val.text is not None else '')
                    # print(all_columns)
                col_query = f"select column_name from INFORMATION_SCHEMA.COLUMNS  " \
                            f"where TABLE_NAME='{TableName}'" \
                            f" and TABLE_SCHEMA='{schema}'"
                db_columns_result = (cursor.execute(col_query).fetchall())
                db_columns = [col[0].lower().replace(" ", "") for col in db_columns_result]
                db_create_col = difference(all_columns, db_columns)
                # print(db_create_col)

                try:
                    if len(db_create_col) > 0:
                        for c in db_create_col:
                            add_col_query = f'alter table {table} add [{c}] text null;'
                            cursor.execute(add_col_query)
                            cursor.commit()
                            # print("col added")
                            # add log of added column
                            col_log_query = f"insert into JobDivaAPI.API.added_columns (column_added,tablename) " \
                                            f"values ('{c}','{table}')"
                            # print(col_log_query)
                            cursor.execute(col_log_query)
                            cursor.commit()
                except Exception as e:
                    error_add_col = f"insert into JobDivaAPI.API.added_columns (column_added,tablename) " \
                                    f"values ('{c}','{table},'{e}')"
                    cursor.execute(error_add_col)
                    cursor.commit()
                    # print(e)
    except:
        pass


def parse_api_get_data(tree, output, run_lines):
    for call in tree.iter('Data'):
        for record in call.iter('Row'):
            header = []
            value = []
            for val in record.iter('RowData'):
                children = list(val)
                if children:
                    for child in children:
                        # print(child.tag)
                        if child.tag == "Name":
                            header.append(child.text.replace(" ", "") if child.text is not None else '')
                        else:
                            if child.tag == "Value":
                                value.append(child.text if child.text is not None else '')
            # add API Call Dates  to insert query
            header.append('api_fromDate')
            header.append('api_toDate')
            # add API Call Date values to insert query
            value.append(fromDate)
            value.append(toDate)
            # create dynamic header string for SQL insert
            head_string = ','.join(map(str, header))
            # create dynamic value string for SQL insert
            val_string = ', '.join(['?'] * len(header))
            # if return value is a number, parse number, if string, append as string.
            val_list = []
            for v in value:
                if v == '':
                    val_list.append(None)
                else:
                    val_list.append(v)
            # add date to dictionary

            if run_lines == 0:
                res = dict(zip(header, val_list))
                output = output.append(res, ignore_index=True)
            else:

                if run_lines == 1:
                    # Create insert string into database
                    query_string = "INSERT INTO " + table + " (" + head_string + ") VALUES (%s)" % val_string

                    try:
                        cursor.execute(query_string, val_list)
                    except Exception as e:
                        # set current date for error time
                        error_time = datetime.datetime.now()
                        # create error insert String
                        error_val_string = ', '.join(['?'] * 4)
                        error_list = []
                        error_list.append(MetricName)
                        error_list.append(query_string)
                        query_val = ', '.join(map(str, val_list))
                        error_list.append(query_val)
                        error_list.append(str(e))
                        error_string = "INSERT INTO JobDivaAPI.[API].[ERROR_LOG] ([metric], " \
                                       "[query],[query_val],[error_txt])  VALUES (%s)" % error_val_string
                        cursor.execute(error_string, error_list)
                        # set current date for error time
                        error_time = datetime.datetime.now()
                        pass

        cursor.commit()

    return output



# get run parameters from cmd
if __name__ == "__main__":
    # metric = sys.argv[1]
    # days_back = int(sys.argv[2])
    # iterations = int(sys.argv[3])
	
    # New and Updated Job Record   12 days 6 iterations 
    
    metric = "New/Updated Job Records"
    days_back = 12
    iterations = 6

server = 'localhost'
serv = 'localhost'
database = 'JobDivaAPI'
driver = '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=localhost;"
                      "Database=JobDivaAPI;"
                      "Trusted_Connection=yes;")

engine = create_engine(f'mssql+pyodbc://{serv}/{database}?driver=ODBC Driver 17 for SQL Server?Trusted_Connection=yes')

# open cursor
cursor = cnxn.cursor()
# delete existing data for load
# currently handled by SSIS
# cursor.execute('truncate table ' + table)
output = pd.DataFrame()

MetricName, TableName, schema, table, params, pr = get_metric_info()

# def call_api():
# iterations determines how far back data gets pulled.
# days back must be no larger than 14
# iterations = how many blocks of 14 days in the past the data should returnAPI
# 14 days back * 2 iterations = 28 days from current date

# set current date for relative date calculation (i.e. api will pull data  from 'days_back')
now = datetime.datetime.now()

for x in range(iterations):
    fromDate = (now + timedelta(days=(-days_back * x) - days_back)).strftime("%Y-%m-%d")
    toDate = (now + timedelta(days=(-days_back * x))).strftime("%Y-%m-%d")

    # params required #N=None, D = Dates, P = Params, DP = Date Params
    if pr == 'N':
        url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
              + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!'

    if pr == 'P':
        url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
              + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&Parameters=' + params

    if pr == 'D':
        url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
              + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&FromDate=' + fromDate + '&ToDate=' + \
              toDate
    if pr == 'DP':
        url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
              + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&FromDate=' + fromDate + '&ToDate=' + \
              toDate + params
        # https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=Placement%20Detail&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&FromDate=2018-12-15&ToDate=2018-12-29

    print("\nURL is: \n", url)

    # get API response
    response = requests.get(url)
    print("\nresponse is: \n", response)

    # print API response content
    # print("\nresponse content is: \n", response.content) 

    # parse API response
    tree = ET.fromstring(response.content)
    if x == 0:
        parse_api_check_columns()

    output = parse_api_get_data(tree, output, 0)

    print("\nOutput is: \n", output)

    # cursor.execute(query_string,val_list)
    
    output_file = f'output_{fromDate}_{toDate}.csv'
    print('\nWriting CSV File: \n', output_file)
    output.to_csv(output_file, index=False)

    """
    # insert data into sql database, check for errors
    try:
        # cursor.execute(query_string,val_list)
        output.to_sql(TableName, engine, schema=schema, if_exists='append', index=False)
    except Exception as e:
        # set current date for error time
        error_time = datetime.datetime.now()
        # create error insert String
        error_val_string = ', '.join(['?'] * 2)
        error_list = []
        error_list.append(MetricName)
        error_list.append(str(e))
        error_string = "INSERT INTO JobDivaAPI.[API].[ERROR_LOG] ([metric], " \
                       "[error_txt])  VALUES (%s)" % error_val_string
        cursor.execute(error_string, error_list)

        # if df insert fails, run again pyodbc each line
        error_list = []
        error_list.append(MetricName)
        error_list.append("Ran line by line")
        error_string = "INSERT INTO JobDivaAPI.[API].[ERROR_LOG] ([metric], [error_txt])  " \
                       "VALUES (%s)" % error_val_string
        cursor.execute(error_string, error_list)
        parse_api_get_data(tree, output, 1)
    cnxn.commit()
    """

print("Run Completed")
