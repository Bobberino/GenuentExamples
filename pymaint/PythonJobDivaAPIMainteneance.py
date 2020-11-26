import datetime
import xml.etree.ElementTree as ET
from datetime import timedelta
import pandas as pd
import requests

from pymaint.JobDivaAPIDB import JobDivaAPIDB
from pymaint.job_diva_api import JobDivaAPI

""" 
You need to set:
   This versio will only use local dbs on my Laptop.
   
   2) Set the from_date and to_date

    3) The JobDiva API (the metric) that you wish to call.  All nineteen
    are listed below.  Just uncomment the one you want (and re-comment the 
    ones you  don't want - you can only have one).  
"""

""" 
 ToDo:
 create the output folder if it does not already exist  
"""

# Use today and yesterday for from_date and to_date
now = datetime.datetime.now()
days_back = 1
from_date = (now + timedelta(days=(-days_back))).strftime("%Y-%m-%d")
to_date = now.strftime("%Y-%m-%d")

# Otherwise set your own from_date and to_date
# from_date = "2020-11-11"
# to_date = "2020-11-12"

print("from_date is: ", from_date)
print("to_date is:   ", to_date)

""" These are the valid metric names - uncomment the one for this run """
# metric = "Candidate Actions"
# metric = "Divisions List"
metric = "New/Updated Activity Records"
# metric = "New/Updated Candidate Note Records"
# metric = "New/Updated Candidate Records"
# metric = "New/Updated Company Note Records"
# metric = "New/Updated Company Records"
# metric = "New/Updated Contact Note Records"
# metric = "New/Updated Contact Records"
# metric = "New/Updated Job Records"
# metric = "New/Updated Task Records"
# metric = "User Group Lists"
# metric = "Users List"
# metric = "Submittal/Interview/Hire Activities List"
# metric = "Contact Actions"
# metric = "Updated Approved Salary Records"
# metric = "Deleted Activity Records"
# metric = "Deleted Job Records"
# metric = "Merged Jobs"

print("Metric is: ", metric)


def difference(api, db):
    list_dif = [i.lower() for i in api + db if i.lower() not in db]
    return list_dif


# get run parameters from cmd
if __name__ == "__main__":
    # metric = sys.argv[1]
    # days_back = int(sys.argv[2])
    # iterations = int(sys.argv[3])

    jdaDB = JobDivaAPIDB()

    # output = pd.DataFrame()

    MetricName, TableName, schema, table, params, pr = jdaDB.get_metric_info(metric)

    print("schema is: ", schema)

    jda = JobDivaAPI()
    url = jda.build_url(pr, MetricName, from_date, to_date, params)

    # get API response
    response = requests.get(url)
    print("\nresponse is: \n", response)

    # print API response content
    # print("\nresponse content is: \n", response.content)

    # parse API response
    tree = ET.fromstring(response.content)

    # x in effect is 0
    # if x == 0:
    jda.parse_api_check_columns(tree)

    output = jda.parse_api_get_data(tree, output, 0, from_date, to_date)

    print(output.dtypes)

    cursor.commit()  # where should this go

    print("\nOutput is: \n", output)

    # cursor.execute(query_string,val_list)
    print("TableName is: ", TableName)
    output_file = f'{TableName}_{from_date}_{to_date}.csv'
    output_path = f'output/local_{output_file}'
    print('\nWriting CSV File: ', output_file)
    print('output_path is: ', output_path)
    output.to_csv(output_path, index=False)

    # Fix this
    if remote_sql_server:
        engine = jdaDB.get_engine_to_remote_jobdivaapi()
    else:
        engine = jdaDB.get_engine_to_local_jobdivaapi()
    try:
        # cursor.execute(query_string,val_list)
        output.to_sql(TableName, engine, schema=schema, if_exists='append', index=False)
        print(f"\nTable: {TableName} updated")
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
        jda.parse_api_get_data(tree, output, 1, from_date, to_date)
    finally:
        cnxn.commit()  # if auto-commit not set
        # Close Cursor
        cursor.close()
        # Close connection
        cnxn.close()
        # dispose of the sqlalchemy engine
        engine.dispose()

    print("Run Completed")
