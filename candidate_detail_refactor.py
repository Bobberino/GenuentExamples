import pyodbc
import xml.etree.ElementTree as ET
import requests
import datetime
from urllib.parse import quote

def get_connection():
    # def server to get metric params andsave api return data:
    server = 'localhost'
    database = 'jobdivaapi'
    driver = '{ODBC Driver 17 for SQL Server}'

    cnxn_string = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;' + \
                  'DATABASE=' + database + ';Trusted_Connection=yes'
    print("cnxn_string is: ", cnxn_string)

    return pyodbc.connect(cnxn_string)

# get run parameters from cmd
# if __name__ == "__main__":
#     metric = sys.argv[1]
#     #days_back =   int(sys.argv[2])
#     #iterations = int(sys.argv[3])
#     lkup_tbl =int(sys.argv[2])
#     lkup_val_param1 =  int(sys.argv[3])

# manually set run parameters
metric = 'Candidate Detail'

cnxn = get_connection()

# open cursor
cursor = cnxn.cursor()
# delete existing data for load
# currently handled by SSIS
# cursor.execute('truncate table ' + table)

# Query metrics table JOBDIVA.API.METRICS to get parameters required to call API
query_string = "Select schema_name, MetricName, parameters1, parameters2, parameters3, pr from jobdivaapi.api.Metrics where metricname ='" + metric + "'"

# return parameters required for API Call
results = cursor.execute(query_string).fetchall()

# if more than one metric loop through metric
# results is: [('CANDIDATE', 'Candidate Detail', 'RecordID', '', '', 'P')]
##this file only runs for one metric
for r in results:

    # Metricname = table in SQL Database. Remove any spaces in the metric name to allow for proper table name
    MetricName = quote(r.MetricName)  # 'Candidate Detail'
    table = quote(r.schema_name) + '.' + MetricName.replace(' ', '').replace('%20', '').replace('/', '')

    # Get All lkup_val_param1 (i.e. Employee ID to loop through)
    query_id = 'SELECT distinct act_candidateid  from [RPT_JobDivaAPI].[dbo].[v_job_stage] where act_candidateid is not null and job_candidate_name is  null'
    print("\nquery_id is: ", query_id)

    result_id = cursor.execute(query_id).fetchall()
    print("\nresult_id is:", result_id)

    # create a api call for each act_candidateid
    for id in result_id:
        # print("ID",id.id)
        params = []
        if r.parameters1 != '':
            param1 = r.parameters1
            params.append('&Parameters=' + str(id[0]).replace(" ", ""))
        # pr = type of api call w/ or w/o dates or parameters
        pr = r.pr
        params = ''.join(params)
        # params required #P = Params
        # Always P
        if pr == 'P':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!' + params

        # print(url)

        # make api call
        response = requests.get(url)

        # parse API call
        tree = ET.fromstring(response.content)

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
                                header.append(child.text if child.text is not None else '')
                            else:
                                if child.tag == "Value":
                                    value.append(child.text if child.text is not None else '')

                # create insert string into JOBDIVA table. Get a dynamic string of each column header and value
                head_string = ','.join(map(str, header))
                val_string = ', '.join(['?'] * len(header))

                # if return value is a number, parse number, if string, append as string.
                val_list = []
                for v in value:
                    if v.isdigit():
                        val_list.append(int(v))
                    else:
                        if v == '':
                            val_list.append(None)
                        else:
                            val_list.append(v)

                # Create insert string into database
                query_string = "INSERT INTO " + table + " (" + head_string + ") VALUES (%s)" % val_string

                # value = (  "[%s]" % (','.join(value)))
                # print(value.format(*value))

                # insert data into sql database, check for errors
                try:
                    cursor.execute(query_string, val_list)
                except Exception as e:
                    # set current date for error time
                    error_time = datetime.datetime.now()

                    # create error insert String
                    error_val_string = ', '.join(['?'] * 5)
                    error_list = []
                    error_list.append(error_time)
                    error_list.append(MetricName)
                    error_list.append(query_string)
                    query_val = ', '.join(map(str, val_list))
                    error_list.append(query_val)
                    error_list.append(str(e))
                    error_string = "INSERT INTO [API].[ERROR_LOG] ([error_datettime], [metric], [query], [query_val], [error_txt])  VALUES (%s)" % error_val_string
                    # print(error_string)
                    # print(error_list)
                    cursor.execute(error_string, error_list)
                    # print(val_list)
                    # print(e)
                cnxn.commit()
