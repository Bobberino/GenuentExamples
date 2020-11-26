import pyodbc
import xml.etree.ElementTree as ET
import requests
import datetime
from urllib.parse import quote

# Actual production Run Parameters from SSIS Execute Process Task
# C:/Users/evan.Shulman/Desktop/JobDivaAPI/Candidate_detail_update_daily.py
#        "Candidate Detail" "1" "1"
# get run parameters from cmd
# if __name__ == "__main__":
#     metric = sys.argv[1]
#     #days_back =   int(sys.argv[2])
#     #iterations = int(sys.argv[3])
#     lkup_tbl =int(sys.argv[2])
#     lkup_val_param1 =  int(sys.argv[3])

# manually set run parameters
metric = 'Candidate Detail'
# lkup_tbl = '[RPT_JobDivaAPI].[CANDIDATE].[CANDIDATERECORDS]'
# lkup_val_param1 = 'CandidateID'
# days_back=1 # not needed for detail api call

# def server to get metric params andsave api return data:
server = 'localhost'
database = 'jobdivaapi'
driver = '{ODBC Driver 17 for SQL Server}'

cnxn_string = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;' + \
                                               'DATABASE=' + database + ';Trusted_Connection=yes'
print("cnxn_string is: ", cnxn_string)
cnxn = pyodbc.connect(cnxn_string)

# open cursor
cursor = cnxn.cursor()
# delete existing data for load
# currently handled by SSIS
# cursor.execute('truncate table ' + table)

# Query metrics table JOBDIVA.API.METRICS to get parameters required to call API
query_string_report = "Select schema_name, MetricName, parameters1, parameters2, parameters3, pr from jobdivaapi.api.Metrics where metricname ='" + metric + "'"
print("query_string_report is: ", query_string_report)

# return parameters required for API Call
result_report = cursor.execute(query_string_report).fetchall()
print("result_report is: ", result_report)

# if more than one metric loop through metric
# this file only runs for one metric
for r in result_report:

    # Metricname = table in SQL Database. Remove any spaces in the metric name to allow for proper table name
    MetricName = quote(r.MetricName)
    print("MetricName is: ", MetricName)
    table = quote(r.schema_name) + '.' + MetricName.replace(' ', '').replace('%20', '').replace('/', '')
    print("table is: ", table)
    # Get All lkup_val_param1 (i.e. Employee ID to loop through)
    query_id = 'SELECT distinct act_candidateid  from [RPT_JobDivaAPI].[dbo].[v_job_stage] where act_candidateid is not null and job_candidate_name is  null'
    print("query_id is: ", query_id)
    result_id = cursor.execute(query_id).fetchall()
    print("result_id is: ", result_id)

    # create a api call for each 'lkup_val_param1' (i.e. EmployeeID)
    # In this case, the result_id is the act_candidateid
    for id in result_id:
        # print("ID",id.id)
        params = []
        if r.parameters1 != '':
            param1 = r.parameters1
            params.append('&Parameters=' + str(id[0]).replace(" ", ""))
        # pr = type of api call w/ or w/o dates or parameters
        pr = r.pr
        params = ''.join(params)
        # params required #N=None, D = Dates, P = Params, DP = Date Params
        if pr == 'N':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!'

        if pr == 'P':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + MetricName \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!' + params


        print(url)

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

                print("Length is: ", len(val_list))

                try:
                    cursor.execute(query_string, val_list)
                except Exception as e:
                    # set current date for error time
                    error_time = datetime.datetime.now()

                    # create error insert String
                    error_val_string = ', '.join(['?'] * 5)
                    error_list = []
                    error_list.append((error_time))
                    error_list.append(MetricName)
                    error_list.append(query_string)
                    query_val = ', '.join(map(str, val_list))
                    error_list.append(query_val)
                    error_list.append(str(e))
                    error_string = "INSERT INTO [API].[ERROR_LOG] ([error_datettime], [metric], [query], [query_val], [error_txt])  VALUES (%s)" % error_val_string
                    # print(error_string)
                    # print(error_list)
                    cursor.execute(error_string, (error_list))
                    # print(val_list)
                    # print(e)
                # cnxn.commit()
