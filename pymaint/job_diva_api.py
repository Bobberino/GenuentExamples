import datetime
import pandas as pd


class JobDivaAPI:
    """description of class"""

    def __init__(self):
        pass

    def difference(self, api, db):
        list_dif = [i.lower() for i in api + db if i.lower() not in db]
        return list_dif

    # Table_Name, schema and table don't rlesolve
    def parse_api_check_columns(self, tree, cursor):
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
                    db_create_col = self.difference(all_columns, db_columns)
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
        except Exception:
            pass

    # table and MetricName don't resolve
    def parse_api_get_data(self, tree, run_lines, from_date, to_date, cursor):
        output = pd.dataframe()
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
                value.append(from_date)
                value.append(to_date)
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

        return output

    def build_url(self, pr, metric_name, from_date, to_date, params):
        # params required #N=None, D = Dates, P = Params, DP = Date Params
        if pr == 'N':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + metric_name \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!'

        if pr == 'P':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + metric_name \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&Parameters=' + params

        if pr == 'D':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + metric_name \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&FromDate=' + from_date + \
                  '&ToDate=' + to_date
        if pr == 'DP':
            url = 'https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=' + metric_name \
                  + '&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&FromDate=' + from_date + \
                  '&ToDate=' + to_date + params
            # https://ws.jobdiva.com/axis2/services/BIData/getBIData?MetricName=Placement%20Detail&ClientID=288&Username=reporting@genuent.com&Password=Gen2019!&FromDate=2018-12-15&ToDate=2018-12-29

        print("\nURL is: \n", url)

        return url
