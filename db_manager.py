"""
a module to read data (urls) from database

"""

import os
import pathlib
import logging
import pandas as pd
import yaml
import psycopg2


def get_db_config(filename):
    with open(filename) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def read_data_from_file(filepath, batchsize):
    """
    :param filepath: (string) full path of file
    :param batchsize: (int) number of rows to be read in one request
    :return: urls to be loaded
    """
    # check if given file path is valid
    is_valid = os.path.exists(filepath)
    if not is_valid:
        logging.debug("file path provided for 'read_data_from_file' does not exist")
        return None

    # check file type - json or cvs
    # /Users/bhupi/Desktop/[DarkNightRises]/Rho/Rho Deployment/imput_data.csv
    file_extension = pathlib.Path(filepath).suffix
    if file_extension == "csv":
        input_data = pd.read_csv(filepath, na_filter=False, encoding='utf-8-sig')
        # select top rows as per batch size

    elif file_extension == "json":
        pass


# noinspection PyUnboundLocalVariable
def get_rows_from_table(table_name, batch_size):
    """

    :param table_name: (str) name of target table
    :param batch_size: number of rows to be fetched
    :return: a list of tuples, one tuple for each row
    """


    """
        Add exception for null
    """
    try:

        db_config = get_db_config("config.yaml")

        user = db_config["db_config"]["user"]
        password = db_config["db_config"]["password"]
        host = db_config["db_config"]["host"]
        port = db_config["db_config"]["port"]
        database = db_config["db_config"]["database"]

        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)

        cursor = connection.cursor()

        print("Table Before updating record ")
        sql_select_query = """select * from public.{} where status = 'New' limit {}""".format(table_name, batch_size)
        print(sql_select_query)
        cursor.execute(sql_select_query)
        records = cursor.fetchall()
        # add null exception here with the query clause
        print(records)
        ids = ', '.join([str(record[0]) for record in records])
        print(ids)

        # set status to in progress
        # update the status later
        sql_update_query = """ update {} set status = 'New', timestamp = CURRENT_TIMESTAMP
                                    where id in ({}) """.format(table_name, ids)

        cursor.execute(sql_update_query)
        connection.commit()
        count = cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        return "Error in read/update operation: " + str(error)
    else:
        return records
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


# noinspection PyUnboundLocalVariable
def insert_records_to_table(table_name, **column_name_and_values):
    """

    :param table_name: (str) name of the target table
    :param column_name_and_values: (dict i.e. {"column_name":"column_value"})
    :return:       number of rows affected
    """
    try:
        # create the column_name and column_value lists
        columns_names = ', '.join([key for key in column_name_and_values.keys()])
        column_values = ', '.join(["'" + value + "'" for value in column_name_and_values.values()])

        # create insert statement

        sql_insert_statement = """ INSERT INTO {} ({}) VALUES ({});""".format(table_name, columns_names, column_values)

        db_config = get_db_config("config.yaml")

        user = db_config["db_config"]["user"]
        password = db_config["db_config"]["password"]
        host = db_config["db_config"]["host"]
        port = db_config["db_config"]["port"]
        database = db_config["db_config"]["database"]

        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)

        cursor = connection.cursor()
        conn_active = True

        cursor.execute(sql_insert_statement)
        connection.commit()
        count = cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        return "Error in read/update operation: " + str(error)
    else:
        return str(count) + ": Record(s) Inserted successfully"
    finally:
        # closing database connection.
        # noinspection PyUnboundLocalVariable
        if connection:
            cursor.close()
            connection.close()


# noinspection PyUnboundLocalVariable
def update_records_to_table(table_name, *ids_list, **column_name_and_values):
    """
    :param table_name: (str) name of the target table
    :param ids_list: a list of ids (primary key) for the rows which are to be updated
    :param column_name_and_values: (dict i.e. {"column_name":"column_value"})
    :return:       number of rows affected
    """
    try:

        values = ', '.join([key + " = '" + value + "'" for key, value in column_name_and_values.items()])
        print(values)
        ids = ', '.join([str(value) for value in ids_list])
        print(ids)
        # create update statement
        sql_update_statement = """ UPDATE {} 
                                    
                                SET {} where id in ({})""".format(table_name, values, ids)
        print(sql_update_statement)
        db_config = get_db_config("config.yaml")

        user = db_config["db_config"]["user"]
        password = db_config["db_config"]["password"]
        host = db_config["db_config"]["host"]
        port = db_config["db_config"]["port"]
        database = db_config["db_config"]["database"]

        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)

        cursor = connection.cursor()
        cursor.execute(sql_update_statement)
        connection.commit()
        count = cursor.rowcount


    except (Exception, psycopg2.Error) as error:
        return "Error in read/update operation: " + str(error)
    else:
        return str(count) + ": Record(s) Updated successfully"
    finally:
        # closing database connection.
        # noinspection PyUnboundLocalVariable
        if connection:
            cursor.close()
            connection.close()


if __name__ == '__main__':
    print(get_rows_from_table('nfn_ref', 10))
    insert_records_to_table('nfn_ref', **{"pageurl": "https://www.python.org", "status": "New", "domain": "Test"})
    # update_records_to_table('nfn_ref', *[88, 87], **{"status": "New1", "domain": "Test1"})