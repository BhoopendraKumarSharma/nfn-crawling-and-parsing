"""
a module to read data (urls) from database

"""
import pandas as pd
import yaml
import psycopg2


def get_db_config(filename):
    with open(filename) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


# select setval ('crawling_engine_nfn_id_seq', (select max(id) from crawling_engine_nfn ))
# noinspection PyUnboundLocalVariable

def read_data_from_sql_query(sql_query):
    """
    :param sql_query: sql query to get the required data
    :return: a pandas dataframe with required data
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

        data = pd.read_sql_query(sql_query, connection)
        # add null exception here with the query clause
        # print(records)
        # urls = [str(r[3]) for r in records]
    except (Exception, psycopg2.Error) as error:
        return "Error in read/update operation: " + str(error)
    else:
        return data
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()




def get_crawled_data_from_table(table_name):
    """
    :param table_name:
    :return:
    """

    connection = None
    cursor = None
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

        # print("Table Before updating record ")
        sql_select_query = """select * from public.{} where status = 'Crawled' """.format(table_name)
        # print(sql_select_query)
        cursor.execute(sql_select_query)
        records = cursor.fetchall()
        # add null exception here with the query clause
        # print(records)
        # urls = [str(r[3]) for r in records]
    except (Exception, psycopg2.Error) as error:
        return "Error in read/update operation: " + str(error)
    else:
        return records
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()


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

        # print("Table Before updating record ")
        sql_select_query = """select * from public.{} where status = 'New'""".format(table_name)
        # print(sql_select_query)
        cursor.execute(sql_select_query)
        records = cursor.fetchall()
        # add null exception here with the query clause
        # print(records)
        ids = ', '.join([str(record[3]) for record in records])
        # print(ids)

        # set status to in progress
        # update the status later
        sql_update_query = """ update {} set status = 'InQueue', timestamp = CURRENT_TIMESTAMP
                                    where id in ({}) """.format(table_name, ids)

        cursor.execute(sql_update_query)
        connection.commit()
        count = cursor.rowcount

    except Exception as error:
        return "Error in read/update operation: " + str(error)
    else:
        return records
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()



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
        # print(columns_names)
        # print(column_values)
        # create insert statement

        sql_insert_statement = """ INSERT INTO {} ({}) VALUES ({});""".format(table_name, columns_names, column_values)
        # print(sql_insert_statement)
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
        # print('trying insert statement')
        cursor.execute(sql_insert_statement)
        # print('insert statement completed')
        connection.commit()
        count = cursor.rowcount
        # print(count)

    except Exception as error:
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
        # print(sql_update_statement)
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


    except Exception as error:
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
