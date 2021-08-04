from html_table_parser import HTMLTableParser
import pandas as pd


def get_tables(html):
    """
    :param html: html page source
    :return: all the tables found in the page source
    """

    p = HTMLTableParser()
    p.feed(html)
    list_of_tables = []
    number_of_tables = len(p.tables)
    for i in range(number_of_tables):
        df = pd.DataFrame(p.tables[i])
        print(p.tables[i])
        new_headers = df.iloc[0]
        df = df[1:]
        df.columns = new_headers
        list_of_tables.append(df)
        print(df)

    return list_of_tables