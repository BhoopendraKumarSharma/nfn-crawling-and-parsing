from gen_parser.funds.dividend_history_parser import get_dividend_history
import pandas as pd
import pdb


def validate_table_format(df):
    """
    :param df: dataframe that consists the target table
    :return:
    """
    # see if table is just fine and can be processed without any helper function
    # check headers one by one and look at the values if they are as expected

    columns_set = set(df.columns)
    for col in columns_set:
        is_any_value_blank = df[col].isnull().values.any()
        print(is_any_value_blank)
    pdb.trace()


def slice_html_by_xpath(html, xpath):
    """
    :param html: html decoded as text using encoding 'utf-8'
    :param xpath: xpath of the element
    :return: sliced element from the html
    """

    pass


def transpose_rows(dataframe):
    """
    :param dataframe:
    :return:
    """
    # check headers in rows
    # rest of the columns are null for these headers


def transpose_cols(dataframe):
    """
    :param dataframe:
    :return:
    """
    pass


