import os, ntpath
import json
import csv
import pandas as pd

headers = ['Domain', 'Page URL', 'Fund Name/Ticker', 'Record Date', 'Ex Date', 'Payble Date', 'Reinvest Date',
           'Ordinary Income', 'ST Cap Gains', 'LT Cap Gains', 'Total Distribution',
           'Reinvest Nav/Price', 'Class Name', 'Income/Amount/Share/Dividend', 'YEAR',
           'Annual Distribution Rate* (%)',
           'Period', 'Investment Income', 'Return of Capital', 'Capital Gains', 'Rate', 'Year End NAV']


def convert_to_csv(filepath):
    """
    :param filepath: filepath
    :return: converts a list based json to csv
    """

    df = pd.read_json(filepath)
    filename = filepath.replace('json', 'csv')
    df.fillna('')
    # detect column to split
    test_df = pd.DataFrame(columns=df.columns)
    for i in range(len(df)):
        row_df = df.iloc[[i]]
        row_df.reset_index(inplace=True, drop=True)
        columns_to_split = get_columns_to_split(row_df)
        columns_to_be_repeated = list(set(df.columns) - set(columns_to_split))
        new_df = (row_df.set_index(columns_to_be_repeated)
                  .apply(lambda x: x.str.split(',').explode())
                  .reset_index())
        test_df = pd.concat([new_df, test_df], ignore_index=True)
    # if columns_to_split:
    #     new_df = (df.set_index(columns_to_be_repeated)
    #               .apply(lambda x: x.str.split(',').explode())
    #               .reset_index())
    # explode dataframe
    new_df = test_df[headers]
    new_df.to_csv(filename, index=False, encoding='utf-8-sig')



def get_columns_to_split(row_df):
    """
    :param row_df: python dataframe with cell values as python lists
    :return: converts df values from list to string and returns column names
    """
    columns_to_split = []
    row_df.fillna('')
    for i in range(len(row_df)):
        for col in list(row_df.columns):
            val = row_df.loc[i, col]
            # check if val is a list
            if val:
                if isinstance(val, list):
                    if col not in columns_to_split:
                        columns_to_split.append(col)
                    str_val = ','.join(val)
                    row_df.loc[i, col] = str_val
            else:
                row_df.loc[i, col] = ''
    return columns_to_split


# file_name = '/Users/bhupi/PycharmProjects/splash_engine/output_files/div_history_catalystmf_2021_07_29-04:51:21_PM.json'
# convert_to_csv(file_name)
