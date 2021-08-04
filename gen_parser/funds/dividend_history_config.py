from functools import reduce
import numpy as np
import pandas as pd

# sql_query = """select * from public.xpath where (domain like '%calamos%')
# --like '%hennessyfunds%'or domain like '%blackcreekdiversified%'
# --or domain like '%bouldercef%' or domain like'%bridgebuildermutualfunds%'
# --or domain like '%calamos%' or domain like '%cambiar%' or domain like '%cloughglobal%'
# --or domain like '%fwcapitaladvisors%')
# and fieldtype not in ('Key Value Pair','Minicrawl', 'Simple Text / Number','URL of PDF') and fieldname like '%Entire%';"""

sql_query1 = """select * from public.xpath where (domain
like '%primecap%')
--or domain like '%Buffalo%'
--or domain like '%Centerstone%')
and fieldtype not in ('Key Value Pair','Minicrawl', 'Simple Text / Number','URL of PDF') and fieldname like '%Entire%';"""

sql_query2 = """select * from nfn_fundlist where fund_domain like '%primecap%';"""

div_hist_headers = {"Class", "Record Date", "ExDate", "Ex-Date", "EX-DIVIDEND DATE", "Ex-Date", "Reinvestment Ex-Date",
                    "Ordinary Income", "Income", "Dividend Income", "STCG", "ST CAP GAINS", "Short-Term",
                    "Short-Term Capital Gains", "Type of Earnings", "Rate per Share (US$)",
                    "LTCG", "LT CAP GAINS", "Long-Term", "Long-Term Capital Gains", "ReInvest NAV", "Reinvestment NAV",
                    "Reinvestment Price", "Pay Date", "PAYABLE DATE", "Payable", "Type", "DISTRIBUTION TYPE",
                    "Amount", "Share", "Fund Name", "Ticker", "Year", "TOTAL DISTRIBUTION", "Distribution Total",
                    "PER SHARE DISTRIBUTION", "Income Dividends per Share ($)", "Distribution Type",
                    "Short-Term Capital Gains per Share ($)", "Payment Date", "Type", "Cash Amount", "Ex – Date",
                    "Long-Term Capital Gains per Share ($)", "Total Distribution per Share ($)",
                    "Annual Distribution Rate* (%)", "Reinvestment NAV (US$)", "Payable Date",
                    "Reinvest Price", "Reinvest Date", "EX-DIVIDEND/REINVEST DATE", "LT Cap Gains", "ST Cap Gains",
                    "Reinvest Nav/Price",
                    "Income/Amount/Share/Dividend", "Payable Date", "Investment Income", "Distribution NAV",
                    "Payble Date", "Ex Date", "Section 19a", "$/PER SHARE", "DECLARATION DATE", "Type"}

# expected_values = {"Class": set('A', 'I', 'Retail', 'C', 'Z', 'R', 'R1', 'R2'), "Date": set('12/10/2020', '6/30/2020')}
div_hist_cols = {"Year": "Year", "Income": "Income/Amount/Share/Dividend",
                 "Fund Name/Ticker": "Fund Name/Ticker", "Ticker": "Fund Name/Ticker",
                 "Income/Amount/Share/Dividend": "Income/Amount/Share/Dividend",
                 "Cash Amount": "Income/Amount/Share/Dividend",
                 "Cash": "Income/Amount/Share/Dividend",
                 "Ordinary Income": "Income/Amount/Share/Dividend",
                 "Dividend Income": "Income/Amount/Share/Dividend",
                 "Distribution NAV": "Income/Amount/Share/Dividend",
                 "Income Dividends per Share ($)": "Income/Amount/Share/Dividend",
                 "Short-Term Capital Gains per Share ($)": "ST Cap Gains",
                 "ST Cap Gains": "ST Cap Gains", "SHORT TERM": "ST Cap Gains",
                 "Long-Term Capital Gains per Share ($)": "LT Cap Gains",
                 "LT Cap Gains": "LT Cap Gains",
                 "LONG TERM": "LT Cap Gains",
                 "REINVESTMENT PRICE ($)": "Reinvest Nav/Price",
                 "Reinvest Nav/Price": "Reinvest Nav/Price",
                 "Year- End NAV": "Reinvest Nav/Price",
                 "Total Distribution per Share ($)": "Total Distribution",
                 "TOTAL DISTRIBUTION": "Total Distribution",
                 "Distribution Total": "Total Distribution",
                 "Total Distribution": "Total Distribution",
                 "PER SHARE DISTRIBUTION": "Total Distribution",
                 "Ex Date": "Ex Date", "EX-DATE": "Ex Date",
                 "Payble Date": "Payble Date",
                 "Record Date": "Record Date",
                 "Reinvest Date": "Reinvest Date",
                 "Capital Gains Distribution": "Capital Gains", "Capital Gains": "Capital Gains",
                 "Class Name": "Class Name", "Investment Income": "Investment Income"
                 }

unique_type_column_values = {'regular': 'dividend', "ordinary income": 'Income/Amount/Share/Dividend',
                             'cash': 'Income/Amount/Share/Dividend'}
classname_column_names = {"a", "c", "i", "r", "l", "p", "inst", "inv"}
garbage_columns = ['Section 19a']


def identify_column(col):
    """
    :param col: col name coming from fund page
    :return: standard col name as per output
    """
    if col in div_hist_cols:  # get the header in case of direct match
        header_value = div_hist_cols[col]
        return header_value
    else:  # guess the header in case of no direct match
        if 'payable' in col.lower() and 'date' in col.lower():
            return 'Payble Date'
        elif (
                'pay' in col.lower() and 'date' in col.lower()) or 'payble' in col.lower() or 'declaration' in col.lower():
            return 'Payble Date'
        elif 'record' in col.lower() and 'date' in col.lower():
            return 'Record Date'
        elif 'ex' in col.lower() and 'date' in col.lower():
            return 'Ex Date'
        elif 'long' in col.lower() or 'ltcg' in col.lower() or ('LT' in col and 'term' in col.lower()) \
                or ('LT' in col and 'gains' in col.lower() and 'cap' in col.lower()):
            return 'LT Cap Gains'
        elif 'short' in col.lower() or 'stcg' in col.lower() or ('ST' in col and 'term' in col.lower()) \
                or ('ST' in col and 'gains' in col.lower() and 'cap' in col.lower()):
            return 'ST Cap Gains'
        elif ('incom' in col.lower() or 'income' in col.lower() or 'dividend' in col.lower()) \
                and 'investment' not in col.lower() \
                and ('total' not in col.lower() and 'distribution' not in col.lower()) \
                and 'ordinary' not in col.lower():
            return 'Income/Amount/Share/Dividend'
        elif 'ordinary' in col.lower() and 'incom' in col.lower() and \
                'qualified' not in col.lower():
            return 'Ordinary Income'
        elif 'reinvest' in col.lower() and 'date' in col.lower():
            return 'Reinvest Date'
        elif 'class' in col.lower():
            return 'Class Name'
        elif 'reinvest' in col.lower() and ('nav' in col.lower() or 'price' in col.lower() or 'amount' in col.lower()):
            return 'Reinvest Nav/Price'
        elif 'distribution' in col.lower() and 'nav' in col.lower() and 'total' not in col.lower():
            return 'Income/Amount/Share/Dividend'
        elif 'distribution' in col.lower() and 'type' not in col.lower() and '19' not in col.lower():
            return 'Total Distribution'
        elif 'invest' in col.lower() and 'income' in col.lower():
            return 'Investment Income'
        elif 'dividend' in col.lower() and 'income' in col.lower():
            return 'Income/Amount/Share/Dividend'
        elif 'dividend' in col.lower() and 'date' not in col.lower():
            return 'Income/Amount/Share/Dividend'
        elif 'income' in col.lower() and 'distribution' in col.lower():
            return 'Capital Gains'
        elif 'ticker' in col.lower() or 'fund' in col.lower():
            return 'Fund Name/Ticker'
        elif 'name' in col.lower():
            return 'Fund Name/Ticker'
        elif 'total' in col.lower():
            return 'Total Distribution'
        elif 'section' in col.lower() and '19' in col.lower():
            return "Section 19a"
        else:
            return col


def transform_type_col_df(df, type_col_name):
    """
    :param type_col_name: exact column name for types
    :param df: df with a type column
    :return: adjusted dataframe with transposed type column values as columns
    """
    """
    loop through the data row by row and change/add column as per type, keep dates as base
    meaning if the dates are same, only change/add columns but if dates are different in a new row
    create a new row
    """
    # unique_date_key = []

    columns_set = list(df.columns)
    columns_set.remove(type_col_name)
    # if 'Record Date' in columns_set:
    #     unique_date_key.append('Record Date')
    # if 'Ex Date' in columns_set:
    #     unique_date_key.append('Ex Date')
    # if 'Payble Date' in columns_set:
    #     unique_date_key.append('Payble Date')

    # add columns to df as per type col

    unique_types = df[type_col_name].unique()
    res_df_final = pd.DataFrame()
    target_col = get_target_col(df)
    if target_col in columns_set:
        columns_set.remove(target_col)
    # filter and create new dfs
    # check if there are more than one types
    # target_col = 'Rate per Share (US$)'
    new_df = []
    if len(unique_types) > 1:
        for i in range(len(unique_types)):
            u_df = df[df[type_col_name] == unique_types[i]]
            # convert column name to identifiable format
            if unique_types[i] in unique_type_column_values:
                unique_types[i] = unique_type_column_values[unique_types[i]]
            # get the standard column name
            col_name = identify_column(unique_types[i])
            # change target column name

            u_df.rename(columns={target_col: col_name}, inplace=True)
            # drop the type column
            n_df = u_df.drop([type_col_name], axis=1)
            # update dataframe
            new_df.append(n_df)
            # concat the dataframes
        # df_all = reduce(
        #     lambda d1, d2: pd.merge(d1, d2, how='outer', on=columns_set),
        #     new_df)
        df_all = pd.concat(new_df, sort=False)
        df_all_groupby = df_all.groupby(columns_set)
        for key, item in df_all_groupby:
            key_group = df_all_groupby.get_group(key)
            if res_df_final.empty:
                res_df_final = key_group
            else:
                res_df_final = pd.concat([res_df_final, key_group])
        df = res_df_final.fillna('')
    elif len(unique_types) == 1:
        col_name = identify_column(unique_types[0])
        df.rename(columns={target_col: col_name}, inplace=True)
        n_df = df.drop([type_col_name], axis=1)
        df = n_df
        df = df.fillna('')
    # detect if class in a singlw row
    print(df)
    return df


def get_target_col(df):
    """
    :param df: dataframe consisting div history
    :return: the column holding values for defined types
    """
    target_col = None
    df_columns_set = set(df.columns)

    std_columns_set = set(div_hist_cols.values())

    # first check non_std_cols if available
    non_std_cols = df_columns_set - std_columns_set
    expected_words = ['price', 'rate', 'amount', 'per', '/', 'share', '(US$)']
    if non_std_cols:
        for col in non_std_cols:
            if all(keyword in col.lower() for keyword in expected_words):
                target_col = col
            elif any(keyword in col.lower() for keyword in expected_words):
                target_col = col
    return target_col


def hanle_blank_headers(df):
    """
    :param df:
    :return:
    """
    return df


def blank_cells(df):
    """
    :param df:
    :return:
    """
    return df


def handle_single_row_entities(df):
    """
    :param df:
    :return:
    """
    return df
