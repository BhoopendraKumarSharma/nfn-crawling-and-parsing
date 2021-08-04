# noinspection DuplicatedCode
import pandas as pd
from functools import reduce
from html_table_parser import HTMLTableParser
from gen_parser.funds.dividend_history_config import identify_column, transform_type_col_df, div_hist_headers


def get_dividend_history(domain_name, html, class_name=None, fund_name=None, other_header_name=None, other_header_value=None):
    """
    :param other_header_value:
    :param other_header_name:
    :param domain_name:
    :param fund_name: if fund name is mentioned under remarks column
    :param class_name: if class name is mentioned under remarks column
    :param html: html data for div history tables
    :return: if div history found then returns the data
    """

    div_history_found = False
    df_result = None
    flipped_headers = False
    # soup = BeautifulSoup(html, 'lxml')  # Parse the HTML as a string
    # tables = soup.findAll("table")

    # for table in tables:
    #     print(str(table))
    #     if table.findParent("table") is None:
    #         print(str(table))

    p = HTMLTableParser()
    p.feed(html)

    number_of_tables = len(p.tables)
    for i in range(number_of_tables):
        # print(p.tables[i])
        # check if headers are there in dataframe
        is_it_div_history = False
        df = pd.DataFrame(p.tables[i])
        # check if the headers are flipped in rows
        flipped_headers = are_headers_flipped(df, 'bool')
        if flipped_headers:
            df = df.transpose()
        if not df.empty:
            if flipped_headers:
                new_headers = df.iloc[0]
                df = df[1:]
                df.columns = new_headers
            else:
                # df = df[1:]
                # print(df)
                new_headers = df.iloc[0]
                df = df[1:]
                df.columns = new_headers
            # create a set of headers
            columns_set = set(df.columns)
            columns_set = set(filter(None, columns_set))
            # compare with the headers set
            columns_set_std = {identify_column(str(each_key)) for each_key in columns_set}
            columns_set_lowers = {each_key.lower() for each_key in columns_set_std}
            div_hist_headers_lowers = {each_key.lower() for each_key in div_hist_headers}
            match_list = list(div_hist_headers_lowers.intersection(columns_set_lowers))
            # check how many headers match
            if len(match_list) >= len(list(columns_set)) / 2 and len(match_list) > 3:
                is_it_div_history = True
                div_history_found = True
                if 'diamond' in domain_name or 'permanentportfoliofunds' in domain_name or 'needhamfunds' in domain_name:
                    df.columns.values[0] = 'Class Name'
            if is_it_div_history:

                if isinstance(df_result, pd.DataFrame):
                    """
                    assumes column names and format remain the same in case two div tables
                    are present on the same page
                    
                    """
                    df_result = pd.concat([df_result, df], ignore_index=True)

                else:
                    df_result = df

    # check if class name or fund name already exists in div history, if not add the columns if provided in remakrs
    if isinstance(df_result, pd.DataFrame):
        if class_name:
            df_result['Class Name'] = class_name
        if other_header_name:
            df_result[other_header_name] = other_header_value
        if fund_name:
            df_result['Fund Name/Ticker'] = fund_name

        trfs_result = transform_div_history(df_result)
    # return json_result
    # return an empty dataframe if div history not available
    if not div_history_found:
        return pd.DataFrame()
    else:
        final_res = trfs_result.fillna('')
        return final_res


def transform_div_history(df):
    """
    :param df: data frame carrying a div history table
    :return: transforms the data in col-row standard format
    """
    # get all the headers and change as standard (per dictionary)
    columns_set = list(df.columns)
    is_type_column_available = False
    type_col_name = None

    for col in columns_set:
        if 'type' in col.lower():
            is_type_column_available = True
            type_col_name = col
        std_col_name = identify_column(col)
        columns_set = list(df.columns)
        if std_col_name not in columns_set:
            df.rename(columns={col: std_col_name}, inplace=True)

    res_df = df

    # check for type column and transpose the rows as columns taking dates as base
    if is_type_column_available:
        res_df = transform_type_col_df(df, type_col_name)

    if multiple_classnames_in_headers(df, 'bool'):
        res_df = transform_multiple_classnames(df)

    if head_row(df,  'bool'):
        res_df = transform_head_row(df)

    return res_df


def is_class_available(div_history):
    """
    :param div_history:
    :return:
    """
    pass


def is_fund_available(div_history):
    """
    :param div_history:
    :return:
    """
    pass


def multiple_classnames_in_headers(df, return_type):
    """
    :param return_type: return type, bool or list
    :param df: dataframe consisting div history
    :return: True if there are multiple class names available
    """
    columns_set = set(df.columns)
    class_col_count = 0
    classname_cols = []
    for col in columns_set:
        if ('class' in col.lower() or 'inst' in col.lower() or 'inv' in col.lower()) and col not in div_hist_headers:
            col_values = df[col].tolist()
            col_values = list(filter(None, col_values))
            if all(isinstance(x, (int, float)) for x in col_values):
                class_col_count = class_col_count + 1
                classname_cols.append(col)
    if class_col_count > 1:
        if return_type == 'bool':
            return True
        elif return_type == 'list':
            return classname_cols


def transform_multiple_classnames(df):
    """
    :param df:
    :return:
    """
    # get the list of class col names
    new_df = []
    columns_set = set(df.columns)
    classname_cols = set(multiple_classnames_in_headers(df, 'list'))
    other_cols = list(columns_set - classname_cols)
    class_col_available = False
    class_col_name = ''
    new_col_name = 'Income/Amount/Share/Dividend'
    for col in other_cols:
        if 'class' in col.lower():
            class_col_available = True
            class_col_name = col
    if not class_col_available:
        class_col_name = 'Class Name'
        other_cols.append(class_col_name)
        df[class_col_name] = ''

    # create new df with other cols
    for col in classname_cols:
        rCols = other_cols.copy()
        rCols.append(col)
        n_df = df[rCols]
        n_df.rename(columns={col: new_col_name})
        n_df = n_df[class_col_name].apply(lambda x: col)
        new_df.append(n_df)
    # concat all the dfs
    df_all = reduce(
        lambda d1, d2: pd.concat(d1, d2),
        new_df)
    df = df_all
    df = df.fillna('')
    return df


def head_row(df, return_type):
    """
    :param return_type:
    :param df:
    :return:
    """
    # check if any column contains word class in it
    has_head_rows = False
    columns_set = set(df.columns)
    col_name_which_has_class = None
    df_to_be_used = df.fillna('')
    new_df = pd.DataFrame()

    for col in columns_set:
        try:
            new_df = pd.DataFrame()
            new_df = df_to_be_used[df_to_be_used[col].str.contains("class", case=False)]
        except (ValueError, AttributeError) as v:
            pass
        if not new_df.empty:
            col_name_which_has_class = col
    # apply filter and check if other values are None
    if col_name_which_has_class:
        try:
            new_df = df[df[col_name_which_has_class].str.contains("class", case=False)]
        except KeyError as k:
            pass
    if isinstance(new_df, pd.DataFrame) and col_name_which_has_class:
        has_head_rows = True
        for col_name, values in new_df.items():
            if col_name != col_name_which_has_class:
                for val in values:
                    if val:
                        has_head_rows = False

    # check if rest of the columns are none:
    if return_type == 'bool':
        return has_head_rows
    elif return_type == 'col_name':
        return col_name_which_has_class


def transform_head_row(df):
    """
    :param df:
    :return:
    """

    col_name_which_has_class = head_row(df, 'col_name')
    columns_set = list(df.columns)
    columns_set.append('Class Name')
    new_df = pd.DataFrame(columns=columns_set)
    class_name = None
    is_head_row = False
    j = 1
    for i in range(len(df)):
        is_head_row = False
        for col in list(df.columns):
            if col == col_name_which_has_class:
                val = df.loc[i+1, col]
                if val:
                    if 'class' in str(val).lower():
                        class_name = val
                        is_head_row = True
            else:
                val = df.loc[i+1, col]
            if not is_head_row:
                new_df.loc[j, col] = val
                new_df.loc[j, 'Class Name'] = class_name
        if not is_head_row:
            j = j+1
    return new_df



def are_headers_flipped(df, return_type):
    """
    :param return_type:
    :param df:
    :return:
    """
    # go through all the columns to see if headers are flipped
    new_headers = df.iloc[0]
    df = df[1:]
    df.columns = new_headers
    # check if a column header is blank
    columns_set = list(df.columns)
    is_flipped = False
    for col in columns_set:
        try:
            values_to_compare = df[col].tolist()
            is_type_col = False
            if isinstance(col, str):
                if 'type' in col.lower():
                    is_type_col = True
            # first check: if all the values are strings
            if all(isinstance(item, str) for item in values_to_compare):
                # match the values with columns
                columns_set_std = {identify_column(str(each_key)) for each_key in values_to_compare}
                columns_set_lowers = {each_key.lower() for each_key in columns_set_std}
                div_hist_headers_lowers = {each_key.lower() for each_key in div_hist_headers}
                match_list = list(div_hist_headers_lowers.intersection(columns_set_lowers))

                if len(match_list) >= len(list(columns_set)) / 2 and len(match_list) > 3 and not is_type_col:
                    if return_type == 'bool':
                        is_flipped = True
                    else:
                        is_flipped = col
        except AttributeError as ae:
            pass
    return is_flipped
