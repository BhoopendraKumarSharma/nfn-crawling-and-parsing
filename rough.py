# import pandas as pd
# from functools import reduce
#
# df = pd.DataFrame()
# print(df)
#
# df['Record Date'] = ['12 / 28 / 20', '12 / 29 / 20', '12 / 30 / 20', '12 / 21 / 20']
# df['Ex Date'] = ['12 / 28 / 20', '12 / 29 / 20', '12 / 30 / 20', '12 / 21 / 20']
# df['Payable Date'] = ['12 / 29 / 20', '12 / 30 / 20', '12 / 31 / 20', '12 / 21 / 20']
# df['Short Term Capital Gain'] = [0.01308, 0.01300, 0.01305, 0.01306]
# df['Reinvestment NAV (US$)'] = [21.56, 21.56, 21.56, 21.56]
#
# print(df)
#
# df1 = pd.DataFrame()
# print(df1)
#
# df1['Record Date'] = ['12 / 28 / 20', '12 / 29 / 20', '12 / 30 / 20', '12 / 21 / 20']
# df1['Ex Date'] = ['12 / 28 / 20', '12 / 29 / 20', '12 / 30 / 20', '12 / 21 / 20']
# df1['Payable Date'] = ['12 / 29 / 20', '12 / 30 / 20', '12 / 31 / 20', '12 / 21 / 20']
# df1['Long Term Capital Gain'] = [0.22345, 0.22346, 0.22347, 0.22348]
# df1['Reinvestment NAV (US$)'] = [21.56, 21.56, 21.56, 21.56]
#
# df2 = pd.DataFrame()
# df2['Record Date'] = ['12 / 28 / 20', '12 / 29 / 20', '12 / 30 / 20', '12 / 21 / 20', '12 / 24 / 20', '12 / 25 / 20']
# df2['Ex Date'] = ['12 / 29 / 20', '12 / 29 / 20', '12 / 30 / 20', '12 / 21 / 20', '12 / 24 / 20', '12 / 25 / 20']
# df2['Payable Date'] = ['12 / 29 / 20', '12 / 30 / 20', '12 / 31 / 20', '12 / 21 / 20', '12 / 26 / 20', '12 / 27 / 20']
# df2['Dividend'] = [0.26335, 0.26336, 0.26337, 0.26338, 0.26339, 0.26330]
# df2['Reinvestment NAV (US$)'] = [21.56, 21.57, 21.56, 21.56, 21.56, 21.57]
#
# nr = 'Dividend'
#
#
#
#
# dfs = [df, df1, df2]
# df_m = pd.merge(df, df1, on=['Record Date', 'Ex Date', 'Payable Date', 'Reinvestment NAV (US$)'])
# df_all = reduce(lambda d1, d2: pd.merge(d1, d2, how='outer', on=['Record Date', 'Ex Date', 'Payable Date', 'Reinvestment NAV (US$)']), dfs)
# if not df_all.empty:
#     newdf = df_all.drop([nr], axis=1)
# print(df_all)
#
# print(df_m)
# print(df_all)


# df = pd.read_json('div_history_mfs_2021_06_22-12:55:53_AM.json')
# print(df)
# data_dict = df.to_dict()
# print(data_dict)
# columns_list = data_dict.keys()
# new_df = pd.DataFrame(columns=columns_list)
# for i in range(len(data_dict[columns_list[0]])):
#     pass


# setA = {1, 2, 3, 5}
# setB = {1, 2, 3, 5}
# setC = setA - setB
# if not setC:
#     print("EMpty Set")
# print(setC)
# for n in setA:
#     print(n)
#

# sql_query = """select * from public.xpath where domain in ('www.bouldercef.com')
#     # and fieldtype not in ('Key Value Pair','Minicrawl', 'Simple Text / Number','URL of PDF')"""
# from db_manager import read_data_from_sql_query
# # sql_query = """ select * from public.nfn_fundlist where fund_domain like '%hardingloevner%'"""
# data = read_data_from_sql_query(sql_query)
# print(data)


# import requests
# url = 'https://www.hardingloevner.com/ways-to-invest/us-mutual-funds/international-equity-portfolio/'
# r = requests.get(url)
# with open('file.txt', 'w') as file:
#     file.write(r.text)


# # Define a dictionary containing Students data
# data = {'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'],
#         'Height': [5.1, 6.2, 5.1, 5.2],
#         'Qualification': ['Msc', 'MA', 'Msc', 'Msc']}
#
# # Convert the dictionary into DataFrame
# df = pd.DataFrame(data)
#
# # Declare a list that is to be converted into a column
#
#
# # Using 'Address' as the column name
# # and equating it to the list
# df['Address'] = ''
# print(df)


# from load_with_selenium import load_html_with_click
# url = 'http://www.bridgebuildermutualfunds.com/fund-listing/bridge-builder-core-bond-fund/'
# xpath = '/html/body/div[1]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div/div/div/ul/li[2]/a'
# test = load_html_with_click(url, click_on_text='Historical Distributions')


import pandas as pd

# Define a dictionary containing employee data
# data = {'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj', None],
#         'Age': [27, 24, 22, 32, None],
#         'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj', 'Sahibabad'],
#         'Qualification': ['Msc', 'MA', 'MCA', 'Phd', None]}
#
# # Convert the dictionary into DataFrame
# list1 = ['A', 'B', 'C', 'D']
# list2 = [12,23,12,13]
# df = pd.DataFrame(list(zip(list1, list2)))
# res = df.transpose()
# print(res)
# print(df)
# col = 'Address'
# new_df = df[df[col].str.contains("Badads", case=False)]
# if not new_df.empty:
#     for col_name, value in new_df.items():
#         if col_name != col:
#             for val in value:
#                 if val:
#                     print("Yes")
#                 else:
#                     print("No")




# select two columns
# new_df = df[['Name', 'Qualification']]
# print(new_df)
# col_names = ['Name', 'Age', 'Address', 'Qualification', 'class']
# create_df = pd.DataFrame(columns=col_names)
# for i in range(len(df)):
#     for col in list(df.columns):
#         var = df.loc[i, col]
#         if 'bad' in var.lower():
#             create_df.loc[i, col] = 'var'
#         else:
#             create_df.loc[i, col] = var
#
# print(create_df)
# list_b = None
# list_a = ['a', 'b', 'c']
# list_b = list_a.copy()
# list_b.append('d')
# print(list_a)
# print(list_b)
# set_a = {'a', 'b', 'c', None}
# set_b = set(filter(None, set_a))
# print(set_b)
#
# import pandas as pd
#
# df_1 = pd.DataFrame(
#     [['Somu', 68, 84, 78, 96],
#      ['Kiku', 74, 56, 88, 85],
#      ['Ajit', 77, 73, 82, 87]],
#     columns=['name', 'physics', 'chemistry', 'algebra', 'calculus'])
#
# df_2 = pd.DataFrame(
#     [['Amol', 72],
#      ['Lini', 78],
#      ['Kiku', 78]],
#     columns=['name', 'geometry'])
#
# df_3 = pd.DataFrame(
#     [['Amol', 172],
#      ['Lini', 178],
#      ['Kiku', 178]],
#     columns=['name', 'overall'])
#
# frames = [df_1, df_2, df_3]
#
# # concatenate dataframes
# df = pd.concat(frames, sort=False)
# df_new = df.groupby('name')
# df_final = df_new.first()
#
# # print dataframe
# print("df_1\n------\n", df_1)
# print("\ndf_2\n------\n", df_2)
# print("\ndf\n--------\n", df)
