import pandas
import pandas as pd
import json
import csv

# test parsing for config file
"""
with open('config.yaml') as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
    print(data["db_config"]["user"])

"""
import numpy


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, (numpy.int_, numpy.intc, numpy.intp, numpy.int8,
                            numpy.int16, numpy.int32, numpy.int64, numpy.uint8,
                            numpy.uint16, numpy.uint32, numpy.uint64)):
            return int(obj)
        elif isinstance(obj, (numpy.float_, numpy.float16, numpy.float32,
                              numpy.float64)):
            return float(obj)
        elif isinstance(obj, (numpy.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


# test file download function
"""
test = s3_download('nfncrawlingandparsing', 'test.html', 'dev')
print(test)

"""

"""
splash API:
http://localhost:8050/render.html?url=https://www.bouldercef.com/the-fund&timeout=20&wait=5
parse
http://localhost:8080/parse?url=http://www.cullenfunds.com/mutual-funds/high-dividend/dividends&data_struct=tables

get_html_by_xpath
http://localhost:8080/get_html_by_xpath?url=http://www.cullenfunds.com/mutual-funds/high-dividend/dividends&xpath=//*[@id="dividendByYearReturnAJAX"]/table
http://localhost:8080/get_html_by_xpath?url=http://www.cullenfunds.com/mutual-funds/high-dividend/dividends&xpath=//*[@id="dividendByYearReturnAJAX"]/table&action=save_to_s3

crawl

read url
API End Point Method
Batch (Table)(100)


"""

"""
from db_manager import get_rows_from_table
import requests as r
import json
import pdb, ast
import pandas as pd
from parser import transpose_cols
from parser import validate_table_format

response = get_rows_from_table("nfn_ref", 86)
urls = [str(r[3]) for r in response]
print(urls)
link = 'http://localhost:8080/parse?url={}&action={}&data_struct=getdivhistory'
for url in urls:
    if url == r"http://www.cullenfunds.com/mutual-funds/high-dividend/dividends":
        link = 'http://localhost:8080/parse?url={}&action={}&data_struct=getdivhistory'.format(url, 'save_to_s3')
        response = r.get(link)
        # pdb.set_trace()
        res = ast.literal_eval(response.text)
        url = res['url']
        status = res['status']
        result = res['response']
        print("Result>>>>>>>>>>>")
        print(result)
        # noinspection PyBroadException

        json_result = json.loads(result)
        df = pd.DataFrame.from_dict(json_result)
        validate_table_format(df)
        break

"""

import ast

# with open('../../Desktop/[DarkNightRises]/Programming/Python/Json/html_files/Q4 Confidential_Wave1_20210317.json') as file:
#     data = json.loads(file)
# df = pd.json_normalize(data)
# df.to_csv('q4_data.csv', encoding='utf-8-sig')

import csv

#
# def make_json(csv_file_path, json_file_path):
#     # create a dictionary
#     data = {}
#
#     # Open a csv reader called DictReader
#     with open(csv_file_path, encoding='utf-8') as csvf:
#         csv_reader = csv.DictReader(csvf)
#
#         # Convert each row into a dictionary
#         # and add it to data
#         for rows in csv_reader:
#             # Assuming a column named 'No' to
#             # be the primary key
#             key = rows['Domain']
#             data[key] = rows
#
#     # Open a json writer, and use the json.dumps()
#     # function to dump data
#     with open(json_file_path, 'w', encoding='utf-8') as jsonf:
#         jsonf.write(json.dumps(data, indent=4))
#
#
# # Driver Code
#
# # Decide the two file paths according to your
# # computer system
# csvFilePath = 'output_data_202104131950.csv'
# jsonFilePath = 'div_history_sample2.json'
#
# # Call the make_json function
# make_json(csvFilePath, jsonFilePath)
#
# import pandas as pd
#
# df = pd.read_csv('output_data_202104131950.csv')
# df.to_json('div_history_sample2.json')

my_dict = dict()
my_dict['domain'] = ['bridgewayfunds.com', 'www.diamond-hill.com', 'www.cullenfunds.com']
my_dict['Fund Name/Ticker'] = ['AGGRESSIVE INVESTORS 1 FUND', 'www.diamond-hill.com', '']
my_dict['record_date'] = [[12 / 15 / 2020, 12 / 16 / 2019, 12 / 17 / 2018, 12 / 14 / 2016],
                          [12 / 29 / 20, 12 / 27 / 19, 11 / 12 / 19, 12 / 27 / 18],
                          [3 / 29 / 2021, 2 / 24 / 2021, 1 / 27 / 2021, 3 / 29 / 2021]]
my_dict['ordinary_income'] = [[0.7228, 0.8189, 0.3613, 0.3570], [None, None, None, None],
                              [0.03907000, 0.04867000, 0.01192000, 0.02900000], []]

print(my_dict['domain'])
my_dict['domain'].append('New Fund')
print(my_dict['domain'])

# # convert to json
#
# with open("sample.json", "w") as outfile:
#     json.dump(my_dict, outfile)

my_dict = dict()
df = pd.read_csv('output_files/output_data_202104131950.csv')
df.fillna('NULL', inplace=True)

# add keys with blank rows
for col in df.columns:
    my_dict[col] = []
# print(df['Domain'])
domains = df.Domain.unique()

for i in range(len(domains) - 1):
    domain = (domains[i])
    domain_data = df[df['Domain'] == domain]
    #     # add the data to dictionary
    #     page_url = domain_data['Page URL'].unique()
    #     fund_name = domain_data['Fund Name/Ticker'].unique()
    #     record_date = domain_data['Fund Name/Ticker']

    for key in my_dict.keys():
        if key in ['Domain', 'Page URL', 'Fund Name/Ticker']:
            value = ''.join(domain_data[key].unique())
            my_dict[key].append(value)
        else:
            key_data = list(domain_data[key])
            print(key_data)
            my_dict[key].append(key_data)
# print(my_dict['Domain'])
# print(my_dict['Page URL'])
# print(my_dict['Record Date'])


with open("output_files/div_history_sample1.json", "w") as outfile:
    json.dump(my_dict, outfile)

# test = df[df['Domain'] == 'bridgewayfunds.com']
# headers = test.columns
# print(headers)
# r_dates = test['Record Date']
# print(list(r_dates))
