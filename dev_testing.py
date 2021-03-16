# test parsing for config file
"""
with open('config.yaml') as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
    print(data["db_config"]["user"])

"""

# test file download function
"""
test = s3_download('nfncrawlingandparsing', 'test.html', 'dev')
print(test)

"""

"""
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
from db_manager import get_rows_from_table
import requests as r
import json
import pdb, ast

response = get_rows_from_table("nfn_ref", 86)
urls = [str(r[3]) for r in response]
link = 'http://localhost:8080/parse?url={}&action={}&data_struct=getdivhistory'
for url in urls:
    link = 'http://localhost:8080/parse?url={}&action={}&data_struct=getdivhistory'.format(url, 'save_to_s3')
    response = r.get(link)
    res = ast.literal_eval(response.text)
    url = res['url']
    status = res['status']
    result = res['response']
    json_result = json.loads(result)
    print(json_result)



