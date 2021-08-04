from datetime import datetime
import json
import asyncio
from db_manager import *
from gen_parser.funds.benchmark_parser import parse_benchmarks
from gen_parser.funds.cdm_config import xpath_key_value
from gen_parser.funds.cdm_config import sql_query
from gen_parser.funds.cdm_parser import parse_xpath_key_value


async def main():  # entry point for the program

    # read data and loop through the cases
    domain_name_col = 'domain'
    page_url_col = 'pageurl'
    xpath_col = 'xpath'
    connector_col = 'connector'
    fieldname_col = 'fieldname'
    fieldtype_col = 'fieldtype'

    # create a resultant df

    res_df = pd.DataFrame(columns=[domain_name_col, page_url_col, xpath_col, fieldname_col, fieldtype_col, 'parsed value'])
    data_query = sql_query

    df = read_data_from_sql_query(data_query)
    for r in range(len(df)):
        # get the details needed for parsing
        domain_name = df.loc[r, domain_name_col]
        page_url = df.loc[r, page_url_col]
        xpath = df.loc[r, xpath_col]
        fieldname = df.loc[r, fieldname_col]
        fieldtype = df.loc[r, fieldtype_col]

        if not str(df.loc[r, connector_col]):
            if 'benchmark' in fieldname.lower():
                res_df = await parse_benchmarks(domain_name, page_url, xpath, fieldname, fieldtype, res_df)
                print(res_df)
            elif 'multiple data field' in fieldname.lower():
                pass
                # res_df = await parse_multipledatafields(domain_name, page_url, xpath, fieldname, fieldtype, res_df)
            elif any(keyword in fieldname for keyword in xpath_key_value):
                res_df = await parse_xpath_key_value(domain_name, page_url, xpath, fieldname, fieldtype, res_df)

    # convert the output to json
    date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
    filename = f"output_files/CDM_{date}.json"
    res_df.reset_index().to_json(filename, orient='records')
    filename = f"output_files/CDM_{date}.csv"
    res_df.to_csv(filename, index=False, encoding='utf-8-sig')

asyncio.run(main())