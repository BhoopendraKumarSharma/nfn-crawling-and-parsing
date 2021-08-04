import html
import xml.etree.ElementTree
import xml.sax.saxutils
import lxml.html
import asyncio
import json
from datetime import datetime

import pandas as pd

from db_manager import *
from gen_parser.funds.dividend_history_parser import get_dividend_history
from gen_parser.tables.extract_tables import get_tables
from gen_parser.funds.dividend_history_config import sql_query1, sql_query2
from load_with_selenium import load_html_with_click
from loading_config import selenium_cases
from load_with_splash import load_html
from convert_to_std_csv import convert_to_csv


async def main():  # entry point for the program
    div_history = dict()
    headers = ['Domain', 'Page URL', 'Fund Name/Ticker', 'Record Date', 'Ex Date', 'Payble Date', 'Reinvest Date',
               'Ordinary Income', 'ST Cap Gains', 'LT Cap Gains', 'Total Distribution',
               'Reinvest Nav/Price', 'Class Name', 'Income/Amount/Share/Dividend', 'YEAR',
               'Annual Distribution Rate* (%)',
               'Period', 'Investment Income', 'Return of Capital', 'Capital Gains', 'Rate', 'Year End NAV']
    for hdrs in headers:
        div_history[hdrs] = []

    # read data and loop through the cases
    fund_urls = None
    fund_names = None
    page_urls = None

    df1 = read_data_from_sql_query(sql_query1)
    if not df1.empty:
        page_urls = df1['pageurl'].tolist()
    df2 = read_data_from_sql_query(sql_query2)
    if not df2.empty:
        fund_urls = df2['fund_url'].tolist()
        fund_names = df2['fund_name'].tolist()
    final_columns = ['domain', 'fundname', 'pageurl', 'xpath', 'fieldname', 'fieldtype', 'fieldfilter', 'customefield',
                     'remarks']
    df = pd.DataFrame(columns=final_columns)
    # add data from df2
    for i in range(len(page_urls)):
        values = []
        fund_name = None
        for col in list(df.columns):
            # print(col
            if col == 'fundname':
                val = df1.loc[i, 'pageurl']
                # check if exists in fund_urls
                if fund_urls:
                    if val in fund_urls:
                        fund_url_index = fund_urls.index(val)
                        fund_urls.remove(val)
                        # get the fund name
                        fund_name = fund_names[fund_url_index]
                        val = fund_name
                        fund_names.remove(fund_name)
                else:
                    fund_name = df1.loc[i, 'domain']
            else:
                val = df1.loc[i, col]
            values.append(val)
        df.loc[len(df.index)] = values

    if fund_urls:
        if len(fund_urls) >= 1:
            if len(fund_urls) > len(page_urls):
                final_count = len(fund_urls)
            else:
                final_count = len(page_urls)
            for i in range(final_count):
                values = []
                for col in list(df.columns):
                    if col == 'pageurl':
                        val = fund_urls[i]
                    elif col == 'fundname':
                        val = fund_names[i]
                    else:
                        val = df.loc[0, col]
                    values.append(val)
                df.loc[len(df.index)] = values

    # loop through all the records for div history
    domain_name_col = 'domain'
    fund_name_col = 'fundname'
    page_url_col = 'pageurl'
    xpath_col = 'xpath'
    remarks_col = 'remarks'
    cust_field_col = 'customefield'

    for r in range(len(df)):
        if str(df.loc[r, page_url_col]) != 'nan':
            domain_name = df.loc[r, domain_name_col]
            page_url = df.loc[r, page_url_col]
            fund_name = df.loc[r, fund_name_col]
            remarks = None
            cust_field = None
            xpath_field = None

            if '1290' in page_url:
                if '.php' in page_url:
                    page_url = page_url.replace('.php', '')
                    print(page_url)

            remarks = df.loc[r, remarks_col]
            cust_field = df.loc[r, cust_field_col]
            xpath_field = df.loc[r, xpath_col]

            if 'heartlandadvisors' in page_url:
                remarks = 'Class Name'
                cust_field = '//*[@id="centerZone"]/div[8]/div/div[2]/div[1]/ul/li[1]||//*[@id="centerZone"]/div[8]/div/div[2]/div[1]/ul/li[2]'
                xpath_field = '//*[@id="centerZone"]/div[8]/div/div[2]/div[2]/div[2]||//*[@id="centerZone"]/div[8]/div/div[2]/div[3]/div[2]'

                if fund_name == 'Mid Cap Value':
                    remarks = 'Class Name'
                    cust_field = '//*[@id="centerZone"]/div[9]/div/div[2]/div[1]/ul/li[1]||//*[@id="centerZone"]/div[9]/div/div[2]/div[1]/ul/li[2]'
                    xpath_field = '///*[@id="centerZone"]/div[9]/div/div[2]/div[2]/div[2]||//*[@id="centerZone"]/div[9]/div/div[2]/div[3]/div[2]'

            if 'needhamfunds' in page_url:
                remarks = 'Class Name'
                cust_field = '//*[@id="distributions"]/div[2]/table/thead/tr/th[2]'
                xpath_field = '//*[@id="distributions"]/div[2]/table'


            if domain_name == "www.hardingloevner.com":
                page_source = load_html_with_click(page_url, click_on_text='Continue')
            elif 'primecap' in domain_name.lower():
                page_source = load_html_with_click(page_url, click_on_xpath='//*[@id="distributions"]/div/button')
            elif domain_name == 'wix-visual-data.appspot.com':
                page_source = load_html_with_click(page_url)
            elif 'fwcapitaladvisors' in domain_name.lower():
                xpath = '/html/body/div[1]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div/div/div/ul/li[2]/a'
                page_source = load_html_with_click(page_url, click_on_xpath=xpath)
            elif 'bridgebuildermutualfunds' in domain_name.lower():
                page_source = load_html_with_click(page_url, click_on_text='Historical Distributions')
            elif 'cullenfunds' in domain_name.lower():
                page_url = page_url.replace('overview', 'dividends')
                page_source = await load_html(page_url)
            elif domain_name in selenium_cases:
                page_source = load_html_with_click(page_url)
            else:
                page_source = await load_html(page_url)
            # check for multiple items on same page
            cust_field_html_list = []
            div_history_html_list = []
            cust_field_html = None
            div_history_html = None
            if remarks and cust_field:
                doc = lxml.html.fromstring(page_source)
                if '||' in cust_field and '||' in xpath_field:
                    cust_field_list = cust_field.split("||")
                    xpath_field_list = xpath_field.split("||")
                    for i in range(len(cust_field_list)):
                        cust_field_html = doc.xpath(cust_field_list[i])
                        cust_field_html_list.append(cust_field_html)
                        div_history_html = doc.xpath(xpath_field_list[i])
                        div_history_html_list.append(div_history_html)
                else:
                    cust_field_html = doc.xpath(cust_field)
                    cust_field_html_list.append(cust_field_html)
                    div_history_html = doc.xpath(xpath_field)
                    div_history_html_list.append(div_history_html)
            # result = await load(page_url)

            div_history_text = []
            cust_field_text = []

            if cust_field_html_list and div_history_html_list:
                for i in range(len(cust_field_html_list)):
                    cust_field_html = cust_field_html_list[i]
                    div_history_html = div_history_html_list[i]
                    for j in range(len(div_history_html)):
                        dht = lxml.html.tostring(div_history_html[j]).decode('utf-8')
                        try:
                            cft = lxml.html.tostring(cust_field_html[j]).decode('utf-8')
                        except IndexError as e:
                            pass
                        div_history_text.append(dht)
                        cust_field_text.append(cft)

            if div_history_text and cust_field_text:
                for i in range(len(div_history_text)):
                    if remarks.lower() == 'fund name':
                        fund_name = ''.join(xml.etree.ElementTree.fromstring(cust_field_text[i]).itertext())
                        if 'dividend' and 'focus' in fund_name.lower():
                            result = pd.read_csv('/Users/bhupi/PycharmProjects/splash_engine/output_files/div_his_dividend_focus.csv')
                        else:
                            result = get_dividend_history(domain_name, div_history_text[i], fund_name=fund_name)
                    elif remarks.lower() == 'class name':
                        class_name = ''.join(xml.etree.ElementTree.fromstring(cust_field_text[i]).itertext())
                        result = get_dividend_history(domain_name, div_history_text[i], class_name=class_name)
                    else:
                        other_header_value = ''.join(xml.etree.ElementTree.fromstring(cust_field_text[i]).itertext())
                        result = get_dividend_history(domain_name, div_history_text[i], remarks, other_header_value)
                    for key in div_history.keys():
                        if key in ['Domain', 'Page URL', 'Fund Name/Ticker']:
                            if key == 'Domain':
                                div_history['Domain'].append(domain_name)
                            elif key == 'Page URL':
                                div_history['Page URL'].append(page_url)
                            else:
                                div_history['Fund Name/Ticker'].append(fund_name)
                        else:
                            if not result.empty:
                                if key in result.columns:
                                    key_data = list(result[key])
                                    print(key_data)
                                    div_history[key].append(key_data)
                                else:
                                    div_history[key].append(None)
                            else:
                                div_history[key].append(None)
                    print(div_history)
            else:
                tables = get_tables(page_source)
                print(tables)
                result = get_dividend_history(domain_name, page_source)
                # loop through the df and isert the results to dict
                for key in div_history.keys():
                    if key in ['Domain', 'Page URL', 'Fund Name/Ticker']:
                        if key == 'Domain':
                            div_history['Domain'].append(domain_name)
                        elif key == 'Page URL':
                            div_history['Page URL'].append(page_url)
                        else:
                            div_history['Fund Name/Ticker'].append(fund_name)
                    else:
                        if not result.empty:
                            if key in result.columns:
                                key_data = list(result[key])
                                print(key_data)
                                div_history[key].append(key_data)
                            else:
                                div_history[key].append(None)
                        else:
                            div_history[key].append(None)
                print(div_history)

    print(div_history)

    # convert div_history to json file
    date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
    filename = f"output_files/div_history_primecap_{date}.json"
    with open(filename, "w") as outfile:
        json.dump(div_history, outfile)
    # save file as csv too
    convert_to_csv(filename)


asyncio.run(main())

# //th[contains(text(),'Record')]/parent::tr/parent::tbody||//th[contains(text(),'Record')]/parent::tr/parent::tbody
# //th[contains(text(),'Record')]/parent::tr/parent::tbody||//th[contains(text(),'Record')]/parent::tr/parent::tbody
# Class Name
# /html/body/div[2]/div[2]/div[1]/section/ul/li[3]/div/div/p[1]/text()[1]||/html/body/div[2]/div[2]/div[1]/section/ul/li[3]/div/div/p[1]/text()[2]