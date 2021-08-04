import html
import xml.etree.ElementTree
import xml.sax.saxutils
import lxml.html
from dateutil import parser
# from gen_parser.NER.spcay_ner import extract_orgs
from load_with_selenium import load_html_with_click
from load_with_splash import load_html
from loading_config import selenium_cases
from db_manager import *


async def parse_xpath_key_value(domain_name, page_url, xpath, fieldname, fieldtype, res_var):
    """
    :param domain_name:
    :param page_url:
    :param xpath:
    :param fieldname:
    :param fieldtype:
    :param res_var:
    :return:
    """
    # query from nfn funf list table

    data_set = get_funds_data()

    if domain_name == "www.hardingloevner.com":
        page_source = load_html_with_click(page_url, click_on_text='Continue')
    elif domain_name in selenium_cases:
        page_source = load_html_with_click(page_url)
    else:
        page_source = await load_html(page_url)
    # result = await load(page_url)

    # slice the given xpath
    print(page_source)
    r_text = get_xpath_value(page_source, xpath)
    if 'date' in fieldname.lower():
        r_text = format_date(r_text)
    if 'table' in fieldtype.lower():
        value = r_text
        res_var.loc[len(res_var.index)] = [domain_name, page_url, xpath, fieldname, fieldtype, value]
    else:
        # expcted_bechmark_values = extract_orgs(r_text)
        value = 'dt function missing'
        res_var.loc[len(res_var.index)] = [domain_name, page_url, xpath, fieldname, fieldtype, value]
    print(res_var)
    return res_var


def get_xpath_value(page_source, xpath) -> str:
    """
    :param page_source:
    :param xpath:
    :return:
    """
    xpath_value = ''
    doc = lxml.html.fromstring(page_source)
    xpath_html = doc.xpath(xpath)
    print(xpath_html)
    if isinstance(xpath_html, list):
        for x in range(len(xpath_html)):
            try:
                x_value = lxml.html.tostring(xpath_html[x])
                xpath_value = xpath_value + ''.join(xml.etree.ElementTree.fromstring(x_value).itertext())
            except TypeError:
                x_value = str(xpath_html[x])
                xpath_value = xpath_value + x_value
    print(xpath_value)
    xpath_value = html.unescape(xpath_value)
    xpath_value_without_line_breaks = xpath_value.replace("\\n", " ")
    print(xpath_value_without_line_breaks)
    return xpath_value_without_line_breaks


def format_date(date_as_string):
    """
    :param date_as_string:
    :return:
    """
    res_date = parser.parse(date_as_string, fuzzy=True)
    formatted_date = res_date.isoformat()
    return formatted_date


def get_funds_data(domain_name):
    """
    :param domain_name:
    :return:
    """
    sql_query = ''
    data = read_data_from_sql_query()

    return data