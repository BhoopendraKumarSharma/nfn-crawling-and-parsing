"""
crawl:
rest client to call the crawling engine
query a table in batches, crawl the urls and save the html html_files to s3, update the source table with status
parse:
look for crawled urls in db table, query the records in batches, download the html_files from s3, parse the data
and save it to db table/json/csv file and update the status
"""

from db_manager import *

# define scope variables here


table_name = 'nfn_ref'
batch_size = '10'
page_url_index = 3  # (first column in table starts with 0 index)

# get first n new rows from table

records = get_rows_from_table(table_name, batch_size)
# get the page url to be downloaded
urls = [str(r[page_url_index]) for r in records]

# loop through all the records and save the page to s3
for url in urls:
    pass




