import asyncio, time
from asyncio import gather, create_task
from aiohttp import web, ClientSession
from bs4 import BeautifulSoup
from s3_manager import s3_upload
import os, uuid, lxml.html
from gen_parser import *


async def load(url):
    proxy_url = (
        f'http://localhost:8050/render.html?'
        f'url={url}&timeout=60&wait=1'
    )
    # print(proxy_url)

    async with ClientSession() as session:
        async with session.get(proxy_url) as resp:
            data_html = await resp.read()
            data = data_html.decode('utf-8')

            # -------------------------------------
            # to be integrated
            # -------------------------------------
            # doc = lxml.html.fromstring(data)
            # el_html = doc.xpath('//*[@id="dividendByYearReturnAJAX"]/table')[0]
            # el_text = lxml.html.tostring(el_html)
            get_dividend_history(data)
            filename = str(uuid.uuid4())
            with open(os.getcwd() + "/" + filename + '.html', 'wb') as html_file:
                html_file.write(data_html)
    upload_to_s3 = s3_upload(os.getcwd() + "/" + filename + '.html',
                             'nfncrawlingandparsing', 'dev/')
    print(upload_to_s3)
    # return data
    # /html/body/div/main/div/div/div/div[1]/div[2]/div/div[2]/table
    # //*[@id="dividendByYearReturnAJAX"]/table


async def main():
    result = await load("http://www.cullenfunds.com/mutual-funds/high-dividend/dividends")
    # print(result)


asyncio.run(main())
