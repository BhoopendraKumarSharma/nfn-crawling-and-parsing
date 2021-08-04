import html
import xml.etree.ElementTree
import xml.sax.saxutils
import lxml.html
from aiohttp import ClientSession


async def load_html(url):
    proxy_url = (
        f'http://localhost:8050/render.html?'
        f'url={url}&timeout=60&wait=5'
    )

    async with ClientSession() as session:
        async with session.get(proxy_url) as resp:
            data_html = await resp.read()
            data = data_html.decode('utf-8')
            # doc = lxml.html.fromstring(data)
            # el_html = doc.xpath("//div[@class = 'tb-heading']/following-sibling::table")
            # el_text = lxml.html.tostring(el_html[0]).decode('utf-8')
            # print(el_text)
            # return el_text

            return data

            # print(data)

            # -------------------------------------
            # to be integrated later
            # -------------------------------------
            # doc = lxml.html.fromstring(data)
            # el_html = doc.xpath('//div[@class="oflow-xs-x-auto"]')
            # el_text = lxml.html.tostring(el_html)
            # print(el_text)
            # get_dividend_history(data)
            # filename = str(uuid.uuid4())
            # with open(os.getcwd() + "/html_files/" + filename + '.html', 'wb') as html_file:
            #     html_file.write(data_html)
            #     upload_to_s3 = s3_upload(os.getcwd() + "/html_files/" + filename + '.html',
            #                              'nfncrawlingandparsing', 'prod/')
            #     if upload_to_s3 == 1:
            #         insert_to_db = insert_records_to_table('nfn_div_his_ref',
            #                                                **{"fund_landing_url": url, "fund_url": url, "status": "New", "fund_domain": url,
            #                                                   "bucket_name": 'nfncrawlingandparsing',
            #                                                   "htmlfile_name": filename + '.html',
            #                                                   "htmlfile_path": 'prod/' + filename + '.html'})
            # print(insert_to_db)
            # print(upload_to_s3)
            # response_data = get_dividend_history(data)
            # print(response_data)
            # return response_data