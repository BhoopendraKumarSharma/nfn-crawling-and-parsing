import lxml.html
import os
import uuid

import lxml.html
from aiohttp import web, ClientSession

from db_manager import *
from gen_parser.funds.dividend_history_parser import get_dividend_history
from gen_parser.tables.extract_tables import get_tables
from s3_manager import *


async def crawl(request):
    params = request.rel_url.query
    # make sure all the required params are included in the request
    request_params = {field: request.rel_url.query[field] for field in params}
    if 'url' in request_params and 'action' in request_params:
        result = await load(request_params['url'], request_params['action'])
    else:
        result = "status: {}, error: {}".format("failed", "invalid request")
    return web.Response(text=str(result), status=200)


async def load(url, postprocess):
    proxy_url = (
        f'http://localhost:8050/render.html?'
        f'url={url}&timeout=60&wait=1'
    )

    async with ClientSession() as session:
        async with session.get(proxy_url) as resp:
            html_data = await resp.read()
            parsing_data = html_data.decode('utf-8')
    if postprocess == 'save_to_s3':
        filename = str(uuid.uuid4())
        with open(os.getcwd() + "/" + filename + '.html', 'wb') as html_file:
            html_file.write(html_data)
        status = s3_upload(os.getcwd() + "/" + filename + '.html',
                           'nfncrawlingandparsing', 'dev/')

        if status == 1:
            insert_records_to_table('nfn_ref', **{"pageurl": url, "status": "New", "domain": url,
                                                  "bucket_name": 'nfncrawlingandparsing',
                                                  "htmlfile_name": filename + '.html',
                                                  "htmlfile_path": 'dev/' + filename + '.html'})
            return {'url': url, 'status': 'file uploaded successfully', 'file_name': 'dev/' + filename + '.html'}
        else:
            return {'url': url, 'status': 'upload failed', 'Exception': status}
    elif postprocess == 'return_html':
        return {"url": url, 'status': 'success', 'response': parsing_data}
    else:
        result = "status: {}, action: {} not found".format("failed", "'" + postprocess + "'")
        return result


async def parser(url, data_struct):
    proxy_url = (
        f'http://localhost:8050/render.html?'
        f'url={url}&timeout=60&wait=1'
    )
    response_data = None
    async with ClientSession() as session:
        async with session.get(proxy_url) as resp:
            html_data = await resp.read()
            parsing_data = html_data.decode('utf-8')
    if data_struct == 'getdivhistory':
        response_data = get_dividend_history(parsing_data)
        # number_of_tables = len(tables)
        # tables_data = "\n".join([str(pd.DataFrame(tables[i])) for i in range(number_of_tables)])
    elif data_struct == 'tables':
        response_data = get_tables(parsing_data)
    return {'url': url, 'status': 'success', 'response': response_data}


async def get_html_by_xpath(request):
    params = request.rel_url.query
    # make sure all the required params are included in the request
    request_params = {field: request.rel_url.query[field] for field in params}
    if 'url' in request_params and 'xpath' in request_params:
        url = request_params['url']
        result = await load(request_params['url'], 'return_html')
        response_text = result["response"]
        page_html = response_text
        doc = lxml.html.fromstring(page_html)
        sliced_html = doc.xpath(request_params['xpath'])[0]
        sliced_text = lxml.html.tostring(sliced_html)
        if 'action' in request_params:
            action = request_params['action']
            if action == 'return_html':
                result = {'url': request_params['url'], 'status': 'success', 'response': sliced_text}
            elif action == 'save_to_s3':
                filename = str(uuid.uuid4())
                with open(os.getcwd() + "/" + filename + '.html', 'wb') as html_file:
                    html_file.write(sliced_text)
                status = s3_upload(os.getcwd() + "/" + filename + '.html',
                                   'nfncrawlingandparsing', 'dev/')

                if status == 1:
                    insert_records_to_table('nfn_ref', **{"pageurl": url, "status": "New", "domain": url,
                                                          "bucket_name": 'nfncrawlingandparsing',
                                                          "htmlfile_name": filename + '.html',
                                                          "htmlfile_path": 'dev/' + filename + '.html'})
                    result = {'url': url, 'status': 'file uploaded successfully',
                              'file_name': 'dev/' + filename + '.html'}
                else:
                    result = {'url': url, 'response': sliced_text, 'status': 'upload failed', 'Exception': status}
            else:
                result = {'url': url, 'status': 'success', 'response': sliced_text, 'action': 'not available'}
        else:
            result = {'url': request_params['url'], 'status': 'success', 'response': sliced_text}
    else:
        result = "status: {}, error: {}".format("failed", "invalid request")
    return web.Response(text=str(result), status=200)


async def parse(request):
    params = request.rel_url.query
    # make sure all the required params are included in the request
    request_params = {field: request.rel_url.query[field] for field in params}
    if 'url' in request_params and 'data_struct' in request_params:
        result = await parser(request_params['url'], request_params['data_struct'])
    else:
        result = "status: {}, error: {}".format("failed", "invalid request")
    return web.Response(text=str(result), status=200)


if __name__ == '__main__':
    app = web.Application()
    app.router.add_route('GET', "/crawl", crawl)
    app.router.add_route('GET', "/parse", parse)
    app.router.add_route('GET', "/get_html_by_xpath", get_html_by_xpath)
    web.run_app(app, host='localhost', port=8080)
