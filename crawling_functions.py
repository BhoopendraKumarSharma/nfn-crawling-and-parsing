"""
extension for crawling engine
"""

import os
import uuid
import yaml

from db_manager import insert_records_to_table
from s3_manager import s3_upload
from gen_parser import funds


def get_db_config(filename):
    with open(filename) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data



def url_lookup(url):
    """
    :param url: url to be checked for
    :return: if file already exists in s3 bucket, download the file rather than crawling again
    """





def save_to_s3(html_data, url):
    """
    :param html_data: data in html format with utf-8 encoding
    :param url: url of the data source
    :return: {'url': ?, 'status': ?, 'filename': ? or 'Exception': ?}
    """

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
        return {'url': url, 'status': 'upload failed', 'Exception': str(status)}
