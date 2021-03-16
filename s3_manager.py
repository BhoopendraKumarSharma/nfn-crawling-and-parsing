"""
a module to download and upload files to s3

"""

from config_reader import access_key, secret_key
import boto3
import os
import sys


# noinspection PyBroadException
def s3_upload(filepath, bucket_name, subdir=None):
    """

    :param filepath:            (str) path of the file to be uploaded
    :param bucket_name:         (str) aws bucket name
    :param subdir:              (optional str) if there is a subdir in bucket
    :return: 0 or 1
    """
    client = boto3.client('s3', aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
    targeted_bucket = bucket_name
    # make sure subdir has '/' at the end
    if not subdir.endswith('/'):
        subdir = subdir + '/'
    # grab the filename from path
    filename = os.path.basename(filepath)
    if subdir:
        targeted_path = subdir + filename
    else:
        targeted_path = filename

    # upload the file
    # noinspection PyBroadException
    print(filepath)
    try:
        client.upload_file(filepath, targeted_bucket, targeted_path)
    except Exception as u:
        return str(u)
    else:
        return 1


def s3_download(bucket_name, filename, subdir=None):
    """

    :param bucket_name:     (str) name of the bucket
    :param filename:        (str) file name with extension like data.json
    :param subdir:          (optional str) subdir name if applicable
    :return:                (str) filepath of downloaded file
    """
    client = boto3.client('s3', aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
    targeted_bucket = bucket_name
    # make sure subdir has '/' at the end
    print(subdir)
    if not subdir.endswith('/'):
        subdir = subdir + '/'
    # create object name
    if subdir:
        object_name = subdir + filename
    else:
        object_name = filename
    print(object_name)
    # download the file
    # noinspection PyBroadException
    try:
        client.download_file(bucket_name, object_name, filename)
    except Exception as u:
        return str(u)
    else:
        return os.getcwd() + '/' + filename
