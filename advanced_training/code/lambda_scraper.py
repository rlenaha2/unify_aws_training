# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:07:33 2020

@author: Ryan
"""

import requests
from bs4 import BeautifulSoup
import os
import uuid
import boto3
import logging
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3_bucket_name = os.environ['s3bucketname']


def move_old_file_to_history():
    s3 = boto3.resource('s3')       
    s3_client = boto3.client('s3')
    my_bucket = s3.Bucket(s3_bucket_name) 
    for obj in my_bucket.objects.all():
        if obj.key.startswith('text/'):
            copy_source = '/' + s3_bucket_name + '/' + str(obj.key)
            logger.info(f"Historical file found, moving file {copy_source}")
            s3_client.copy_object(Bucket=s3_bucket_name,
                                  CopySource=copy_source,
                                  Key='history/' + str(obj.key))
            s3_client.delete_objects(Bucket=s3_bucket_name,
                                     Delete={
                                         'Objects':[
                                             {
                                                 'Key': str(obj.key)
                                                 }]})


def lambda_handler(event, context):
    s3 = boto3.resource('s3')       
    
# =============================================================================
 
#    URL = 'https://www.unifyconsulting.com/'
#    page = requests.get(URL)
#     
#    soup = BeautifulSoup(page.content, 'html.parser') 
#    whitelist = ['p'] 
#    text_elements = [t for t in soup.find_all(text=True) if t.parent.name in whitelist]
#    text_elements = " ".join(text_elements)
#    text_elements = re.sub('\n', '', text_elements)
#    text_elements = " ".join(text_elements.split())
 
 
# =============================================================================

    URL= 'https://docs.python.org/3/glossary.html'
    page = requests.get(URL)
    
    soup = BeautifulSoup(page.content, 'html.parser')
    whitelist = ['p' ]    
    text_elements = [t for t in soup.find_all(text=True) if t.parent.name in whitelist]
    text_elements = ' '.join(text_elements)
    text_elements = re.sub('\n', '', text_elements)
    
# =============================================================================
    
    logger.info(f"Lambda to scrape website {URL}")
    logger.info(f"Uploading to {s3_bucket_name}")
    move_old_file_to_history ()   

    file_uuid = str(uuid.uuid4().hex)
    text_filename = '/tmp/text_' + file_uuid + '.txt'
    s3_text_filename = 'text/text_' + file_uuid + '.txt'
    
    with open(text_filename, "w") as text_file:
        text_file.write(text_elements)
    
    s3.Bucket(s3_bucket_name).upload_file(text_filename, s3_text_filename)       

    output_dict = {'url' : URL, 'uuid' : file_uuid}
    
           
    return output_dict
