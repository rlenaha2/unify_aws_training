# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:07:33 2020

@author: Ryan
"""

import requests
from bs4 import BeautifulSoup
import re
import os

import boto3
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3_bucket_name = os.environ['s3bucketname']

def lambda_handler(event, context):
    logger.info(f"entered lambda to scrap website")
# =============================================================================
# 
# URL = 'https://docs.python.org/3/tutorial/index.html'
# page = requests.get(URL)
# 
# soup = BeautifulSoup(page.content, 'html.parser')
# 
# whitelist = [
#   'p'
# ]
# 
# text_elements = [t for t in soup.find_all(text=True) if t.parent.name in whitelist]
# 
# print(text_elements)
# 
# 
# =============================================================================
    URL2= 'https://docs.python.org/3/glossary.html'
    page = requests.get(URL2)
    
    soup = BeautifulSoup(page.content, 'html.parser')
    
    whitelist = [
      'p'
    ]
    
    text_elements = [t for t in soup.find_all(text=True) if t.parent.name in whitelist]
    #text_elements = list(map(lambda x:x.strip(),text_elements))
    #text_elements = list(map(lambda x:x.replace("/n", ""),text_elements))

    text_elements = ' '.join(text_elements)
    text_elements = re.sub('\n', '', text_elements)
    logger.info(f"Uploading to {s3_bucket_name}")
    
    
    s3 = boto3.resource('s3')
    with open("/tmp/Output.txt", "w") as text_file:
        text_file.write(text_elements)
    

    s3.Bucket(s3_bucket_name).upload_file('/tmp/Output.txt', 'raw_text')    
           

