# -*- coding: utf-8 -*-
"""
Created on Sun Mar 22 16:25:55 2020

@author: Ryan
"""


import boto3


s3 = boto3.resource('s3')

s3_bucket_name = 'wordcloud-bucket-3212020'

s3.Bucket(s3_bucket_name).download_file('raw_text', 'word_corpus.txt')
with open("word_corpus.txt", encoding='utf-8') as text:
    val = text.read()
    print(val)
  