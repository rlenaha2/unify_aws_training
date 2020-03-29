# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:59:09 2020

@author: Ryan
"""

import boto3
from wordcloud import WordCloud, STOPWORDS 
import os
import logging
import uuid
import datetime



logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_bucket_name = os.environ['s3bucketname']
dynamodbtable = os.environ['dynamotable']


def lambda_handler(event, context):
    logger.info(f'analyzing {event["url"]}')
    logger.info(f's3 bucket name {s3_bucket_name}')
    s3 = boto3.resource('s3')
    
    s3.Bucket(s3_bucket_name).download_file('text/raw_text', '/tmp/inputtxt.txt')
    with open("/tmp/inputtxt.txt", encoding='utf-8') as text:
        val = text.read()
      
    comment_words = ' '
    stopwords = set(STOPWORDS) 
    stopwords.update([',', '.'])
       
      
    # split the value 
    tokens = val.split() 
      
    # Converts each token into lowercase 
    for i in range(len(tokens)): 
        tokens[i] = tokens[i].lower() 
    
    word_count_dict = {}
    
    for words in tokens: 
        comment_words = comment_words + words + ' '
        if words in word_count_dict:
            if words in stopwords:
                pass
            else:
                word_count_dict[words] += 1
        else:
            if words in stopwords:
                pass
            else:
                word_count_dict[words] = 1
    
    
    top_5_words = []
    word_count = sorted(word_count_dict.items(), key=lambda x: x[1], reverse=True)
    for word in word_count[:6]:
        top_5_words.append(word[0])
    logger.info(f'top 5 words {top_5_words}')

    
    logger.info(f'word count {word_count}')
    logger.info(f'stopwords {stopwords}')
      
    wordcloud = WordCloud(width = 800, height = 800, 
                    background_color ='white', 
                    stopwords = stopwords, 
                    min_font_size = 10).generate(comment_words) 
    
    s3_client = boto3.client('s3')
    my_bucket = s3.Bucket(s3_bucket_name) 
    for obj in my_bucket.objects.all():
        if obj.key.startswith('wordcloud/'):
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
    
    file_uuid = str(uuid.uuid4().hex)
    wordcloud_filename = '/tmp/wordcloud' + file_uuid + '.png'
    s3_wordcloud_filename = 'wordcloud/wordcloud' + file_uuid + '.png'
    wordcloud.to_file(wordcloud_filename)
    s3.Bucket(s3_bucket_name).upload_file(wordcloud_filename, s3_wordcloud_filename)       

    d = datetime.datetime.today()

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodbtable)
    dynamo_item = {
            'id': file_uuid,
            'url' : event["url"],
            'date': str(d)}
    logger.info(f'Dynamo item {dynamo_item}')
    table.put_item(
        Item={
            'id': file_uuid,
            'url' : event["url"],
            'date': str(d),
            'top_5_words': set(top_5_words)})

