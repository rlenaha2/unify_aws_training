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


def get_text_file():
    s3 = boto3.resource('s3')
    
    my_bucket = s3.Bucket(s3_bucket_name) 
    for obj in my_bucket.objects.all():
        if obj.key.startswith('text/'):
            logger.info(f'downloading file {obj}')
            s3.Bucket(s3_bucket_name).download_file(str(obj.key), '/tmp/inputtxt.txt')
    with open("/tmp/inputtxt.txt", encoding='utf-8') as text:
        val = text.read()
        
    return val
    

def create_top_5(word_count_dict):
    top_5_words = []
    word_count = sorted(word_count_dict.items(), key=lambda x: x[1], reverse=True)
    for word in word_count[:6]:
        top_5_words.append(word[0])
        
    return top_5_words, word_count
    
    
def move_old_file_to_history():
    s3 = boto3.resource('s3')
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
    
def create_wordcloud(stopwords, comment_words):
    wordcloud = WordCloud(width = 800, height = 800, 
                background_color ='white', 
                stopwords = stopwords, 
                min_font_size = 10).generate(comment_words) 

    return wordcloud
    

def generate_words(val):
    comment_words = ' '
    stopwords = set(STOPWORDS) 
    stopwords.update([',', '.'])
             
    tokens = val.split() 
      
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
    
    return word_count_dict, comment_words, stopwords

    
def store_wordclouds(wordcloud):
    s3 = boto3.resource('s3')

    file_uuid = str(uuid.uuid4().hex)
    wordcloud_filename = '/tmp/wordcloud_' + file_uuid + '.png'
    s3_wordcloud_filename = 'wordcloud/wordcloud_' + file_uuid + '.png'
    wordcloud.to_file(wordcloud_filename)
    s3.Bucket(s3_bucket_name).upload_file(wordcloud_filename, s3_wordcloud_filename)       

    return file_uuid


def store_in_dynamo(file_uuid, event, top_5_words, sentiment):
    dynamodb = boto3.resource('dynamodb')
    
    d = datetime.datetime.today()
    table = dynamodb.Table(dynamodbtable)
    table.put_item(
        Item={
            'id': file_uuid,
            'url' : event["outputdict"]["url"],
            'text_uuid': event["outputdict"]['uuid'],
            'date': str(d),
            'top_5_words': set(top_5_words),
            'sentiment': sentiment})


def lambda_handler(event, context):
    logger.info(f'analyzing {event}')
    logger.info(f's3 bucket name {s3_bucket_name}')

    val = get_text_file()
    logger.info(f'val {val}')
    word_count_dict, comment_words, stopwords = generate_words(val)
    top_5_words, word_count = create_top_5(word_count_dict)

    logger.info(f'top 5 words {top_5_words}')
    logger.info(f'word count {word_count}')
      
    move_old_file_to_history()
    wordcloud = create_wordcloud(stopwords, comment_words)
    file_uuid = store_wordclouds(wordcloud)   
    
    comprehend = boto3.client('comprehend', region_name='us-west-2')
    sentiments = comprehend.detect_sentiment(Text=comment_words[:4980], LanguageCode='en')
    logger.info(f'sentiment response {sentiments}')
    
    sentiment = sentiments["Sentiment"]

    store_in_dynamo(file_uuid, event, top_5_words, sentiment)
