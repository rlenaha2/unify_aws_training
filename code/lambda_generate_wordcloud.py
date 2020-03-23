# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:59:09 2020

@author: Ryan
"""

import boto3
from wordcloud import WordCloud, STOPWORDS 
import os

#import matplotlib.pyplot as plt 

s3_bucket_name = os.environ['s3bucketname']

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    
    s3.Bucket(s3_bucket_name).download_file('raw_text', '/tmp/inputtxt.txt')
    with open("/tmp/inputtxt.txt", encoding='utf-8') as text:
        val = text.read()
      
    comment_words = ' '
    stopwords = set(STOPWORDS) 
       
      
    # split the value 
    tokens = val.split() 
      
    # Converts each token into lowercase 
    for i in range(len(tokens)): 
        tokens[i] = tokens[i].lower() 
        
    for words in tokens: 
        comment_words = comment_words + words + ' '
      
      
    wordcloud = WordCloud(width = 800, height = 800, 
                    background_color ='white', 
                    stopwords = stopwords, 
                    min_font_size = 10).generate(comment_words) 
      
    # plot the WordCloud image                        
    #plt.figure(figsize = (8, 8), facecolor = None) 
    #plt.imshow(wordcloud) 
    #plt.axis("off") 
    #plt.tight_layout(pad = 0) 
    wordcloud.to_file('/tmp/wordcloud.png')
    s3.Bucket(s3_bucket_name).upload_file('/tmp/wordcloud.png', 'wordcloud.png')       

