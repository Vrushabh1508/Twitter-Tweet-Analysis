import requests
from google.cloud import pubsub_v1, language_v1, bigquery
from flask import jsonify
import json
from datetime import datetime
from config import get_secret
import os
import logging
import re

logging.basicConfig(level="INFO")

def twitter_to_pubsub(request):
    input_data = request.get_json()
    id = input_data['id']
    image_name = input_data['image_name']
    tags = input_data['tags'][0]
    keywords = tags.get("tags")

    bearer_token = get_secret()
    headers = {"Authorization": f"Bearer {bearer_token}"}
    params = {"max_results": 10}

    tweets_info = {}
    url_hit = 0
    tweets_data = {"data": []}
    
    bq_client = bigquery.Client()
    dataset_id = os.environ.get('DATASET_ID')
    tweet_table = os.environ.get('TWEET_TABLE')

    for keyword in keywords:
        params['query'] = keyword
        try :
            response = requests.get("https://api.twitter.com/2/tweets/search/recent", headers=headers, params=params,timeout= 40)
        except :
            url_hit += 1
        if url_hit == 6 :
            return {}
        logging.info(f"Fetched tweets for keyword: {keyword}")
        tweets = response.json().get('data')
        if tweets:
            tweet_ids = []
            for tweet in tweets :
                tweet_id = int(tweet["id"])
                tweet_temp  = tweet['text'].encode('ascii', 'ignore').decode('ascii')  
                temp = ""
                for tweet_str in tweet_temp.split() :
                    if tweet_str == "\n" or re.search(r'\@.*:|RT|http|//',tweet_str,flags = re.I)  :
                        continue
                    else :
                        temp += tweet_str + " "
                
                query_job = bq_client.query(f"SELECT * FROM `{dataset_id}.{tweet_table}` WHERE tweet_id = {tweet_id}")
                result = query_job.result()
                if len(list(result))>0:
                    continue
                else:
                    tweet_ids.append(temp)
                    tweets_data["data"].append(
                        {
                            "img_name": image_name,
                            "id": tweet["id"],
                            "tweet_text": temp.replace('"', "'"),
                            "tag": keyword
                            })

            tweets_info[keyword] = tweet_ids
        else:
            tweets_info[keyword] = []

    
    table_id = os.environ.get('TABLE_ID')

    # Get the current datetime
    current_datetime = datetime.now()
    # Convert datetime to string in the required format
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
    tags_json = json.dumps(tags)
    tweets_json = json.dumps(tweets_data)
    query = f"""
        insert into `{dataset_id}.{table_id}` (unique_id, image_name, tags, raw_tweets, processing_datetime, created_at) 
        values ("{id}", "{image_name}", PARSE_JSON('{tags_json}'), PARSE_JSON('''{tweets_json}'''), DATETIME("{formatted_datetime}"), DATE("{formatted_datetime}"))
        """

    logging.info(f"query : {query}")
    try:
        query_job = bq_client.query(query)
        query_job.result()
        logging.info("Row inserted successfully in staging table.")
        return "TWEETS_INFO_ADDED_SUCCESSFULLY"
    except Exception as e:
        logging.info("Row insertion in staging table failed.")
        logging.info(f"Error:{e}")
        return "FAILED_TO_INSERT_DATA_INTO_BIGQUERY"