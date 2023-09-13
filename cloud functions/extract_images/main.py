import base64
import requests
import json
import os
from google.cloud import storage,bigquery
from datetime import datetime
import logging
logging.basicConfig(level="INFO")

# Specifying the BigQuery dataset and table information
project_id = os.environ.get('project_id')
dataset_id = os.environ.get('dataset_id')
staging_table = os.environ.get('staging_table')
main_table = os.environ.get('main_table')

def write_to_bq(unique_id,user_input,image_name,image_id,tagslist):
     d={"tags":tagslist}
     tags=json.dumps(d)

     # Get the current datetime
     current_datetime = datetime.now()

     # Convert datetime to string in the required format
     processing_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

     # Get current date
     created_date=current_datetime.date()
     
     # Instantiating a BigQuery client
     client = bigquery.Client()

     
     query = f"""
     INSERT INTO `{dataset_id}.{staging_table}` (unique_id, keyword, image_id, image_name, tags, processing_datetime, created_date) 
     VALUES ("{unique_id}", "{user_input}","{image_id}", "{image_name}",PARSE_JSON('{tags}'),DATETIME("{processing_datetime}"),DATE("{processing_datetime}"))
     """
     try:
          # Execute the query
          query_job = client.query(query)
          query_job.result()
          logging.info("Row inserted successfully in staging table.")
     except Exception as e:
          logging.info("Row insertion in staging table failed.")
          logging.info(f"Error:{e}")

     query = f"""
     INSERT INTO `{dataset_id}.{main_table}` (unique_id, keyword, image_id, image_name, tags, processing_datetime, created_date) 
     VALUES ("{unique_id}", "{user_input}","{image_id}", "{image_name}",PARSE_JSON('{tags}'),DATETIME("{processing_datetime}"),DATE("{processing_datetime}"))
     """
     try:
          # Execute the query
          query_job = client.query(query)
          query_job.result()
          logging.info("Row inserted successfully in main table.")
     except Exception as e:
          logging.info("Row insertion in main table failed.")
          logging.info(f"Error:{e}")


def extract_images(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     
     m = base64.b64decode(event['data']).decode('utf-8')
     m = eval(m)
     unique_id=m['id']
     user_input=m['message']
     logging.info(f"user_input->{user_input}")

     CLOUD_STORAGE_BUCKET_NAME=os.environ.get('CLOUD_STORAGE_BUCKET_NAME')
     NUMBER_OF_IMAGES=10

     # Unsplash API credentials
     UNSPLASH_ACCESS_KEY = os.environ.get('UNSPLASH_ACCESS_KEY')

     # Set the API endpoint URL
     url = f'https://api.unsplash.com/search/photos?query={user_input}&per_page={NUMBER_OF_IMAGES}'

     # Set the headers with the API access key
     headers = {
     'Accept-Version': 'v1',
     'Authorization': f'Client-ID {UNSPLASH_ACCESS_KEY}'
     }

     # Instantiating a BigQuery client
     bq_client = bigquery.Client()

     try:
          # Send GET request to the Unsplash API
          response = requests.get(url, headers=headers)

          # Check if the request was successful
          if response.status_code == 200:
               storage_client=storage.Client()
               # Parse the JSON response
               data = json.loads(response.content)
               
               # Extract the list of images from the response
               images = data['results']
               # Loop through the list of images and store it to cloud storage
               for image in images:
                    image_id=image["id"]
                    #checks if this image is already present in the table
                    query_job = bq_client.query(f"SELECT * FROM `{dataset_id}.{main_table}` WHERE image_id = '{image_id}'")
                    result = query_job.result()
                    if len(list(result))>0:
                         continue
                    image_name=image["alt_description"]
                    if image_name==None:
                         image_name=user_input+str(images.index(image))
                    tagslist=[]
                    for t in image["tags"]:
                         tagslist.append(t["title"])
                    tags=",".join(tagslist)
                    logging.info(f"tags->{tags}")
                    metadata={'unique_id':unique_id,'input':user_input,'tags':tags,'image_id':image_id}
                    write_to_bq(unique_id,user_input,image_name,image_id,tagslist)
                    image_url=image['urls']['regular']
                    # Fetch the image data
                    image_data = requests.get(image_url).content
                    # Upload the image to Cloud Storage
                    bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET_NAME)
                    blob = bucket.blob(image_name)
                    blob.metadata=metadata
                    blob.upload_from_string(image_data, content_type="image/jpeg")
                    logging.info(f"Image {image_name} uploaded to Cloud Storage.")                    
          else:
               logging.error(f"Error: {response.status_code}, {response.text}")
     except Exception as e:
          logging.error(f"Error:{e}")