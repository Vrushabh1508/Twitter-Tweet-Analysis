from google.cloud import storage,bigquery
from PIL import Image
import json
import os
import logging
logging.basicConfig(level="INFO")

def thumbnail(source_filename, destination_filename):
    THUMBNAIL_SIZE = os.environ.get('THUMBNAIL_SIZE')
    THUMBNAIL_SIZE=eval(THUMBNAIL_SIZE)
    with Image.open(source_filename) as image:
        image=image.resize(THUMBNAIL_SIZE)
        image.save(destination_filename)

def write_to_bq(url,image_name):
    bigquery_client=bigquery.Client()
    # Set your BigQuery dataset and table name
    dataset_id = os.environ.get('dataset_id')
    staging_table = os.environ.get('staging_table')
    main_table = os.environ.get('main_table')

    # Define the BigQuery insert query
    query = f"""
        UPDATE `{dataset_id}.{staging_table}` SET thumbnail_URL="{url}"
        WHERE image_name="{image_name}"
        """

    try:
        # Execute the query
        query_job = bigquery_client.query(query)
        query_job.result()
        logging.info("Row inserted successfully in staging table.")
    except Exception as e:
        logging.error("Row insertion in staging table failed.")

    # Define the BigQuery insert query
    query = f"""
        UPDATE `{dataset_id}.{main_table}` SET thumbnail_URL="{url}"
        WHERE image_name="{image_name}"
        """

    try:
        # Execute the query
        query_job = bigquery_client.query(query)
        query_job.result()
        logging.info("Row inserted successfully in main table.")
    except Exception as e:
        logging.error("Row insertion in main table failed.")

    
def generate_thumbnail(event, context):
    # source bucket and object name from the event data
    source_bucket_name = event['bucket']
    source_object_name = event['name']

    # destination bucket and object name for the thumbnail image
    destination_bucket_name = os.environ.get('destination_bucket_name')
    destination_object_name = f'thumbnail_{source_object_name}'

    try:
        # Download the source image to a local file
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_object_name)
        tags=event['metadata']['tags']
        Created_at=event['timeCreated']
        source_URL = source_blob.public_url
        source_filename = '/tmp/source.jpeg'
        source_blob.download_to_filename(source_filename)

        # Generate the thumbnail image
        destination_filename = '/tmp/thumbnail.jpeg'
        thumbnail(source_filename, destination_filename)

        # Upload the thumbnail image to the destination bucket
        destination_bucket = storage_client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_object_name)
        with open(destination_filename, 'rb') as thumbnail_file:
            destination_blob.upload_from_string(thumbnail_file.read(), content_type="image/jpeg")
        logging.info(f"Thumbnail file {destination_object_name} uploaded to bucket successfully.")
        write_to_bq(destination_blob.public_url,source_object_name)
    except Exception as e:
        logging.error(f"Error:{e}")