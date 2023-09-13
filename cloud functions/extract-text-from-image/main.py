import io
import os
import re
from google.cloud import storage, vision, bigquery
from google.cloud.vision_v1 import types
import logging

logging.basicConfig(level="INFO")

# Initialize the client
client = vision.ImageAnnotatorClient()

# Initialize the storage client
storage_client = storage.Client()

# Initialize the BigQuery client
bigquery_client = bigquery.Client()

# Set your BigQuery dataset and table name
dataset_id = os.environ.get('dataset_id')
staging_table = os.environ.get('staging_table')
main_table = os.environ.get('main_table')

# Define the Cloud Function
def detect_text(event, context):
    """Cloud Function triggered by Cloud Storage when a new file is uploaded."""

    # Get the file name from the event object
    file_name = event['name']

    # Get the bucket name from the event object
    bucket = event['bucket']

    # Get a reference to the Cloud Storage object
    blob = storage_client.bucket(bucket).get_blob(file_name)

    # Load the image into memory
    content = blob.download_as_bytes()

    # Check if the image content is empty
    if not content or len(content) == 0:
        print(f'The image file {file_name} is empty.')
    else:
        image_name = file_name

        image = types.Image(content=content)

        # Call the Cloud Vision API to detect text
        response = client.text_detection(image=image)
        texts = response.text_annotations

        # Check if there are any text annotations
        text_flag = False
        if texts:
            # Print the detected text
            response_text = texts[0].description
            if response_text:
                text_flag = True
            # Check if the text contains any letters or digits
            if re.search('[a-z]', response_text, flags=re.I):
                # Print the detected text
                temp = ""
                ignore_space = ""
                for strg in response_text:
                    if strg.isalpha() and re.search('[a-z]', strg, flags=re.I):
                        temp += strg
                        ignore_space = ""
                    elif ignore_space == " ":
                        continue
                    elif strg == " ":
                        temp += strg
                        ignore_space = " "
                response_text = temp
                logging.info(f'Text detected in the image file {file_name}: {response_text}')
            else:
                logging.info(f'No alphanumeric text detected in the image file {file_name}.')
                text_flag = False
            rows = [(image_name, response_text, text_flag)]
        else:
            logging.info(f'No text detected in the image file {file_name}.')
            rows = [(image_name, '', False)]
            response_text=''
        # Define the BigQuery insert query
        query = f"""
            UPDATE `{dataset_id}.{staging_table}` SET text="{response_text}",text_flag={text_flag}
            WHERE image_name="{image_name}"
            """
        try:
            # Execute the query
            query_job = bigquery_client.query(query)
            query_job.result()
            logging.info("Row inserted successfully in staging table.")
        except Exception as e:
            logging.info("Row insertion in staging table failed.")
            logging.error(f"Error:{e}")

        # Define the BigQuery insert query
        query = f"""
            UPDATE `{dataset_id}.{main_table}` SET text="{response_text}",text_flag={text_flag}
            WHERE image_name="{image_name}"
            """
        try:
            # Execute the query
            query_job = bigquery_client.query(query)
            query_job.result()
            logging.info("Row inserted successfully in main table.")
        except Exception as e:
            logging.info("Row insertion in main table failed.")
            logging.error(f"Error:{e}")