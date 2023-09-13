from google.cloud import secretmanager
import os

def get_secret():
    # Create a client object.
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get('PROJECT_ID')
    secret_id = os.environ.get('SECRET_TOKEN')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    # # Retrieve the secret.
    response = client.access_secret_version(name=name)

    # Extract the payload from the response.
    bearerToken = response.payload.data.decode("UTF-8")

    return bearerToken