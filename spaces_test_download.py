import boto3
from botocore.client import Config

# Your Spaces credentials
access_key = 'DO00NZR2HT4JRVM9MGPQ'
secret_key = 'wT0fhEAu2AzA8/+9acqiMP8KY/nlPzcmDHtRdr09HhQ'
space_name = 'marketingos'
region_name = 'nyc3'  # e.g., nyc3

# Initialize a session using your credentials
session = boto3.session.Session()
client = session.client('s3',
                        region_name=region_name,
                        endpoint_url=f'https://{region_name}.digitaloceanspaces.com',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)

# Function to download a file
def download_file_from_space(space_file_name, local_file_name):
    try:
        client.download_file(space_name, space_file_name, local_file_name)
        print(f"File {space_file_name} downloaded successfully as {local_file_name}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
download_file_from_space('_4gPq-ngtRE-reel.json', 'downloaded_file.txt')
