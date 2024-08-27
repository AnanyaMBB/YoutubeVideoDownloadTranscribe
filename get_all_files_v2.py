import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv
import redis
import json
from multiprocessing import Pool, cpu_count

load_dotenv()

redisClient = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=os.getenv("REDIS_PORT"),
    username=os.getenv("REDIS_USERNAME"),
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
)

session = boto3.session.Session()
client = session.client(
    "s3",
    region_name=os.getenv("SPACES_REGION_NAME"),
    endpoint_url=f"https://{os.getenv('SPACES_REGION_NAME')}.digitaloceanspaces.com",
    aws_access_key_id=os.getenv("SPACES_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SPACES_SECRET_KEY"),
)

def process_file(file_name):
    try:
        subscriberCount = -1
        id = file_name.split("/")[-1].split(".")[0]
        print("Processing ID: ", id)

        fileResponse = client.get_object(Bucket=os.getenv("SPACES_SPACE_NAME"), Key=f"youtube_files/dataset/channel_shorts_json/{id}.json")
        file_content = fileResponse['Body'].read().decode('utf-8')
        data = json.loads(file_content)
        subscriberCount = data.get("subscriber", -1)
        
        if redisClient.zscore("channel_downloaded", id) is None: 
            redisClient.zadd("channel_downloaded", {id: subscriberCount})
            # print("Score: ", redisClient.zscore("channel_downloaded", id))
            print(f"Added to channel_downloaded: ID - {id} : Subscriber - {subscriberCount}")
    except Exception as e: 
        print("Error processing ID: ", id, " - Error: ", e)

def main():
    # Initialize the paginator
    paginator = client.get_paginator('list_objects_v2')

    # Set up the parameters for the paginator
    page_iterator = paginator.paginate(
        Bucket=os.getenv("SPACES_SPACE_NAME"),
        Prefix="youtube_files/dataset/channel_shorts/",
    )

    file_list = []
    for page in page_iterator:
        for item in page.get("Contents", []):
            file_list.append(item["Key"])

    print(f"Total files: {len(file_list)}")

    # Use multiprocessing to process files in parallel
    pool = Pool(processes=cpu_count())
    pool.map(process_file, file_list)
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()
