import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv
import redis
import json

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

paginator = client.get_paginator('list_objects_v2')

# response = client.list_objects_v2(
#     Bucket=os.getenv("SPACES_SPACE_NAME"),
#     Prefix="youtube_files/dataset/channel_shorts/",
# )

page_iterator = paginator.paginate(
    Bucket=os.getenv("SPACES_SPACE_NAME"),
    Prefix="youtube_files/dataset/channel_shorts/",
)


# file_list = [item["Key"] for item in response.get("Contents", [])]

file_list = []
for page in page_iterator:
    for item in page.get("Contents", []):
        file_list.append(item["Key"])

print(len(file_list))

# for file_name in file_list:
#     try: 
#         subscriberCount = -1
#         id = file_name.split("/")[-1].split(".")[0]
#         print("ID: ", id)

#         fileResponse = client.get_object(Bucket=os.getenv("SPACES_SPACE_NAME"), Key=f"youtube_files/dataset/channel_shorts_json/{id}.json")
#         file_content = fileResponse['Body'].read().decode('utf-8')
#         data = json.loads(file_content)
#         subscriberCount = data.get("subscriber", -1)
        
        
#         if redisClient.zscore("channel_downloaded", id) is None: 
#             redisClient.zadd("channel_downloaded", {id: subscriberCount})
#             print(f"Added to channel_downloaded: ID - {id} : Subscriber - {subscriberCount}")
#     except Exception as e: 
#         print("Error: ", e)


# print(redisClient.zscore("channel_downloaded", "LimKly-bDdw"))