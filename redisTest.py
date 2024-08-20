import redis 
import os 
from dotenv import load_dotenv

load_dotenv()

redisClient = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), username=os.getenv('REDIS_USERNAME'), password=os.getenv('REDIS_PASSWORD'), db=0)

print(redisClient.zrevrange('channel_download_queue', 0, 0, withscores=True))