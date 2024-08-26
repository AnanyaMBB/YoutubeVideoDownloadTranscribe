import json 
import os
import redis 
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count
import time 
import csv

load_dotenv()
i = 0
redisClient = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), username=os.getenv('REDIS_USERNAME'), password=os.getenv('REDIS_PASSWORD'), db=0)

class Extractor: 
    def __init__(self):
        self.files = sorted(os.listdir(os.getcwd() + './dataset/channel_json'), key=self.extractNumber, reverse=True)

    def extractNumber(self, filename): 
        return int(filename.split('.')[0])

    def getChannelFiles(self, batch_size=10):
        channels = []
        for _ in range(batch_size):
            if len(self.files) > 0: 
                channels.append(self.files.pop())
            else: 
                break
        return channels 


    def extractChannelList(self, channelFile):
        with open(f'./dataset/channel_json/{channelFile}', 'r', encoding='utf-8') as f: 
            channelData = json.load(f)
            try: 
                extractedChannels = channelData['onResponseReceivedCommands'][0]['appendContinuationItemsAction']['continuationItems'][0]['itemSectionRenderer']['contents']
                
                for channel in extractedChannels: 
                    channelId = channel['channelRenderer']['channelId']
                    channelUrl = channel['channelRenderer']['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url']
                    subscriberCount = channel['channelRenderer']['videoCountText']['simpleText'].split(' ')[0]
                    

                    if subscriberCount[-1] == 'M':
                        subscriberCount = int(float(subscriberCount[:-1]) * 1000000)
                    elif subscriberCount[-1] == 'K':
                        subscriberCount = int(float(subscriberCount[:-1]) * 1000)

                    with open(f'./dataset/channel_csv/channels.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([channelId, channelUrl, subscriberCount])

                    # if not redisClient.sismember('channel_list', channelId):
                    if not redisClient.zscore('channel_download_queue', channelId):
                        # redisClient.rpush('channel_download_queue', channelId)
                        redisClient.sadd('channel_list', channelId)
                        redisClient.zadd('channel_download_queue', {channelId: subscriberCount})  

                        # channel = redisClient.zrevrange('channel_subscribers', 0, 0, withscores=True)
                        # channelId, subscriberCount = channel[0]
                        # redisClient.zrem('channel_subscribers', channelId)

                
                # redisClient.zadd('channel_list', {channelData['onResponseReceivedCommands'][0]['appendContinuationItemsAction']['continuationItems'][0]['itemSectionRenderer']['contents'][0]})
            except Exception as e: 
                pass 


        

if __name__ == "__main__":
    extractor = Extractor()
    num_processes = cpu_count()

    # channelFiles = extractor.getChannelFiles(100)   
    # for file in channelFiles: 
    #     print(file)
    #     extractor.extractChannelList(file)
    with Pool(processes=num_processes) as pool:
        while True: 
            channelFiles = extractor.getChannelFiles()
            if channelFiles: 
                pool.map(extractor.extractChannelList, channelFiles)
            else: 
                print("No more files to process.")
                time.sleep(5)
            