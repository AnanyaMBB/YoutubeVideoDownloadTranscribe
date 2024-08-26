import requests
import json
import os
import redis
import yt_dlp
import time
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count
import boto3
from botocore.client import Config

load_dotenv()
directory = os.getcwd()
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

proxy = f"{os.getenv('PROXY_USERNAME')}:{os.getenv('PROXY_PASSWORD')}@{os.getenv('PROXY_HOST')}:{os.getenv('PROXY_PORT')}"
print("PROXY: ", proxy) 

class DownloadShorts:
    def getChannelIds(self, batch_size=10):
        channelIds = []
        for _ in range(batch_size):
            channel = redisClient.zrevrange(
                "channel_download_queue", 0, 0, withscores=True
            )
            channelId, subscriberCount = channel[0]
            redisClient.zrem("channel_download_queue", channelId)

            if channelId:
                channelIds.append([channelId, subscriberCount])
            else:
                break
        return channelIds

    def isShort(self, url):
        return "/shorts/" in url

    def downloadShorts(self, channelId):
        ydl_opts = {
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ],
            "outtmpl": "./dataset/channel_shorts/%(id)s.%(ext)s",
            "download_archive": "downloaded_shorts.txt",
            "ignoreerrors": True,
            "force_generic_extractor": False,
            "extract_flat": True,
            "proxy": f"http://{proxy}",
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            channel_url = f"https://www.youtube.com/channel/{channelId[0].decode('utf-8')}"
            shorts_url = f"{channel_url}/shorts"
            result = ydl.extract_info(shorts_url, download=False)
            if 'entries' in result:
                for entry in result["entries"]:
                    ydl.download([entry['url']])    

                    self.uploadToSpaces(
                        f"./dataset/channel_shorts/{entry['id']}.mp3",
                        f"youtube_files/dataset/channel_shorts/{entry['id']}.mp3",
                    )

                    if os.path.exists(f"./dataset/channel_shorts/{entry['id']}.mp3"):
                        os.remove(f"./dataset/channel_shorts/{entry['id']}.mp3")


                    redisClient.zadd('channel_downloaded', {entry['id']: channelId[1]}) 
                    redisClient.zadd('channel_video_data_download_queue', {entry['id']: channelId[1]}) 
                    redisClient.sadd('channel_downloaded_videos', entry['id'])


    # def downloadShorts(self, videoId):
    #     videoId = videoId.decode("utf-8")
    #     url = "https://youtube.com/shorts"
    #     videoUrl = f"https://youtube.com/shorts/{videoId}"

    #     ydl_opts = {
    #         "format": "bestaudio/best",
    #         "postprocessors": [
    #             {
    #                 "key": "FFmpegExtractAudio",
    #                 "preferredcodec": "mp3",
    #                 "preferredquality": "192",
    #             }
    #         ],
    #         "outtmpl": "%(title)s.%(ext)s",
    #         "outtmpl": f"./dataset/audio_files/{videoId}.%(ext)s",
    #         "verbose": True,
    #         "ignoreerrors": True,
    #         "proxy": f"http://{proxy}",
    #     }

    #     with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    #         try:
    #             # First, extract info to check available formats
    #             info = ydl.extract_info(url, download=False)
    #             ydl.download([videoUrl])
    #             self.uploadToSpaces(
    #                 f"./dataset/audio_files/{videoId}.mp3",
    #                 f"youtube_files/dataset/audio_files/{videoId}.mp3",
    #             )
    #             if os.path.exists(f"./dataset/audio_files/{videoId}.mp3"):
    #                 os.remove(f"./dataset/audio_files/{videoId}.mp3")

    #             redisClient.rpush("downloaded_youtube_shorts", videoId)
    #             print(f"Successfully downloaded the YouTube Short: {url}")
    #         except Exception as e:
    #             print(f"*An error occurred while downloading: {str(e)}")
    #             print(
    #                 "Try using a different format or check if the video is available."
    #             )

    def uploadToSpaces(self, filePath, destinationPath):
        try:
            client.upload_file(
                filePath, os.getenv("SPACES_SPACE_NAME"), destinationPath
            )
            print(f"File {filePath} uploaded successfully as {destinationPath}")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    downloadShorts = DownloadShorts()
    num_processes = cpu_count()

    # channelIds = downloadShorts.getChannelIds()

    # if channelIds: 
    #     for channelId in channelIds:
    #         downloadShorts.downloadShorts(channelId)
    with Pool(processes=num_processes) as pool:
        while True:
            channelIds = downloadShorts.getChannelIds()

            # channelIds = [[b'UCZ0PnRz4jxOLZZ9XvGCiqfA', 87600]]
            if channelIds:
                results = pool.map(downloadShorts.downloadShorts, channelIds)
            else:
                print("No files to download")
                time.sleep(5)
