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
redisClient = redis.Redis(host="localhost", port=6379, db=0)
session = boto3.session.Session()
client = session.client('s3',
                        region_name=os.getenv("SPACES_REGION_NAME"),
                        endpoint_url=f"https://{os.getenv('SPACES_REGION_NAME')}.digitaloceanspaces.com",
                        aws_access_key_id=os.getenv("SPACES_ACCESS_KEY"),
                        aws_secret_access_key=os.getenv("SPACES_SECRET_KEY"))

class DownloadShorts:
    def getVideoIds(self, batch_size=10):
        videoIds = []
        for _ in range(batch_size):
            videoId = redisClient.lpop("youtube_shorts")
            if videoId:
                videoIds.append(videoId)
            else:
                break
        return videoIds

    def downloadShorts(self, videoId):
        videoId = videoId.decode("utf-8")
        url = "https://youtube.com/shorts"
        videoUrl = f'https://youtube.com/shorts/{videoId}'

        ydl_opts = {
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ],
            "outtmpl": "%(title)s.%(ext)s",
            "outtmpl": f"./dataset/audio_files/{videoId}.%(ext)s",
            "verbose": True,
            "ignoreerrors": True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                # First, extract info to check available formats
                info = ydl.extract_info(url, download=False)
                ydl.download([videoUrl])
                self.uploadToSpaces(f"./dataset/audio_files/{videoId}.mp3", f"youtube_files/dataset/audio_files/{videoId}.mp3")
                if os.path.exists(f"./dataset/audio_files/{videoId}.mp3"):
                    os.remove(f"./dataset/audio_files/{videoId}.mp3")

                redisClient.rpush("downloaded_youtube_shorts", videoId)
                print(f"Successfully downloaded the YouTube Short: {url}")
            except Exception as e:
                print(f"*An error occurred while downloading: {str(e)}")
                print("Try using a different format or check if the video is available.")

    def uploadToSpaces(self, filePath, destinationPath): 
        try:
            client.upload_file(filePath, os.getenv("SPACES_SPACE_NAME"), destinationPath)
            print(f"File {filePath} uploaded successfully as {destinationPath}")
        except Exception as e:
            print(f"An error occurred: {e}")
if __name__ == "__main__":
    downloadShorts = DownloadShorts()
    num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        while True:
            videoIds = downloadShorts.getVideoIds()

            if videoIds:
                results = pool.map(downloadShorts.downloadShorts, videoIds)
            else:
                print("No files to download")
                time.sleep(5)
