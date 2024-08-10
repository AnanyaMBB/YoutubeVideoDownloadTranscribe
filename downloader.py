import requests
import json
import os
import redis
import yt_dlp
import time
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count

load_dotenv()
directory = os.getcwd()
redisClient = redis.Redis(host="localhost", port=6379, db=0)

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
                redisClient.rpush("downloaded_youtube_shorts", videoId)
                print(f"Successfully downloaded the YouTube Short: {url}")
            except Exception as e:
                print(f"An error occurred while downloading: {str(e)}")
                print("Try using a different format or check if the video is available.")


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
