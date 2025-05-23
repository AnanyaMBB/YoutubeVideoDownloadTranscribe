from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

import time
import json
import os
import requests
from hashlib import md5
import yt_dlp
import redis
from dotenv import load_dotenv

import boto3
from botocore.client import Config

load_dotenv()

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Setting up the proxy for Oxylabs
# proxy = f"{os.getenv('PROXY_USERNAME')}:{os.getenv('PROXY_PASSWORD')}@{os.getenv('PROXY_HOST')}:{os.getenv('PROXY_PORT')}"

# seleniumwire_options = {
#     "proxy": {
#         "http": f"http://{proxy}",
#         "https": f"https://{proxy}",
#         "no_proxy": "localhost,127.0.0.1",  # Bypass the proxy for local addresses
#     }
# }

directory = os.getcwd()
redisClient = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=os.getenv("REDIS_PORT"),
    username=os.getenv("REDIS_USERNAME"),
    password=os.getenv("REDIS_PASSWORD"),
    db=0,
)
print(f"REDIS HOST: {os.getenv('REDIS_HOST')}")
session = boto3.session.Session()
client = session.client(
    "s3",
    region_name=os.getenv("SPACES_REGION_NAME"),
    endpoint_url=f"https://{os.getenv('SPACES_REGION_NAME')}.digitaloceanspaces.com",
    aws_access_key_id=os.getenv("SPACES_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SPACES_SECRET_KEY"),
)


def downloader(player_json: json, count: int, username: str):
    videoId = player_json["videoDetails"]["videoId"]
    videoUrl = f"https://youtube.com/shorts/{videoId}"

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
        "outtmpl": f"./dataset/audio_files/{count}-{username}.%(ext)s",
        "verbose": True,
        "ignoreerrors": True,
    }

    print("VIDEO URL: ", videoUrl)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            # First, extract info to check available formats
            info = ydl.extract_info(url, download=False)
            ydl.download([videoUrl])
            print(f"Successfully downloaded the YouTube Short: {url}")
        except Exception as e:
            print(f"An error occurred while downloading: {str(e)}")
            print("Try using a different format or check if the video is available.")


url = "https://www.youtube.com/shorts"
driver = webdriver.Chrome(
    options=chrome_options
)


try:
    driver.get(url)
    actions = ActionChains(driver)
    for i in range(100000):
        time.sleep(4)
        for request in driver.requests:
            if request.response is not None:
                # print(request.url)
                if "https://www.youtube.com/youtubei/v1/player" in request.url:
                    body = decode(
                        request.response.body,
                        request.response.headers.get("Content-Encoding", "identity"),
                    ).decode("utf-8")

                    jsonParsed1 = json.loads(body)
                    videoID = jsonParsed1["videoDetails"]["videoId"]
                    print(videoID)
                    username = jsonParsed1["microformat"]["playerMicroformatRenderer"][
                        "ownerProfileUrl"
                    ].split("/")[-1]

                elif (
                    "https://www.youtube.com/youtubei/v1/reel/reel_item_watch"
                    in request.url
                ):
                    body = decode(
                        request.response.body,
                        request.response.headers.get("Content-Encoding", "identity"),
                    ).decode("utf-8")

                    jsonParsed2 = json.loads(body)
                    try:
                        username = jsonParsed2["overlay"]["reelPlayerOverlayRenderer"][
                            "reelPlayerHeaderSupportedRenderers"
                        ]["reelPlayerHeaderRenderer"]["channelTitleText"]["runs"][0][
                            "text"
                        ]
                        videoID = jsonParsed1["videoDetails"]["videoId"]

                        if not redisClient.sismember(
                            "unique_youtube_video_ids", videoID
                        ):
                            try:
                                # client.upload_file(f'youtube_files/dataset/unparsed_json/{videoID}-player.json', os.getenv('SPACES_SPACE_NAME'), f'youtube_files/dataset/unparsed_json/{videoID}-player.json')
                                client.put_object(
                                    Bucket=os.getenv("SPACES_SPACE_NAME"),
                                    Key=f"youtube_files/dataset/unparsed_json/{videoID}-player.json",
                                    Body=body,
                                    ContentType="application/json",
                                )
                                client.put_object(
                                    Bucket=os.getenv("SPACES_SPACE_NAME"),
                                    Key=f"youtube_files/dataset/unparsed_json/{videoID}-reel.json",
                                    Body=body,
                                    ContentType="application/json",
                                )
                                # with open(f'./dataset/unparsed_json/{videoID}-player.json', 'w', encoding='utf-8') as f:
                                #     f.write(body)
                                # with open(f'./dataset/unparsed_json/{videoID}-reel.json', 'w', encoding='utf-8') as f:
                                #     f.write(body)
                                redisClient.rpush("youtube_shorts", f"{videoID}")
                                redisClient.sadd("unique_youtube_video_ids", videoID)
                            except Exception as e:
                                print(f"An error occurred while downloading: {str(e)}")
                                pass

                    except:
                        pass
        del driver.requests
        actions.send_keys(Keys.ARROW_DOWN).perform()
        print("KEYDOWN")
        if i % 1000 == 0:
            print("REFRESHING")
            driver.refresh()
finally:
    driver.quit()
