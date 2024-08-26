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
from bs4 import BeautifulSoup
import re

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

proxies = {"http": "http://" + proxy}


class DownloadData:
    def getChannelIds(self, batch_size=10):
        channelIds = []
        for _ in range(batch_size):
            channel = redisClient.zpopmax("channel_video_data_download_queue")[0]
            if channel:
                channelId, subscriberCount = channel
                channelIds.append([channelId, subscriberCount])
            else:
                break
        return channelIds

    def isShort(self, url):
        return "/shorts/" in url

    def downloadShorts(self, channelId, max_retries=3):
        channel_url = f"https://www.youtube.com/shorts/{channelId[0].decode('utf-8')}"
        for attempt in range(max_retries):
            try:
                print(f"Attempt {attempt + 1}: Fetching URL {channel_url}")
                response = requests.get(channel_url, proxies=proxies)
                response.raise_for_status()  # Raise an exception for HTTP errors

                webpage_content = response.text
                soup = BeautifulSoup(webpage_content, "html.parser")

                script_tag = soup.find("script", text=re.compile(r"var ytInitialData ="))
                if not script_tag:
                    raise ValueError("ytInitialData not found")

                script_content = script_tag.string
                json_text = re.search(
                    r"var ytInitialData = ({.*?});", script_content, re.DOTALL
                ).group(1)

                data_dict = json.loads(json_text)

                # Extracting various fields safely
                likeCount = data_dict.get("overlay", {}).get(
                    "reelPlayerOverlayRenderer", {}
                ).get("likeButton", {}).get("likeButtonRenderer", {}).get("likeCount", None)

                commentCount = data_dict.get("overlay", {}).get(
                    "reelPlayerOverlayRenderer", {}
                ).get("viewCommentsButton", {}).get("buttonRenderer", {}).get("text", {}).get("simpleText", None)

                title = data_dict.get("engagementPanels", [])[1].get(
                    "engagementPanelSectionListRenderer", {}).get(
                    "content", {}).get("structuredDescriptionContentRenderer", {}).get(
                    "items", [])[0].get("videoDescriptionHeaderRenderer", {}).get("title", {}).get("runs", [])[0].get("text", None)

                description = "".join(
                    text.get("text", "")
                    for text in data_dict.get("engagementPanels", [])[1].get(
                        "engagementPanelSectionListRenderer", {}).get(
                        "content", {}).get("structuredDescriptionContentRenderer", {}).get(
                        "items", [])[1].get("expandableVideoDescriptionBodyRenderer", {}).get(
                        "descriptionBodyText", {}).get("runs", [])
                )

                views = data_dict.get("engagementPanels", [])[1].get(
                    "engagementPanelSectionListRenderer", {}).get(
                    "content", {}).get("structuredDescriptionContentRenderer", {}).get(
                    "items", [])[0].get("videoDescriptionHeaderRenderer", {}).get("views", {}).get(
                    "simpleText", "").split(" ")[0].replace(",", "")

                publishDate = data_dict.get("engagementPanels", [])[1].get(
                    "engagementPanelSectionListRenderer", {}).get(
                    "content", {}).get("structuredDescriptionContentRenderer", {}).get(
                    "items", [])[0].get("videoDescriptionHeaderRenderer", {}).get("publishDate", {}).get("simpleText", None)

                channel = data_dict.get("engagementPanels", [])[1].get(
                    "engagementPanelSectionListRenderer", {}).get(
                    "content", {}).get("structuredDescriptionContentRenderer", {}).get(
                    "items", [])[0].get("videoDescriptionHeaderRenderer", {}).get(
                    "channelNavigationEndpoint", {}).get("commandMetadata", {}).get(
                    "webCommandMetadata", {}).get("url", None)

                video_data = {
                    "channel_id": channel,
                    "id": channelId[0].decode("utf-8"),
                    "title": title,
                    "description": description,
                    "view_count": views,
                    "like_count": likeCount,
                    "comment_count": commentCount,
                    "publish_date": publishDate,
                }

                with open(
                    f'./dataset/channel_shorts_json/{video_data["id"]}.json',
                    "w",
                ) as json_file:
                    json.dump(video_data, json_file, indent=4)
                    print(
                        f"Successfully downloaded the YouTube Short: {channelId[0].decode('utf-8')}"
                    )

                self.uploadToSpaces(
                    f"./dataset/channel_shorts_json/{video_data['id']}.json",
                    f"youtube_files/dataset/channel_shorts_json/{video_data['id']}.json",
                )

                if os.path.exists(
                    f"./dataset/channel_shorts_json/{video_data['id']}.json"
                ):
                    os.remove(f"./dataset/channel_shorts_json/{video_data['id']}.json")

                break  # Exit loop if successful

            except Exception as e:
                print(f"Exception on attempt {attempt + 1} for {channelId[0].decode('utf-8')}: {e}")
                if attempt < max_retries - 1:
                    print("Retrying...")
                    time.sleep(2)  # Wait a little before retrying
                else:
                    print("Max retries reached. Moving on to the next video.")

    def uploadToSpaces(self, filePath, destinationPath):
        try:
            client.upload_file(
                filePath, os.getenv("SPACES_SPACE_NAME"), destinationPath
            )
            print(f"File {filePath} uploaded successfully as {destinationPath}")
        except Exception as e:
            print(f"An error occurred while uploading: {e}")


if __name__ == "__main__":
    downloadShorts = DownloadData()
    num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        while True:
            channelIds = downloadShorts.getChannelIds()

            if channelIds:
                results = pool.map(downloadShorts.downloadShorts, channelIds)
            else:
                print("No files to download")
                time.sleep(5)
