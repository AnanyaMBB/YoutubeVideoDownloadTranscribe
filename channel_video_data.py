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

# proxies = {"http": "http://" + proxy, "https": "https://" + proxy}
proxies = {"http": "http://" + proxy}


class DownloadData:
    def getChannelIds(self, batch_size=10):
        channelIds = []
        for _ in range(batch_size):
            channel = redisClient.zpopmax("channel_video_data_download_queue")[0]
            channelId, subscriberCount = channel
            # redisClient.zrem("channel_download_queue", channelId)

            if channelId:
                channelIds.append([channelId, subscriberCount])
            else:
                break
        return channelIds

    def isShort(self, url):
        return "/shorts/" in url

    def downloadShorts(self, channelId):
        print(f"URL : https://www.youtube.com/shorts/{channelId[0].decode('utf-8')}")
        channel_url = f"https://www.youtube.com/shorts/{channelId[0].decode('utf-8')}"
        response = requests.get(channel_url, proxies=proxies)
        webpage_content = response.text

        # Step 2: Parse the content with BeautifulSoup
        soup = BeautifulSoup(webpage_content, "html.parser")

        # Step 3: Locate the <script> tag containing the JavaScript object
        script_tag = soup.find("script", text=re.compile(r"var ytInitialData ="))

        # Step 4: Extract the JavaScript object as a string
        # try:
        if script_tag:
            script_content = script_tag.string

            # Use regex to extract the JSON-like object from the script content
            json_text = re.search(
                r"var ytInitialData = ({.*?});", script_content, re.DOTALL
            ).group(1)

            # Step 5: Convert the JSON-like string into a Python dictionary
            data_dict = json.loads(json_text)
            # print(data_dict)
            # Now `data_dict` holds the parsed dictionary, and you can access its values
            likeCount = 0
            try: 
                likeCount = data_dict["overlay"]["reelPlayerOverlayRenderer"][
                    "likeButton"
                ]["likeButtonRenderer"]["likeCount"]
            except:
                pass

            try: 
                commentCount = "" 
                print("COMMENT COUNT: ", data_dict["overlay"]["reelPlayerOverlayRenderer"])
                commentCount = data_dict["overlay"]["reelPlayerOverlayRenderer"][
                    "viewCommentsButton"
                ]["buttonRenderer"]["text"]["simpleText"]
            except: 
                pass

            try: 
                title = "" 

                for text in data_dict["engagementPanels"][1][
                    "engagementPanelSectionListRenderer"
                ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
                    "videoDescriptionHeaderRenderer"
                ][
                    "title"
                ][
                    "runs"
                ]:
                    title += text["text"]
                # title = data_dict["engagementPanels"][1][
                #     "engagementPanelSectionListRenderer"
                # ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
                #     "videoDescriptionHeaderRenderer"
                # ][
                #     "title"
                # ][
                #     "runs"
                # ][
                #     0
                # ][
                #     "text"
                # ]

            except:
                pass
            
            try: 
                # description = data_dict['engagementPanels'][1]['engagementPanelSectionListRenderer']['content']['structuredDescriptionContentRenderer']['items'][1]['expandableVideoDescriptionBodyRenderer']['descriptionBodyText']['runs']
                description = ""
                # print("PRE DESCRIPTION", data_dict["engagementPanels"][1][
                #     "engagementPanelSectionListRenderer"
                # ]["content"]["structuredDescriptionContentRenderer"]["items"][1][
                #     "expandableVideoDescriptionBodyRenderer"
                # ][
                #     "descriptionBodyText"
                # ][
                #     "runs"
                # ])
                for text in data_dict["engagementPanels"][1][
                    "engagementPanelSectionListRenderer"
                ]["content"]["structuredDescriptionContentRenderer"]["items"][1][
                    "expandableVideoDescriptionBodyRenderer"
                ][
                    "descriptionBodyText"
                ][
                    "runs"
                ]:
                    description += text["text"]
            except: 
                pass

            try: 
                views = 0
                views = data_dict["engagementPanels"][1][
                    "engagementPanelSectionListRenderer"
                ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
                    "videoDescriptionHeaderRenderer"
                ][
                    "views"
                ][
                    "simpleText"
                ].split(
                    " "
                )[
                    0
                ]

                if "," in views:
                    views = int(views.replace(",", ""))
            except: 
                pass

            
            try: 
                publishDate = ""
                publishDate = data_dict["engagementPanels"][1][
                    "engagementPanelSectionListRenderer"
                ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
                    "videoDescriptionHeaderRenderer"
                ][
                    "publishDate"
                ][
                    "simpleText"
                ]
            except: 
                pass
            
            channel = "" 
            try: 
                channel = data_dict["engagementPanels"][1][
                    "engagementPanelSectionListRenderer"
                ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
                    "videoDescriptionHeaderRenderer"
                ][
                    "channelNavigationEndpoint"
                ][
                    "commandMetadata"
                ][
                    "webCommandMetadata"
                ][
                    "url"
                ]
            except: 
                pass
            
            try: 
                video_data = {
                    "channel_id": channel,
                    "id": channelId[0].decode("utf-8"),
                    "subscriber": channelId[1],
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

                # self.uploadToSpaces(
                #     f"./dataset/channel_shorts/{video_data['id']}.mp3",
                #     f"youtube_files/dataset/channel_shorts/{video_data['id']}.mp3",
                # )

                self.uploadToSpaces(
                    f"./dataset/channel_shorts_json/{video_data['id']}.json",
                    f"youtube_files/dataset/channel_shorts_json/{video_data['id']}.json",
                )


                if os.path.exists(
                    f"./dataset/channel_shorts_json/{video_data['id']}.json"
                ):
                    os.remove(f"./dataset/channel_shorts_json/{video_data['id']}.json")

                # redisClient.zadd(
                #     "channel_downloaded", {video_data["id"]: channelId[1]}
                # )
                # redisClient.sadd(
                #     "channel_downloaded_videos", video_data["id"]
                # )
            except Exception as e: 
                print("ERROR OCCURRED ON VIDEO: ", channelId[0].decode("utf-8"))
                print("ERROR: ", e)
                # print("*"*30)
                # print(data_dict["engagementPanels"])
                # # print("LIKE", data_dict["overlay"]["reelPlayerOverlayRenderer"])
                # print("*"*30)
                redisClient.zadd(
                    "channel_video_data_download_queue", {channelId[0]: channelId[1]})

        else:
            print("Script tag containing 'ytInitialData' not found.")
        # except Exception as e:
        #     print("EXCEPTION OCCURRED: ", e)

    def uploadToSpaces(self, filePath, destinationPath):
        try:
            client.upload_file(
                filePath, os.getenv("SPACES_SPACE_NAME"), destinationPath
            )
            print(f"File {filePath} uploaded successfully as {destinationPath}")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    downloadShorts = DownloadData()
    num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        while True:
            channelIds = downloadShorts.getChannelIds()

            # channelIds = [[b'UCZ0PnRz4jxOLZZ9XvGCiqfA', 87600]]
            if channelIds:
                results = pool.map(downloadShorts.downloadShorts, channelIds)
            else:
                print("No files to download")
                time.sleep(5)
