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

print(f"URL : https://www.youtube.com/shorts/XJbyZ7fjM4E")
channel_url = f"https://www.youtube.com/shorts/XJbyZ7fjM4E"
proxy = f"{os.getenv('PROXY_USERNAME')}:{os.getenv('PROXY_PASSWORD')}@{os.getenv('PROXY_HOST')}:{os.getenv('PROXY_PORT')}"

proxies = {"http": "http://" + proxy}
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
    # print(data_dict["engagementPanels"][1]["engagementPanelSectionListRenderer"]["content"]["structuredDescriptionContentRenderer"]["items"][0]["videoDescriptionHeaderRenderer"]["title"]["runs"])
    print(data_dict["engagementPanels"])
    # Now `data_dict` holds the parsed dictionary, and you can access its values
    
    likeCount = data_dict["overlay"]["reelPlayerOverlayRenderer"][
        "likeButton"
    ]["likeButtonRenderer"]["likeCount"]


    
    commentCount = data_dict["overlay"]["reelPlayerOverlayRenderer"][
        "viewCommentsButton"
    ]["buttonRenderer"]["text"]["simpleText"]

    title = data_dict["engagementPanels"][1][
        "engagementPanelSectionListRenderer"
    ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
        "videoDescriptionHeaderRenderer"
    ][
        "title"
    ][
        "runs"
    ][
        0
    ][
        "text"
    ]

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
    
    # description = data_dict['engagementPanels'][1]['engagementPanelSectionListRenderer']['content']['structuredDescriptionContentRenderer']['items'][1]['expandableVideoDescriptionBodyRenderer']['descriptionBodyText']['runs']
    description = ""

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
    publishDate = data_dict["engagementPanels"][1][
        "engagementPanelSectionListRenderer"
    ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
        "videoDescriptionHeaderRenderer"
    ][
        "publishDate"
    ][
        "simpleText"
    ]
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
    # print(channel)
    # video_data = {
    #     "channel_id": channel,
    #     "id": channelId[0].decode("utf-8"),
    #     "title": title,
    #     "description": description,
    #     "view_count": views,
    #     "like_count": likeCount,
    #     "comment_count": commentCount,
    #     "publish_date": publishDate,
    # }

    

else:
    print("Script tag containing 'ytInitialData' not found.")
# except Exception as e:
#     print("EXCEPTION OCCURRED: ", e)