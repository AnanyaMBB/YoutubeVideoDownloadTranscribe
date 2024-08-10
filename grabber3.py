from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

import time
import json
import os
import requests
from hashlib import md5
import youtube_dl
import yt_dlp


directory = os.getcwd()


# def downloader(player_json: json, count: int, username: str):
#     print("IN DOWNLOADER")
#     # url = player_json['streamingData']['adaptiveFormats'][0]['url'] if 'url' in player_json['streamingData']['adaptiveFormats'][0].keys(
#     # ) else player_json['streamingData']['adaptiveFormats'][0]['signatureCipher'].split('&')[2].split('=')[1]

#     url = (player_json['streamingData']['formats'][1]['url'] if 'url' in player_json['streamingData']['formats'][1].keys(
#     ) else player_json['streamingData']['formats'][1]['signatureCipher'].split('&')[2].split('=')[1]) if len(player_json['streamingData']['formats']) >= 2 else (player_json['streamingData']['formats'][0]['url'] if 'url' in player_json['streamingData']['formats'][0].keys(
#     ) else player_json['streamingData']['formats'][0]['signatureCipher'].split('&')[2].split('=')[1])

#     # print("Found url: Downloading...")
#     print(f"==>url: {url}")
#     # r = requests.get(url, allow_redirects=True)
#     # open(
#     #     f'./dataset/video_files/{count}-{username}.mp4', 'wb').write(r.content)
#     # print("Downloaded")
#     ydl_opts = {
#         'external_downloader': 'aria2c',
#         'outtmpl': f'./dataset/video_files/{count}-{username}.%(ext)s',
#         'format': 'bestvideo+bestaudio/best',
#         'postprocessors': [{
#             'key': 'FFmpegVideoConvertor',
#             'preferedformat': 'mp4',
#         }],
#     }
#     success = True
#     try:
#         with youtube_dl.YoutubeDL(ydl_opts) as ydl:
#             ydl.download([url])
#     except Exception as e:
#         print("EXCEPTION", e)
#         # os.remove(
#         #     f'{directory}\\dataset\\unparsed_json\\{count}-player-{username}.json')
#         # os.remove(
#         #     f'{directory}\\dataset\\unparsed_json\\{count}-reel-{username}.json')
#         # success = False
#         # pass
#     return success

def downloader(player_json: json, count: int, username: str):
    videoId = player_json['videoDetails']['videoId']
    videoUrl = f'https://youtube.com/shorts/{videoId}'

    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': '%(title)s.%(ext)s',
        'outtmpl': f'./dataset/audio_files/{count}-{username}.%(ext)s',
        'verbose': True,
        'ignoreerrors': True,
    }

    print("VIDEO URL: ", videoUrl)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            # First, extract info to check available formats
            info = ydl.extract_info(url, download=False)
            
            # if 'formats' in info:
            #     print("Available formats:")
            #     for format in info['formats']:
            #         print(f"Format: {format['format_id']}, Extension: {format.get('ext', 'N/A')}, Resolution: {format.get('resolution', 'N/A')}")
            
            # Then attempt to download
            ydl.download([videoUrl])
            print(f"Successfully downloaded the YouTube Short: {url}")
        except Exception as e:
            print(f"An error occurred while downloading: {str(e)}")
            print("Try using a different format or check if the video is available.")

url = "https://www.youtube.com/shorts"
driver = webdriver.Chrome()

try:
    driver.get(url)

    actions = ActionChains(driver)

    # processed_requests = set()
    # processed_hashes = set()

    count1 = max([int(jsonFile.split('-')[0]) for jsonFile in os.listdir(
        f'{directory}/dataset/unparsed_json') if jsonFile.endswith('.json') and 'player' in jsonFile.split('-')], default=-1) + 1
    count2 = max([int(jsonFile.split('-')[0]) for jsonFile in os.listdir(
        f'{directory}/dataset/unparsed_json') if jsonFile.endswith('.json') and 'reel' in jsonFile.split('-')], default=-1) + 1
    for _ in range(100000):
        # print("LOOP")
        for _ in range(10):
            # print("LOOP2")
            time.sleep(6)
            for request in driver.requests:
                if request.response is not None:
                    # if request not in processed_requests:
                        # print("REQUEST")
                    if 'https://www.youtube.com/youtubei/v1/player' in request.url:
                        body = decode(request.response.body, request.response.headers.get(
                            'Content-Encoding', 'identity')).decode('utf-8')
                        # body_hash = md5(body.encode('utf-8')).hexdigest()

                        # if body_hash in processed_hashes:
                        #     continue

                        jsonParsed1 = json.loads(body)
                        username = jsonParsed1['microformat']['playerMicroformatRenderer']['ownerProfileUrl'].split(
                            '/')[-1]
                        with open(f'./dataset/unparsed_json/{count1}-player-{username}.json', 'w', encoding='utf-8') as f:
                            f.write(body)

                        # processed_hashes.add(body_hash)
                        # if not success:
                        #     continue

                    elif 'https://www.youtube.com/youtubei/v1/reel/reel_item_watch' in request.url:
                        body = decode(request.response.body, request.response.headers.get(
                            'Content-Encoding', 'identity')).decode('utf-8')
                        # body_hash = md5(body.encode('utf-8')).hexdigest()

                        # if body_hash in processed_hashes:
                        #     continue

                        jsonParsed2 = json.loads(body)
                        try:
                            username = jsonParsed2['overlay']['reelPlayerOverlayRenderer']['reelPlayerHeaderSupportedRenderers'][
                                'reelPlayerHeaderRenderer']['channelTitleText']['runs'][0]['text']
                            with open(f'./dataset/unparsed_json/{count2}-reel-{username}.json', 'w', encoding='utf-8') as f:
                                f.write(body)
                            success = downloader(jsonParsed1, count1, username)

                            # processed_hashes.add(body_hash)
                            # if success:
                            count1 += 1
                            count2 += 1
                        except: 
                            pass

                # processed_requests.add(request)
            actions.send_keys(Keys.PAGE_DOWN).perform()
            print("KEYDOWN")
        # del driver.requests
        # processed_requests.clear()
        # processed_hashes.clear()
finally:
    driver.quit()
