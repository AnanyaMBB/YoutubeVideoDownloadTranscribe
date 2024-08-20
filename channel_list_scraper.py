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

chrome_options = Options()
# chrome_options.add_argument("--headless=new")
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--disable-dev-shm-usage')

# Setting up the proxy for Oxylabs
proxy_username = 'customer-testuser_vCUqq'
proxy_password = '123456789~ABCabc'
proxy_host = 'pr.oxylabs.io'
proxy_port = '7777'

proxy = f'{proxy_username}:{proxy_password}@{proxy_host}:{proxy_port}'

seleniumwire_options = {
    'proxy': {
        'http': f'http://{proxy}',
        'https': f'https://{proxy}',
        'no_proxy': 'localhost,127.0.0.1'  # Bypass the proxy for local addresses
    }
}

directory = os.getcwd()
redisClient = redis.Redis(host='localhost', port=6379, db=0)

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
            ydl.download([videoUrl])
            print(f"Successfully downloaded the YouTube Short: {url}")
        except Exception as e:
            print(f"An error occurred while downloading: {str(e)}")
            print("Try using a different format or check if the video is available.")


# fitness workout exercise health yoga physical-training wellness
url = "https://www.youtube.com/results?search_query=muscle&sp=EgIQAg%253D%253D"
# driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=seleniumwire_options)
driver = webdriver.Chrome(options=chrome_options)

def extract_number(filename): 
    return int(filename.split('.')[0])

try: 
    count = int(sorted(os.listdir(os.getcwd() + './dataset/channel_json'), key=extract_number)[-1].split('.')[0]) 
except: 
    count = 0

try:
    driver.get(url)
    actions = ActionChains(driver)
    for i in range(100000):
        time.sleep(2)
        for request in driver.requests:
            if request.response is not None:
                if 'https://www.youtube.com/youtubei/v1/search?prettyPrint=false' in request.url:
                    print("REQUEST FOUND")
                    body = decode(request.response.body, request.response.headers.get(
                        'Content-Encoding', 'identity')).decode('utf-8')
                    jsonParsed = json.loads(body)
                    with open(f'./dataset/channel_json/{count}.json', 'w', encoding='utf-8') as f:
                        f.write(body)
                    count += 1

                                       
        del driver.requests
        actions.send_keys(Keys.PAGE_DOWN).perform()
        # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        print("KEYDOWN")
        # if i % 1000 == 0:
        #     print("REFRESHING")
        #     driver.refresh()
finally:
    driver.quit()
