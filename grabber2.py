from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import time
import json
import os
import youtube_dl
from hashlib import md5

def downloader(url, count, username):
    ydl_opts = {
        'external_downloader': 'aria2c',
        'outtmpl': f'./dataset/video_files/{count}-{username}.%(ext)s',
        'format': 'bestvideo+bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegVideoConvertor',
            'preferedformat': 'mp4',
        }],
    }
    try:
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True
    except Exception as e:
        print(f"Download failed for {username}: {str(e)}")
        return False

def get_url_from_player_json(player_json):
    formats = player_json['streamingData']['formats']
    for format in formats:
        if 'url' in format:
            return format['url']
        elif 'signatureCipher' in format:
            return format['signatureCipher'].split('&')[2].split('=')[1]
    raise ValueError("No suitable URL found in player JSON")

def process_request(request, processed_hashes, count):
    if request.response is None:
        return None, None, None

    body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity')).decode('utf-8')
    body_hash = md5(body.encode('utf-8')).hexdigest()

    if body_hash in processed_hashes:
        return None, None, None

    processed_hashes.add(body_hash)
    json_data = json.loads(body)

    if 'https://www.youtube.com/youtubei/v1/player' in request.url:
        username = json_data['microformat']['playerMicroformatRenderer']['ownerProfileUrl'].split('/')[-1]
        return 'player', username, json_data
    elif 'https://www.youtube.com/youtubei/v1/reel/reel_item_watch' in request.url:
        username = json_data['overlay']['reelPlayerOverlayRenderer']['reelPlayerHeaderSupportedRenderers']['reelPlayerHeaderRenderer']['channelTitleText']['runs'][0]['text']
        return 'reel', username, json_data

    return None, None, None

def main():
    url = "https://www.youtube.com/shorts"
    driver = webdriver.Chrome()
    directory = os.getcwd()

    try:
        driver.get(url)
        actions = ActionChains(driver)

        processed_hashes = set()
        count = max([int(jsonFile.split('-')[0]) for jsonFile in os.listdir(f'{directory}/dataset/unparsed_json') if jsonFile.endswith('.json')], default=-1) + 1

        for _ in range(100000):
            for _ in range(10):
                time.sleep(6)
                player_data = None

                for request in driver.requests:
                    request_type, username, json_data = process_request(request, processed_hashes, count)

                    if request_type == 'player':
                        player_data = json_data
                        with open(f'./dataset/unparsed_json/{count}-player-{username}.json', 'w', encoding='utf-8') as f:
                            json.dump(json_data, f)
                    elif request_type == 'reel':
                        with open(f'./dataset/unparsed_json/{count}-reel-{username}.json', 'w', encoding='utf-8') as f:
                            json.dump(json_data, f)

                        if player_data:
                            url = get_url_from_player_json(player_data)
                            if downloader(url, count, username):
                                count += 1

                actions.send_keys(Keys.PAGE_DOWN).perform()

            del driver.requests
            processed_hashes.clear()

    finally:
        driver.quit()

if __name__ == "__main__":
    main()