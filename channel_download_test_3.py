import yt_dlp
from seleniumwire.utils import decode

# Replace 'CHANNEL_URL' with the actual URL of the YouTube channel you want to download.
channel_url = 'https://www.youtube.com/channel/UClibF66CCQvnK7IoDvxw7QA'

# Base options for yt-dlp
ydl_opts = {
    'format': 'bestvideo+bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegVideoConvertor',
        'preferedformat': 'mp4',
    }],
    'merge_output_format': 'mp4',
    'outtmpl': 'Shorts/%(id)s.%(ext)s',
    'download_archive': 'downloaded_shorts.txt',
    'ignoreerrors': True,
    'force_generic_extractor': False,
}

def is_short(url):
    return '/shorts/' in url

def download_shorts():
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        # Use the Shorts playlist URL
        shorts_url = f'{channel_url}/shorts'
        result = ydl.extract_info(shorts_url, download=False)
        # print(result['view_count'])
        # print(result['like_count'])
        # print(result)

        # if 'entries' in result:
        #     print("ENTRIES: ", result['entries'][0]['url'])
            # video_urls = [entry['url'] for entry in result['entries'] if is_short(entry['url'])]
            # ydl.download(video_urls)
        if 'entries' in result:
            for entry in result['entries']:
                # print("ENTRY: ", entry)
                # Some entries may not have 'url' directly, so we use 'webpage_url' instead
                webpage_url = entry.get('webpage_url')
                print("WEBPAGE URL: ", webpage_url)
                # if webpage_url and is_short(webpage_url):
                print(f"Video ID: {entry['id']}")
                print(f"Title: {entry['title']}")
                print(f"View Count: {entry.get('view_count', 'N/A')}")
                print(f"Like Count: {entry.get('like_count', 'N/A')}")
                print(f"Comment Count: {entry.get('comment_count', 'N/A')}")
                print(f"Title: {entry['title']}")
                print(f"Description: {entry.get('description', 'N/A')}")
                print(f"URL: {entry.get('uploader_id', 'N/A')}")
                print("-" * 40)
                # Download the video
                ydl.download([webpage_url])

# Download Shorts
download_shorts()