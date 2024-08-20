import yt_dlp

# Replace 'CHANNEL_URL' with the actual URL of the YouTube channel you want to download.
channel_url = 'https://www.youtube.com/@ShtsNGigsPodcast'

# Base options for yt-dlp
base_ydl_opts = {
    'format': 'bestvideo+bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegVideoConvertor',
        'preferedformat': 'mp4',
    }],
    'merge_output_format': 'mp4',
    'download_archive': 'downloaded_videos.txt',
    'ignoreerrors': True,
}

# Function to download videos based on tab type
def download_tab(tab_name, output_dir):
    ydl_opts = base_ydl_opts.copy()
    ydl_opts.update({
        'outtmpl': f'{output_dir}/%(title)s.%(ext)s',
        'extractor-args': f'youtube:tab={tab_name}',
    })

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([channel_url])

# Download "Videos" tab
# download_tab('videos', 'Videos')

# Download "Shorts" tab
download_tab('shorts', 'Shorts')

# Download "Live" tab
# download_tab('live', 'Lives')
