import yt_dlp

# Replace 'CHANNEL_URL' with the actual URL of the YouTube channel you want to download.
channel_url = 'https://www.youtube.com/@ShtsNGigsPodcast'

# Define the options for yt-dlp
ydl_opts = {
    'outtmpl': '%(uploader)s/%(title)s.%(ext)s',  # Organize downloads by uploader/channel name
    'format': 'bestvideo+bestaudio/best',         # Download the best video + audio quality
    'postprocessors': [{
        'key': 'FFmpegVideoConvertor',
        'preferedformat': 'mp4',                  # Convert to mp4 format
    }],
    'merge_output_format': 'mp4',
    'download_archive': 'downloaded_videos.txt',  # Keep track of downloaded videos
    'ignoreerrors': True,                         # Skip errors and continue downloading
    'extractor-args': 'youtube:tab=videos',       # Only download videos from the "Videos" tab
}

# Use yt-dlp to download the channel
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    ydl.download([channel_url])
