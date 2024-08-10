import yt_dlp

def download_youtube_short(url):
    # ydl_opts = {
    #     'outtmpl': '%(title)s.%(ext)s',
    #     'verbose': True,  # Enable verbose output
    #     'ignoreerrors': True,  # Continue on download errors
    # }
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': '%(title)s.%(ext)s',
        'verbose': True,
        'ignoreerrors': True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            # First, extract info to check available formats
            info = ydl.extract_info(url, download=False)
            
            if 'formats' in info:
                print("Available formats:")
                for format in info['formats']:
                    print(f"Format: {format['format_id']}, Extension: {format.get('ext', 'N/A')}, Resolution: {format.get('resolution', 'N/A')}")
            
            # Then attempt to download
            ydl.download([url])
            print(f"Successfully downloaded the YouTube Short: {url}")
        except Exception as e:
            print(f"An error occurred while downloading: {str(e)}")
            print("Try using a different format or check if the video is available.")
# Example usage
# url = "https://youtube.com/shorts/Ce7ej15VEUw"
url = "https://youtube.com/shorts/MI2CMUYOnDY"
download_youtube_short(url)