import requests
from bs4 import BeautifulSoup
import json
import re

# Step 1: Fetch the webpage content
url = 'https://www.youtube.com/shorts/MObF6KspTC4'
response = requests.get(url)
webpage_content = response.text

# Step 2: Parse the content with BeautifulSoup
soup = BeautifulSoup(webpage_content, 'html.parser')

# Step 3: Locate the <script> tag containing the JavaScript object
script_tag = soup.find('script', text=re.compile(r'var ytInitialData ='))

# Step 4: Extract the JavaScript object as a string
if script_tag:
    script_content = script_tag.string

    # Use regex to extract the JSON-like object from the script content
    json_text = re.search(r'var ytInitialData = ({.*?});', script_content, re.DOTALL).group(1)

    # Step 5: Convert the JSON-like string into a Python dictionary
    data_dict = json.loads(json_text)

    # Now `data_dict` holds the parsed dictionary, and you can access its values
    print(data_dict)

else:
    print("Script tag containing 'ytInitialData' not found.")
