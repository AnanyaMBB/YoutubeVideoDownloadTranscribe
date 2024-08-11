from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Set up Chrome options
chrome_options = Options()

# Initialize the Chrome WebDriver
driver = webdriver.Chrome(service=Service(), options=chrome_options)

# Get the version of the Chrome browser used by Selenium
chrome_version = driver.capabilities['browserVersion']
print(f"Chrome version: {chrome_version}")

# Get the version of ChromeDriver used by Selenium
chromedriver_version = driver.capabilities['chrome']['chromedriverVersion'].split(' ')[0]
print(f"ChromeDriver version: {chromedriver_version}")

# Close the driver
driver.quit()
