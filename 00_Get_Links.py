import requests
from bs4 import BeautifulSoup
import json
import io
import urllib
import os
from os.path import join, exists, basename
# from time import strftime, gmtime
from datetime import datetime
import logging

SCRIPT_ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))

# ==================== Inject Dependencies ====================
settings_file = join(SCRIPT_ROOT_FOLDER, "resource/settings.json")
if exists(settings_file):
    with open(settings_file, 'r') as f:
        GLOBAL = json.load(f)
        SETTINGS = GLOBAL[basename(__file__)]
        GLOBAL = GLOBAL['global']
else:
    print("'{}' does not exists! Contact author!".format(settings_file))
    quit()

# ==================== Logger Declaration ====================

# Define the script root folder used for modules internal referencing
SCRIPT_ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))

# Define the log directory
LOG_DIR = join(SCRIPT_ROOT_FOLDER, GLOBAL['LOG_DIR'])
if not exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Define the logging modules
logFormatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define the file handler for root logger
# fileHandler = logging.FileHandler("logs/00_Get_Links_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
# LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/00_Get_Links_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, SETTINGS["LOG_NAME"].format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
fileHandler = logging.FileHandler(LOG_PATH)
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)      # Add handler to root logger

# Define the handler for stdout logger
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)   # Add handler to root logger

# Define the handler for stdout logger
LINFO    = lambda s: logger.info(s)
LDEBUG   = lambda s: logger.debug(s)
LERROR   = lambda s: logger.error(s)
LWARNING = lambda s: logger.warning(s)


# ==================== Global Variable Declaration ====================

# 1. Variables
URL = SETTINGS["MAIN_URL"]                   # Main URL to scrape

COMPANY_INFO_PRECEDING_URL = SETTINGS['COMPANY_INFO_PRECEDING_URL']                    # Common variable
COMPANY_NEWS_PRECEDING_URL = SETTINGS['COMPANY_NEWS_PRECEDING_URL']                    # Common variable
COMPANY_ANNOUNCEMENT_PRECEDING_URL = SETTINGS['COMPANY_ANNOUNCEMENT_PRECEDING_URL']    # Common variable
TARGET_DATA_ATTRIBUTE = SETTINGS['TARGET_DATA_ATTRIBUTE']
LINFO(" ".join(["Target Data Attribute:", TARGET_DATA_ATTRIBUTE]))

# 2. Paths and Files Declaration
COMPANY_SAVE_FOLDER = join(SCRIPT_ROOT_FOLDER, SETTINGS['COMPANY_SAVE_FOLDER'])
if not exists(COMPANY_SAVE_FOLDER):
    # Creating folder if directory does not exist
    LINFO("'{0}' directory not exist. Creating folder '{0}'".format(COMPANY_SAVE_FOLDER))
    os.makedirs(COMPANY_SAVE_FOLDER)

# 2.1 Paths derived from COMPANY_SAVE_FOLDER
COMPANY_INFO_SAVE_PATH = join(COMPANY_SAVE_FOLDER, SETTINGS['COMPANY_INFO_SAVE_PATH'])
COMPANY_NEWS_SAVE_PATH = join(COMPANY_SAVE_FOLDER, SETTINGS['COMPANY_NEWS_SAVE_PATH'])
COMPANY_ANNOUNCEMENT_SAVE_PATH = join(COMPANY_SAVE_FOLDER, SETTINGS['COMPANY_ANNOUNCEMENT_SAVE_PATH'])
LDEBUG(" ".join(["Info Save Path:", COMPANY_INFO_SAVE_PATH]))
LDEBUG(" ".join(["News Save Path:", COMPANY_NEWS_SAVE_PATH]))
LDEBUG(" ".join(["Announcement Save Path:", COMPANY_ANNOUNCEMENT_SAVE_PATH]))

# ==================== Main Process ====================
# try:
    # 1. Requesting HTML GET
LINFO("Requesting page {}".format(URL))
page = requests.get(URL)
LINFO("Page returns <{}> code".format(page.status_code))

if page.status_code == 200:
    # 2. Feeding HTML contents into BeautifulSoup object
    LINFO("Souping page content")
    soup = BeautifulSoup(page.content, 'html5lib')
    LINFO("Finished souping page content ")

    # 3. Extracting targeted information
    LINFO("Finding target information")
    target_data = soup.find_all('td', {'class':TARGET_DATA_ATTRIBUTE})
    LINFO("Length of the target data: {}".format(len(target_data)))
    if len(target_data) > 0:
        # 4. If target_data is found, start main algorithm
        LINFO("Target data is not empty. Starting algorithm")
        cmp_info_links = {}             # Creating links for retrieving Company INFO
        cmp_news_links = {}             # Creating links for retrieving Company NEWS
        cmp_announcement_links = {}     # Creating links for retrieving Company ANNOUNCEMENT
        
        # ==================== Main Algorithm ====================
        LINFO("Iterating through all found links...")
        for i, d in enumerate(target_data):
            if (i+1) % 10 == 0:
                LINFO("Iteration {}...".format(i+1))
            big5_link = d.contents[0]['href'].split('=')[-1].encode('big5')     # Extract the webpage within 'href' tag
            cmp_info_links[i+1] = COMPANY_INFO_PRECEDING_URL + urllib.parse.quote_plus(big5_link)     # Process and convert URL to big5 format
            cmp_news_links[i+1] = COMPANY_NEWS_PRECEDING_URL + urllib.parse.quote_plus(big5_link)     # Process and convert URL to big5 format
            cmp_announcement_links[i+1] = COMPANY_ANNOUNCEMENT_PRECEDING_URL + urllib.parse.quote_plus(big5_link) # Process and convert URL to big5 format
            # LDEBUG("{}. ".format(i+1) + " -> ".join([big5_link.encode('utf8'), urllib.parse.quote_plus(big5_link)]))
        LINFO("Finish iterating through all found links")
        # ==================== Algorithm Finish ====================

        # ==================== Information Export ====================
        LINFO("Saving captured INFO links into {}".format(COMPANY_INFO_SAVE_PATH))
        with open(COMPANY_INFO_SAVE_PATH, 'w') as fp:
            json.dump(cmp_info_links, fp, indent=4, sort_keys=True)
        LINFO("Saved all the INFO links into {}".format(COMPANY_INFO_SAVE_PATH))

        LINFO("Saving captured NEWS links into {}".format(COMPANY_NEWS_SAVE_PATH))
        with open(COMPANY_NEWS_SAVE_PATH, 'w') as fp:
            json.dump(cmp_news_links, fp, indent=4, sort_keys=True)
        LINFO("Saved all the NEWS links into {}".format(COMPANY_NEWS_SAVE_PATH))

        LINFO("Saving captured ANNOUNCEMENT links into {}".format(COMPANY_ANNOUNCEMENT_SAVE_PATH))
        with open(COMPANY_ANNOUNCEMENT_SAVE_PATH, 'w') as fp:
            json.dump(cmp_announcement_links, fp, indent=4, sort_keys=True)
        LINFO("Saved all the ANNOUNCEMENT links into {}".format(COMPANY_ANNOUNCEMENT_SAVE_PATH))
        # ==================== End of Export ====================
        
        LINFO("Script finished\n\n")
    else:
        LERROR("Data not found! Please fix the following code:")
        LERROR("\ttarget_data = soup.find_all('td', {'class':TARGET_DATA_ATTRIBUTE})")
else:
    LERROR("The page returns a '{}' status code. Please check".format(page.status_code))
# except Exception as e:
#     LERROR("'{}' exception was raised!".format(type(e)))
#     LERROR("Message: {}".format(' - '.join(e.args)))
#     LWARNING("Script finished with exception(s)!")
