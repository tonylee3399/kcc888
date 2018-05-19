import requests
from bs4 import BeautifulSoup
import json
import io
import urllib
import os
from os.path import join, exists
# from time import strftime, gmtime
from datetime import datetime
import logging

SCRIPT_ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = join(SCRIPT_ROOT_FOLDER, "logs")
if not exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logFormatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# fileHandler = logging.FileHandler("logs/00_Get_Links_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/00_Get_Links_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
fileHandler = logging.FileHandler(LOG_PATH)
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)

LINFO    = lambda s: logger.info(s)
LDEBUG   = lambda s: logger.debug(s)
LERROR   = lambda s: logger.error(s)
LWARNING = lambda s: logger.warning(s)


# Global Variable Declaration
URL = 'http://www.berich.com.tw/DP/OrderList/List_kakelu.asp'
COMPANY_INFO_PRECEDING_URL = "http://www.berich.com.tw/DP/Cmpinfo/Cmpinfo.asp?cmpname="
COMPANY_NEWS_PRECEDING_URL = "http://www.berich.com.tw/DP/Cmpinfo/Cmpinfo_News.asp?cmpname="
COMPANY_ANNOUNCEMENT_PRECEDING_URL = "http://www.berich.com.tw/DP/Cmpinfo/Cmpinfo_Ancs.asp?cmpname="
TARGET_DATA_ATTRIBUTE = "cmd_name_sin"
COMPANY_SAVE_FOLDER = join(SCRIPT_ROOT_FOLDER, 'resource')
if not exists(COMPANY_SAVE_FOLDER):
    LINFO("'{0}' directory not exist. Creating folder '{0}'".format(COMPANY_SAVE_FOLDER))
    os.makedirs(COMPANY_SAVE_FOLDER)

COMPANY_INFO_SAVE_PATH = join(COMPANY_SAVE_FOLDER, 'cmp_info_links.json')
COMPANY_NEWS_SAVE_PATH = join(COMPANY_SAVE_FOLDER, 'cmp_news_links.json')
COMPANY_ANNOUNCEMENT_SAVE_PATH = join(COMPANY_SAVE_FOLDER, 'cmp_announcement_links.json')

LINFO("Requesting page {}".format(URL))
page = requests.get(URL)
LINFO("Page returns <{}> code".format(page.status_code))

if page.status_code == 200:
    LINFO("Souping page content")
    soup = BeautifulSoup(page.content, 'html5lib')
    LINFO("Finished souping page content ")

    LINFO("Finding target information")
    target_data = soup.find_all('td', {'class':TARGET_DATA_ATTRIBUTE})
    LINFO("Length of the target data: {}".format(len(target_data)))
    if len(target_data) > 0:
        LINFO("Target data is not empty. Starting algorithm")
        cmp_info_links = {}
        cmp_news_links = {}
        cmp_announcement_links = {}
        
        LINFO("Iterating through all found links...")
        for i, d in enumerate(target_data):
            if (i+1) % 10 == 0:
                LINFO("Iteration {}...".format(i+1))
            big5_link = d.contents[0]['href'].split('=')[-1].encode('big5')
            cmp_info_links[i+1] = COMPANY_INFO_PRECEDING_URL + urllib.quote_plus(big5_link)
            cmp_news_links[i+1] = COMPANY_NEWS_PRECEDING_URL + urllib.quote_plus(big5_link)
            cmp_announcement_links[i+1] = COMPANY_ANNOUNCEMENT_PRECEDING_URL + urllib.quote_plus(big5_link)

        LINFO("Finish iterating through all found links")

        LINFO("Saving captured info links into {}".format(COMPANY_INFO_SAVE_PATH))
        with open(COMPANY_INFO_SAVE_PATH, 'w') as fp:
            json.dump(cmp_info_links, fp, indent=4, sort_keys=True)
        LINFO("Saved all the info links into {}".format(COMPANY_INFO_SAVE_PATH))

        LINFO("Saving captured news links into {}".format(COMPANY_NEWS_SAVE_PATH))
        with open(COMPANY_NEWS_SAVE_PATH, 'w') as fp:
            json.dump(cmp_news_links, fp, indent=4, sort_keys=True)
        LINFO("Saved all the news links into {}".format(COMPANY_NEWS_SAVE_PATH))

        LINFO("Saving captured news links into {}".format(COMPANY_ANNOUNCEMENT_SAVE_PATH))
        with open(COMPANY_ANNOUNCEMENT_SAVE_PATH, 'w') as fp:
            json.dump(cmp_announcement_links, fp, indent=4, sort_keys=True)
        LINFO("Saved all the news links into {}".format(COMPANY_ANNOUNCEMENT_SAVE_PATH))

        LINFO("Script finished\n\n")
    else:
        LERROR("Data not found! Please fix the following code:")
        LERROR("\ttarget_data = soup.find_all('td', {'class':TARGET_DATA_ATTRIBUTE})")
else:
    LERROR("The page returns a '{}' status code. Please check".format(page.status_code))

LINFO("Script finished")