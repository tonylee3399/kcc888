# coding: utf-8

# # Changelogs
# <b>V01</b> - Prototyping<br>
# <b>V02</b> - Combining prototypes, scraping all news for a company<br>
# <b>V03</b> - Writing to JSON

# In[9]:


from __future__ import print_function
import requests
from bs4 import BeautifulSoup
import json
import io
import urllib
import os
from os.path import exists, join, basename
import re
import time
# from time import strftime, gmtime
from datetime import datetime
import logging
import shutil

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
    time.sleep(1)

# Define the logging modules
logFormatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define the file handler for root logger
# fileHandler = logging.FileHandler("logs/02_Company_News_Scraping_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
# LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/02_Company_News_Scraping_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, SETTINGS['LOG_NAME'].format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
fileHandler = logging.FileHandler(LOG_PATH)
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

# Define the handler for stdout logger
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)

# Define the handler for stdout logger
LINFO    = lambda s: logger.info(s)
LDEBUG   = lambda s: logger.debug(s)
LERROR   = lambda s: logger.error(s)
LWARNING = lambda s: logger.warning(s)


# ==================== Global Variable Declaration ====================

# 1. Variables
LINKS = []
TITLE_YEARS_PATTERN = re.compile(r'-?(?P<year>201[7|8])/[0-9]{1,2}/[0-9]{1,2}')
NEWS_YEAR = ['2017', '2018']        # News years to be included. Add '2016' to include 2016 news

# 2. Paths and Files declaration
NEWS_RESULT_DIR = join(SCRIPT_ROOT_FOLDER, SETTINGS["NEWS_RESULT_DIR"])
REQUIRED_JSON_FILE = join(SCRIPT_ROOT_FOLDER, SETTINGS["REQUIRED_JSON_FILE"])

# 2.1 Paths and Files existence check
LINFO("Checking if '{}' already existed".format(NEWS_RESULT_DIR))
if exists(NEWS_RESULT_DIR):
    LINFO("Deleting previous directory")
    shutil.rmtree(NEWS_RESULT_DIR)

if not exists(NEWS_RESULT_DIR):
    LINFO("'{0}' directory not exist. Creating folder '{0}'".format(NEWS_RESULT_DIR))
    os.makedirs(NEWS_RESULT_DIR)
    time.sleep(1)

if not exists(REQUIRED_JSON_FILE):
    LERROR("'{}' does not exist".format(REQUIRED_JSON_FILE))
    LERROR("Please run 'release/00_Get_Links.py' to generate links")
    quit()
    
with open(REQUIRED_JSON_FILE) as json_file:
    LINKS = json.load(json_file)


# ==================== Supporting method declaration ====================

def write_to_json(path, data):
    assert type(data) is dict, "data has to be a 'dict' data type"

    LINFO("Writing into JSON file...")
    with io.open(path, 'w', encoding='utf8') as fp:
        json_data = json.dumps(data, ensure_ascii=False, indent=4)
        fp.write(json_data)
    LINFO("Finished writing JSON file to: {}\n".format(join(NEWS_RESULT_DIR, JSON_FILENAME).encode('utf8')))


# ==================== Main Process ====================

# try:
_iteration = 0
start_time = time.time()

# Iterate through every link
for k, link in LINKS.items():
    index = 0
    news_dict = {}    # contains all news about a company

    LINFO("Starting iteration {} / {}".format(_iteration + 1, len(LINKS)).center(70, "="))
    LINFO("Getting page: {}".format(link))
    page = requests.get(link)
    LINFO("Page returns <{}> code".format(page.status_code))

    next_page = []                                  # Flag for a page having a next page
    JSON_FILENAME = "{:0>2}_news.json".format(k)    # Exported JSON name
    number_of_news_found = 0                        # Tracker for number of news found
    NEWS_YEARS_NOT_FOUND = False                    # Flag for particular news years

    if page.status_code == 200:
        LINFO("HTML GET returns [{}] status code. Starting initial soup..".format(page.status_code))
        while True:
            LINFO("Souping page")
            soup = BeautifulSoup(page.content, 'html5lib')

            # 2. Find the table containing the information with the the specified attribute
            # LINFO("Locating news information table...")
            # table = soup.find('table', {'width':'100%', 'cellpadding':'1', 'cellspacing':'1'})
            # LINFO("Finished locating news information table!")

            # 3. Find the `td` tags with `onclick`: starting with `hello`, and get the text
            LINFO("Locating news titles...")
            # news_title = soup.find_all('td', {'onclick':re.compile('^hello.*')})
            # news_title = [news.get_text(separator='<br>', strip=True) for news in news_title]
            news_title = [title.get_text(separator='<br>', strip=True) for title in soup.select('td.cmp_news_02_Sharon')]
            LINFO("Finished locating {} news titles...".format(len(news_title)))
            LINFO("Finished locating news titles...")
            
            # 4. Find the news content with `class`: `Sharon_add_news_content`
            LINFO("Locating news content...")
            # news_contents = soup.find_all('td', {'class':'Sharon_add_news_content'})
            # news_contents = [content.get_text(separator='<br>', strip=True) for content in news_contents]
            news_contents = [content.get_text(separator='<br>', strip=True) for content in soup.select('td.Sharon_add_news_content')]
            LINFO("Finished locating {} news contents...".format(len(news_contents)))
            LINFO("Finished locating news contents!")

            # Add another check if the content of this website shows 無資料
            content_is_empty = [p for p in soup.select('p font') if p.get_text() == u"無 資 料"]
            if content_is_empty:
                LINFO("The content of this website is empty")

            # Every page only have 20 news to show
            # Populating the news dictionary as <title> : <content>
            # Iterating to every news_title and news_content
            for t, c in zip(news_title, news_contents):
                # Filter if news title matched TITLE_YEARS_PATTERN
                YEARS_FOUND = re.search(TITLE_YEARS_PATTERN, t)
                if YEARS_FOUND:
                    # Validate if the year is within NEWS_YEAR
                    if YEARS_FOUND.group('year') in NEWS_YEAR:
                        news_dict[t] = c
                        number_of_news_found += 1
                        index += 1
                else:
                    # Flag as no more news is within specified NEWS_YEAR to quit loop
                    NEWS_YEARS_NOT_FOUND = True
                    if NEWS_YEARS_NOT_FOUND:
                        LINFO("Cannot find {} news anymore".format(" and ".join(NEWS_YEAR)))
                        LINFO("Cooling up the soup..")
                        break
            # Finished processing 20 news on the page.

            # Check for next page, reiterate if found a 'Next Page' link
            if not NEWS_YEARS_NOT_FOUND:  # If still have 2017 or 2018 news --> check for next page
                LINFO("Checking for next pages...")
                # Next page is defined with a 下一頁 button
                next_page = [a for a in soup.select('td[onmouseover] > a') if a.get_text() == u'下一頁']  # return [] if not available
                if next_page: # If current page has next page
                    _second = GLOBAL["DELAY"]
                    if _second:
                        LINFO("Now sleeps for {} seconds for not spamming the website".format(_second))
                        time.sleep(_second)
                        LINFO("Waking up.. Souping next news page..\n")

                    next_page = next_page[0]['href'].split('&')[-1] # Extract 'Page=n'
                    LINFO("Next {} found..".format(next_page))
                    next_page_link = "&".join([link, next_page]) # join &Page=n to the original link
                    LINFO("Requesting URL GET at {}".format(next_page_link))
                    page = requests.get(next_page_link)
                    if page.status_code == 200:
                        LINFO("HTML GET returns [{}] status code. Starting {} soup..".format(page.status_code, next_page))
                    else:
                        LERROR("Page returns [{}] status code. Please check".format(page.status_code))
                        LERROR("Terminating loop. Continue to the next link")
                        break
                else: # if current page does not have next page
                    LINFO("No next page found! End of the news section")

                    # Write into a .json file
                    write_to_json(join(NEWS_RESULT_DIR, JSON_FILENAME), news_dict)

                    # Stream to logs
                    if SETTINGS["Log_Content"]:
                        for k, v in news_dict.items():
                            LDEBUG("{}\n\t{}".format(k, v))

                    break
            else: # If no 2017 or 2018 news anymore --> Write to JSON file and terminate the loop. Continue to next company
                LINFO("Terminating due to no {} news found anymore".format(" and ".join(NEWS_YEAR)))
                # Write into a .json file
                write_to_json(join(NEWS_RESULT_DIR, JSON_FILENAME), news_dict)
                # Stream to logs
                if SETTINGS["Log_Content"]:
                    for k, v in news_dict.items():
                        LDEBUG("{}\n\t{}".format(k, v))
                break
    else:
        LERROR("Page returns [{}] status code. Please check".format(page.status_code))
        LERROR("Continue to the next link")
        
    LINFO("Total news found: {}\n".format(number_of_news_found))
    _second = GLOBAL["DELAY"]
    LINFO("Now sleeps for {} seconds for not spamming the website".format(_second))
    time.sleep(_second)
    LINFO("Waking up.. Souping next company..\n")
    _iteration += 1


    LINFO("Finished scraping all news in: {:.3f}s".format(time.time() - start_time))
# except Exception as e:
#     LERROR("'{}' exception was raised!".format(type(e)))
#     LERROR("Message: {}".format(' - '.join(e.args)))
#     LWARNING("Script finished with exception(s)!")
