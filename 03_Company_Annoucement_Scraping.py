# coding: utf-8

import requests
from bs4 import BeautifulSoup
import json
import io
import urllib
import os
from os.path import exists, join
import re
import time
# from time import strftime, gmtime
from datetime import datetime
import logging

SCRIPT_ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = join(SCRIPT_ROOT_FOLDER, "logs")
if not exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    time.sleep(1)

logFormatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# fileHandler = logging.FileHandler("logs/03_Company_Announcement_Scraping_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/03_Company_Announcement_Scraping_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
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
LINKS = []
RESULT_PATH = join(SCRIPT_ROOT_FOLDER, "result/03_announcement")
if not exists(RESULT_PATH):
    LWARNING("'{0}' directory not exist. Creating folder '{0}'".format(RESULT_PATH))
    os.makedirs(RESULT_PATH)
    time.sleep(1)

JSON_FILE = join(SCRIPT_ROOT_FOLDER, 'resource/cmp_announcement_links.json')
if not exists(JSON_FILE):
    LERROR("'{}' does not exist".format(JSON_FILE))
    LERROR("Please run 'release/00_Get_Links.py' to generate links")
    quit()
    
YEARS_TO_SEARCH = re.compile(r'-?(?P<year>201[7|8])/[0-9]{1,2}/[0-9]{1,2}')
ANNOUNCEMENT_YEAR = ['2017', '2018']

with open(JSON_FILE) as json_file:
    LINKS = json.load(json_file)

# Algo start
_iteration = 0
start_time = time.time()
for k, link in LINKS.iteritems():
    index = 0
    announcement_dict = {}    # contains all announcement about a company

    LINFO(" Iteration {} / {} ".format(_iteration + 1, len(LINKS)).center(70, "="))
    LINFO("Getting page: {}".format(link))
    page = requests.get(link)
    next_page = []
    JSON_FILENAME = "{:0>2}_announcement.json".format(k)
    number_of_announcement_found = 0
    ANNOUNCEMENT_YEARS_NOT_FOUND = False

    if page.status_code == 200:
        LINFO("HTML GET returns [{}] status code. Starting initial soup..".format(page.status_code))
        while True:
            soup = BeautifulSoup(page.content, 'html5lib')

            # 2. Find the table containing the information with the the specified attribute
            LINFO("Locating announcement information table...")
            table = soup.find('table', {'width':'100%', 'cellpadding':'1', 'cellspacing':'1'})
            LINFO("Finished locating announcement information table!")

            # 3. Find the `td` tags with `onclick`: starting with `hello`, and get the text
            LINFO("Locating announcement titles...")
            announcement_title = soup.find_all('td', {'onclick':re.compile('^hello.*')})
            announcement_title = [announcement.get_text(separator='<br>', strip=True) for announcement in announcement_title]
            LINFO("Finished locating announcement titles...")
            

            # 4. Find the announcement content with `class`: `Sharon_add_news_content`
            LINFO("Locating announcement content...")
            announcement_contents = soup.find_all('td', {'class':'Sharon_add_news_content'})
            announcement_contents = [content.get_text(separator='<br>', strip=True) for content in announcement_contents]
            LINFO("Finished locating announcement contents!")


            # Populating the announcement dictionary as <announcement_index> : <{<title>: <content>}>
            for t, c in zip(announcement_title, announcement_contents):
                YEARS_FOUND = re.search(YEARS_TO_SEARCH, unicode(t).encode('utf8'))
                if YEARS_FOUND:
                    if YEARS_FOUND.group('year') in ANNOUNCEMENT_YEAR:
                        # announcement_dict[index] = {unicode(t): unicode(c)}
                        announcement_dict[unicode(t)] = unicode(c)
                        number_of_announcement_found += 1
                        index += 1
                else:
                    ANNOUNCEMENT_YEARS_NOT_FOUND = True
                    if ANNOUNCEMENT_YEARS_NOT_FOUND:
                        LINFO("Cannot find {} announcement anymore".format(" and ".join(ANNOUNCEMENT_YEAR)))
                        LINFO("Cooling up the soup..")
                        break
                # announcement_dict[index] = {unicode(t): unicode(c)}
                # index += 1
            

            # Check for next page, reiterate if found
            if not ANNOUNCEMENT_YEARS_NOT_FOUND:  # If still have 2017 or 2018 announcement --> check for next page
                LINFO("Checking for next pages...")
                next_page = [a for a in soup.find_all('a') if a.get_text() == u'下一頁']  # return [] if not available
                if next_page: # if current page has next page
                    _second = 0.5
                    LINFO("Now sleeps for {} seconds for not spamming the website".format(_second))
                    time.sleep(_second)
                    LINFO("Waking up.. Souping next announcement page..\n")

                    next_page = next_page[0]['href'].split('&')[-1] # splitted &Page=n
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
                    LINFO("No next page found! End of the announcement section")

                    # Write into a .json file
                    LINFO("Writing into JSON file...")
                    with io.open(join(RESULT_PATH, JSON_FILENAME), 'w', encoding='utf8') as fp:
                        data = json.dumps(announcement_dict, fp, ensure_ascii=False, indent=4)
                        fp.write(unicode(data))
                    LINFO("Finished writing JSON file to: {}\n".format(join(RESULT_PATH, JSON_FILENAME).encode('utf8')))
                    break
            else: # If no 2017 or 2018 announcement anymore --> Write to JSON file and terminate the loop. Continue to next company
                LINFO("Terminating due to no {} announcement found anymore".format(" and ".join(ANNOUNCEMENT_YEAR)))
                LINFO("Writing into JSON file...")
                with io.open(join(RESULT_PATH, JSON_FILENAME), 'w', encoding='utf8') as fp:
                    data = json.dumps(announcement_dict, fp, ensure_ascii=False, indent=4)
                    fp.write(unicode(data))
                LINFO("Finished writing JSON file to: {}\n".format(join(RESULT_PATH, JSON_FILENAME).encode('utf8')))
                break
    else:
        LERROR("Page returns [{}] status code. Please check".format(page.status_code))
        LERROR("Continue to the next link")
        
    LINFO("Total announcement found: {}\n".format(number_of_announcement_found))
    _second = 0.5
    LINFO("Now sleeps for {} seconds for not spamming the website".format(_second))
    time.sleep(_second)
    LINFO("Waking up.. Souping next company..\n")
    _iteration += 1


LINFO("Finished scraping all announcements in: {:.3f}s".format(time.time() - start_time))