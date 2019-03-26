# coding: utf8

# # Changelog
# <b>V00</b> - Original commit<br>
# <b>V01</b> - added `handle_content()` to recursively find the last `NavigableString` to print<br>
# <b>V02</b> - checkpoint for `handle_content()`<br>
# <b>V03</b> - fixed `handle_content()` method. Finally working. Changed to simple algorithm<br>
# <b>V04</b> - combine `cmpinfo_title_bg` and `cmpinfo_nbo_bg`<br>
# <b>V05</b> - dump into a `.json` or `.tsv` file<br>
# <b>V06</b> - Finishing touch<br>
# <b>V07</b> - Pre-release prorotype

from __future__ import print_function
import requests
import bs4
from bs4 import BeautifulSoup
import re
import json
import io
import time
# from time import strftime, gmtime
from datetime import datetime
import os
from os.path import join, exists, basename
import logging
import shutil
from util import time_the_process


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
# fileHandler = logging.FileHandler("logs/01_Company_Info_Scraper_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
# LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/01_Company_Info_Scraper_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
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
TARGET_TITLE_VALUE_CLASS = "cmpinfo_title_bg|cmpinfo_nbo_bg"
COMPANY_NAME_CLASS = "cmpinfo_cmp_b|cmpinfo_cmp"
LDEBUG(" ".join(["Target Title Value Class:", TARGET_TITLE_VALUE_CLASS]))
LDEBUG(" ".join(["Target Company Name Class:", COMPANY_NAME_CLASS]))

COMPANY_RANK_INDEX = {}     # Used for tracking the company rank
COMPANY_STOCK_NO   = {}     # Used for tracking the company stock number

# 2. Paths and Files declaration
COMPANY_INFO_RESULT_DIR = join(SCRIPT_ROOT_FOLDER, SETTINGS['COMPANY_INFO_RESULT_DIR'])
COMPANY_NAMES_JSON = join(SCRIPT_ROOT_FOLDER, SETTINGS['COMPANY_NAMES_JSON'])
COMPANY_NAME_PARSER_FILE = join(SCRIPT_ROOT_FOLDER, SETTINGS['COMPANY_NAME_PARSER_FILE'])
RESOURCE_DIR = join(SCRIPT_ROOT_FOLDER, SETTINGS['RESOURCE_DIR'])

# 2.1. Derivative of RESOURCE_DIR
COMPANY_RANK_INDEX_JSON_FILENAME = join(RESOURCE_DIR, SETTINGS['COMPANY_RANK_INDEX_JSON_FILENAME'])
COMPANY_STOCK_NO_JSON_FILENAME = join(RESOURCE_DIR, SETTINGS['COMPANY_STOCK_NO_JSON_FILENAME'])


# 2.2. Directory existence check
COMPANY_INFO_JSON_SOURCE_FILE = join(SCRIPT_ROOT_FOLDER, SETTINGS['COMPANY_INFO_JSON_SOURCE_FILE'])
if not exists(COMPANY_INFO_JSON_SOURCE_FILE):
    LERROR("'{}' does not exist".format(COMPANY_INFO_JSON_SOURCE_FILE))
    LERROR("Please run '11_Get_Links.py' to generate links")
    quit()
    
with open(COMPANY_INFO_JSON_SOURCE_FILE) as json_file:
    LINKS = json.load(json_file)
    LINFO("Successfully loading '{}' links".format(COMPANY_INFO_JSON_SOURCE_FILE))

LINFO("Checking if '{}' already existed".format(COMPANY_INFO_RESULT_DIR))
if exists(COMPANY_INFO_RESULT_DIR):
    LINFO("Deleting previous directory")
    shutil.rmtree(COMPANY_INFO_RESULT_DIR)

if not exists(COMPANY_INFO_RESULT_DIR):
    LWARNING("'{0}' directory not exist. Creating folder '{0}'".format(COMPANY_INFO_RESULT_DIR))
    os.makedirs(COMPANY_INFO_RESULT_DIR)
    LWARNING("Folder '{}'' created".format(COMPANY_INFO_RESULT_DIR))
    time.sleep(1)


# Supporting method declaration
@time_the_process
def parse_company_name(company_name):
    '''Parse company name discrepancy between Berich.com and Database'''
    # assert type(company_name) == unicode, "company_name has to be unicode typed. Type: {}".format(type(company_name))
        
    pattern = re.compile(r"(?P<name>.*)\((?P<status>.*)\)")
    
    # If pattern does not match, return original company_name
    if not re.match(pattern, company_name):
        # LWARNING("{} does not comply to the format!".format(company_name.encode('utf8')))
        LWARNING(u"{} does not comply to the format!".format(company_name))
        return
    
    if exists(COMPANY_NAMES_JSON):
        with io.open(COMPANY_NAME_PARSER_FILE, 'r', encoding='utf8') as f:
            NAMES = json.load(f)
            LINFO("Searching company names")
            _company_name   = re.search(pattern, company_name).group('name')
            LINFO("Searching company status")
            _company_status = re.search(pattern, company_name).group('status')
            if _company_name in NAMES.keys():
                LINFO("Name discrepancy between Berich.com and Database detected..")
                LINFO("Converting '{}' to '{}' and returning as unicode".format(company_name.encode('utf8'), NAMES[_company_name].encode('utf8')))
                return NAMES[_company_name]
            else:
                LINFO("Name discrepancy between Berich.com and Database not found..")
                LINFO("Returning original company name '{}'".format(_company_name.encode('utf8')))
                return _company_name
    else:
        LERROR("'{}' does not exists. Please run 00_generate_company_name_parser.py".format(COMPANY_NAMES_JSON))


# ==================== Main Process ====================
# try:
start_time = time.time()
_iteration = 0

# Iterate through every link
for k, v in LINKS.items():
    LINFO("Starting iteration {} / {}".format(_iteration + 1, len(LINKS)).center(70, "="))
    LINFO("Getting page: {}".format(v))
    page = requests.get(v)
    LINFO("Page returns <{}> code".format(page.status_code))

    if page.status_code == 200:
        LINFO("Souping page")
        soup = BeautifulSoup(page.content, 'html5lib')

        # Retrieving company name and its status
        LINFO("Retrieving company name")
        company_name = soup.find('td', {'class':re.compile(COMPANY_NAME_CLASS)}).get_text()
        LINFO("Parsing company name to  Bifoo DB friendly name")
        company_name = parse_company_name(company_name)
        LDEBUG("Company name: {}".format(company_name.encode('utf8')))
        LDEBUG("Company URL: {}".format(v))
        LINFO(" Analyzing ".center(70, "-"))

        # Create an indexed filename according to its rank
        JSON_FILENAME = "{:0>2}".format(k) + "_" + company_name + ".json"

        # Find table headers and its values
        td_cmp_tags = soup.find_all("td", {"class": re.compile(TARGET_TITLE_VALUE_CLASS)})

        # Check if is empty
        LINFO("Length of td_cmp_tags information pair: {}".format(len(td_cmp_tags)))

        # Declare list variables to contain title value pairs
        title = []
        value = []

        title.append('Company Name')
        title.append('Company URL')
        value.append(company_name)
        value.append(v)

        # Insert the company rank
        COMPANY_RANK_INDEX["{:0>2}".format(k)] = company_name

        # Arrange the data into a dictionary friendly format
        for i, info in enumerate(td_cmp_tags):
            if i % 2 == 0:
                title.append(info.get_text().strip() if info.get_text().strip() != '' else '-')
            else:
                value.append(info.get_text().strip() if info.get_text().strip() != '' else '-')

        # Check the length of both title and value
        # Length MUST be the same, or the program will treat it as a corrupted format and quit program
        LINFO("Length of title variable: {}".format(len(title)))
        LINFO("Length of value variable: {}".format(len(value)))
        if len(title) != len(value):
            LERROR("The title and value cannot be paired. Quitting..")
            quit()

        # Input both titles and values into a dictionary
        LINFO("Populating company information dictionary...")
        i=0
        DATA = {}
        for _t, _v in zip(title, value):
            DATA[_t] = _v
            i += 1
        LINFO("Finished populating company information dictionary!")
        for k, v in DATA.items():
            LDEBUG(": ".join([k, v]))

        # Write into a .json file
        LINFO("Writing into JSON file...")
        with io.open(join(COMPANY_INFO_RESULT_DIR, JSON_FILENAME), 'w', encoding='utf8') as fp:
            data = json.dumps(DATA, ensure_ascii=False, indent=4, sort_keys=True)
            fp.write(data)
        LINFO("Finished writing JSON file to: {}".format(join(COMPANY_INFO_RESULT_DIR, JSON_FILENAME).encode('utf8')))

        # Define a sleep interval. Recommended to not set to 0 for not spamming the server
        _second = GLOBAL["DELAY"]
        if _second:
            LINFO("Now sleeps for {} seconds for not spamming the website".format(_second))
            time.sleep(_second)
            LINFO("Waking up.. Starting next iteration..\n")

        # Insert the company stock number
        COMPANY_STOCK_NO[DATA["Company Name"]] = DATA[u"股票代號"]

        # Increase iteration counter
        _iteration += 1
    else:
        LERROR("Page returns [{}] status code. Please check".format(page.status_code))
        LERROR("Continue to the next link")
        _iteration += 1
        # continue


# Write COMPANY_RANK_INDEX into a .json file
LINFO("Writing into JSON file...")
with io.open(COMPANY_RANK_INDEX_JSON_FILENAME, 'w', encoding='utf8') as fp:
    data = json.dumps(COMPANY_RANK_INDEX, ensure_ascii=False, indent=4, sort_keys=True)
    fp.write(data)
LINFO("Finished writing JSON file to: {}\n".format(COMPANY_RANK_INDEX_JSON_FILENAME.encode('utf8')))

# Write COMPANY_RANK_INDEX into a .json file
LINFO("Writing into JSON file...")
with io.open(COMPANY_STOCK_NO_JSON_FILENAME, 'w', encoding='utf8') as fp:
    data = json.dumps(COMPANY_STOCK_NO, ensure_ascii=False, indent=4, sort_keys=True)
    fp.write(data)
LINFO("Finished writing JSON file to: {}\n".format(COMPANY_STOCK_NO_JSON_FILENAME.encode('utf8')))

LINFO("Finished scraping all data in: {:.3f}s".format(time.time() - start_time))
# except Exception as e:
#     LERROR("'{}' exception was raised!".format(type(e)))
#     LERROR("Message: {}".format(' - '.join(e.args)))
#     LWARNING("Script finished with exception(s)!")

    