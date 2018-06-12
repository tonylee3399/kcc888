from __future__ import print_function
import json
import re
import io
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
# fileHandler = logging.FileHandler("logs/00_generate_company_name_parser_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
# LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/00_generate_company_name_parser_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, SETTINGS['LOG_NAME'].format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
fileHandler = logging.FileHandler(LOG_PATH)
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)      # Add handler to root logger

# Define the handler for stdout logger
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)   # Add handler to root logger

# Define easy to read/call method for logging
LINFO    = lambda s: logger.info(s)
LDEBUG   = lambda s: logger.debug(s)
LERROR   = lambda s: logger.error(s)
LWARNING = lambda s: logger.warning(s)


# ==================== Global Variable Declaration ====================

# 1. Variables
raw_lines = []

# 2. Paths and Files Declaration
REQUIRED_FILE = join(SCRIPT_ROOT_FOLDER, SETTINGS['REQUIRED_FILE'])
SAVE_DIR = join(SCRIPT_ROOT_FOLDER, SETTINGS['SAVE_DIR'])
SAVE_FILENAME = SETTINGS['SAVE_FILENAME']

# 2.1. Check if saving directory exists
if not exists(SAVE_DIR):
    # Create saving directory if does not exist
    LINFO("'{}' save folder does not exist.".format(SAVE_DIR))
    os.makedirs(SAVE_DIR)
    LINFO("'{}' save folder created".format(SAVE_DIR))

# 2.2. Check if required file exists
if exists(REQUIRED_FILE):
    # If exists: read every line in parse_bifoo.php to raw_lines variable
    LDEBUG("File '{}' exists".format(REQUIRED_FILE))
    with open(REQUIRED_FILE, 'r') as f:
        for line in f.readlines():
            raw_lines.append(line)
        LDEBUG("Finished reading all content")
        LDEBUG("\n".join(raw_lines))
else:
    # If doesn't exist: Need to supply parse_bifoo.php to this 
    LERROR("'{}' required file does not exists!! Please supply!".format(REQUIRED_FILE))


# 3.1 Extracting Company Name in the web
LINFO("Preprocessing information read from parse_bifoo.php")
result_bifeng = [c.strip().split(' ')[1][1:-2] for c in raw_lines if 'case' in c]
LINFO("Finished preprocessing information read from parse_bifoo.php")

# Company name patterns to capture within the string
pattern = r"\$array\[\$i\]\['cmpname'\] = \'(?P<cmpname>.*)\';"

# 3.2 Extracting DB friendly name by finding string matching the patterns
LINFO("Find lines matching pattern specified")
result_db = re.findall(pattern, "\n".join(raw_lines))
LINFO("Finished finding lines matching pattern specified")

# UTF8 decoded to unicode
LINFO("Decoding UTF8 decoded strings into unicode")
result_bifeng = [x.decode('utf8') for x in result_bifeng]
result_db = [x.decode('utf8') for x in result_db]
LINFO("Finished decoding..")

# 3.3 Zip all information into a dictionary to prepare feeding to JSON module
LINFO("Creating dictionary containing all information")
dictionary = dict(zip(result_bifeng, result_db))
for k, v in dictionary.iteritems():
    LDEBUG(" -> ".join([k.encode('utf8'), v.encode('utf8')]))

# 4. Exporting the dictionary to JSON file
with io.open(join(SAVE_DIR, SAVE_FILENAME), 'w', encoding='utf8') as f:
    data = json.dumps(dictionary, f, ensure_ascii=False, indent=4, sort_keys=True)
    f.write(unicode(data))
    LINFO("Sucessfully written all information to {}".format(join(SAVE_DIR, SAVE_FILENAME)))
    
LINFO("Finished running script.\n\n")
