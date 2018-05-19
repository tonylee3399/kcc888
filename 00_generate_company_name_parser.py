import json
import re
import io
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

# fileHandler = logging.FileHandler("logs/00_generate_company_name_parser_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/00_generate_company_name_parser_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
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


raw_lines = []

REQUIRED_FILE = join(SCRIPT_ROOT_FOLDER, 'reference/parse_bifoo.php')
SAVE_DIR = join(SCRIPT_ROOT_FOLDER, 'resource')
SAVE_FILENAME = 'company_name_parser.json'



# Check all required path and files
if not exists(SAVE_DIR):
	LINFO("'{}' save folder does not exist.".format(SAVE_DIR))
	os.makedirs(SAVE_DIR)
	LINFO("'{}' save folder created".format(SAVE_DIR))

# Reading all information within parse_bifoo.php
if exists(REQUIRED_FILE):
	LDEBUG("File '{}' exists".format(REQUIRED_FILE))
	with open(REQUIRED_FILE, 'r') as f:
	    for line in f.readlines():
	        raw_lines.append(line)
	    LDEBUG("Finished reading all content")
	    LDEBUG(raw_lines)
else:
	LERROR("'{}' required file does not exists!! Please supply!".format(REQUIRED_FILE))


# Preprocess the information
LINFO("Preprocessing information read from parse_bifoo.php")
result_bifeng = [c.strip().split(' ')[1][1:-2] for c in raw_lines if 'case' in c]
LINFO("Finished preprocessing information read from parse_bifoo.php")

# Company name patterns to capture within the string
pattern = r"\$array\[\$i\]\['cmpname'\] = \'(?P<cmpname>.*)\';"

# Find all respective patterns
LINFO("Find lines matching pattern specified")
result_db = re.findall(pattern, "\n".join(raw_lines))
LINFO("Finished finding lines matching pattern specified")

# UTF8 decoded to unicode
LINFO("Decoding UTF8 decoded strings into unicode")
result_bifeng = [x.decode('utf8') for x in result_bifeng]
result_db = [x.decode('utf8') for x in result_db]
LINFO("Finished decoding..")

# Contain all information within dictionary variable
LINFO("Creating dictionary containing all information")
dictionary = dict(zip(result_bifeng, result_db))
LDEBUG(dictionary)

with io.open(join(SAVE_DIR, SAVE_FILENAME), 'w', encoding='utf8') as f:
    data = json.dumps(dictionary, f, ensure_ascii=False, indent=4, sort_keys=True)
    f.write(unicode(data))
    LINFO("Sucessfully written all information to {}".format(join(SAVE_DIR, SAVE_FILENAME)))
    
LINFO("Finished running script.\n\n")
