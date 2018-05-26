import os
from os.path import join
from multiprocessing import Process
import subprocess
import time
# from time import strftime, gmtime
from datetime import datetime
import logging

SCRIPT_ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = join(SCRIPT_ROOT_FOLDER, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    time.sleep(1)

logFormatter = logging.Formatter("[%(asctime)s] [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# fileHandler = logging.FileHandler("logs/05_Wrappers_{}.log".format(strftime("%Y-%m-%d_%H%M%S", gmtime())))
LOG_PATH = join(SCRIPT_ROOT_FOLDER, "logs/05_Wrappers_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
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


def scrape_info():
    os.system("python {}".format(join(SCRIPT_ROOT_FOLDER, "01_Company_Info_Scraper.py")))
    # LINFO(os.getpid())

def scrape_news():
    os.system("python {}".format(join(SCRIPT_ROOT_FOLDER, "02_Company_News_Scraping.py")))
    # LINFO(os.getpid())

def scrape_announcement():
    os.system("python {}".format(join(SCRIPT_ROOT_FOLDER, "03_Company_Annoucement_Scraping.py")))
    # LINFO(os.getpid())

def db_operations():
    os.system("python {}".format(join(SCRIPT_ROOT_FOLDER, "04_DB_Operations.py")))

if __name__ == "__main__":
    start_time = time.time()
    LINFO("Generating company name parser")
    # os.system("python {}".format(join(SCRIPT_ROOT_FOLDER, "00_generate_company_name_parser.py")))
    generate = subprocess.Popen("python {}".format(join(SCRIPT_ROOT_FOLDER, "00_generate_company_name_parser.py")), shell=True)
    # time.sleep(2)
    LINFO("Generating target links")
    # os.system("python {}".format(join(SCRIPT_ROOT_FOLDER, "00_Get_Links.py")))
    get_link = subprocess.Popen("python {}".format(join(SCRIPT_ROOT_FOLDER, "00_Get_Links.py")), shell=True)
    # time.sleep(2)
    LINFO("Wait for resource generation to complete")
    exit_codes = [p.wait() for p in generate, get_link]

    LINFO("Running multiple processes simultaneously")
    LINFO("Running company info scraping")
    # info = Process(target=scrape_info)
    # info.start()
    print("python {}".format(join(SCRIPT_ROOT_FOLDER, "01_Company_Info_Scraper.py")))
    info = subprocess.Popen("python {}".format(join(SCRIPT_ROOT_FOLDER, "01_Company_Info_Scraper.py")), shell=True)

    LINFO("Running company news scraping")
    # news = Process(target=scrape_news)
    # news.start()
    news = subprocess.Popen("python {}".format(join(SCRIPT_ROOT_FOLDER, "02_Company_News_Scraping.py")), shell=True)

    LINFO("Running company announcement scraping")
    # announcement = Process(target=scrape_announcement)
    # announcement.start()
    announcement = subprocess.Popen("python {}".format(join(SCRIPT_ROOT_FOLDER, "03_Company_Announcement_Scraping.py")), shell=True)

    # info.join()
    # news.join()
    # announcement.join()

    # info.terminate()
    # news.terminate()
    # announcement.terminate()

    LINFO("Now waiting for info, news, announcement scraping to finish..")
    exit_codes = [p.wait() for p in info, news, announcement]

    LINFO("Running DB Operation in 10 seconds")
    for _n in range(10, 0, -1):
        print("{} ".format(_n)),
    print("")
    LINFO("Running DB Operation...")

    # Uncomment the following 3 LOC for release
    # db = subprocess.Popen("python {}".format(join(SCRIPT_ROOT_FOLDER, "04_DB_Operations.py")), shell=True)
    # LINFO("Now waiting for DB Operations to finish..")
    # exit_codes = db.wait()

    LINFO("Terminating DB Operation process")
    
    LINFO("Processes finished within: {:.3f}".format(time.time() - start_time))
