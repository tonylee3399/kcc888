#-*- coding: utf8 -*-

from __future__ import print_function
import logging
import json
from os.path import exists, join
import pyodbc
import re
import os
from os.path import exists, isfile, join, basename
import time
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


root_dir = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = join(root_dir, GLOBAL["LOG_DIR"])
if not exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    time.sleep(1)

logFormatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Logger L type
# LOG_PATH1 = join(root_dir, "logs/04_DB_Operations_{}.log".format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
LOG_PATH1 = join(root_dir, SETTINGS["LOG_NAME"].format(datetime.strftime(datetime.now(), "%Y-%m-%d_%H%M%S")))
fileHandler = logging.FileHandler(LOG_PATH1)
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

# Logger for inserted company ID
id_logger = logging.getLogger("idLogger")
id_logger.setLevel(logging.DEBUG)

# LOG_PATH2 = join(root_dir, "logs/000_inserted_id.log")
LOG_PATH2 = join(root_dir, SETTINGS["INSERTED_ID_LOG_NAME"])
fileHandler3 = logging.FileHandler(LOG_PATH2)
fileHandler3.setFormatter(logFormatter)
fileHandler3.setLevel(logging.DEBUG)
id_logger.addHandler(fileHandler3)


DB_INFO_JSON_FILE = join(root_dir, SETTINGS["DB_INFO_JSON_FILE"])
KEY_LIST = ['ip', 'database', 'uid', 'pw']
DB_INFO = None
REGEX_TABLE_PATTERN = re.compile(".* FROM (?P<table_name>\w*) (.*)", re.IGNORECASE)

# Get company rank through its index
CMP_INDEX_MAP = None
CMP_INDEX_MAP_PATH = join(root_dir, SETTINGS["CMP_INDEX_MAP_PATH"])
with open(CMP_INDEX_MAP_PATH, 'r') as f:
    CMP_INDEX_MAP = json.load(f)

# The pattern of the news posted in Berich. Identify news_title and the date_posted
NEWS_TITLE_PATTERN = re.compile(r'(?P<news_title>.*) -(?P<date_posted>201[7|8]/[\d]{1,2}/[\d]{1,2})')

# Translates the naming convention of the 00 to 04 scripts
FILE_NAMING_INDEX_PATTERN = re.compile(r".*(?P<cmp_index>\d{2,3})_(?P<type>news|announcement)\.json")

# The pattern of the news/announcement content. Identify the content source
SOURCE_PATTERN = re.compile(u".*<摘錄(?P<source>.*)>.*".encode('utf8'))

# This variable is used for mapping company name scraped from Berich to DB friendly
INFO_SOURCE_PATH = join(root_dir, SETTINGS["INFO_SOURCE_PATH"])
with open(INFO_SOURCE_PATH, 'r') as f:
    SOURCE_MAP = json.load(f)

def get_cmp_name(json_filename):
    cmp_index = re.match(FILE_NAMING_INDEX_PATTERN, json_filename)
    if cmp_index:
        if cmp_index.group('cmp_index') == '00':
            # This is a temporary fix
            # Python has the following problem to determine the group.
            # Further fix is needed
            # In [67]: name
            # Out[67]: '100_announcement.json'

            # In [68]: name2
            # Out[68]: '99_announcement.json'

            # In [69]: FILE_NAMING_INDEX_PATTERN = re.compile(r".*(?P<cmp_index>\d{2,})_(?P<type>news|announcement)\.json")

            # In [70]: re.findall(FILE_NAMING_INDEX_PATTERN, name)
            # Out[70]: [('00', 'announcement')]

            # In [71]: re.findall(FILE_NAMING_INDEX_PATTERN, name2)
            # Out[71]: [('99', 'announcement')]
            return CMP_INDEX_MAP["100"]
        return CMP_INDEX_MAP["{:0>2}".format(cmp_index.group('cmp_index'))]
    else:
        return "json_filename: {}. Company Name Not Found".format(json_filename)

def find_date_posted(a_string):
    '''Find the date within a raw title of news posted. Takes unicode -> Return unicode'''
    assert type(a_string) is unicode, "Type has to be unicode. Please decode or encode to unicode"
    date = re.match(NEWS_TITLE_PATTERN, a_string.encode('utf8'))
    if date:
        return date.group('date_posted').decode('utf8')


def find_title(a_string):
    '''Find the title within a raw title of news posted. Takes unicode -> Return unicode'''
    assert type(a_string) is unicode, "Type has to be unicode. Please decode or encode to unicode"
    date = re.match(NEWS_TITLE_PATTERN, a_string.encode('utf8'))
    if date:
        return date.group('news_title').decode('utf8')


def find_source(a_string):
    '''Find the news/announcement source. Takes unicode -> Return unicode'''
    assert type(a_string) is unicode, "Type has to be unicode. Please decode or encode to unicode"
    source = re.match(SOURCE_PATTERN, a_string.encode('utf8'))
    if source:
        if source.group('source').decode('utf8') in SOURCE_MAP.keys():
            return SOURCE_MAP[source.group('source').decode('utf8')]
        else:
            return None
    else:
        return None


def setup_db_conn(db_info):
    '''Connect to the database specified and return a ODBC cursor object'''
    # Some other example server values are
    # server = 'localhost\sqlexpress' # for a named instance
    # server = 'myserver,port' # to specify an alternate port
    cnxn = pyodbc.connect('DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={ip};DATABASE={db};'
                          'UID={uid};PWD={pw}'.format(ip=db_info['ip'], db=db_info['database'], uid=db_info['uid'], pw=db_info['pw']))
    return (cnxn, cnxn.cursor())


def validate_conn(cursor):
    cursor.execute("SELECT @@version;") 
    row = cursor.fetchone() 
    while row: 
        LINFO(row[0])
        # print(row[0])
        row = cursor.fetchone()


# def execute_and_print(cursor, sql, start_index=None, end_index=1):
#     table = re.search(REGEX_TABLE_PATTERN, unicode(sql))
#     if table:
#         table_name = table.group('table_name')
#         if table_exists(cursor, table.group('table_name')):
#             cursor.execute(sql)
#             cursor_print(cursor, start_index, end_index)
#         else:
#             print("Table specified in the query does not exist")
#     else:
#         logger.warning("Cannot seem to find the table specified..")


def execute_and_print(cursor, sql, table_name, start_index=None, end_index=1, record_id=False):
    if table_exists(cursor, table_name):
        cursor.execute(sql)
        cursor_print(cursor, start_index, end_index, record_id=record_id)
        if record_id:
            id_logger.debug(sql)
    else:
        msg = "Table specified in the query does not exist"
        LWARNING(msg)


def table_exists(cursor, table_name):
    if cursor.tables(table=table_name, tableType='TABLE').fetchone():
        # print("exists")
        return True
    else:
        # print("doesn't exist")
        return False


def cursor_print(cursor, start_index=None, end_index=1, record_id=False):
    row = cursor.fetchone()

    if not row:
        msg = "Empty Query"
        LWARNING(msg)
        return

    while row:
        if start_index is None and end_index is None:
            LINFO(row)
            if record_id:
                id_logger.debug(row)

        else:
            LINFO(row[start_index:end_index])
            if record_id:
                id_logger.debug(row[start_index:end_index])
        row = cursor.fetchone()
    # else: # This else will still execute even without going into the while loop
    #     logger.warning("End of query")
    

def in_database(cursor, sql):
    if cursor.execute(sql).rowcount != 0:
        return True
    else:
        return False


def handle_values(s):
    assert type(s) is unicode, "Type has to be unicode"
    if s == u"-":
        return ""
    else:
        return s


def handle_announcement(db_settings, key='announcement'):
    announcement_found = 0
    total_announcement_inserted = 0
    total_duplicate_announcement = 0
    ANNOUNCEMENT_PATH = join(root_dir, db_settings[key]['SAVE_PATH'])
    TARGET_TABLE = db_settings[key]['TARGET_TABLE']
    LDEBUG("Announcement Save Folder: {}".format(ANNOUNCEMENT_PATH))
    LDEBUG("Target DB Table: {}".format(TARGET_TABLE))

    if not exists(ANNOUNCEMENT_PATH):
        LERROR("{} does not exist! Please run 03_Company_Announcement_Scraping.py".format(ANNOUNCEMENT_PATH))
        return

    ANNOUNCEMENT_FILES = sorted([join(ANNOUNCEMENT_PATH, f) for f in os.listdir(ANNOUNCEMENT_PATH) if isfile(join(ANNOUNCEMENT_PATH, f))])
    for a_file in ANNOUNCEMENT_FILES:
        with open(a_file, 'r') as f:
            announcement = json.load(f)

        _cmp_name = get_cmp_name(a_file)
        if announcement.keys():     # Make sure announcement has at least 1 keys
            for n in announcement.keys():
                announcement_found += 1     # Add announcement counter

                _title = find_title(n)
                _content = announcement[n].replace("'", "''")
                _source = find_source(announcement[n])

                # Check if already have announcement with the title
                # Condition: Have same title and same news
                sql = u'''
                    SELECT * FROM {table_name} WHERE [Name]=N'{title}' AND [content]=N'{content}'
                '''.format(table_name=TARGET_TABLE, title=_title, content=_content)
                LDEBUG(sql)
                cursor.execute(sql)
                if not cursor.fetchone():   # If it NOT returns an object
                    # Insert the record
                    sql = u'''
                        INSERT INTO {table_name}
                            ([AnnouncementDir_ID], [Name], [content], [Post], [from], [CompanyName], [AdminID])
                        OUTPUT 
                            Inserted.*
                        VALUES
                            (888, N'{title}', N'{content}', N'必富網', N'{source}', N'{cmp_name}', 888)
                    '''.format(table_name=TARGET_TABLE, title=_title, content=_content, source=_source, cmp_name=_cmp_name)
                    LDEBUG(sql)
                    execute_and_print(cursor, sql, TARGET_TABLE, end_index=None)
                    total_announcement_inserted += 1    # Add announce inserted counter
                else:
                    total_duplicate_announcement += 1   # Add duplicate counter
                    LWARNING("Already have this announcement in the database")
                    LDEBUG(u"Title: {}".format(_title))

                _second = 0
                if _second:
                    LINFO("Sleep for {} seconds".format(_second))
                    time.sleep(_second)
        else:
            LINFO("\tCompany has no announcement")
        
    LINFO("Total announcement found: {}".format(announcement_found))
    LINFO("Total announcement inserted: {}".format(total_announcement_inserted))
    LINFO("Total duplicate announcement: {}".format(total_duplicate_announcement))
    LINFO("Commiting connection")
    cnxn.commit()
    LINFO("Finished commit!")
    LINFO("Finished inserting all announcements in: {:.3f}s".format(time.time() - start_time))


def handle_news(db_settings, key='news'):
    news_found = 0
    total_news_inserted = 0
    total_duplicate_news = 0
    NEWS_PATH = join(root_dir, db_settings[key]['SAVE_PATH'])
    TARGET_TABLE = db_settings[key]['TARGET_TABLE']
    LDEBUG("News Save Folder: {}".format(NEWS_PATH))
    LDEBUG("Target DB Table: {}".format(TARGET_TABLE))

    if not exists(NEWS_PATH):
        LERROR("{} does not exist! Please run 02_Company_News_Scraping.py".format(NEWS_PATH))
        return

    NEWS_FILES = sorted([join(NEWS_PATH, f) for f in os.listdir(NEWS_PATH) if isfile(join(NEWS_PATH, f))])
    for a_file in NEWS_FILES:
        with open(a_file, 'r') as f:
            news = json.load(f)

        _cmp_name = get_cmp_name(a_file)
        if news.keys():     # Make sure news has at least 1 keys
            for n in news.keys():
                news_found += 1     # Add news counter

                _title = find_title(n)
                _content = news[n].replace("'", "''")
                _source = find_source(news[n])

                # Check if already have news with the title
                # Condition: Have same title and same news
                sql = u'''
                    SELECT * FROM {table_name} WHERE [Name]=N'{title}' AND [content]=N'{content}'
                '''.format(table_name=TARGET_TABLE, title=_title, content=_content)
                LDEBUG(sql)
                cursor.execute(sql)
                if not cursor.fetchone():   # If it NOT returns an object
                    # Insert the record
                    sql = u'''
                        INSERT INTO {table_name}
                            ([NewsDir_ID], [Name], [content], [Post], [from], [CompanyName], [AdminID])
                        OUTPUT 
                            Inserted.*
                        VALUES
                            (888, N'{title}', N'{content}', N'必富網', N'{source}', N'{cmp_name}', 888)
                    '''.format(table_name=TARGET_TABLE, title=_title, content=_content, source=_source, cmp_name=_cmp_name).format()
                    LDEBUG(sql)
                    execute_and_print(cursor, sql, TARGET_TABLE, end_index=None)
                    total_news_inserted += 1    # Add announce inserted counter
                else:
                    total_duplicate_news += 1   # Add duplicate counter
                    LWARNING("Already have this news in the database")
                    LDEBUG(u"Title: {}".format(_title))

                _second = 0
                if _second:
                    LINFO("Sleep for {} seconds".format(_second))
                    time.sleep(_second)
        else:
            LINFO("\tCompany has no news")
        
    LINFO("Total news found: {}".format(news_found))
    LINFO("Total news inserted: {}".format(total_news_inserted))
    LINFO("Total duplicate news: {}".format(total_duplicate_news))
    LINFO("Commiting connection")
    cnxn.commit()
    LINFO("Finished commit!")
    LINFO("Finished inserting all news in: {:.3f}s".format(time.time() - start_time))


def handle_company_info(db_settings, key='company_info'):
    company_info_found = 0
    total_company_info_inserted = 0
    total_company_additional_info_inserted = 0
    total_duplicate_company_info = 0
    total_update_company_additional_info = 0
    COMPANY_INFO_PATH = join(root_dir, db_settings[key]['SAVE_PATH'])

    # ========== Company basic information ==========
    TARGET_TABLE = db_settings[key]['TARGET_TABLE']
    TARGET_TABLE_FOR_ADDITIONAL_INFO = db_settings[key]['TARGET_TABLE_FOR_ADDITIONAL_INFO']
    LDEBUG("Company Info Save Folder: {}".format(COMPANY_INFO_PATH))
    LDEBUG("Target DB Table: {}".format(TARGET_TABLE))

    if not exists(COMPANY_INFO_PATH):
        print("{} does not exist! Please run 01_Company_info_Scraper.py".format(COMPANY_INFO_PATH))
      
    COMPANY_INFO_FILES = sorted([join(COMPANY_INFO_PATH, f) for f in os.listdir(COMPANY_INFO_PATH) if isfile(join(COMPANY_INFO_PATH, f))])
    for a_file in COMPANY_INFO_FILES:
        with open(a_file, 'r') as f:
            company_info = json.load(f)

        if company_info.keys():     # Make sure company_info has at least 1 keys
            company_info_found += 1     # Add company_info counter

            # Used for preliminary check
            _stock_no       = handle_values(company_info[u'股票代號'])               #1 db.股票代號
            _short_name     = handle_values(company_info[u'Company Name'])          #2 db.公司簡稱

            # Used for insertion/update
            _long_name      = handle_values(company_info[u'未上市櫃股票公司名稱'])     #3 db.公司名稱
            _shares         = handle_values(company_info[u'普通股'])                 #4 db.普通股
            _sshares        = handle_values(company_info[u'特別股'])                 #5 db.特別股
            _found          = handle_values(company_info[u'成立日期'])               #6 db.公司成立日期
            _found2         = handle_values(company_info[u'公開發行日期'])            #7 db.公開發行日期
            _capital        = handle_values(company_info[u'實收資本額(元)'])          #8 db.實收資本額
            _tax_invoice    = handle_values(company_info[u'統一編號'])               #9 db.營利事業統一編號
            _cmp_tel        = handle_values(company_info[u'公司電話'])               #10 db.總機
            _cmp_fax        = handle_values(company_info[u'公司傳真'])               #11 db.傳真機號
            _cmp_addr       = handle_values(company_info[u'公司地址'])               #12 db.地址
            _cmp_web        = handle_values(company_info[u'公司網址'])               #13 db.網址
            _cmp_email      = handle_values(company_info[u'電子郵件信箱'])           #14 db.電子郵件信箱
            _cmp_field      = handle_values(company_info[u'營業項目'])               #15 db.主要經營業務
            _cmp_pres       = handle_values(company_info[u'董事長'])                 #16 db.董事長
            _cmp_ceo        = handle_values(company_info[u'總經理'])                 #17 db.總經理
            _cmp_repr       = handle_values(company_info[u'發言人'])                 #18 db.發言人
            _cmp_repr_tel   = handle_values(company_info[u'發言人電話'])              #19 db.發言人電話
            _cmp_repr_agent = handle_values(company_info[u'代理發言人'])              #20 db.代理發言人
            _stock_agent    = handle_values(company_info[u'股務代理'])               #21 db.股票過戶機構
            _stock_tel      = handle_values(company_info[u'股務電話'])               #22 db.電話
            _stock_addr     = handle_values(company_info[u'股務地址'])               #23 db.過戶地址
            _cmp_accountant = handle_values(company_info[u'簽證會計師'])             #24 db.簽證會計師事務所

            # Used for second insertion/update for additional information needed by robot
            _meeting_hist   = handle_values(company_info[u'歷年股東會'])
            _dividend       = handle_values(company_info[u'歷年除權除息'])
            _cash_dividend  = handle_values(company_info[u'歷年現金增(減)資'])


            ################################################################################################
            ###                              First Insertion Algorithm                                   ###
            ################################################################################################
            # Check if already have company_info with the title
            # Condition: Have same long_name and same short_name
            sql = u'''
                SELECT TOP(1) * FROM {table_name} 
                WHERE 
                    [公司名稱]=N'{long_name}' OR [公司簡稱]=N'{short_name}'

            '''.format(table_name=TARGET_TABLE, long_name=_long_name, short_name=_short_name)
            LDEBUG(sql)
            cursor.execute(sql)
            if not cursor.fetchone():   # If it NOT returns an object
                # Insert the record
                sql = u'''
                    INSERT INTO {table_name}
                        ([股票代號], [公司簡稱], [公司名稱], [普通股], [特別股], [公司成立日期], [公開發行日期],
                        [實收資本額], [營利事業統一編號], [總機], [傳真機號], [地址], [網址], [電子郵件信箱], [主要經營業務], [董事長],
                        [總經理], [發言人], [發言人電話] ,[代理發言人] ,[股票過戶機構] ,[電話] ,[過戶地址] ,[簽證會計師事務所])
                    OUTPUT 
                        INSERTED.CompanyID, INSERTED.[公司名稱], INSERTED.[公司簡稱], INSERTED.[股票代號]
                    VALUES
                        (N'{stock_no}', '{short_name}', N'{long_name}', N'{shares}', N'{sshares}', N'{found}', N'{found2}', N'{capital}' , N'{tax_invoice}',
                        N'{cmp_tel}', N'{cmp_fax}', N'{cmp_addr}', N'{cmp_web}', N'{cmp_email}', N'{cmp_field}', N'{cmp_pres}', N'{cmp_ceo}',
                        N'{cmp_repr}', N'{cmp_repr_tel}', N'{cmp_repr_agent}', N'{stock_agent}', N'{stock_tel}', N'{stock_addr}', N'{cmp_accountant}')
                '''.format(table_name=TARGET_TABLE, 
                    stock_no=_stock_no, short_name=_short_name, long_name=_long_name, shares=_shares, sshares=_sshares,
                    found=_found, found2=_found2, capital=_capital, tax_invoice=_tax_invoice, cmp_tel=_cmp_tel, cmp_fax=_cmp_fax,
                    cmp_addr=_cmp_addr, cmp_web=_cmp_web, cmp_email=_cmp_email, cmp_field=_cmp_field, cmp_pres=_cmp_pres, cmp_ceo=_cmp_ceo,
                    cmp_repr=_cmp_repr, cmp_repr_tel=_cmp_repr_tel, cmp_repr_agent=_cmp_repr_agent, stock_agent=_stock_agent,
                    stock_tel=_stock_tel, stock_addr=_stock_addr, cmp_accountant=_cmp_accountant)
                LDEBUG(sql)
                execute_and_print(cursor, sql, TARGET_TABLE, end_index=None, record_id=True)    # Logger type 'c'
                total_company_info_inserted += 1    # Add announce inserted counter
            else:
                total_duplicate_company_info += 1   # Add duplicate counter
                LWARNING("Already have this company_info in the database")
                LDEBUG(u"[股票代號]=N'{stock_no}' AND [公司簡稱]=N'{short_name}".format(stock_no=_stock_no, short_name=_short_name))

            _second = 0
            if _second:
                LINFO("Sleep for {} seconds".format(_second))
                time.sleep(_second)

            ################################################################################################
            ###                              Second Insertion Algorithm                                   ###
            ################################################################################################
            # Check if already have company_info with the title
            # Condition: Have same long_name and same short_name
            _query_key = 'CompanyID'
            sql = u'''
                SELECT TOP(1) {query_key} FROM {table_name} 
                WHERE 
                    [公司名稱]=N'{long_name}' OR [公司簡稱]=N'{short_name}'
            '''.format(query_key=_query_key, table_name=TARGET_TABLE, long_name=_long_name, short_name=_short_name)
            LDEBUG(sql)
            cursor.execute(sql)
            _company_id = cursor.fetchone()
            if _company_id:   # If it returns an object
                # Insert the record
                _company_id = _company_id[0]

                # Check if primary key is already in it.
                # if not: INSERT
                # is yes: UPDATE
                sql = u'''
                    SELECT TOP(1) {query_key} FROM {table_name} WHERE CompanyID={company_id}
                '''.format(query_key=_query_key, table_name = TARGET_TABLE_FOR_ADDITIONAL_INFO,
                           company_id=_company_id)
                cursor.execute(sql)       

                if not cursor.fetchone():
                    sql = u'''
                        INSERT INTO {table_name}
                            (CompanyID, [歷年股東會], [歷年除權除息], [歷年現金增(減)資])
                        OUTPUT 
                            INSERTED.CompanyID, INSERTED.[歷年股東會], INSERTED.[歷年除權除息], INSERTED.[歷年現金增(減)資]
                        VALUES
                            ('{company_id}', N'{meeting_history}', N'{dividend}', N'{cash_dividend}')
                    '''.format(table_name=TARGET_TABLE_FOR_ADDITIONAL_INFO, 
                        company_id=_company_id, 
                        meeting_history=_meeting_hist, dividend=_dividend, cash_dividend=_cash_dividend)
                    LDEBUG(sql)
                    execute_and_print(cursor, sql, TARGET_TABLE_FOR_ADDITIONAL_INFO, end_index=None, record_id=True)    # Logger type 'c'
                    total_company_additional_info_inserted += 1    # Add announce inserted counter
                else:
                    LWARNING(u"'{}' Company ID already exist in '{}' table. Updating instead".format(_company_id, TARGET_TABLE_FOR_ADDITIONAL_INFO))
                    total_update_company_additional_info += 1
                    sql = u'''
                        UPDATE {table_name}
                        SET 
                            [歷年股東會]={meeting_history}, 
                            [歷年除權除息]={dividend}, 
                            [歷年現金增(減)資]={cash_dividend}
                        OUTPUT 
                            INSERTED.CompanyID, DELETED.[歷年股東會], INSERTED.[歷年股東會],
                                                DELETED.[歷年除權除息], INSERTED.[歷年除權除息],
                                                DELETED.[歷年現金增(減)資], INSERTED.[歷年現金增(減)資]
                        WHERE
                            CompanyID={company_id}
                    '''.format(table_name=TARGET_TABLE_FOR_ADDITIONAL_INFO, 
                        company_id=_company_id, 
                        meeting_history=_meeting_hist, dividend=_dividend, cash_dividend=_cash_dividend)

            else:
                LWARNING(u"Already have this additional info in the '{}' database".format(TARGET_TABLE_FOR_ADDITIONAL_INFO))
                LDEBUG(u"[股票代號]=N'{stock_no}' AND [公司簡稱]=N'{short_name}".format(stock_no=_stock_no, short_name=_short_name))

            _second = 0
            if _second:
                LINFO("Sleep for {} seconds".format(_second))
                time.sleep(_second)
        else:
            LINFO("\tCompany has no company_info")

    LINFO("Total company_info count           : {}".format(company_info_found))
    LINFO("Total company_info inserted        : {}".format(total_company_info_inserted))
    LINFO("Total duplicate company_info found : {}".format(total_duplicate_company_info))
    LINFO("Total company additional info inserted : {}".format(total_company_additional_info_inserted))
    LINFO("Total company additional info updated  : {}".format(total_update_company_additional_info))
    LINFO("Commiting connection")
    cnxn.commit()
    LINFO("Finished commit!")
    LINFO("Finished inserting all company_info in: {:.3f}s".format(time.time() - start_time))

if __name__=="__main__":
    try:
        start_time = time.time()
        logger.info("Checking resource availability")
        if exists(DB_INFO_JSON_FILE):
            with open(DB_INFO_JSON_FILE, 'r') as f:
                DB_INFO = json.load(f)
                logger.debug("Captured DB_INFO: {}".format(DB_INFO))

        for key in KEY_LIST:
            logger.debug("{:<10}: {}".format(key, DB_INFO[key]))

        if DB_INFO:
            # Validating the connection to database and its cursor
            cnxn, cursor = setup_db_conn(DB_INFO)
            validate_conn(cursor)

            # Check if required file exists
            DB_SETTINGS_FILE = join(SCRIPT_ROOT_FOLDER, 'resource/db_settings.json')
            if not exists(DB_SETTINGS_FILE):
                print("'{}' file does not exists".format(DB_SETTINGS_FILE))
                quit()

            # Loading Database Settings
            with open(DB_SETTINGS_FILE, 'r') as f:
                db_settings = json.load(f)

            # ==================== Main process start ====================
            LINFO("Handling Announcement content in: ")
            for _n in range(3, 0, -1):
                LINFO("{} ".format(_n))
                time.sleep(1)
            handle_announcement(db_settings)

            LINFO("Handling News content in: ")
            for _n in range(3, 0, -1):
                LINFO("{} ".format(_n))
                time.sleep(1)
            LINFO("Now handling News content")
            handle_news(db_settings)
            
            LINFO("Handling Company Info in: ")
            for _n in range(3, 0, -1):
                LINFO("{} ".format(_n))
                time.sleep(1)        
            handle_company_info(db_settings)
            # ==================== End of main process ====================

            LINFO("Finished handling database operations")

        else:
            logger.critical("DB_INFO is empty")
    except Exception as e:
        LERROR(u"'{}' exception was raised!".format(type(e)))
        LERROR(u"Message: {}".format(' - '.join(e.args)))
        LWARNING("Script finished with exception(s)!")


