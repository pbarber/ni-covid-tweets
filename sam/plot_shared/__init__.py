import logging
import os

from selenium import webdriver

def get_chrome_driver():
    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-gpu")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--disable-infobars")
    options.add_argument("--enable-logging")
    options.add_argument("--log-level=0")
    options.add_argument("--v=99")
    options.add_argument("--single-process")
    options.add_argument("--user-data-dir=/tmp/user-data/")
    options.add_argument("--data-path=/tmp/data/")
    options.add_argument("--homedir=/tmp/homedir/")
    options.add_argument("--disk-cache-dir=/tmp/disk-cache/")
    options.add_argument("--disable-async-dns")
    driver = None
    for attempt in range(3):
        try:
            driver = webdriver.Chrome(service_log_path='/tmp/chromedriver.log', options=options)
        except:
            logging.exception('Failed to setup chromium')
            with open('/tmp/chromedriver.log') as log:
                logging.warning(log.read())
            logging.error([f for f in os.listdir('/tmp/')])
        else:
            break
    else:
        logging.error('Failed to set up webdriver after %d attempts' %(attempt+1))
    return driver
