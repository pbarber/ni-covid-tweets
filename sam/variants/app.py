import json
import datetime
import os
import logging

import requests
from bs4 import BeautifulSoup
import boto3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from shared import S3_scraper_index, launch_lambda_async, get_url, get_and_sort_index

def check_for_cog_files(s3, bucketname, indexkey):
    today = datetime.datetime.today().date()
    session = requests.Session()

    previous, index = get_and_sort_index(bucketname, indexkey, s3, 'filedate')

    if len(previous) == 0:
        last = datetime.datetime.strptime('2021-05-11', '%Y-%m-%d').date()
    else:
        last = datetime.datetime.strptime(previous[0]['filedate'], '%Y-%m-%d').date()

    if last > today:
        return None

    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("window-size=1400,1500")
    options.add_argument("--disable-gpu")
    options.add_argument("start-maximized")
    options.add_argument("enable-automation")
    options.add_argument("--disable-infobars")
    driver = webdriver.Chrome(options=options)
    driver.get('http://sars2.cvr.gla.ac.uk/cog-uk/')
    WebDriverWait(driver, 20).until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, "#shiny-tab-vui_voc"), "Northern Ireland"))
    #WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#shiny-tab-vui_voc")))
    html = BeautifulSoup(driver.page_source,features="html.parser")
    button = html.find("a", {"id": "downloadTable3"})
    url = 'http://sars2.cvr.gla.ac.uk/cog-uk/' + button['href']
    resp = session.get(url)
    print(resp.content)
#    s3.put_object(Bucket=bucketname, Key=keyname, Body=get_url(session, url, 'content'))
    raise Exception('Here: %s' %driver)
    found = []
    while today >= last:
        url = "https://cog-uk-microreact.s3.climb.ac.uk/{today}/cog_metadata_microreact_geocodes_only.csv".format(today=today.isoformat())
        resp = session.head(url)
        if (resp.headers['Content-Type'] == 'binary/octet-stream'):
            modified = datetime.datetime.strptime(resp.headers['Last-Modified'],'%a, %d %b %Y %H:%M:%S %Z') # e.g Mon, 08 Mar 2021 06:12:35 GMT
            if (modified.isoformat() != previous[0]['modified']) and (int(resp.headers['Content-Length']) != int(previous[0]['length'])):
                found.append({
                    'url': url,
                    'modified': modified.isoformat(),
                    'length': int(resp.headers['Content-Length']),
                    'filedate': today.isoformat(),
                })
        today -= datetime.timedelta(days=1)

    if len(found) == 0:
        return None

    found.extend(previous)
    index.put_dict(found)

    return found[0]

def check_cog(secret, s3, notweet):
    # Check the COG bucket for file changes
    latest = check_for_cog_files(s3, secret['bucketname'], secret['cog-variants-index'])

    # Launch tweeter for any changes
    if latest is not None:
        message = 'Found file'
        print(latest)
        print('Launching COG variants tweeter')
        latest['testtweet'] = True
        launch_lambda_async(os.getenv('VARIANTS_TWEETER_LAMBDA'),latest)
        message += ', and launched variants tweet lambda'
    else:
        message = 'Did nothing'

    return message

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Set up S3 client
    s3 = boto3.client('s3')

    messages = []
    try:
        messages.append(check_cog(secret, s3, event.get('cog-notweet', False)))
    except:
        logging.exception('Caught exception accessing COG variants data')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "messages": messages,
        }),
    }
