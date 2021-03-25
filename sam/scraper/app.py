import json
import datetime
import re

import requests
from bs4 import BeautifulSoup
import boto3

from shared import S3_scraper_index

def extract_doh_file_list(text,extension,number):
    html = BeautifulSoup(text,features="html.parser")
    files = []
    regex = re.compile('-(\d{6}).*\.%s$' %extension)
    for nigovfile in html.find_all("div", {"class": "nigovfile"}):
        for a in nigovfile.find_all('a', href=True):
            m = regex.search(a['href'])
            if m:
                resp = requests.head(a['href'])
                resp.raise_for_status()
                filedate = datetime.datetime.strptime(m.group(1),'%d%m%y')
                modified = datetime.datetime.strptime(resp.headers['Last-Modified'],'%a, %d %b %Y %H:%M:%S %Z') # e.g Mon, 08 Mar 2021 06:12:35 GMT
                files.append({'url': a['href'],'modified': modified.isoformat(), 'length': int(resp.headers['Content-Length']), 'filedate': filedate.date().isoformat()})
                if len(files)>=number:
                    break
        if len(files)>=number:
            break
    return files

def check_for_dd_files(s3client, bucket, previous, files_to_check):
    # Attempt to pull this month's list of daily data publications
    today = datetime.datetime.today()
    url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(today.strftime("%B").lower(),today.year)
    resp = requests.get(url)
    if resp.status_code == 404:
        print('Failed to get latest month from %s, rolling back to previous month' %url)
        excels = []
    else:
        excels = extract_doh_file_list(resp.text, 'xlsx', files_to_check)
    # Pull last month's as well, if we need to, to ensure we always have N days of data checked
    if len(excels) <= files_to_check:
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(last_month.strftime("%B").lower(),last_month.year)
        resp = requests.get(url)
        resp.raise_for_status()
        excels.extend(extract_doh_file_list(resp.text, 'xlsx', files_to_check-len(excels)))
    excels = sorted(excels, key=lambda k: k['filedate'], reverse=True)
    # Work through the list checking against the previous data list
    changes = []
    for e in excels:
        match = next((p for p in previous if p["filedate"] == e["filedate"]), None)
        if (match is None) or ((match['modified'] != e['modified']) or (match['length'] != e['length'])):
            # Store the new or updated file in S3
            resp = requests.get(e['url'])
            resp.raise_for_status()
            keyname = "DoH-DD/%s/%s-%s.xlsx" %(e['filedate'],e['modified'].replace(':','_'),e['length'])
            s3client.put_object(Bucket=bucket, Key=keyname, Body=resp.content)
            e['keyname'] = keyname
            changes.append(e)
    # Create the new list
    for p in previous:
        if (p not in excels) and (p['filedate'] not in [e['filedate'] for e in excels]):
            excels.insert(0,p)
    return excels, changes

def check_for_r_files(s3client, bucket, previous):
    # Attempt to pull the list of R number publications
    url = 'https://www.health-ni.gov.uk/r-number'
    resp = requests.get(url)
    resp.raise_for_status()
    pdfs = extract_doh_file_list(resp.text, 'pdf', 1)
    change = False
    if len(pdfs) != 0:
        if len(previous) > 0:
            match = next((p for p in previous if p["filedate"] == pdfs[0]["filedate"]), None)
            if (match is None) or ((match['modified'] != pdfs[0]['modified']) or (match['length'] != pdfs[0]['length'])):
                change = True
        else:
            change = True
    if change is True:
        # Store the new or updated file in S3
        resp = requests.get(pdfs[0]['url'])
        resp.raise_for_status()
        keyname = "DoH-R/%s/%s-%s.pdf" %(pdfs[0]['filedate'],pdfs[0]['modified'].replace(':','_'),pdfs[0]['length'])
        s3client.put_object(Bucket=bucket, Key=keyname, Body=resp.content)
        pdfs[0]['keyname'] = keyname
    return pdfs, change

def check_doh_dd(secret, s3, notweet):
    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], secret['doh-dd-index'])
    previous = status.get_dict()

    # Check the DoH site for file changes
    current, changes = check_for_dd_files(s3, secret['bucketname'], previous, int(secret['doh-dd-files-to-check']))

    # Write any changes back to S3
    if len(changes) > 0:
        status.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), secret['doh-dd-index'], len(changes))

        # Find the most recent date and make sure that its file has changed, if it has then tweet
        current = sorted(current, key=lambda k: k['filedate'], reverse=True)
        if not notweet and (current[0]['filedate'] in [c['filedate'] for c in changes]):
            print('Launching tweeter')
            lambda_client = boto3.client('lambda')
            lambda_client.invoke(
                FunctionName='ni-covid-tweets-NICOVIDTweeter-7GUXQLKTJDEK',
                InvocationType='Event',
                Payload=json.dumps(current)
            )
            message += ', and launched tweet lambda'
    else:
        message = 'Did nothing'

    return message

def check_doh_r(secret, s3, notweet):
    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], secret['doh-r-index'])
    previous = status.get_dict()

    # Check the DoH site for file changes
    current, change = check_for_r_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if change is True:
        status.put_dict([current])
        message = 'Wrote updated item to %s' %secret['doh-r-index']

        print('Launching tweeter with event %s' %current)
        if not notweet:
            lambda_client = boto3.client('lambda')
            lambda_client.invoke(
                FunctionName='ni-covid-tweets-NICOVIDRTweeter-1D5BES9FN6F5B',
                InvocationType='Event',
                Payload=json.dumps(current)
            )
            message += ', and launched tweet lambda'
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

    # Run the scraper
    messages = []
    messages.append(check_doh_dd(secret, s3, event.get('tests-notweet', False)))
    messages.append(check_doh_r(secret, s3, event.get('r-notweet', False)))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "messages": messages,
        }),
    }
