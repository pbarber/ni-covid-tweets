import json
import datetime
import re

import requests
from bs4 import BeautifulSoup
import boto3

keyname = 'scraper_data.json'
bucketname = 'ni-covid-tweets'
files_to_check = 5

def extract_excel_list(text):
    html = BeautifulSoup(text,features="html.parser")
    excels = []
    regex = re.compile('-(\d{6}).*\.xlsx$')
    for nigovfile in html.find_all("div", {"class": "nigovfile"}):
        for a in nigovfile.find_all('a', href=True):
            m = regex.search(a['href'])
            if m:
                resp = requests.head(a['href'])
                resp.raise_for_status()
                filedate = datetime.datetime.strptime(m.group(1),'%d%m%y')
                modified = datetime.datetime.strptime(resp.headers['Last-Modified'],'%a, %d %b %Y %H:%M:%S %Z') # e.g Mon, 08 Mar 2021 06:12:35 GMT
                excels.append({'url': a['href'],'modified': modified.isoformat(), 'length': int(resp.headers['Content-Length']), 'filedate': filedate.date().isoformat()})
    return excels

def check_for_files(s3client, bucket, previous):
    # Attempt to pull this month's list of daily data publications
    today = datetime.datetime.today()
    url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(today.strftime("%B").lower(),today.year)
    resp = requests.get(url)
    if resp.status_code == 404:
        print('Failed to get latest month from %s, rolling back to previous month' %url)
        excels = []
    else:
        excels = extract_excel_list(resp.text)
    # Pull last month's as well, if we need to, to ensure we always have N days of data checked
    if len(excels) <= files_to_check:
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(last_month.strftime("%B").lower(),last_month.year)
        resp = requests.get(url)
        resp.raise_for_status()
        excels.extend(extract_excel_list(resp.text))
    excels = sorted(excels, key=lambda k: k['filedate'], reverse=True)
    excels = excels[:files_to_check]
    # Work through the list checking against the previous data list
    changes = 0
    for e in excels:
        match = next((p for p in previous if p["filedate"] == e["filedate"]), None)
        if (match is None) or ((match['modified'] != e['modified']) or (match['length'] != e['length'])):
            # Store the new or updated file in S3
            resp = requests.get(e['url'])
            resp.raise_for_status()
            keyname = "DoH-DD/%s/%s-%s.xlsx" %(e['filedate'],e['modified'].replace(':','_'),e['length'])
            s3client.put_object(Bucket=bucket, Key=keyname, Body=resp.content)
            e['keyname'] = keyname
            changes += 1
    # Create the new list
    for p in previous:
        if (p not in excels) and (p['filedate'] not in [e['filedate'] for e in excels]):
            excels.insert(0,p)
    return excels, changes

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    s3 = boto3.client('s3')

    # Get the previous data file list from S3
    try:
        dataobj = s3.get_object(Bucket=bucketname,Key=keyname)
        previous = json.load(dataobj['Body'])
    except s3.exceptions.NoSuchKey:
        print("The object %s does not exist in bucket %s." %(keyname, bucketname))
        previous = []

    # Check the DoH site for file changes
    current, changes = check_for_files(s3, bucketname, previous)

    # Write any changes back to S3
    if changes > 0:
        s3.put_object(Bucket=bucketname, Key=keyname, Body=json.dumps(current))
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), keyname, changes)
    else:
        message = 'Did nothing'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
