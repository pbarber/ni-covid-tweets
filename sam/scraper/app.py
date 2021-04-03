import json
import datetime
import re
import os
import logging

import requests
from bs4 import BeautifulSoup
import boto3

from shared import S3_scraper_index, launch_lambda_async, get_url

def extract_doh_file_list(text,number,regex,datesub=[],datefmt='%d%m%y'):
    html = BeautifulSoup(text,features="html.parser")
    files = []
    regex = re.compile(regex, flags=re.IGNORECASE)
    for nigovfile in html.find_all("div", {"class": "nigovfile"}):
        for a in nigovfile.find_all('a', href=True):
            m = regex.search(a['href'])
            if m:
                if len(datesub) == 2:
                    datestr = re.sub(datesub[0],datesub[1],m.group(1))
                else:
                    datestr = m.group(1)
                datestr = datestr.replace('%20',' ')
                resp = requests.head(a['href'])
                resp.raise_for_status()
                filedate = datetime.datetime.strptime(datestr,datefmt)
                if 'Last-Modified' in resp.headers:
                    modified = datetime.datetime.strptime(resp.headers['Last-Modified'],'%a, %d %b %Y %H:%M:%S %Z') # e.g Mon, 08 Mar 2021 06:12:35 GMT
                    files.append({'url': a['href'],'modified': modified.isoformat(),'length': int(resp.headers['Content-Length']), 'filedate': filedate.date().isoformat()})
                else:
                    files.append({'url': a['href'],'filedate': filedate.date().isoformat()})
                if len(files)>=number:
                    break
        if len(files)>=number:
            break
    return files

def check_file_list_against_previous(current, previous):
    changes = []
    for e in reversed(current):
        match = next((p for p in previous if p["filedate"] == e["filedate"]), None)
        if match is None:
            changes = [c+1 for c in changes]
            changes.append({'index': 0, 'change': 'added'})
            previous.insert(0, e)
        elif 'modified' in match and ((match['modified'] != e['modified']) or (match['length'] != e['length'])):
            changes.append({'index': previous.index(match), 'change': 'modified'})
            previous[changes[-1]['index']] = e
    return previous, changes

def upload_changes_to_s3(s3client, bucket, dirname, index, changes, fileext):
    session = requests.Session()
    for change in changes:
        e = index[change['index']]
        keyname = "%s/%s/%s-%s.%s" %(dirname,e['filedate'],e.get('modified', '1').replace(':','_'),e.get('length','1'),fileext)
        s3client.put_object(Bucket=bucket, Key=keyname, Body=get_url(session, e['url'],'content'))
        index[change['index']]['keyname'] = keyname
    return index

def check_for_dd_files(s3client, bucket, previous, files_to_check):
    session = requests.Session()
    # Attempt to pull this month's list of daily data publications
    today = datetime.datetime.today()
    url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(today.strftime("%B").lower(),today.year)
    excels = []
    try:
        excels.extend(extract_doh_file_list(get_url(session, url, 'text'), files_to_check, r'-(\d{6}).*\.xlsx$'))
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 404:
            print('Failed to get latest month from %s, rolling back to previous month' %url)
        else:
            raise err
    # Pull last month's as well, if we need to, to ensure we always have N days of data checked
    if len(excels) <= files_to_check:
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(last_month.strftime("%B").lower(),last_month.year)
        excels.extend(extract_doh_file_list(get_url(session, url, 'text'), files_to_check-len(excels), r'-(\d{6}).*\.xlsx$'))
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(excels, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'DoH-DD', index, changes, 'xlsx')
    return index, changes

def check_for_r_files(s3client, bucket, previous):
    session = requests.Session()
    # Attempt to pull the list of R number publications
    url = 'https://www.health-ni.gov.uk/R-Number'
    pdfs = extract_doh_file_list(get_url(session, url,'text'), 1, r'-(\d{6}).*\.pdf$')
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(pdfs, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'DoH-R', index, changes, 'pdf')
    return index, changes

def check_doh(secret, s3, notweet, mode):
    if mode=='dd':
        indexkey = secret['doh-dd-index']
        lambdaname = os.getenv('TWEETER_LAMBDA')
    else:
        indexkey = secret['doh-r-index']
        lambdaname = os.getenv('R_TWEETER_LAMBDA')

    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], indexkey)
    previous = status.get_dict()
    previous = sorted(previous, key=lambda k: k['filedate'], reverse=True)

    # Check the DoH site for file changes
    if mode=='dd':
        current, changes = check_for_dd_files(s3, secret['bucketname'], previous, int(secret['doh-dd-files-to-check']))
    else:
        current, changes = check_for_r_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if len(changes) > 0:
        status.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))

        # If the most recent file has changed then tweet
        totweet = [c['index'] for c in changes if (c['change'] == 'added') or (c['index'] == 0)]
        if not notweet and (0 in totweet):
            print('Launching %s tweeter' %mode)
            launch_lambda_async(lambdaname,[current[a] for a in totweet])
            message += ', and launched %s tweet lambda' %mode
    else:
        message = 'Did nothing'

    return message

def check_hscni(bucketname, indexkey, s3):
    session = requests.Session()

    status = S3_scraper_index(s3, bucketname, indexkey)
    previous = status.get_dict()
    if len(previous) > 0:
        previous = sorted(previous, key=lambda k: k['Last Updated'], reverse=True)

    # Get the COVID-19 site homepage
    url = 'https://covid-19.hscni.net/'
    html = BeautifulSoup(get_url(session, url,'text'),features="html.parser")

    # Extract the vaccine data
    div = html.find("div", {"class": "bg-vaccine-soft"})
    heads = []
    for head in div.find_all("h4"):
        heads.append(head.text)
    items = []
    for item in div.find_all("div", {"class": "text-5xl"}):
        items.append(item.text)
    data = {}
    if len(heads) == len(items) and len(items) > 0:
        for i in range(len(heads)):
            data[heads[i]] = int(items[i].replace(',',''))
    rawdate = div.find("div", {"class": "mt-8"}).text.partition(':')[2].strip()

    # Format silly date string properly, if not this year assume last year
    noyeardate = datetime.datetime.strptime(rawdate, '%d %B, %I:%M %p').date() # 25 March, 12:07 pm
    today = datetime.datetime.today()
    if noyeardate.month > today.month:
        reportdate = noyeardate.replace(year=today.year-1) - datetime.timedelta(days=1)
    else:
        reportdate = noyeardate.replace(year=today.year) - datetime.timedelta(days=1)

    # Account for one day delay in reporting
    data['Last Updated'] = reportdate.isoformat()
    data['First Doses pc'] = round((100 * data['Total First Doses']) / 1452962, 1)
    data['Second Doses pc'] = round((100 * data['Total Second Doses']) / 1452962, 1)
    data['Source'] = 'HSCNI'

    # Do change detection
    change = False
    if len(previous) > 0:
        if (data['Last Updated'] != previous[0]['Last Updated']):
            data['First Doses Registered'] = data['Total First Doses'] - previous[0]['Total First Doses']
            data['Second Doses Registered'] = data['Total Second Doses'] - previous[0]['Total Second Doses']
            previous.insert(0, data)
            change = True
        elif (data['Total Doses'] != previous[0]['Total Doses']):
            data['First Doses Registered'] = data['Total First Doses'] - previous[1]['Total First Doses']
            data['Second Doses Registered'] = data['Total Second Doses'] - previous[1]['Total Second Doses']
            previous[0] = data
            change = True
    else:
        data['First Doses Registered'] = data['Total First Doses']
        data['Second Doses Registered'] = data['Total Second Doses']
        previous.insert(0, data)
        change = True

    if change:
        status.put_dict(previous)

    return previous[0], change

def check_phe(bucketname, indexkey, s3):
    session = requests.Session()

    status = S3_scraper_index(s3, bucketname, indexkey)
    previous = status.get_dict()
    if len(previous) > 0:
        previous = sorted(previous, key=lambda k: k['Last Updated'], reverse=True)

    # Get the PHE data from the API
    url = 'https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&areaCode=N92000002&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&metric=cumVaccinationFirstDoseUptakeByPublishDatePercentage&metric=cumVaccinationSecondDoseUptakeByPublishDatePercentage&format=json'
    ordered = sorted(get_url(session, url,'json').get('body',[]), key=lambda k: k['date'], reverse=True)

    change = False
    if (len(ordered) > 1):
        data = {
            'Last Updated': ordered[0]['date'],
            'Total Doses': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'] + ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'],
            'Total First Doses': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'],
            'Total Second Doses': ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'],
            'First Doses Registered': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'] - ordered[1]['cumPeopleVaccinatedFirstDoseByPublishDate'],
            'Second Doses Registered': ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'] - ordered[1]['cumPeopleVaccinatedSecondDoseByPublishDate'],
            'First Doses pc': ordered[0]['cumVaccinationFirstDoseUptakeByPublishDatePercentage'],
            'Second Doses pc': ordered[0]['cumVaccinationSecondDoseUptakeByPublishDatePercentage'],
            'Source': 'PHE'
        }
        if len(previous) > 0:
            if (data['Last Updated'] != previous[0]['Last Updated']):
                previous.insert(0, data)
                change = True
            elif (data['Total Doses'] != previous[0]['Total Doses']):
                previous[0] = data
                change = True
        else:
            previous.insert(0, data)
            change = True

    if change:
        status.put_dict(previous)

    return previous[0], change


def check_vaccine(bucketname, pheindexkey, hscniindexkey, s3, notweet):

    try:
        phe, phechange = check_phe(bucketname, pheindexkey, s3)
    except:
        phechange = False
        logging.exception('Caught exception accessing PHE vaccine data')
    try:
        hsc, hscchange = check_hscni(bucketname, hscniindexkey, s3)
    except:
        hscchange = False
        logging.exception('Caught exception accessing HSCNI vaccine data')

    # If there has been a change, then tweet
    message = 'Did nothing'
    change = False
    if hscchange and (hsc['Last Updated'] > phe['Last Updated']):
        chosen = hsc
        change = True
    elif phechange and (phe['Last Updated'] > hsc['Last Updated']):
        chosen = phe
        change = True
    elif hscchange and phechange and (phe['Last Updated'] == hsc['Last Updated']):
        chosen = phe
        change = True

    if change is True:
        message = 'New last updated date of %s from %s' %(chosen['Last Updated'],chosen['Source'])
        print(chosen)
        if not notweet:
            print('Launching vaccine tweeter')
            launch_lambda_async(os.getenv('VACCINE_TWEETER_LAMBDA'),chosen)
            message += ', and launched vaccine tweet lambda'

    return(message)

def check_for_nisra_files(s3client, bucket, previous):
    session = requests.Session()

    # Attempt to pull the link to this week's publications
    url = 'https://www.nisra.gov.uk/statistics/death-statistics/weekly-death-registrations-northern-ireland'
    html = BeautifulSoup(get_url(session,url,'text'),features="html.parser")
    durl = None
    for a in html.find_all('a', href=True):
        if a.text.strip() == 'Latest Weekly Deaths Bulletin':
            durl = a['href']
            break
    if durl is None:
        raise Exception('Failed to find link to deaths records at %s' %url)
    if durl.startswith('/'):
        durl = 'https://www.nisra.gov.uk' + durl
    # e.g. https://www.nisra.gov.uk/system/files/statistics/Weekly_Deaths%20-%20w%20e%2019th%20March%202021.XLSX
    excels = extract_doh_file_list(get_url(session,durl,'text'), 1, r'w%20e%20(\d+[a-z]+%20[A-Za-z]+%20\d+).*\.xlsx$', [r'(\d)(st|nd|rd|th)', r'\1'], r'%d %B %Y')
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(excels, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'NISRA-deaths', index, changes, 'xslx')
    return index, changes

def check_nisra(secret, s3, notweet):
    indexkey = secret['nisra-deaths-index']

    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], indexkey)
    previous = status.get_dict()
    previous = sorted(previous, key=lambda k: k['filedate'], reverse=True)

    # Check the NISRA site for file changes
    current, changes = check_for_nisra_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if len(changes) > 0:
        status.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))

        # If the most recent file has changed then tweet
        totweet = [c['index'] for c in changes if (c['change'] == 'added') or (c['index'] == 0)]
        if not notweet and (0 in totweet):
            print('Launching NISRA tweeter')
            launch_lambda_async(os.getenv('NISRA_TWEETER_LAMBDA'),[current[a] for a in totweet])
            message += ', and launched NISRA tweet lambda'
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
    try:
        messages.append(check_doh(secret, s3, event.get('tests-notweet', False), 'dd'))
    except:
        logging.exception('Caught exception accessing DOH daily data')
    try:
        messages.append(check_doh(secret, s3, event.get('r-notweet', False), 'r'))
    except:
        logging.exception('Caught exception accessing DOH R number')
    try:
        messages.append(check_vaccine(secret['bucketname'], secret['phe-vacc-index'], secret['hscni-vacc-index'], s3, event.get('vacc-notweet', False)))
    except:
        logging.exception('Caught exception accessing vaccine data')
    try:
        messages.append(check_nisra(secret, s3, event.get('nisra-notweet', False)))
    except:
        logging.exception('Caught exception accessing NISRA weekly data')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "messages": messages,
        }),
    }
