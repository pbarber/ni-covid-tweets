import json
import datetime
import re
import os
import logging
from copy import deepcopy

import requests
from bs4 import BeautifulSoup
import boto3
from user_agent import generate_user_agent

from shared import S3_scraper_index, launch_lambda_async, get_url, get_and_sort_index

def extract_doh_file_list(text,number,regex,datesub=[],datefmt='%d%m%y',element="div",htmlclass="nigovfile",matchgroup=1):
    html = BeautifulSoup(text,features="html.parser")
    files = []
    regex = re.compile(regex, flags=re.IGNORECASE)
    for nigovfile in html.find_all(element, {"class": htmlclass}):
        for a in nigovfile.find_all('a', href=True):
            m = regex.search(a['href'])
            if m:
                if len(datesub) == 2:
                    datestr = re.sub(datesub[0],datesub[1],m.group(matchgroup))
                else:
                    datestr = m.group(matchgroup)
                datestr = datestr.replace('%20','-')
                resp = requests.head(a['href'])
                resp.raise_for_status()
                filedate = datetime.datetime.strptime(datestr,datefmt)
                if 'Last-Modified' in resp.headers:
                    modified = datetime.datetime.strptime(resp.headers['Last-Modified'],'%a, %d %b %Y %H:%M:%S %Z') # e.g Mon, 08 Mar 2021 06:12:35 GMT
                    if 'Content-Length' in resp.headers:
                        files.append({'url': a['href'],'modified': modified.isoformat(),'length': int(resp.headers['Content-Length']), 'filedate': filedate.date().isoformat()})
                    else:
                        files.append({'url': a['href'],'modified': modified.isoformat(),'filedate': filedate.date().isoformat()})
                else:
                    files.append({'url': a['href'],'filedate': filedate.date().isoformat()})
                if len(files)>=number:
                    break
        if (number > 0) and (len(files)>=number):
            break
    return files

def check_file_list_against_previous(current, previous):
    changes = []
    maxdate = '1970-01-01'
    for i in range(len(previous)):
        if previous[i]['filedate'] > maxdate:
            maxdate = previous[i]['filedate']
    for e in reversed(current):
        match = next((p for p in previous if p["filedate"] == e["filedate"]), None)
        if match is None and (e['filedate'] > maxdate):
            # If a new, later date
            for i in range(len(changes)):
                changes[i]['index'] = changes[i]['index']+1
            changes.append({'index': 0, 'change': 'added'})
            previous.insert(0, e)
        elif match is None:
            # If a new, older date
            changes.append({'index': len(previous), 'change': 'added'})
            previous.append(e)
        elif 'modified' in match:
            if 'length' in match:
                if ((match['modified'] != e['modified']) or (match['length'] != e['length'])):
                    changes.append({'index': previous.index(match), 'change': 'modified'})
                    previous[changes[-1]['index']] = e
            else:
                if match['modified'] != e['modified']:
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

def check_for_dd_files(s3client, bucket, previous, files_to_check, store=True):
    session = requests.Session()
    session.headers = {
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'User-Agent': generate_user_agent(),
        'Upgrade-Insecure-Requests': '1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    html = BeautifulSoup(get_url(session, 'https://www.health-ni.gov.uk/articles/covid-19-daily-dashboard-updates', 'text'),features="html.parser")
    durl = None
    for a in html.find_all('a', href=True):
        if a.text.strip().lower().startswith('latest pdf version'):
            durl = a['href']
    if durl is None:
        raise('Unable to find starting URL')
    url = 'https://www.health-ni.gov.uk/' + durl.lstrip('/')
    # Attempt to pull this month's list of daily data publications
    excels = []
    date_to_try = datetime.datetime.today()
    # Pull last month's as well, if we need to, to ensure we always have N days of data checked, or just get everything
    while (len(excels) < files_to_check) or ((files_to_check == 0)):
        try:
            excels.extend(
                extract_doh_file_list(
                    get_url(
                        session,
                        url,
                        'text',
                    ),
                    files_to_check-len(excels),
                    r'-(\d{6}).*\.xlsx$',
                    datefmt='%d%m%y'
                )
            )
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                if len(excels) == 0:
                    print('Failed to get latest month from %s, rolling back to previous month' %url)
                else:
                    print('Failed to get data from %s, stopping trying' %url)
                    break
            else:
                raise err
        date_to_try = date_to_try.replace(day=1) - datetime.timedelta(days=1)
        url = 'https://www.health-ni.gov.uk/Daily%%20dashboard%%20updates%%20on%%20COVID-19%%20-%%20%s%%20%d' %(date_to_try.strftime("%B").lower(),date_to_try.year)
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(excels, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'DoH-DD', index, changes, 'xlsx')
    return index, changes

def check_for_hospital_files(s3client, bucket, previous, files_to_check=1, store=True):
    session = requests.Session()
    session.headers = {
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'User-Agent': generate_user_agent(),
        'Upgrade-Insecure-Requests': '1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    html = BeautifulSoup(get_url(session, 'https://www.health-ni.gov.uk/articles/covid-19-dashboard-updates', 'text'),features="html.parser")
    durl = None
    for a in html.find_all('a', href=True):
        if a.text.strip().lower().startswith('covid-19 hospitalisations'):
            durl = a['href']
    if durl is None:
        raise('Unable to find starting URL')
    url = 'https://www.health-ni.gov.uk/' + durl.lstrip('/')
    # Attempt to pull this month's list of daily data publications
    excels = []
    # Pull last month's as well, if we need to, to ensure we always have N days of data checked, or just get everything
    while (len(excels) < files_to_check) or ((files_to_check == 0)):
        excels.extend(
            extract_doh_file_list(
                get_url(
                    session,
                    url,
                    'text',
                ),
                files_to_check-len(excels),
                r'-(\d{6}).*\.xlsx$',
                datefmt='%d%m%y'
            )
        )
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(excels, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'DoH-hospitalisations', index, changes, 'xlsx')
    return index, changes

def check_for_r_files(s3client, bucket, previous):
    session = requests.Session()
    # Attempt to pull the list of R number publications
    url = 'https://www.health-ni.gov.uk/R-Number'
    pdfs = extract_doh_file_list(
        get_url(
            session,
            url,
            'text',
            useragent=generate_user_agent(),
            referer='https://www.health-ni.gov.uk/'
        ),
        1,
        r'-(\d{6}).*\.pdf$'
    )
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(pdfs, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'DoH-R', index, changes, 'pdf')
    return index, changes

def check_doh(secret, s3, notweet, mode):
    if mode=='dd':
        indexkey = secret['doh-dd-index']
        lambdaname = os.getenv('TWEETER_LAMBDA')
    elif mode=='hospital':
        indexkey = secret['doh-hospital-index']
        lambdaname = os.getenv('HOSPITAL_TWEETER_LAMBDA')
    else:
        indexkey = secret['doh-r-index']
        lambdaname = os.getenv('R_TWEETER_LAMBDA')

    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], indexkey)
    previous = status.get_dict()
    previous = sorted(previous, key=lambda k: k['filedate'], reverse=True)

    # Check the DoH site for file changes
    if mode=='dd':
        current, changes = check_for_dd_files(s3, secret['bucketname'], previous, int(secret['doh-dd-files-to-check']), store=(not notweet))
    elif mode=='hospital':
        current, changes = check_for_hospital_files(s3, secret['bucketname'], previous, store=(not notweet))
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

def check_symptoms():
    url = 'https://services-eu1.arcgis.com/CbFuxzn9jT2gu2G1/arcgis/rest/services/Prod_Symptoms_By_Hex_Tess_All_Public_View/FeatureServer/0/query'
    formdata = {
        "f": "json",
        "groupByFieldsForStatistics": "CAST(EXTRACT(YEAR FROM DateOfReport + (CASE  WHEN DateOfReport BETWEEN timestamp '2021-03-04 00:00:00' AND timestamp '2021-03-28 00:59:59' THEN -INTERVAL '-1:59:59' HOUR TO SECOND WHEN DateOfReport BETWEEN timestamp '2021-03-28 01:00:00' AND timestamp '2021-04-01 00:00:00' THEN +INTERVAL '0:59:59' HOUR TO SECOND END)) AS VARCHAR(4)) || '-' || CAST(EXTRACT(MONTH FROM DateOfReport + (CASE  WHEN DateOfReport BETWEEN timestamp '2021-03-04 00:00:00' AND timestamp '2021-03-28 00:59:59' THEN -INTERVAL '-1:59:59' HOUR TO SECOND WHEN DateOfReport BETWEEN timestamp '2021-03-28 01:00:00' AND timestamp '2021-04-01 00:00:00' THEN +INTERVAL '0:59:59' HOUR TO SECOND END)) AS VARCHAR(2)) || '-' || CAST(EXTRACT(DAY FROM DateOfReport + (CASE  WHEN DateOfReport BETWEEN timestamp '2021-03-04 00:00:00' AND timestamp '2021-03-28 00:59:59' THEN -INTERVAL '-1:59:59' HOUR TO SECOND WHEN DateOfReport BETWEEN timestamp '2021-03-28 01:00:00' AND timestamp '2021-04-01 00:00:00' THEN +INTERVAL '0:59:59' HOUR TO SECOND END)) AS VARCHAR(2))",
        "outFields": "OBJECTID,ChecksToday,DateOfReport",
        "outStatistics": "[{\"onStatisticField\":\"ChecksToday\",\"outStatisticFieldName\":\"value\",\"statisticType\":\"sum\"}]",
        "resultType": "standard",
        "returnGeometry": "false",
        "spatialRel": "esriSpatialRelIntersects",
        "where": "(DateOfReport BETWEEN timestamp '2021-03-04 00:00:00' AND CURRENT_TIMESTAMP) AND ((DateOfReport BETWEEN timestamp '2021-03-04 00:00:00' AND timestamp '2021-03-28 00:59:59' OR DateOfReport BETWEEN timestamp '2021-03-28 01:00:00' AND timestamp '2021-04-01 00:00:00'))"
    }
    print('POST %s to %s' %(formdata,url))

def check_for_nisra_files(s3client, bucket, previous):
    session = requests.Session()

    # Attempt to pull the link to this week's publications
    url = 'https://www.nisra.gov.uk/statistics/death-statistics/weekly-death-registrations-northern-ireland'
    html = BeautifulSoup(
        get_url(
            session,
            url,
            'text',
            useragent=generate_user_agent(),
            referer='https://www.nisra.gov.uk/statistics/ni-summary-statistics/coronavirus-covid-19-statistics'
        )
        ,features="html.parser")
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
    # e.g. https://www.nisra.gov.uk/system/files/statistics/Weekly-Deaths-we-17-September-2021.XLSX
    # e.g. https://www.nisra.gov.uk/system/files/statistics/Weekly_Deaths%20-%20w%20e%205th%20November%202021.XLSX
    excels = extract_doh_file_list(
        get_url(
            session,
            durl,
            'text',
            useragent=generate_user_agent(),
            referer=url
        ),
        1,
        r'w(%20)*e(%20|-)(\d+[a-z]*(%20|-)[A-Za-z]+(%20|-)\d+).*\.(?:xlsx|XLSX)$',
        [r'(\d)(st|nd|rd|th)', r'\1'],
        r'%d-%B-%Y',
        matchgroup=3
    )
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

def check_for_ons_files(s3client, bucket, previous):
    session = requests.Session()

    # Attempt to pull the link to this week's publications
    url = 'https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/conditionsanddiseases/datasets/covid19infectionsurveynorthernireland'
    html = BeautifulSoup(
        get_url(
            session,
            url,
            'text',
            useragent=generate_user_agent()
        )
        ,features="html.parser")
    durl = None
    for a in html.find_all('a', href=True):
        if a.text.strip()[:4] == 'xlsx':
            durl = a['href']
            break
    if durl is None:
        raise Exception('Failed to find link to ONS infection survey at %s' %url)
    if durl.startswith('/'):
        durl = 'https://www.ons.gov.uk' + durl
    # Example: https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/healthandsocialcare/conditionsanddiseases/datasets/covid19infectionsurveynorthernireland/2022/20220617covid19infectionsurveydatasetsni1.xlsx
    # Example: https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fhealthandsocialcare%2fconditionsanddiseases%2fdatasets%2fcovid19infectionsurveynorthernireland%2f2022/20220520covid19infectionsurveydatasetsni.xlsx
    # Example: https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fhealthandsocialcare%2fconditionsanddiseases%2fdatasets%2fcovid19infectionsurveynorthernireland%2f2021/20220107covid19infectionsurveydatasetsni.xlsx
    regex = re.compile(r'(\d{8})covid19infectionsurveydatasetsni\d*\.(?:xlsx|XLSX)$', flags=re.IGNORECASE)
    m = regex.search(durl)
    if m is None:
        raise Exception('Failed to find ONS infection survey date in %s' %durl)
    datestr = m.group(1)
    filedate = datetime.datetime.strptime(datestr,'%Y%m%d')
    metadata = {'url': durl, 'filedate': filedate.date().isoformat()}

    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous([metadata], previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'ONS-infections', index, changes, 'xslx')
    return index, changes

def check_ons(secret, s3, notweet):
    indexkey = secret['ons-infection-index']

    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], indexkey)
    previous = status.get_dict()
    previous = sorted(previous, key=lambda k: k['filedate'], reverse=True)

    # Check the ONS site for file changes
    current, changes = check_for_ons_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if len(changes) > 0:
        status.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))

        # If the most recent file has changed then tweet
        totweet = [c['index'] for c in changes if (c['change'] == 'added') or (c['index'] == 0)]
        if not notweet and (0 in totweet):
            print('Launching ONS tweeter')
            launch_lambda_async(os.getenv('ONS_TWEETER_LAMBDA'),[current[a] for a in totweet])
            message += ', and launched ONS tweet lambda'
    else:
        message = 'Did nothing'

    return message

def check_for_ukhsa_variants_files(s3client, bucket, previous):
    session = requests.Session()
    # Attempt to pull the index page for all publications
    url = 'https://www.gov.uk/government/publications/covid-19-variants-genomically-confirmed-case-numbers'
    html = BeautifulSoup(
        get_url(
            session,
            url,
            'text',
            useragent=generate_user_agent(),
        )
        ,features="html.parser")
    pages = []
    for a in html.find_all('a', href=True):
        if a.text.strip().startswith('Variants: distribution of case data'):
            page = {}
            if a['href'].startswith('/'):
                page['url'] = 'https://www.gov.uk' + a['href']
            else:
                page['url'] = a['href']
            regex = re.compile(r'(\d+\-[a-z]+\-\d{4})$', flags=re.IGNORECASE)
            m = regex.search(page['url'])
            if m is None:
                raise Exception('Failed to find variant report date in %s' %page['url'])
            datestr = m.group(1)
            filedate = datetime.datetime.strptime(datestr,'%d-%B-%Y')
            page['filedate'] = filedate.date().isoformat()
            pages.append(page)
    if len(pages) < 1:
        raise Exception('Failed to find links to variant links at %s' %url)
    # e.g. https://www.gov.uk/government/publications/covid-19-variants-genomically-confirmed-case-numbers/variants-distribution-of-case-data-27-may-2022
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(pages, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'UKHSA-variants', index, changes, 'html')
    return index, changes

def check_ukhsa_variants(secret, s3, notweet):
    indexkey = secret['ukhsa-variants-index']
    lambdaname = os.getenv('UKHSA_VARIANTS_TWEETER_LAMBDA')

    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], indexkey)
    previous = status.get_dict()
    previous = sorted(previous, key=lambda k: k['filedate'], reverse=True)

    # Check the gov.uk site for file changes
    current, changes = check_for_ukhsa_variants_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if len(changes) > 0:
        status.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))

        # If the most recent file has changed then tweet
        totweet = [c['index'] for c in changes if (c['change'] == 'added') or (c['index'] == 0)]
        if not notweet and (0 in totweet):
            print('Launching UKHSA variants tweeter')
            launch_lambda_async(lambdaname,[current[a] for a in totweet])
            message += ', and launched UKHSA variants tweet lambda'
    else:
        message = 'Did nothing'

    return message

def check_for_cluster_files(s3, bucketname, previous):
    # Attempt to pull the list of cluster publications
    session = requests.Session()
    url = 'https://www.publichealth.hscni.net/publications/covid-19-clusteroutbreak-summary'
    pdfs = extract_doh_file_list(get_url(session, url,'text'), 1, r'(\d{2}_\d{2}_\d{2})\.pdf$', datefmt='%d_%m_%y', element='span', htmlclass='file--application-pdf')
    # Check whether we have new files
    index, changes = check_file_list_against_previous(pdfs, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3, bucketname, 'PHA-clusters', index, changes, 'pdf')
    return index, changes

def check_clusters(secret, s3, notweet):
    indexkey = secret['pha-clusters-index']
    previous, index = get_and_sort_index(secret['bucketname'], indexkey, s3, 'filedate')

    # Check the PHA page for new/updated PDFs
    current, changes = check_for_cluster_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if len(changes) > 0:
        index.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))

        # If the most recent file has changed then tweet
        totweet = [c['index'] for c in changes if (c['change'] == 'added') or (c['index'] == 0)]
        if not notweet and (0 in totweet):
            print('Launching PHA clusters tweeter')
            launch_lambda_async(os.getenv('CLUSTERS_TWEETER_LAMBDA'),[current[a] for a in totweet])
            message += ', and launched clusters tweet lambda'
    else:
        message = 'Did nothing'

    return message

def check_for_bulletin_files(s3, bucketname, previous):
    # Attempt to pull the list of cluster publications
    session = requests.Session()
    url = 'https://www.publichealth.hscni.net/publications/coronavirus-bulletin'
    pdfs = extract_doh_file_list(get_url(session, url,'text'), 1, r'files\/(\d{4}-\d{2})\/.+\.pdf$', datefmt='%Y-%m', element='span', htmlclass='file--application-pdf')
    # Give proper dates to the files to prevent clashes (day is missing)
    for i in range(len(pdfs)):
        pdfs[i]['filedate'] = pdfs[i]['modified'][:10]
    # Check whether we have new files
    index, changes = check_file_list_against_previous(pdfs, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3, bucketname, 'PHA-bulletin', index, changes, 'pdf')
    return index, changes

def check_bulletins(secret, s3, notweet):
    indexkey = secret['pha-bulletin-index']
    previous, index = get_and_sort_index(secret['bucketname'], indexkey, s3, 'filedate')

    # Check the PHA page for new/updated PDFs
    current, changes = check_for_bulletin_files(s3, secret['bucketname'], previous)

    # Write any changes back to S3
    if len(changes) > 0:
        index.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))

        # If the most recent file has changed then tweet
        totweet = [c['index'] for c in changes if (c['change'] == 'added') or (c['index'] == 0)]
        if not notweet and (0 in totweet):
            print('Launching generic tweeter for bulletins')
            launch_lambda_async(os.getenv('GENERIC_TWEETER_LAMBDA'),[dict(current[a],type='PHA bulletin',tweet=False) for a in totweet])
            message += ', and launched generic tweet lambda'
    else:
        message = 'Did nothing'

    return message

def get_all_doh(secret, s3):
    indexkey = secret['doh-dd-index']

    # Get the previous data file list from S3
    status = S3_scraper_index(s3, secret['bucketname'], indexkey)
    previous = status.get_dict()
    previous = sorted(previous, key=lambda k: k['filedate'], reverse=True)

    # Check the DoH site for file changes
    current, changes = check_for_dd_files(s3, secret['bucketname'], previous, 0)

    # Write any changes back to S3
    if len(changes) > 0:
        status.put_dict(current)
        message = 'Wrote %d items to %s, of which %d were changes' %(len(current), indexkey, len(changes))
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
    if event.get('get-all-doh-dd'):
        messages.append(get_all_doh(secret, s3))
    else:
        # Run the scraper
        try:
            messages.append(check_doh(secret, s3, event.get('hospital-notweet', False), 'hospital'))
        except:
            logging.exception('Caught exception accessing DOH hospital data')
        try:
            messages.append(check_doh(secret, s3, event.get('r-notweet', False), 'r'))
        except:
            logging.exception('Caught exception accessing DOH R number')
        try:
            messages.append(check_nisra(secret, s3, event.get('nisra-notweet', False)))
        except:
            logging.exception('Caught exception accessing NISRA weekly data')
        try:
            messages.append(check_ukhsa_variants(secret, s3, event.get('variants-notweet', False)))
        except:
            logging.exception('Caught exception accessing UKHSA variants data')
        try:
            messages.append(check_clusters(secret, s3, event.get('clusters-notweet', False)))
        except:
            logging.exception('Caught exception accessing PHA clusters data')
        try:
            messages.append(check_bulletins(secret, s3, event.get('bulletin-notweet', False)))
        except:
            logging.exception('Caught exception accessing PHA bulletin data')
        try:
            messages.append(check_ons(secret, s3, event.get('ons-notweet', False)))
        except:
            logging.exception('Caught exception accessing DOH daily data')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "messages": messages,
        }),
    }
