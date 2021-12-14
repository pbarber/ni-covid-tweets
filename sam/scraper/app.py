import json
import datetime
import re
import os
import logging
from copy import deepcopy

import requests
from bs4 import BeautifulSoup
import boto3

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

def check_for_dd_files(s3client, bucket, previous, files_to_check):
    session = requests.Session()
    # Attempt to pull this month's list of daily data publications
    excels = []
    date_to_try = datetime.datetime.today()
    # Pull last month's as well, if we need to, to ensure we always have N days of data checked, or just get everything
    while (len(excels) < files_to_check) or ((files_to_check == 0)):
        url = 'https://www.health-ni.gov.uk/Daily%%20dashboard%%20updates%%20on%%20COVID-19%%20-%%20%s%%20%d' %(date_to_try.strftime("%B").lower(),date_to_try.year)
        try:
            excels.extend(
                extract_doh_file_list(
                    get_url(
                        session,
                        url,
                        'text',
                        referer='https://www.health-ni.gov.uk/articles/covid-19-daily-dashboard-updates'
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
    # Merge the new data into the previous list and detect changes
    index, changes = check_file_list_against_previous(excels, previous)
    # Upload the changed files to s3
    index = upload_changes_to_s3(s3client, bucket, 'DoH-DD', index, changes, 'xlsx')
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

def check_hscni(original):
    session = requests.Session()

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

    if 'Vaccinations Total' in data:
        data['Total Doses'] = data['Vaccinations Total']
    if 'Vaccinations Total (Dose 1)' in data:
        data['Total First Doses'] = data['Vaccinations Total (Dose 1)']
    if 'Vaccinations Total (Dose 2)' in data:
        data['Total Second Doses'] = data['Vaccinations Total (Dose 2)']

    # Account for one day delay in reporting
    data['Last Updated'] = reportdate.isoformat()
    data['First Doses pc'] = round((100 * data['Total First Doses']) / 1597898, 1) # NI 16 and over
    data['Second Doses pc'] = round((100 * data['Total Second Doses']) / 1597898, 1)
    data['Source'] = 'HSCNI'

    # Do change detection
    previous = deepcopy(original)
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

    return previous, change

def check_phe(original):
    session = requests.Session()

    # Get the PHE data from the API
    url = 'https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&areaCode=N92000002&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&format=json'
    ordered = sorted(get_url(session, url,'json').get('body',[]), key=lambda k: k['date'], reverse=True)

    previous = deepcopy(original)
    change = False
    if (len(ordered) > 1):
        data = {
            'Last Updated': ordered[0]['date'],
            'Total Doses': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'] + ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'],
            'Total First Doses': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'],
            'Total Second Doses': ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'],
            'First Doses Registered': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'] - ordered[1]['cumPeopleVaccinatedFirstDoseByPublishDate'],
            'Second Doses Registered': ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'] - ordered[1]['cumPeopleVaccinatedSecondDoseByPublishDate'],
            'Source': 'PHE'
        }
        data['First Doses pc'] = round((100 * data['Total First Doses']) / 1597898, 1)  # NI 16 and over
        data['Second Doses pc'] = round((100 * data['Total Second Doses']) / 1597898, 1)
        if len(previous) > 0:
            if (data['Last Updated'] > previous[0]['Last Updated']):
                previous.insert(0, data)
                change = True
            elif (data['Last Updated'] == previous[0]['Last Updated']) and (data['Total Doses'] > previous[0]['Total Doses']):
                previous[0] = data
                change = True
        else:
            previous.insert(0, data)
            change = True

    return previous, change

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

def check_vaccine(bucketname, indexkey, s3, notweet):
    index, indexobj = get_and_sort_index(bucketname, indexkey, s3)

    # Check both the PHE API and HSCNI site
    phechange = False
    hscchange = False
    try:
        phe, phechange = check_phe(index)
    except:
        logging.exception('Caught exception accessing PHE vaccine data')
    try:
        hsc, hscchange = check_hscni(index)
    except:
        logging.exception('Caught exception accessing HSCNI vaccine data')

    # Choose which data to tweet
    chosen = None
    doses = ['First','Second']
    if hscchange and phechange and (phe[0]['Last Updated'] == hsc[0]['Last Updated']):
        print('vaccines: both changed')
        if any([phe[0]['Total %s Doses' %dose] < hsc[0]['Total %s Doses' %dose] for dose in doses]):
            chosen = hsc[0]
        else:
            chosen = phe[0]
    elif hscchange and ((len(index) == 0) or (hsc[0]['Last Updated'] >= index[0]['Last Updated'])):
        if any([index[0]['Total %s Doses' %dose] < hsc[0]['Total %s Doses' %dose] for dose in doses]):
            chosen = hsc[0]
    elif phechange and ((len(index) == 0) or (phe[0]['Last Updated'] >= index[0]['Last Updated'])):
        if any([index[0]['Total %s Doses' %dose] < phe[0]['Total %s Doses' %dose] for dose in doses]):
            chosen = phe[0]

    # If there has been a change, then tweet
    message = 'Did nothing'
    if chosen is not None:
        message = 'New last updated date of %s from %s' %(chosen['Last Updated'],chosen['Source'])
        print(chosen)
        if not notweet:
            # Update the S3 indexes
            if chosen['Source']=='HSCNI':
                indexobj.put_dict(hsc)
            else:
                indexobj.put_dict(phe)
            print('Launching vaccine tweeter')
            launch_lambda_async(os.getenv('VACCINE_TWEETER_LAMBDA'),chosen)
            message += ', and launched vaccine tweet lambda'

    return(message)

def check_for_nisra_files(s3client, bucket, previous):
    session = requests.Session()

    # Attempt to pull the link to this week's publications
    url = 'https://www.nisra.gov.uk/statistics/death-statistics/weekly-death-registrations-northern-ireland'
    html = BeautifulSoup(
        get_url(
            session,
            url,
            'text',
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

def requests_stream(session, url):
    with session.get(url, stream=True) as stream:
        stream.raise_for_status()

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

    found = []
    while today >= last:
        url = "https://cog-uk-microreact.s3.climb.ac.uk/{today}/cog_metadata_microreact_uk_geocoded.csv".format(today=today.isoformat())
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
        launch_lambda_async(os.getenv('VARIANTS_TWEETER_LAMBDA'),latest)
        message += ', and launched variants tweet lambda'
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
            messages.append(check_doh(secret, s3, event.get('tests-notweet', False), 'dd'))
        except:
            logging.exception('Caught exception accessing DOH daily data')
        try:
            messages.append(check_doh(secret, s3, event.get('r-notweet', False), 'r'))
        except:
            logging.exception('Caught exception accessing DOH R number')
        try:
            messages.append(check_vaccine(secret['bucketname'], secret['shared-vacc-index'], s3, event.get('vaccine-notweet', False)))
        except:
            logging.exception('Caught exception accessing vaccine data')
        try:
            messages.append(check_nisra(secret, s3, event.get('nisra-notweet', False)))
        except:
            logging.exception('Caught exception accessing NISRA weekly data')
        try:
            messages.append(check_cog(secret, s3, event.get('cog-notweet', False)))
        except:
            logging.exception('Caught exception accessing COG variants data')
        try:
            messages.append(check_clusters(secret, s3, event.get('clusters-notweet', False)))
        except:
            logging.exception('Caught exception accessing PHA clusters data')
        try:
            messages.append(check_bulletins(secret, s3, event.get('bulletin-notweet', False)))
        except:
            logging.exception('Caught exception accessing PHA bulletin data')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "messages": messages,
        }),
    }
