import json

import boto3
import requests
from bs4 import BeautifulSoup

from shared import S3_scraper_index

donotlaunch = False

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the previous data file list from S3
    s3 = boto3.client('s3')
    if secret.get('vacc-source')=='PHE':
        keyname = secret['phe-vacc-index']
    else:
        keyname = secret['hscni-vacc-index']
    status = S3_scraper_index(s3, secret['bucketname'], keyname)
    print('getting: %s/%s' %(secret['bucketname'], keyname))
    previous = status.get_dict()
    if len(previous) > 0:
        previous = sorted(previous, key=lambda k: k['Last Updated'], reverse=True)

    change = False
    if secret.get('vacc-source')=='PHE':
        resp = requests.get('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&areaCode=N92000002&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&metric=cumVaccinationFirstDoseUptakeByPublishDatePercentage&metric=cumVaccinationSecondDoseUptakeByPublishDatePercentage&format=json')
        resp.raise_for_status()
        ordered = sorted(resp.json().get('body',[]), key=lambda k: k['date'], reverse=True)
        if (len(ordered) > 1):
            data = {
                'Last Updated': ordered[0]['date'],
                'Total Doses': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'] + ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'],
                'Total First Doses': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'],
                'Total Second Doses': ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'],
                'First Doses Registered': ordered[0]['cumPeopleVaccinatedFirstDoseByPublishDate'] - ordered[1]['cumPeopleVaccinatedFirstDoseByPublishDate'],
                'Second Doses Registered': ordered[0]['cumPeopleVaccinatedSecondDoseByPublishDate'] - ordered[1]['cumPeopleVaccinatedSecondDoseByPublishDate'],
                'First Doses pc': ordered[0]['cumVaccinationFirstDoseUptakeByPublishDatePercentage'],
                'Second Doses pc': ordered[0]['cumVaccinationSecondDoseUptakeByPublishDatePercentage']
            }
            if len(previous) > 0:
                change = (data['Last Updated'] != previous[0]['Last Updated']) or (data['Total Doses'] != previous[0]['Total Doses'])
            else:
                change = True
            print(data)
    else:
        # Get the COVID-19 site homepage
        resp = requests.get('https://covid-19.hscni.net/')
        resp.raise_for_status()
        html = BeautifulSoup(resp.text,features="html.parser")

        # Extract the vaccine data
        div = html.find("div", {"class": "bg-vaccine-soft"})
        print(div)
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
        data['Last Updated'] = div.find("div", {"class": "mt-8"}).text.partition(':')[2].strip()
        change = data['Last Updated'] not in [item['Last Updated'] for item in previous]

    # Compare against the stored file, tweet if new
    if change:
        previous.insert(0, data)
        status.put_dict(previous)
        message = 'New last updated date of %s' %(data['Last Updated'])
        if not donotlaunch:
            print('Launching tweeter')
            lambda_client = boto3.client('lambda')
            lambda_client.invoke(
                FunctionName='ni-covid-tweets-NICOVIDVaccineTweeter-1Q5CK6FJHAEOY',
                InvocationType='Event',
                Payload=json.dumps(data)
            )
            message += ', and launched tweet lambda'
    else:
        message = 'Did nothing'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
