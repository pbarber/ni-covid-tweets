import json

import boto3
import requests
from bs4 import BeautifulSoup

from shared import S3_scraper_index

donotlaunch = False

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

    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the previous data file list from S3
    s3 = boto3.client('s3')
    status = S3_scraper_index(s3, secret['bucketname'], secret['hscni-vacc-index'])
    print('getting: %s/%s' %(secret['bucketname'], secret['hscni-vacc-index']))
    previous = status.get_dict()

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

    # Compare against the stored file, tweet if new
    if data['Last Updated'] not in [item['Last Updated'] for item in previous]:
        previous.append(data)
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
