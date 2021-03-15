import json
import datetime
import re

import requests
from bs4 import BeautifulSoup

def check_for_files(previous):
    today = datetime.datetime.today()
    url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(today.strftime("%B").lower(),today.year)
    resp = requests.get(url)
    if resp.status_code == 404:
        print('Failed to get latest month from %s, rolling back to previous month' %url)
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        url = 'https://www.health-ni.gov.uk/publications/daily-dashboard-updates-covid-19-%s-%d' %(last_month.strftime("%B").lower(),last_month.year)
        resp = requests.get(url)
    resp.raise_for_status()
    html = BeautifulSoup(resp.text,features="html.parser")
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
    for p in previous:
        if (p not in excels) and (p['filedate'] not in [e['filedate'] for e in excels]):
            excels.insert(0,p)
    return excels

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

    excels = check_for_files([])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "returned %d items" %len(excels),
        }),
    }

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infile")
    parser.add_argument("-o", "--outfile", default='out.json')
    args = parser.parse_args()

    if args.infile:
        with open(args.infile) as previous:
            current = check_for_files(json.load(previous))
    else:
        current = check_for_files([])
    with open(args.outfile, 'w') as output:
        json.dump(current, output)
