import json
import tempfile
import re

import boto3
import textract
import tweepy

from shared import S3_scraper_index

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Download the index
    s3 = boto3.client('s3')
    status = S3_scraper_index(s3, secret['bucketname'], secret['doh-r-index'])
    index = status.get_dict()

    # Download the most recently updated PDF file
    tmp = tempfile.NamedTemporaryFile(suffix='.pdf')
    with open(tmp.name, 'wb') as fp:
        s3.download_fileobj(secret['bucketname'],event['keyname'], fp)
    text = textract.process(tmp.name, method='pdfminer').decode('utf-8')
    first = True
    regex = re.compile(r'^Current estimate of Rt \((.*)\):\s+(.*)$')
    tweet = 'R estimates by Northern Ireland DoH on '
    for line in text.split('\n'):
        m = regex.match(line)
        if first is True:
            tweet += '%s\n\n' %line
        elif m:
            tweet += '\u2022 %s: %s\n' %(m.group(1),m.group(2))
        first = False
    tweet += '\nSource: %s' %event['url']

    if event.get('notweet') is not True:
        auth = tweepy.OAuthHandler(secret['twitter_apikey'], secret['twitter_apisecretkey'])
        auth.set_access_token(secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])

        api = tweepy.API(auth)

        resp = api.update_status(tweet)

        for i in range(len(index)):
            if index[i]['filedate'] == event['filedate']:
                index[i]['tweet'] = resp.id
                break
        status.put_dict(index)

        message = 'Tweeted ID %s and updated %s' %(resp.id, secret['doh-r-index'])
    else:
        print(tweet)
        message = 'Did not tweet'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": message,
        }),
    }
