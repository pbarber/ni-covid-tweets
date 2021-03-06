import json
import tempfile
import re

import boto3
import textract

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    messages = []
    # Download the most recently updated PDF file
    s3 = boto3.client('s3')
    for change in event:
        tmp = tempfile.NamedTemporaryFile(suffix='.pdf')
        with open(tmp.name, 'wb') as fp:
            s3.download_fileobj(secret['bucketname'],change['keyname'],fp)
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
        tweet += '\n%s' %change['url']

        if change.get('notweet') is not True:
            api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
            resp = api.tweet(tweet)

            # Download and update the index
            status = S3_scraper_index(s3, secret['bucketname'], secret['doh-r-index'])
            index = status.get_dict()
            for i in range(len(index)):
                if index[i]['filedate'] == change['filedate']:
                    index[i]['tweet'] = resp.id
                    break
            status.put_dict(index)

            messages.append('Tweeted ID %s and updated %s' %(resp.id, secret['doh-r-index']))
        else:
            print(tweet)
            messages.append('Did not tweet')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": messages,
        }),
    }
