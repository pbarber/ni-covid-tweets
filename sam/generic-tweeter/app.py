import json
import io
import datetime

import boto3

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    messages = []
    for change in event:
        tweet = '''{type}

Change detected in {url}'''.format(
            url=change['url'],
            type=change['type']
        )
        if change.get('notweet') is not True:
            api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
            if change.get('tweet') is True:
                resp = api.tweet(tweet)
                messages.append('Tweeted ID %s, ' %resp.id)
            else:
                resp = api.dm(secret['twitter_dmaccount'], tweet)
                messages.append('Tweeted DM ID %s, ' %resp.id)

        else:
            messages.append('Did not tweet')
            print(tweet)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
