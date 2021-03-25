import json
import io
import datetime

import boto3
import pandas
import tweepy

from shared import S3_scraper_index

good_symb = '\u2193'
bad_symb = '\u2191'

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the previous data file list from S3
    s3 = boto3.client('s3')
    if event['Source']=='PHE':
        keyname = secret['phe-vacc-index']
    else:
        keyname = secret['hscni-vacc-index']
    status = S3_scraper_index(s3, secret['bucketname'], keyname)
    index = status.get_dict()

    tweet = '''{doses_24:,} vaccine doses registered on {date}
\u2022 {f_24:,} ({pct_f:.1%}) first doses
\u2022 {s_24:,} ({pct_s:.1%}) second doses

{total:,} total vaccine doses
\u2022 {total_f:,} total first doses ({pop_f}% of NI adult population)
\u2022 {total_s:,} total second doses ({pop_s}% of NI adult population)

Source: {source}'''.format(
    doses_24=event['First Doses Registered'] + event['Second Doses Registered'],
    f_24=event['First Doses Registered'],
    s_24=event['Second Doses Registered'],
    total=event['Total Doses'],
    total_f=event['Total First Doses'],
    total_s=event['Total Second Doses'],
    pct_f=event['First Doses Registered'] / (event['First Doses Registered'] + event['Second Doses Registered']),
    pct_s=event['Second Doses Registered'] / (event['First Doses Registered'] + event['Second Doses Registered']),
    date=datetime.datetime.strptime(event['Last Updated'],'%Y-%m-%d').strftime('%A %-d %B %Y'),
    pop_f=event['First Doses pc'],
    pop_s=event['Second Doses pc'],
    source=event['Source']
    )

    if event.get('notweet') is not True:
        auth = tweepy.OAuthHandler(secret['twitter_apikey'], secret['twitter_apisecretkey'])
        auth.set_access_token(secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])

        api = tweepy.API(auth)
        resp = api.update_status(tweet)

        for i in range(len(index)):
            if index[i]['Last Updated'] == event['Last Updated']:
                index[i]['tweet'] = resp.id
                break
        status.put_dict(index)

        message = 'Tweeted ID %s and updated %s' %(resp.id, keyname)
    else:
        print(tweet)
        message = 'Did not tweet'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
