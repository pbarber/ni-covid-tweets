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
    if secret.get('vacc-source')=='PHE':
        keyname = secret['phe-vacc-index']
    else:
        keyname = secret['hscni-vacc-index']
    status = S3_scraper_index(s3, secret['bucketname'], keyname)
    index = status.get_dict()

    if secret.get('vacc-source')=='PHE':
        tweet = '''{doses_24:,} vaccine doses registered on {date}
\u2022 {f_24:,} ({pct_f:.1%}) first doses
\u2022 {s_24:,} ({pct_s:.1%}) second doses

{total:,} total vaccine doses
\u2022 {total_f:,} total first doses ({pop_f}% of NI adult population)
\u2022 {total_s:,} total second doses ({pop_s}% of NI adult population)

Source: PHE'''.format(
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
        pop_s=event['Second Doses pc']
        )
    else:
        tweet = '''{doses_24:,} vaccine doses in last 24 hours

{total:,} total vaccine doses
\u2022 {total_f:,} ({pct_f:.2%}) total first doses ({pop_f:.2%} of NI adult population)
\u2022 {total_s:,} ({pct_s:.2%}) total second doses ({pop_s:.2%} of NI adult population)

Last updated: {date}. Source: HSCNI'''.format(
        doses_24=event['Last 24 Hours'],
        total=event['Total Doses'],
        total_f=event['Total First Doses'],
        total_s=event['Total Second Doses'],
        pct_f=event['Total First Doses'] / event['Total Doses'],
        pct_s=event['Total Second Doses'] / event['Total Doses'],
        date=event['Last Updated'],
        pop_f=event['Total First Doses'] / 1466885,
        pop_s=event['Total Second Doses'] / 1466885
        )

    auth = tweepy.OAuthHandler(secret['twitter_apikey'], secret['twitter_apisecretkey'])
    auth.set_access_token(secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])

    api = tweepy.API(auth)

    if event.get('notweet') is not True:
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
