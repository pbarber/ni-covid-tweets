import json
import io
import datetime

import boto3
import pandas

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

good_symb = '\u2193'
bad_symb = '\u2191'

green_block = '\u2705'
white_block = '\u2b1c'
black_block = '\u2b1b'

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the previous data file list from S3
    s3 = boto3.client('s3')
    keyname = secret['shared-vacc-index']
    status = S3_scraper_index(s3, secret['bucketname'], keyname)
    index = status.get_dict()

    tweet = '''{doses_24:,} COVID-19 vaccine doses registered in NI on {date}
\u2022 {f_24:,} first
\u2022 {s_24:,} second
\u2022 {pct_f:.0%}/{pct_s:.0%} dose mix

{total:,} total doses
\u2022 {total_f:,} first
\u2022 {total_s:,} second

Population (16 and over) vaccinated
\u2022 {pop_f}% first
\u2022 {pop_s}% second

{source}'''.format(
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
    source= 'https://coronavirus.data.gov.uk/' if event['Source']=='PHE' else 'https://covid-19.hscni.net/'
    )

    blocks = ['','','','']
    for i in range(20):
        if (i*5)+5 <= event['Second Doses pc']:
            blocks[i//5] += green_block
        elif (i*5)+5 <= event['First Doses pc']:
            blocks[i//5] += white_block
        else:
            blocks[i//5] += black_block
    tweet2 = '''Proportion of NI adults (16 and over) vaccinated against COVID-19:

{blocks0}
{blocks1}
{blocks2}
{blocks3}

One block is one person in 20

{green} - 2nd dose received
{white} - 1st dose received
{black} - no doses'''.format(
    blocks0=blocks[0],
    blocks1=blocks[1],
    blocks2=blocks[2],
    blocks3=blocks[3],
    green=green_block,
    white=white_block,
    black=black_block
)

    if (event.get('tweet2test') is True) or (event.get('notweet') is not True):
        api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
        if event.get('notweet') is not True:
            if event.get('testtweet') is True:
                resp = api.dm(secret['twitter_dmaccount'], tweet)
                resp = api.dm(secret['twitter_dmaccount'], tweet2)
                message = 'Sent test DM'
            else:
                resp = api.tweet(tweet)
                for i in range(len(index)):
                    if index[i]['Last Updated'] == event['Last Updated']:
                        index[i]['tweet'] = resp.id
                        break
                status.put_dict(index)
                message = 'Tweeted ID %s and updated %s' %(resp.id, keyname)

                resp = api.tweet(tweet2, resp.id)
                message = 'Tweeted reply ID %s' %resp.id
    else:
        print(tweet)
        print(tweet2)
        message = 'Did not tweet'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
