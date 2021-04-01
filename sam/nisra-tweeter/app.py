import json
import io
import datetime

import boto3
import pandas

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

good_symb = '\u2193'
bad_symb = '\u2191'

def colclean(old):
    for name in ['Hospital', 'Care Home', 'Hospice', 'Home', 'Other', 'Total', 'Week Ending']:
        if old.startswith(name):
            return name
    return old

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    tweets = []
    # Download the most recently updated Excel file
    s3 = boto3.client('s3')
    for change in event:
        obj = s3.get_object(Bucket=secret['bucketname'],Key=change['keyname'])['Body']
        stream = io.BytesIO(obj.read())

        # Load test data and add extra fields
        df = pandas.read_excel(stream,engine='openpyxl',sheet_name='Table 7', header=3)
        df.dropna('columns',how='all',inplace=True)
        df.rename(columns=colclean,inplace=True)
        df.dropna('rows',subset=['Total'],inplace=True)
        print(df)

        # Get the latest dates with values for tests and rolling
        df['date'] = pandas.to_datetime(df['Week Ending'], format='%d/%m/%Y')
        latest = df.iloc[df['date'].idxmax()]

        # Check against previous day's reports
        status = S3_scraper_index(s3, secret['bucketname'], secret['nisra-deaths-index'])
        index = status.get_dict()
        tweet = '''{deaths:,} deaths registered in week ending {date}
\u2022 {hospitals:,} in hospitals
\u2022 {care:,} in care homes
\u2022 {hospices:,} in hospices
\u2022 {home:,} at home

Deaths include any death where Coronavirus or Covid-19 (suspected or confirmed) was mentioned anywhere on the death certificate.

'''.format(
            date=latest['date'].strftime('%A %-d %B %Y'),
            deaths=int(latest['Total']),
            hospitals=int(latest['Hospital']),
            care=int(latest['Care Home']),
            hospices=int(latest['Hospice']),
            home=int(latest['Home'])
        )

        tweets.append({'text': tweet, 'url': change['url'], 'notweet': change.get('notweet'), 'filedate': change['filedate']})

    donottweet = []
    if len(tweets) > 1:
        for i in range(1,len(tweets)):
            for j in range(0, i):
                if (tweets[i]['text'] == tweets[j]['text']):
                    donottweet.append(i)

    messages = []
    for idx in range(len(tweets)):
        if tweets[idx].get('notweet') is not True:
            if (idx not in donottweet):
                api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
                resp = api.tweet(tweets[idx]['text'] + tweets[idx]['url'])

                messages.append('Tweeted ID %s, ' %resp.id)
            else:
                messages.append('Duplicate found %s, did not tweet, ' %tweets[idx]['filedate'])

            # Update the file index
            for i in range(len(index)):
                if index[i]['filedate'] == tweets[idx]['filedate']:
                    index[i]['tweet'] = resp.id
                    break
            status.put_dict(index)

            messages[-1] += ('updated %s' %secret['nisra-deaths-index'])
        else:
            if (idx not in donottweet):
                messages.append('Did not tweet')
                print(tweets[idx]['text'] + tweets[idx]['url'])
            else:
                messages.append('Duplicate found %s, did not tweet, ' %tweets[idx]['filedate'])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
