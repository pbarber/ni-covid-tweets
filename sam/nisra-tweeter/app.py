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
        df.sort_values('date', inplace=True)
        latest = df.iloc[-1]

        # Check against previous day's reports
        status = S3_scraper_index(s3, secret['bucketname'], secret['nisra-deaths-index'])
        index = status.get_dict()
        if latest['Total'] == 0:
            tweet = '''No deaths registered in Northern Ireland, week ended {date}

'''.format(
                date=latest['date'].strftime('%A %-d %B %Y'),
            )
        else:
            if latest['Total'] == 1:
                tweet = '''One death registered in Northern Ireland, week ended {date}, in:
'''.format(
                    date=latest['date'].strftime('%A %-d %B %Y')
                )
            else:
                tweet = '''{deaths:,} deaths registered in Northern Ireland, week ended {date}, in:
'''.format(
                    date=latest['date'].strftime('%A %-d %B %Y'),
                    deaths=int(latest['Total'])
                )
            for name in ['Hospital', 'Care Home', 'Hospice', 'Home', 'Other']:
                if latest[name] > 0:
                    tweet += '\u2022 %s: %s\n' %(name, int(latest[name]))
            tweet += '\n'
        if len(df) > 1:
            prev = df.iloc[-2]
            diff = latest['Total'] - prev['Total']
            tweet += '''{symb} {diff} {comp} than previous week

'''.format(
                symb=good_symb if diff < 0 else bad_symb,
                diff=abs(int(diff)),
                comp='fewer' if diff < 0 else 'more'
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
