import json
import io
import datetime

import boto3
import pandas

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

good_symb = '\u2193'
bad_symb = '\u2191'

def find_previous(df, newest, colname):
    # Find the date since which the rate was as high/low
    gte = df.iloc[df[(df[colname] >= newest[colname]) & (df['Sample_Date'] < newest['Sample_Date'])]['Sample_Date'].idxmax()]
    lt = df.iloc[df[(df[colname] < newest[colname]) & (df['Sample_Date'] < newest['Sample_Date'])]['Sample_Date'].idxmax()]
    if gte['Sample_Date'] < lt['Sample_Date']:
        est = bad_symb + ' highest'
        prev = gte['printdate']
    else:
        est = good_symb + ' lowest'
        prev = lt['printdate']
    return est, prev

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
        df = pandas.read_excel(stream,engine='openpyxl',sheet_name='Summary Tests')
        df['pos_rate'] = df['INDIVIDUALS TESTED POSITIVE']/df['ALL INDIVIDUALS TESTED']
        df['rolling_pos_rate'] = df['ROLLING 7 DAY POSITIVE TESTS']/df['ROLLING 7 DAY INDIVIDUALS TESTED']
        df['printdate']=df['Sample_Date'].dt.strftime('%-d %B %Y')
        df['rolling_7d_change'] = (df['ROLLING 7 DAY POSITIVE TESTS'] - df['ROLLING 7 DAY POSITIVE TESTS'].shift(7)) * 7

        # Get the latest dates with values for tests and rolling
        latest = df.iloc[df['Sample_Date'].idxmax()]
        latest_7d = df.iloc[df[df['rolling_pos_rate'].notna()]['Sample_Date'].idxmax()]

        # Find the date since which the rate was as high/low
        est, prev = find_previous(df, latest, 'pos_rate')
        est_7d, prev_7d = find_previous(df, latest_7d, 'rolling_pos_rate')

        # Summary stats to allow 'X registered in last 24 hours' info
        deaths = pandas.read_excel(stream,engine='openpyxl',sheet_name='Deaths')
        admissions = pandas.read_excel(stream,engine='openpyxl',sheet_name='Admissions')
        discharges = pandas.read_excel(stream,engine='openpyxl',sheet_name='Discharges')
        totals = {
            'ind_tested': int(df['ALL INDIVIDUALS TESTED'].sum()),
            'ind_positive': int(df['INDIVIDUALS TESTED POSITIVE'].sum()),
            'deaths': int(deaths['Number of Deaths'].sum()),
            'admissions': int(admissions['Number of Admissions'].sum()),
            'discharges': int(discharges['Number of Discharges'].sum())
        }
        print(totals)

        # Check against previous day's reports
        status = S3_scraper_index(s3, secret['bucketname'], secret['doh-dd-index'])
        index = status.get_dict()
    #    previousday = datetime.datetime.strptime(chnage["filedate"],'%Y-%m-%d').date() - datetime.timedelta(days=1)
    #    match = next((p for p in index if datetime.datetime.strptime(p["filedate"],'%Y-%m-%d').date() == previousday), None)
    #    if match and 'totals' in match:
    #        tweet_head = '''{ind_tested:,} people tested, {ind_positive:,} ({pos_rate:.2%}) positive registered on {date}'''.format(
    #            date=latest['Sample_Date'].strftime('%A %-d %B %Y'),
    #            ind_positive=(totals['ind_positive']-match['totals']['ind_positive']),
    #            ind_tested=(totals['ind_tested']-match['totals']['ind_tested']),
    #            pos_rate=(totals['ind_positive']-match['totals']['ind_positive'])/(totals['ind_tested']-match['totals']['ind_tested'])
    #        )
    #    else:
        tweet_head = '''{ind_tested:,} people tested, {ind_positive:,} ({pos_rate:.2%}) positive on {date}
{est} rate since {prev}'''.format(
            date=latest['Sample_Date'].strftime('%A %-d %B %Y'),
            ind_positive=latest['INDIVIDUALS TESTED POSITIVE'],
            ind_tested=latest['ALL INDIVIDUALS TESTED'],
            pos_rate=latest['pos_rate'],
            est=est,
            prev=prev
        )

        tweet = '''{head}

{pos_rate_7d:.2%} 7-day positivity rate
{est_7d} since {prev_7d}

{pos_7d:,} positive in last 7 days
{tag_7d} {dif_7d:,} {dir_7d} than preceding 7 days ({pct_7d:.2%})

'''.format(
            head=tweet_head,
            pos_rate_7d=latest_7d['rolling_pos_rate'],
            est_7d=est_7d,
            prev_7d=prev_7d,
            pos_7d=int(round(latest_7d['ROLLING 7 DAY POSITIVE TESTS']*7,0)),
            tag_7d=good_symb if int(round(latest_7d['rolling_7d_change'],0))<0 else bad_symb,
            dir_7d='fewer' if int(round(latest_7d['rolling_7d_change'],0))<0 else 'more',
            dif_7d=int(abs(round(latest_7d['rolling_7d_change'],0))),
            pct_7d=latest_7d['rolling_7d_change']/(latest_7d['rolling_7d_change']+(latest_7d['ROLLING 7 DAY POSITIVE TESTS']*7)))
        tweets.append({'text': tweet, 'url': change['url'], 'notweet': change.get('notweet'), 'totals': totals, 'filedate': change['filedate']})

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
                    index[i]['totals'] = tweets[idx]['totals']
                    break
            status.put_dict(index)

            messages[-1] += ('updated %s' %secret['doh-dd-index'])
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
