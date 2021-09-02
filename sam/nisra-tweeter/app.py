import json
import io
import datetime
import logging

import boto3
import pandas
import altair

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver

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

        # Get the latest dates with values for tests and rolling
        df['date'] = pandas.to_datetime(df['Week Ending'], format='%d/%m/%Y')
        df.sort_values('date', inplace=True)
        latest = df.iloc[-1]

        # Check against previous day's reports
        status = S3_scraper_index(s3, secret['bucketname'], secret['nisra-deaths-index'])
        index = status.get_dict()
        plots = []
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
            try:
                driver = get_chrome_driver()
                plots = []
                if driver is None:
                    logging.error('Failed to start chrome')
                else:
                    toplot = df[(df['Week Ending'] > df['Week Ending'].max()-pandas.to_timedelta(84, unit='d'))]
                    toplot = toplot.drop(columns=['Week of Death','date','Total']).melt(id_vars='Week Ending', var_name='Location', value_name='Deaths')
                    print(toplot)
                    p = altair.vconcat(
                        altair.Chart(
                            toplot
                        ).mark_area().encode(
                            x = altair.X('Week Ending:T', axis=altair.Axis(title='Week of death')),
                            y = altair.Y('sum(Deaths):Q', axis=altair.Axis(title='Deaths', orient="right", tickMinStep=1)),
                            color=altair.Color('Location', sort=altair.SortField('order',order='descending')),
                        ).properties(
                            height=450,
                            width=800,
                            title='NI COVID-19 Deaths reported by NISRA from %s to %s' %(toplot['Week Ending'].min().strftime('%-d %B %Y'), toplot['Week Ending'].max().strftime('%-d %B %Y'))
                        ),
                    ).properties(
                        title=altair.TitleParams(
                            ['Data from NISRA',
                            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                            baseline='bottom',
                            orient='bottom',
                            anchor='end',
                            fontWeight='normal',
                            fontSize=10,
                            dy=10
                        ),
                    )
                    plotname = 'nisra-deaths-time-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
                    plotstore = io.BytesIO()
                    p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
                    plotstore.seek(0)
                    plots.append({'name': plotname, 'store': plotstore})
            except:
                logging.exception('Error creating plot')

        tweets.append({
            'text': tweet,
            'url': change['url'],
            'notweet': change.get('notweet'),
            'filedate': change['filedate'],
            'plots': plots
        })

    donottweet = []
    if len(tweets) > 1:
        for i in range(1,len(tweets)):
            for j in range(0, i):
                if (tweets[i]['text'] == tweets[j]['text']):
                    donottweet.append(i)

    messages = []
    for idx in range(len(tweets)):
        tweet = tweets[idx]['text'] + tweets[idx]['url']
        if (idx not in donottweet):
            if tweets[idx].get('notweet') is not True:
                api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
                upload_ids = api.upload_multiple(tweets[idx]['plots'])
                if change.get('testtweet') is True:
                    if len(upload_ids) > 0:
                        resp = api.dm(secret['twitter_dmaccount'], tweet, upload_ids[0])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], tweet)
                    messages.append('Tweeted DM ID %s' %(resp.id))
                else:
                    if len(upload_ids) > 0:
                        resp = api.tweet(tweet, media_ids=upload_ids)
                    else:
                        resp = api.tweet(tweet)
                    messages.append('Tweeted ID %s, ' %resp.id)

                    # Update the file index
                    for i in range(len(index)):
                        if index[i]['filedate'] == tweets[idx]['filedate']:
                            index[i]['tweet'] = resp.id
                            break
                    status.put_dict(index)

                    messages[-1] += ('updated %s' %secret['nisra-deaths-index'])
            else:
                messages.append('Did not tweet')
                print(tweet)
        else:
            messages.append('Duplicate found %s, did not tweet, ' %tweets[idx]['filedate'])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
