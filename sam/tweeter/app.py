import json
import io
import datetime
import os
import logging

import boto3
import pandas
import numpy
import altair
from selenium import webdriver

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

good_symb = '\u2193'
bad_symb = '\u2191'

def find_previous(df, newest, colname):
    # Find the date since which the rate was as high/low
    gte = df[(df[colname] >= newest[colname]) & (df['Sample_Date'] < newest['Sample_Date'])]
    lt = df[(df[colname] < newest[colname]) & (df['Sample_Date'] < newest['Sample_Date'])]
    if len(gte)>0:
        gte = df.iloc[gte['Sample_Date'].idxmax()]
    else:
        return bad_symb, 'highest ever'
    if len(lt)>0:
        lt = df.iloc[lt['Sample_Date'].idxmax()]
    else:
        return good_symb, 'lowest ever'
    if gte['Sample_Date'] < lt['Sample_Date']:
        est = bad_symb
        diff = (newest['Sample_Date'] - gte['Sample_Date']).days
        prev = 'highest for %s day%s' %(diff,'s' if diff > 1 else '')
    else:
        est = good_symb
        diff = (newest['Sample_Date'] - lt['Sample_Date']).days
        prev = 'lowest for %s day%s' %(diff,'s' if diff > 1 else '')
    return est, prev

def calc_exp_fit0(data):
    curve = numpy.polyfit(data.index, numpy.log(data.values), 1)
    return curve[0]

def calc_exp_fit1(data):
    curve = numpy.polyfit(data.index, numpy.log(data.values), 1)
    return curve[1]

def fit_exp(curve0, curve1, value):
    return (numpy.exp(curve1) * numpy.exp(curve0 * value))

def create_model(df, to_model, datekey):
    df['x'] = (df[datekey] - df[datekey].min()).dt.days
    df.set_index('x', inplace=True)
    df['%s model0'%to_model] = df.rolling(window=9, center=True)[to_model].apply(calc_exp_fit0)
    df['%s model1'%to_model] = df.rolling(window=9, center=True)[to_model].apply(calc_exp_fit1)
    df['%s model_daily_change' %to_model] = (fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 2) - fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)) / fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)
    df['%s model_weekly_change' %to_model] = (fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 8) - fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)) / fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)
    return(df)

# %%
def plot_points_average_and_trend(df, colour, date):
    df1 = df[(~df['INDIVIDUALS TESTED POSITIVE'].isna()) & (df['INDIVIDUALS TESTED POSITIVE'] != 0)]
    df2 = df[(~df['New cases 7-day rolling mean'].isna()) & (df['New cases 7-day rolling mean'] != 0)]
    return altair.concat(altair.layer(
        altair.Chart(
            df1
        ).mark_point(
            color=colour,
            opacity=0.7,
            filled=True,
            size=15,
        ).encode(
            x=altair.X(
                field='Sample_Date',
                type='temporal',
                axis=altair.Axis(title='Specimen Date'),
            ),
            y=altair.Y(
                field='INDIVIDUALS TESTED POSITIVE',
                type='quantitative',
                aggregate='sum',
                axis=altair.Axis(title='Individuals tested positive'),
                scale=altair.Scale(
                    type='log'
                ),
            )
        ),
        altair.Chart(
            df2
        ).mark_line(
            color=colour
        ).encode(
            x=altair.X(
                field='Sample_Date',
                type='temporal'
            ),
            y=altair.Y(
                field='New cases 7-day rolling mean',
                type='quantitative',
                aggregate='sum',
                scale=altair.Scale(
                    type='log'
                ),
                axis=altair.Axis(title=''),
            )
        ),
    ).properties(
        title=altair.TitleParams(
            ['Dots show daily case reports, line is 7-day rolling average',
            'https://twitter.com/ni_covid19_data'],
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
        height=450,
        width=800
    )).properties(
        title=altair.TitleParams(
            'NI COVID-19 cases (daily and 7-day mean) reported on %s' %date,
            anchor='middle',
        )
    )

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the index
    s3 = boto3.client('s3')
    status = S3_scraper_index(s3, secret['bucketname'], secret['doh-dd-index'])
    index = status.get_dict()

    tweets = []
    # Download the most recently updated Excel file
    for change in event:
        obj = s3.get_object(Bucket=secret['bucketname'],Key=change['keyname'])['Body']
        stream = io.BytesIO(obj.read())

        # Load test data and add extra fields
        df = pandas.read_excel(stream,engine='openpyxl',sheet_name='Summary Tests')
        df['pos_rate'] = df['INDIVIDUALS TESTED POSITIVE']/df['ALL INDIVIDUALS TESTED']
        df['rolling_pos_rate'] = df['ROLLING 7 DAY POSITIVE TESTS']/df['ROLLING 7 DAY INDIVIDUALS TESTED']
        df['printdate']=df['Sample_Date'].dt.strftime('%-d %B %Y')
        df['rolling_7d_change'] = (df['ROLLING 7 DAY POSITIVE TESTS'] - df['ROLLING 7 DAY POSITIVE TESTS'].shift(7)) * 7
        df['New cases 7-day rolling mean'] = df['INDIVIDUALS TESTED POSITIVE'].rolling(7, center=True).mean()
        df.set_index('Sample_Date', inplace=True)
        newind = pandas.date_range(start=df.index.min(), end=df.index.max())
        df = df.reindex(newind)
        df.index.name = 'Sample_Date'
        df.reset_index(inplace=True)
        df['Rolling cases per 100k'] = 100000 * (df['New cases 7-day rolling mean'] / 1893667)
        df = create_model(df,'Rolling cases per 100k','Sample_Date')

        # Get the latest dates with values for tests and rolling
        latest = df.iloc[df['Sample_Date'].idxmax()]
        latest_7d = df.iloc[df[df['ROLLING 7 DAY POSITIVE TESTS'].notna()]['Sample_Date'].idxmax()]
        latest_model = df.iloc[df[df['Rolling cases per 100k model_daily_change'].notna()]['Sample_Date'].idxmax()]
        last_but1_model = df.iloc[df[(df['Rolling cases per 100k model_daily_change'].notna()) & (df['Sample_Date'] != latest_model['Sample_Date'])]['Sample_Date'].idxmax()]

        # Plot the case reports and 7-day average
        options = webdriver.ChromeOptions()
        options.headless = True
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--window-size=1280,720")
        options.add_argument("--disable-gpu")
        options.add_argument("--hide-scrollbars")
        options.add_argument("--disable-infobars")
        options.add_argument("--enable-logging")
        options.add_argument("--log-level=0")
        options.add_argument("--v=99")
        options.add_argument("--single-process")
        options.add_argument("--user-data-dir=/tmp/user-data/")
        options.add_argument("--data-path=/tmp/data/")
        options.add_argument("--homedir=/tmp/homedir/")
        options.add_argument("--disk-cache-dir=/tmp/disk-cache/")
        options.add_argument("--disable-async-dns")
        plotname = None
        plotstore = io.BytesIO()
        try:
            driver = webdriver.Chrome(service_log_path='/tmp/chromedriver.log', options=options)
        except:
            logging.exception('Failed to setup chromium')
            with open('/tmp/chromedriver.log') as log:
                logging.warning(log.read())
            logging.error([f for f in os.listdir('/tmp/')])
        else:
            p = plot_points_average_and_trend(df[(df['Sample_Date'] > (latest['Sample_Date'] - pandas.to_timedelta(42, unit='d')))],'#076543',latest['Sample_Date'].strftime('%A %-d %B %Y'))
            try:
                p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            except:
                logging.exception('Failed to output plot')
                with open('/tmp/chromedriver.log') as log:
                    logging.warning(log.read())
                logging.error([f for f in os.listdir('/tmp/')])
            else:
                plotstore.seek(0)
                plotname = 'ni-cases-%s.png' % datetime.datetime.now().date().strftime('%Y-%d-%m')

        # Find the date since which the rate was as high/low
        symb_7d, est = find_previous(df, latest_7d, 'ROLLING 7 DAY POSITIVE TESTS')

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

        # Build the tweet text
        tweet = '''{ind_tested:,} people tested, {ind_positive:,} ({pos_rate:.2%}) positive on {date}

{symb_7d} {pos_7d:,} positive in last 7 days, {est}

{tag_model} cases {dir_model} by {model_daily:.1%} per day, {model_weekly:.1%} per week, {doub} time {doub_time:.1f} days

'''.format(
            date=latest['Sample_Date'].strftime('%A %-d %B %Y'),
            ind_positive=int(latest['INDIVIDUALS TESTED POSITIVE']),
            ind_tested=int(latest['ALL INDIVIDUALS TESTED']),
            pos_rate=latest['pos_rate'],
            symb_7d=symb_7d,
            est=est,
            model_daily=last_but1_model['Rolling cases per 100k model_daily_change'],
            model_weekly=last_but1_model['Rolling cases per 100k model_weekly_change'],
            pos_7d=int(round(latest_7d['ROLLING 7 DAY POSITIVE TESTS']*7,0)),
            dir_model='falling' if last_but1_model['Rolling cases per 100k model_daily_change']<0 else 'rising',
            tag_model=good_symb if last_but1_model['Rolling cases per 100k model_daily_change']<0 else bad_symb,
            doub='halving' if (last_but1_model['Rolling cases per 100k model0'] < 0) else 'doubling',
            doub_time=abs(numpy.log(2)/last_but1_model['Rolling cases per 100k model0'])
        )

        # If we have the data for it, build the second tweet
        last_week = datetime.datetime.strptime(change['filedate'],'%Y-%m-%d').date() - datetime.timedelta(days=7)
        day_before = datetime.datetime.strptime(change['filedate'],'%Y-%m-%d').date() - datetime.timedelta(days=1)
        yesterday = None
        lastweek = None
        for report in index:
            if (report['filedate'] == last_week.strftime('%Y-%m-%d')) and ('totals' in report):
                lastweek = report
            elif (report['filedate'] == day_before.strftime('%Y-%m-%d')) and ('totals' in report):
                yesterday = report
            if (yesterday is not None) and (lastweek is not None):
                break
        tweet2 = None
        if lastweek is not None:
            ip_change = (totals['admissions'] - totals['discharges']) - (lastweek['totals']['admissions'] - lastweek['totals']['discharges'])
            tweet2 = '''{inpatients} inpatient{ips} reported:
{ip_bullet} {ip_change} {ip_text} than 7 days ago ({admissions} admitted, {discharges} discharged)'''.format(
                inpatients=totals['admissions'] - totals['discharges'],
                ips='s' if (totals['admissions'] - totals['discharges']) else '',
                ip_change=abs(ip_change),
                ip_bullet=good_symb if ip_change < 0 else bad_symb,
                ip_text='fewer' if ip_change < 0 else 'more',
                admissions=totals['admissions'] - lastweek['totals']['admissions'],
                discharges=totals['discharges'] - lastweek['totals']['discharges']
            )
            if yesterday is not None:
                tweet2 += '''

{deaths} death{ds} reported, {deaths_7d} in last 7 days'''.format(
                    deaths=totals['deaths'] - yesterday['totals']['deaths'],
                    ds='s' if ((totals['deaths'] - yesterday['totals']['deaths']) != 1) else '',
                    deaths_7d=totals['deaths'] - lastweek['totals']['deaths']
                )

        tweets.append({
            'text': tweet,
            'text2': tweet2,
            'url': change['url'],
            'notweet': change.get('notweet', False),
            'tweet': change.get('tweet', True),
            'totals': totals,
            'filedate': change['filedate'],
            'plotname': plotname,
            'plot': plotstore
        })

    donottweet = []
    if len(tweets) > 1:
        for i in range(1,len(tweets)):
            for j in range(0, i):
                if (tweets[i]['text'] == tweets[j]['text']):
                    donottweet.append(i)

    messages = []
    for idx in reversed(range(len(tweets))):
        t = tweets[idx]
        if t['notweet'] is False:
            if (idx not in donottweet):
                api = TwitterAPI(
                    secret['twitter_apikey'],
                    secret['twitter_apisecretkey'],
                    secret['twitter_accesstoken'],
                    secret['twitter_accesstokensecret']
                )
                if t['plotname'] is not None:
                    resp = api.upload(t['plot'], t['plotname'])
                if t['tweet'] is True:
                    if t['plotname'] is not None:
                        resp = api.tweet(t['text'] + t['url'], media_ids=[resp.media_id])
                    else:
                        resp = api.tweet(t['text'] + t['url'])
                    messages.append('Tweeted ID %s, ' %resp.id)
                    if t['text2'] is not None:
                        resp = api.tweet(t['text2'], resp.id)
                        messages[-1] += ('ID %s, ' %resp.id)
                else:
                    if t['plotname'] is not None:
                        resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'], resp.media_id)
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'])
                    messages.append('Tweeted DM %s, ' %resp.id)
            else:
                messages.append('Duplicate found %s, did not tweet, ' %t['filedate'])

            # Update the file index
            for i in range(len(index)):
                if index[i]['filedate'] == t['filedate']:
                    index[i]['tweet'] = resp.id
                    index[i]['totals'] = t['totals']
                    break
            status.put_dict(index)

            messages[-1] += ('updated %s' %secret['doh-dd-index'])
        else:
            if (idx not in donottweet):
                messages.append('Did not tweet')
                print(t['text'] + t['url'])
                if t['text2'] is not None:
                    print(t['text2'])
            else:
                messages.append('Duplicate found %s, did not tweet, ' %t['filedate'])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
