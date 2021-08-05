import json
import io
import datetime
import os
import logging

import boto3
import pandas
import numpy

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver, plot_key_ni_stats_date_range

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
        admissions = admissions.groupby('Admission Date')['Number of Admissions'].sum().reset_index()
        admissions.set_index('Admission Date', inplace=True)
        newind = pandas.date_range(start=admissions.index.min(), end=admissions.index.max())
        admissions = admissions.reindex(newind)
        admissions.index.name = 'Admission Date'
        admissions.reset_index(inplace=True)
        admissions.fillna(0, inplace=True)
        admissions['Number of Admissions 7-day rolling mean'] = admissions['Number of Admissions'].rolling(7, center=True).mean()
        admissions = create_model(admissions,'Number of Admissions 7-day rolling mean','Admission Date')
        latest_adm_model = admissions.iloc[admissions[admissions['Number of Admissions 7-day rolling mean model_daily_change'].notna()]['Admission Date'].idxmax()]
        deaths = deaths.groupby('Date of Death')['Number of Deaths'].sum().reset_index()
        deaths.set_index('Date of Death', inplace=True)
        newind = pandas.date_range(start=deaths.index.min(), end=deaths.index.max())
        deaths = deaths.reindex(newind)
        deaths.index.name = 'Date of Death'
        deaths.reset_index(inplace=True)
        deaths.fillna(0, inplace=True)
        deaths['Number of Deaths 7-day rolling mean'] = deaths['Number of Deaths'].rolling(7, center=True).mean()

        # Plot the case reports and 7-day average
        driver = get_chrome_driver()
        plots = []
        if driver is not None:
            p = plot_key_ni_stats_date_range(df, admissions, deaths, latest['Sample_Date'] - pandas.to_timedelta(42, unit='d'), latest['Sample_Date'], 'linear')
            try:
                plot = {'name': None, 'store': io.BytesIO()}
                p.save(fp=plot['store'], format='png', method='selenium', webdriver=driver)
                plots.append(plot)
            except:
                logging.exception('Failed to output plot')
                with open('/tmp/chromedriver.log') as log:
                    logging.warning(log.read())
                logging.error([f for f in os.listdir('/tmp/')])
            else:
                plots[-1]['store'].seek(0)
                plots[-1]['name'] = 'ni-cases-%s.png' % datetime.datetime.now().date().strftime('%Y-%d-%m')
                p = plot_key_ni_stats_date_range(df, admissions, deaths, latest['Sample_Date'] - pandas.to_timedelta(42, unit='d'), latest['Sample_Date'], 'log')
                try:
                    plot = {'name': None, 'store': io.BytesIO()}
                    p.save(fp=plot['store'], format='png', method='selenium', webdriver=driver)
                    plots.append(plot)
                except:
                    logging.exception('Failed to output plot')
                    with open('/tmp/chromedriver.log') as log:
                        logging.warning(log.read())
                    logging.error([f for f in os.listdir('/tmp/')])
                else:
                    plots[-1]['store'].seek(0)
                    plots[-1]['name'] = 'ni-admissions-%s.png' % datetime.datetime.now().date().strftime('%Y-%d-%m')

        # Find the date since which the rate was as high/low
        symb_7d, est = find_previous(df, latest_7d, 'ROLLING 7 DAY POSITIVE TESTS')

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
            model_daily=abs(last_but1_model['Rolling cases per 100k model_daily_change']),
            model_weekly=abs(last_but1_model['Rolling cases per 100k model_weekly_change']),
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
        tweet2 = '''{inpatients} inpatient{ips} reported'''.format(
                inpatients=totals['admissions'] - totals['discharges'],
                ips='s' if ((totals['admissions'] - totals['discharges']) != 1) else ''
        )
        if lastweek is not None:
            ip_change = (totals['admissions'] - totals['discharges']) - (lastweek['totals']['admissions'] - lastweek['totals']['discharges'])
            tweet2 += ''':
{ip_bullet} {ip_change} {ip_text} than 7 days ago ({admissions} admitted, {discharges} discharged)'''.format(
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

        tweet2 += '''

{tag_model} admissions {dir_model} by {model_daily:.1%} per day, {model_weekly:.1%} per week, {doub} time {doub_time:.1f} days'''.format(
            model_daily=abs(latest_adm_model['Number of Admissions 7-day rolling mean model_daily_change']),
            model_weekly=abs(latest_adm_model['Number of Admissions 7-day rolling mean model_weekly_change']),
            dir_model='falling' if latest_adm_model['Number of Admissions 7-day rolling mean model_daily_change']<0 else 'rising',
            tag_model=good_symb if latest_adm_model['Number of Admissions 7-day rolling mean model_daily_change']<0 else bad_symb,
            doub='halving' if (latest_adm_model['Number of Admissions 7-day rolling mean model0'] < 0) else 'doubling',
            doub_time=abs(numpy.log(2)/latest_adm_model['Number of Admissions 7-day rolling mean model0'])
        )

        tweets.append({
            'text': tweet,
            'text2': tweet2,
            'url': change['url'],
            'notweet': change.get('notweet', False),
            'tweet': change.get('tweet', True),
            'totals': totals,
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
                upload_ids = api.upload_multiple(t['plots'])
                if t['tweet'] is True:
                    if len(t['plots']) > 0:
                        resp = api.tweet(t['text'] + t['url'], media_ids=upload_ids)
                    else:
                        resp = api.tweet(t['text'] + t['url'])
                    messages.append('Tweeted ID %s, ' %resp.id)
                    if t['text2'] is not None:
                        resp = api.tweet(t['text2'], resp.id)
                        messages[-1] += ('ID %s, ' %resp.id)
                else:
                    if len(upload_ids) > 0:
                        resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'], upload_ids[0])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'])
                    messages.append('Tweeted DM %s, ' %resp.id)
                    if len(upload_ids) > 1:
                        resp = api.dm(secret['twitter_dmaccount'], t['text2'], upload_ids[1])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], t['text2'])
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
