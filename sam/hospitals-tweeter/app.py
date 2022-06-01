import json
import io
import datetime
import os
import logging

import boto3
import pandas
import numpy
import altair

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver, plot_key_ni_stats_date_range, plot_points_average_and_trend, output_plot, plot_heatmap
from data_shared import get_ni_pop_pyramid, update_datastore

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

def load_ni_time_series(url, sheet_name, date_col, series_col, model=False, filter_col=None, filter=None):
    df = pandas.read_excel(url, engine='openpyxl', sheet_name=sheet_name)
    if filter_col is not None:
        df = df[df[filter_col] == filter]
    df = df.groupby(date_col)[series_col].sum().reset_index()
    df.set_index(date_col, inplace=True)
    newind = pandas.date_range(start=df.index.min(), end=df.index.max())
    df = df.reindex(newind)
    df.index.name = date_col
    df.reset_index(inplace=True)
    df.fillna(0, inplace=True)
    df['%s 7-day rolling mean' %series_col] = df[series_col].rolling(7, center=True).mean()
    if model is True:
        df = create_model(df, '%s 7-day rolling mean' %series_col, date_col)
    return df

def plot_hospital_stats(adm_dis_7d, inpatients, start_date, scale='linear'):
    return plot_points_average_and_trend(
        [
            {
                'points': None,
                'line': adm_dis_7d[(adm_dis_7d['Date'] > start_date)].set_index(['Date','variable'])['value'],
                'colour': 'variable',
                'date_col': 'Date',
                'x_title': 'Date',
                'y_title': 'Number of people (7-day average)',
                'scales': [scale],
                'height': 225
            },
            {
                'points': None,
                'line': inpatients[(inpatients['Date'] > start_date)].set_index(['Date'])['Number of Confirmed COVID Inpatients'],
                'colour': 'red',
                'date_col': 'Date',
                'x_title': 'Date',
                'y_title': 'Inpatients (with confirmed COVID-19)',
                'scales': [scale],
                'height': 225
            },
        ],
        '%s COVID-19 %s (%s scale) reported on %s' %(
            'NI',
            'hospital admissions, discharges and inpatients',
            scale,
            datetime.datetime.today().strftime('%A %-d %B %Y'),
        ),
        [
            'Hospital data from DoH weekly data',
            'Last two days (five for admissions) may be revised upwards',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ]
    )

def lambda_handler(event, context):
    messages = ['Failure']

    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    try:
        # Get the index
        s3 = boto3.client('s3')
        status = S3_scraper_index(s3, secret['bucketname'], secret['doh-hospital-index'])
        index = status.get_dict()

        tweets = []
        # Download the most recently updated Excel file
        for change in event:
            obj = s3.get_object(Bucket=secret['bucketname'],Key=change['keyname'])['Body']
            stream = io.BytesIO(obj.read())

            # Summary stats to allow 'X registered in last 24 hours' info
            admissions = load_ni_time_series(stream,'Admissions','Admission Date','Number of Admissions',True)
            discharges = load_ni_time_series(stream,'Discharges','Discharge Date','Number of Discharges')
            inpatients = load_ni_time_series(stream,'Inpatients','Inpatients at Midnight','Number of Confirmed COVID Inpatients',False,'Sex','All')
            inpatients.rename(columns={'Inpatients at Midnight': 'Date'}, inplace=True)
            totals = {
                'admissions': int(admissions['Number of Admissions'].sum()),
                'discharges': int(discharges['Number of Discharges'].sum())
            }
            print(totals)
            latest_adm_model = admissions.iloc[admissions[admissions['Number of Admissions 7-day rolling mean model_daily_change'].notna()]['Admission Date'].idxmax()]
            adm_dis = admissions.merge(discharges, how='inner', left_on='Admission Date', right_on='Discharge Date', validate='1:1')
            adm_dis.drop(columns=['Discharge Date'], inplace = True)
            adm_dis.rename(columns={'Admission Date': 'Date'}, inplace = True)
            adm_dis['Inpatients'] = adm_dis['Number of Admissions 7-day rolling mean'].cumsum() - adm_dis['Number of Discharges 7-day rolling mean'].cumsum()
            adm_dis_7d = adm_dis.rename(columns={'Number of Admissions 7-day rolling mean': 'Admissions','Number of Discharges 7-day rolling mean': 'Discharges'})[['Date','Admissions','Discharges']]
            adm_dis_7d = adm_dis_7d.melt(id_vars='Date')

            # Plot the case reports and 7-day average
            driver = get_chrome_driver()
            plots = []
            if driver is not None:
                today_str = datetime.datetime.now().date().strftime('%Y-%m-%d')
                p = plot_hospital_stats(adm_dis_7d, inpatients, inpatients['Date'].max() - pandas.to_timedelta(42, unit='d'))
                plots = output_plot(p, plots, driver, 'ni-hospitals-%s.png' % today_str)

            # Build the tweet text
            last_week = datetime.datetime.strptime(change['filedate'],'%Y-%m-%d').date() - datetime.timedelta(days=7)
            lastweek = None
            for report in index:
                if (report['filedate'] == last_week.strftime('%Y-%m-%d')) and ('totals' in report):
                    lastweek = report
                    break
            tweet= '''{inpatients} inpatient{ips} reported on {date}'''.format(
                    date=inpatients['Date'].max().strftime('%A %-d %B %Y'),
                    inpatients=totals['admissions'] - totals['discharges'],
                    ips='s' if ((totals['admissions'] - totals['discharges']) != 1) else ''
            )
            if lastweek is not None:
                ip_change = (totals['admissions'] - totals['discharges']) - (lastweek['totals']['admissions'] - lastweek['totals']['discharges'])
                tweet += ''':
{ip_bullet} {ip_change} {ip_text} than 7 days ago ({admissions} admitted, {discharges} discharged)'''.format(
                    ip_change=abs(ip_change),
                    ip_bullet=good_symb if ip_change < 0 else bad_symb,
                    ip_text='fewer' if ip_change < 0 else 'more',
                    admissions=totals['admissions'] - lastweek['totals']['admissions'],
                    discharges=totals['discharges'] - lastweek['totals']['discharges']
                )

            tweet += '''

{tag_model} admissions {dir_model} by {model_daily:.1%} per day, {model_weekly:.1%} per week, {doub} time {doub_time:.1f} days
'''.format(
                model_daily=abs(latest_adm_model['Number of Admissions 7-day rolling mean model_daily_change']),
                model_weekly=abs(latest_adm_model['Number of Admissions 7-day rolling mean model_weekly_change']),
                dir_model='falling' if latest_adm_model['Number of Admissions 7-day rolling mean model_daily_change']<0 else 'rising',
                tag_model=good_symb if latest_adm_model['Number of Admissions 7-day rolling mean model_daily_change']<0 else bad_symb,
                doub='halving' if (latest_adm_model['Number of Admissions 7-day rolling mean model0'] < 0) else 'doubling',
                doub_time=abs(numpy.log(2)/latest_adm_model['Number of Admissions 7-day rolling mean model0'])
            )

            tweets.append({
                'text': tweet,
                'url': change['url'],
                'notweet': change.get('notweet', False),
                'tweet': change.get('testtweet', False),
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
                    if t['testtweet'] is False:
                        if len(t['plots']) > 0:
                            resp = api.tweet(t['text'] + t['url'], media_ids=upload_ids)
                        else:
                            resp = api.tweet(t['text'] + t['url'])
                        messages.append('Tweeted ID %s, ' %resp.id)

                        # Update the file index
                        for i in range(len(index)):
                            if index[i]['filedate'] == t['filedate']:
                                index[i]['tweet'] = resp.id
                                index[i]['totals'] = t['totals']
                                break
                        status.put_dict(index)

                        messages[-1] += ('updated %s' %secret['doh-hospital-index'])
                    else:
                        if len(upload_ids) > 0:
                            resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'], upload_ids[0])
                        else:
                            resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'])
                        messages.append('Tweeted DM %s, ' %resp.id)
                else:
                    messages.append('Duplicate found %s, did not tweet, ' %t['filedate'])
            else:
                if (idx not in donottweet):
                    messages.append('Did not tweet')
                    print(t['text'] + t['url'])
                else:
                    messages.append('Duplicate found %s, did not tweet, ' %t['filedate'])
    except:
        logging.exception('Caught error in hospitals tweeter')
        api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
        api.dm(secret['twitter_dmaccount'], 'Error in hospitals tweeter')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
