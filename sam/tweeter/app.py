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
from data_shared import get_s3_csv_or_empty_df, push_csv_to_s3, get_ni_pop_pyramid

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

def plot_hospital_stats(adm_dis_7d, inpatients, icu, start_date, scale='linear'):
    return plot_points_average_and_trend(
        [
            {
                'points': None,
                'line': adm_dis_7d[(adm_dis_7d['Date'] > start_date)].set_index(['Date','variable'])['value'],
                'colour': 'variable',
                'date_col': 'Date',
                'x_title': 'Date',
                'y_title': 'Number of people (7-day average)',
                'scale': scale,
                'height': 225
            },
            {
                'points': None,
                'line': inpatients[(inpatients['Date'] > start_date)].set_index(['Date'])['Number of Confirmed COVID Inpatients'],
                'colour': 'red',
                'date_col': 'Date',
                'x_title': 'Date',
                'y_title': 'Inpatients (with confirmed COVID-19)',
                'scale': scale,
                'height': 225
            },
            {
                'points': None,
                'line': icu[(icu['Date'] > start_date)].set_index('Date')['Confirmed COVID Occupied'],
                'colour': 'black',
                'date_col': 'Date',
                'x_title': 'Date',
                'y_title': 'ICU Beds COVID Occupied',
                'scale': scale,
                'height': 225
            },
        ],
        '%s COVID-19 %s (%s scale) reported on %s' %(
            'NI',
            'hospital admissions, discharges, inpatients and ICU',
            scale,
            datetime.datetime.today().strftime('%A %-d %B %Y'),
        ),
        [
            'Hospital data from DoH daily data',
            'Last two days may be revised upwards due to reporting delays',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ]
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

        # Summary stats to allow 'X registered in last 24 hours' info
        deaths = load_ni_time_series(stream,'Deaths','Date of Death','Number of Deaths')
        admissions = load_ni_time_series(stream,'Admissions','Admission Date','Number of Admissions',True)
        discharges = load_ni_time_series(stream,'Discharges','Discharge Date','Number of Discharges')
        inpatients = load_ni_time_series(stream,'Inpatients','Inpatients at Midnight','Number of Confirmed COVID Inpatients',False,'Sex','All')
        inpatients.rename(columns={'Inpatients at Midnight': 'Date'}, inplace=True)
        icu = load_ni_time_series(stream,'ICU','Date','Confirmed COVID Occupied')
        totals = {
            'ind_tested': int(df['ALL INDIVIDUALS TESTED'].sum()),
            'ind_positive': int(df['INDIVIDUALS TESTED POSITIVE'].sum()),
            'deaths': int(deaths['Number of Deaths'].sum()),
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

        # Age band data
        age_bands = pandas.read_excel(stream,engine='openpyxl',sheet_name='Individuals 7 Days - 5yr Age')
        age_bands['Total_Tests'] = age_bands['Positive_Tests'] + age_bands['Negative_Tests'] + age_bands['Indeterminate_Tests']
        age_bands = age_bands.groupby('Age_Band_5yr').sum()[['Positive_Tests','Total_Tests']].reset_index()
        age_bands['Positivity_Rate'] = age_bands['Positive_Tests'] / age_bands['Total_Tests']
        age_bands['Band Start'] = age_bands['Age_Band_5yr'].str.extract('Aged (\d+)').astype(float)
        age_bands['Band End'] = age_bands['Age_Band_5yr'].str.extract('Aged \d+ - (\d+)').astype(float)
        age_bands['Date'] = df['Sample_Date'].max()
        # Get the age bands datastore contents from S3
        s3dir = change['keyname'].split('/',maxsplit=1)[0]
        agebands_keyname = '%s/agebands.csv' %s3dir
        datastore = get_s3_csv_or_empty_df(s3, secret['bucketname'], agebands_keyname, ['Date'])
        # Remove any data already recorded for the current day
        datastore['Date'] = pandas.to_datetime(datastore['Date'])
        datastore = datastore[datastore['Date'] != df['Sample_Date'].max()]
        # Append the new data
        datastore = pandas.concat([datastore, age_bands])
        # Send back to S3
        push_csv_to_s3(datastore, s3, secret['bucketname'], agebands_keyname)
        # Plot the case reports and 7-day average
        driver = get_chrome_driver()
        plots = []
        if driver is not None:
            today_str = datetime.datetime.now().date().strftime('%Y-%m-%d')
            p = plot_key_ni_stats_date_range(df, admissions, deaths, latest['Sample_Date'] - pandas.to_timedelta(42, unit='d'), latest['Sample_Date'], 'linear')
            plots = output_plot(p, plots, driver, 'ni-cases-linear-%s.png' % today_str)
            if len(plots) > 0:
                p = plot_key_ni_stats_date_range(df, admissions, deaths, latest['Sample_Date'] - pandas.to_timedelta(42, unit='d'), latest['Sample_Date'], 'log')
                plots = output_plot(p, plots, driver, 'ni-cases-log-%s.png' % today_str)
                if len(plots) > 1:
                    p = plot_hospital_stats(adm_dis_7d, inpatients, icu, latest['Sample_Date'] - pandas.to_timedelta(42, unit='d'))
                    plots = output_plot(p, plots, driver, 'ni-hospitals-%s.png' % today_str)
                    if len(plots) > 2:
                        toplot = datastore[datastore['Date'] >= (datastore['Date'].max() + pandas.DateOffset(days=-42))]
                        toplot['Date'] = pandas.to_datetime(toplot['Date'])
                        newind = pandas.date_range(start=toplot['Date'].max() + pandas.DateOffset(days=-42), end=toplot['Date'].max())
                        alldates = pandas.Series(newind)
                        alldates.name = 'Date'
                        toplot = toplot.merge(alldates, how='outer', left_on='Date', right_on='Date')
                        toplot['X'] = toplot['Date'].dt.strftime('%e %b')
                        toplot['Most Recent Positive Tests'] = toplot['Positive_Tests'].where(toplot['Date'] == toplot['Date'].max()).apply(lambda x: f"{x:n}" if not pandas.isna(x) else "")
                        toplot['Age_Band_5yr'].fillna('Not Known', inplace=True)
                        bands = toplot.groupby(['Age_Band_5yr','Band Start','Band End'], dropna=False).size().reset_index()[['Age_Band_5yr','Band Start','Band End']]
                        bands = bands[bands['Age_Band_5yr']!='Not Known']
                        bands.fillna(90, inplace=True)
                        bands['Band End'] = bands['Band End'].astype(int)
                        bands['Band Start'] = bands['Band Start'].astype(int)
                        bands['Year'] = bands.apply(lambda x: range(x['Band Start'], x['Band End']+1), axis='columns')
                        bands = bands.explode('Year').reset_index()
                        pops = get_ni_pop_pyramid()
                        pops = pops[pops['Year']==2020].groupby(['Age Band']).sum()['Population']
                        bands = bands.merge(pops, how='inner', validate='1:1', right_index=True, left_on='Year')
                        bands = bands.groupby('Age_Band_5yr').sum()['Population']
                        toplot = toplot.merge(bands, how='left', on='Age_Band_5yr')
                        toplot['Positive per 100k'] = (100000 * toplot['Positive_Tests']) / toplot['Population']
                        toplot['Most Recent Positive per 100k'] = toplot['Positive per 100k'].where(toplot['Date'] == toplot['Date'].max()).apply(lambda x: f"{int(x):n}" if not pandas.isna(x) else "")
                        heatmap2 = plot_heatmap(toplot,'X','Date','Date','Age_Band_5yr','Band Start','Age Band','Positive per 100k','Positive Tests (per 100k people)')

                        p = altair.vconcat(
                            altair.layer(
                                heatmap.properties(
                                    height=450,
                                    width=800,
                                    title='NI COVID-19 Positive Tests by Age Band from %s to %s' %(toplot['Date'].min().strftime('%-d %B %Y'),toplot['Date'].max().strftime('%-d %B %Y')),
                                ),
                                heatmap.mark_text(
                                    align='right',
                                    baseline='middle',
                                    dx=43
                                ).encode(
                                    text = altair.Text('Most Recent Positive Tests'),
                                    color = altair.value('black')
                                )
                            ),
                            altair.layer(
                                heatmap2.properties(
                                    height=450,
                                    width=800,
                                    title='NI COVID-19 Positive Tests by Age Band per 100k people',
                                ),
                                heatmap2.mark_text(
                                    align='right',
                                    baseline='middle',
                                    dx=43
                                ).encode(
                                    text = altair.Text('Most Recent Positive per 100k'),
                                    color = altair.value('black')
                                )
                            )
                        ).properties(
                            title=altair.TitleParams(
                                ['Data from DoH daily downloads',
                                'Numbers to right of chart show most recent 7 day total',
                                'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().strftime('%A %-d %B %Y')],
                                baseline='bottom',
                                orient='bottom',
                                anchor='end',
                                fontWeight='normal',
                                fontSize=10,
                                dy=10
                            ),
                        )
                        plots = output_plot(p, plots, driver, 'ni-cases-age-bands-%s.png' % today_str)

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

                    # Update the file index
                    for i in range(len(index)):
                        if index[i]['filedate'] == t['filedate']:
                            index[i]['tweet'] = resp.id
                            index[i]['totals'] = t['totals']
                            break
                    status.put_dict(index)

                    messages[-1] += ('updated %s' %secret['doh-dd-index'])
                else:
                    if len(upload_ids) > 0:
                        resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'], upload_ids[0])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], t['text'] + t['url'])
                    messages.append('Tweeted DM %s, ' %resp.id)
                    if len(upload_ids) > 1:
                        resp = api.dm(secret['twitter_dmaccount'], t['text2'], upload_ids[-1])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], t['text2'])
            else:
                messages.append('Duplicate found %s, did not tweet, ' %t['filedate'])
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
