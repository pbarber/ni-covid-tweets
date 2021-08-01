import json
import tempfile
import re
import math
import logging
import io

import boto3
import requests
import tabula
import textract
import pandas
import altair
import datetime

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    s3 = boto3.client('s3')

    messages = []
    # Download the most recently updated PDF file
    for change in event:
        tmp = tempfile.NamedTemporaryFile(suffix='.pdf')
        resp = requests.get(change['url'])
        with open(tmp.name, 'wb') as fp:
            fp.write(resp.content)
        # Get the date range covered by the report
        text = textract.process(tmp.name, method='pdfminer').decode('utf-8')
        regex = re.compile(r'between (\d{1,2})(?:st|nd|rd|th)\s+([A-Z][a-z]+)\s+(\d{4})\s+\–+\s+(\d{1,2})(?:st|nd|rd|th)\s+([A-Z][a-z]+)\s+(\d{4})')
        start_date = None
        end_date = None
        for line in text.split('\n'):
            m = regex.search(line)
            if m:
                start_date = pandas.to_datetime('%s %s %s' %(m.group(1),m.group(2),m.group(3)), format='%d %B %Y').date()
                end_date = pandas.to_datetime('%s %s %s' %(m.group(4),m.group(5),m.group(6)), format='%d %B %Y').date()
                break
        if start_date is None:
            logging.error('Unable to find start date in report')
            return {
                "statusCode": 404,
                "body": 'Unable to find start date in report %s' %change['url'],
            }
        # Get the tables from the report - note that it was not possible to get data from 4th April or earlier due to
        # tables that will not parse properly in the PDF
        tables = tabula.read_pdf(tmp.name, pages = "all", multiple_tables = True)
        tablecount = 0
        dataset = pandas.DataFrame()
        for df in tables:
            if 'Total' not in df.columns:
                firstrow = df.iloc[0]
                newcols = []
                for i in range(len(firstrow)):
                    if isinstance(firstrow[i], float) and math.isnan(firstrow[i]):
                        newcols.append(df.columns[i])
                    else:
                        newcols.append(firstrow[i])
                df.columns = newcols
                df = df[1:]
            df['Setting'] = df['Setting'].str.strip()
            df.dropna(axis='index',subset=['Total','Open','Closed'],inplace=True)
            df['Total'] = df['Total'].astype(int)
            df['Open'] = df['Open'].astype(int)
            df['Closed'] = df['Closed'].astype(int)
            df = df[df['Setting']!='Total']
            if tablecount==0:
                df['Metric'] = 'Probable Outbreak'
            elif tablecount==1:
                df['Metric'] = 'Cluster'
            else:
                logging.warning('Unexpected table: %s' %df)
            tablecount += 1
            dataset = pandas.concat([dataset, df])
        dataset['Start Date'] = pandas.to_datetime(start_date)
        dataset['End Date'] = pandas.to_datetime(end_date)
        week = int((end_date - pandas.to_datetime('1 January 2020', format='%d %B %Y').date()).days / 7)
        dataset['Week'] = week
        # Create a simple summary and the tweet text
        summary = dataset.groupby('Metric').sum()
        tweet = 'NI Contact Tracing reports from %s to %s:\n' %(start_date.strftime('%-d %B %Y'), end_date.strftime('%-d %B %Y'))
        for metric,data in summary.to_dict('index').items():
            tweet += '\u2022 %d %ss (%d open, %d closed)\n' %(data['Total'], metric.lower(), data['Open'], data['Closed'])
        tweet += '\n%s' %change['url']
        # Pull current data from s3
        try:
            obj = s3.get_object(Bucket=secret['bucketname'],Key=secret['pha-clusters-datastore'])['Body']
        except s3.exceptions.NoSuchKey:
            print("The object %s does not exist in bucket %s." %(secret['pha-clusters-datastore'], secret['bucketname']))
            datastore = pandas.DataFrame(columns=['Week'])
        else:
            stream = io.BytesIO(obj.read())
            datastore = pandas.read_csv(stream)
        # Clean out any data with matching dates
        datastore = datastore[datastore['Week'] != week]
        # Append the new data
        datastore = pandas.concat([datastore, dataset])
        datastore['Start Date'] = pandas.to_datetime(datastore['Start Date'])
        datastore['End Date'] = pandas.to_datetime(datastore['End Date'])
        # Replace any known duplicates
        datastore['Setting'] = datastore['Setting'].replace({
            'Cinema/ Theatre / Entertainment': 'Cinema / Theatre / Entertainment Venue',
            'Cinema/ Theatre / Entertainment Venue': 'Cinema / Theatre / Entertainment Venue',
            'Funeral / Wakes': 'Funeral / Wake',
            'Restaurant / Cafe': 'Restaurant / Café'
        })
        # Push the data to s3
        stream = io.BytesIO()
        datastore.to_csv(stream, index=False)
        stream.seek(0)
        s3.upload_fileobj(stream, secret['bucketname'], secret['pha-clusters-datastore'])
        # Set up chromedriver so we can save altair plots
        driver = get_chrome_driver()
        plots = []
        if driver is None:
            logging.error('Failed to start chrome')
        else:
            p = altair.vconcat(
                altair.Chart(
                    dataset
                ).mark_bar().encode(
                    x = altair.X('Total:Q', axis=altair.Axis(title='Total reported')),
                    y = altair.Y('Setting:O'),
                    color='Metric'
                ).properties(
                    height=450,
                    width=800,
                    title='NI COVID-19 Contact Tracing reports from %s to %s' %(start_date.strftime('%-d %B %Y'), end_date.strftime('%-d %B %Y'))
                ),
            ).properties(
                title=altair.TitleParams(
                    ['Data from Public Health Agency',
                    'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                    baseline='bottom',
                    orient='bottom',
                    anchor='end',
                    fontWeight='normal',
                    fontSize=10,
                    dy=10
                ),
            )
            plotname = 'pha-outbreaks-week-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
            plotstore = io.BytesIO()
            p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            plotstore.seek(0)
            plots.append({'name': plotname, 'store': plotstore})
            p = altair.vconcat(
                altair.Chart(
                    (datastore.groupby(['End Date','Metric'])['Total'].sum() / 4.0).reset_index()
                ).mark_area().encode(
                    x = altair.X('End Date:T', axis=altair.Axis(title='Date reported (for preceding four weeks)')),
                    y = altair.Y('Total:Q', axis=altair.Axis(title='Total reported divided by 4', orient="right")),
                    color='Metric'
                ).properties(
                    height=450,
                    width=800,
                    title='NI COVID-19 Contact Tracing reports from %s to %s' %(datastore['Start Date'].min().strftime('%-d %B %Y'), datastore['End Date'].max().strftime('%-d %B %Y'))
                ),
            ).properties(
                title=altair.TitleParams(
                    ['Data from Public Health Agency',
                    'Data is reported weekly for the preceding four weeks, so totals are divided by 4',
                    'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                    baseline='bottom',
                    orient='bottom',
                    anchor='end',
                    fontWeight='normal',
                    fontSize=10,
                    dy=10
                ),
            )
            plotname = 'pha-outbreaks-time-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
            plotstore = io.BytesIO()
            p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            plotstore.seek(0)
            plots.append({'name': plotname, 'store': plotstore})
            p = altair.vconcat(
                altair.Chart(
                    (datastore.groupby(['End Date','Setting','Metric'])['Total'].sum() / 4.0).reset_index()
                ).mark_area().encode(
                    x = altair.X('End Date:T', axis=altair.Axis(title='')),
                    y = altair.Y('Total:Q', axis=altair.Axis(title='', orient="right")),
                    color='Metric',
                    facet=altair.Facet('Setting:O', columns=5, title=None, spacing=0),
                ).properties(
                    height=90,
                    width=160,
                    title=altair.TitleParams(
                        'NI COVID-19 Contact Tracing reports by setting from %s to %s' %(datastore['Start Date'].min().strftime('%-d %B %Y'), datastore['End Date'].max().strftime('%-d %B %Y')),
                        anchor='middle',
                    ),
                ),
            ).properties(
                title=altair.TitleParams(
                    ['Data from Public Health Agency',
                    'Data is reported weekly for the preceding four weeks, so totals are divided by 4',
                    'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                    baseline='bottom',
                    orient='bottom',
                    anchor='end',
                    fontWeight='normal',
                    fontSize=10,
                    dy=10
                ),
            )
            plotname = 'pha-outbreaks-small-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
            plotstore = io.BytesIO()
            p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            plotstore.seek(0)
            plots.append({'name': plotname, 'store': plotstore})

        # Convert to dates to ensure correct output to CSV
        datastore['Start Date'] = datastore['Start Date'].dt.date
        datastore['End Date'] = datastore['End Date'].dt.date

        # Tweet out the text and images
        if change.get('notweet') is not True:
            api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
            upload_ids = api.upload_multiple(plots)
            if change.get('testtweet') is True:
                if len(upload_ids) > 0:
                    resp = api.dm(secret['twitter_dmaccount'], tweet, upload_ids[-1])
                else:
                    resp = api.dm(secret['twitter_dmaccount'], tweet)
                messages.append('Tweeted DM ID %s' %(resp.id))
            else:
                if len(upload_ids) > 0:
                    resp = api.tweet(tweet, media_ids=upload_ids)
                else:
                    resp = api.tweet(tweet)
                # Download and update the index
                status = S3_scraper_index(s3, secret['bucketname'], secret['pha-clusters-index'])
                index = status.get_dict()
                for i in range(len(index)):
                    if index[i]['filedate'] == change['filedate']:
                        index[i]['tweet'] = resp.id
                        break
                status.put_dict(index)
                messages.append('Tweeted ID %s and updated %s' %(resp.id, secret['pha-clusters-index']))
        else:
            print(tweet)
            messages.append('Did not tweet')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": messages,
        }),
    }
