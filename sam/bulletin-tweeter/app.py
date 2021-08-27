import json
import tempfile
import re
import math
import logging
import io

from numpy.lib.index_tricks import _fill_diagonal_dispatcher

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
    # Pull current data from s3
    try:
        obj = s3.get_object(Bucket=secret['bucketname'],Key=secret['pha-education-datastore'])['Body']
    except s3.exceptions.NoSuchKey:
        print("The object %s does not exist in bucket %s." %(secret['pha-education-datastore'], secret['bucketname']))
        datastore = pandas.DataFrame(columns=['filedate'])
    else:
        stream = io.BytesIO(obj.read())
        datastore = pandas.read_csv(stream)

    messages = []
    if 'url' in event[0]:
    # Download the most recently updated PDF file
        for change in event:
            tmp = tempfile.NamedTemporaryFile(suffix='.pdf')
            with open(tmp.name, 'wb') as fp:
                s3.download_fileobj(secret['bucketname'],change['keyname'],fp)
            text = textract.process(tmp.name, method='pdfminer').decode('utf-8')
            regex = re.compile(r'Up to [Ww]eek \d{1,2}\s+\((\d{1,2})\s+([A-Z][a-z]+)\s+(\d{4})\)')
            end_date = None
            for line in text.split('\n'):
                m = regex.search(line)
                if m:
                    end_date = datetime.datetime.strptime('%s %s %s' %(m.group(1),m.group(2),m.group(3)), '%d %B %Y')
                    break
            if end_date is None:
                logging.error('Unable to find end date in report %s' %change['keyname'])
                continue
            regex = re.compile(r'Table (\d+)\. Number of Incidents by School and Incident Type')
            tables = tabula.read_pdf(tmp.name, pages = "all", multiple_tables = True, java_options=["-Xmx1024m"])
            dataset = None
            for df in tables:
                match = False
                for col in df.columns:
                    m = regex.search(col)
                    if m:
                        match = True
                        break
                if match is True:
                    if len(df.columns)!=1:
                        logging.error('Too many columns in %s, %s' %(df, change['keyname']))
                        break
                    df.columns=['raw']
                    df = df[df['raw'].str.endswith('%')]
                    df['Proportion'] = df['raw'].str.rsplit(' ', 1,expand=True)[1]
                    df['raw'] = df['raw'].str.rsplit(' ', 1,expand=True)[0]
                    df['Total'] = df['raw'].str.rsplit(' ', 1,expand=True)[1]
                    df['raw'] = df['raw'].str.rsplit(' ', 1,expand=True)[0]
                    df['School Type'] = df['raw'].str.replace('Single Case ', '')
                    df = df[['School Type','Total']].reset_index(drop=True)
                    if len(df)!=12:
                        logging.error('Unexpected number of rows in %s, %s' %(df, change['keyname']))
                        break
                    df['Incident Type'] = 'Cluster (>5 cases)'
                    df.iloc[:8, df.columns.get_loc('Incident Type')] = 'Cluster (2-5 cases)'
                    df.iloc[:4, df.columns.get_loc('Incident Type')] = 'Single Case'
                    dataset = df
                    break
            if dataset is None:
                logging.error('Unable to find table in %s' %change['keyname'])
                continue
            dataset['filedate'] = change['filedate']
            dataset['End Date'] = end_date.strftime('%Y-%m-%d')
            dataset['url'] = change['url']
            # Clean out any data with matching dates
            datastore = datastore[datastore['filedate'] != change['filedate']]
            # Append the new data
            datastore = pandas.concat([datastore, dataset])
            # Push the data to s3
            stream = io.BytesIO()
            datastore.to_csv(stream, index=False)
            stream.seek(0)
            s3.upload_fileobj(stream, secret['bucketname'], secret['pha-education-datastore'])
    else:
        driver = get_chrome_driver()
        plots = []
        if driver is None:
            logging.error('Failed to start chrome')
        else:
            datastore['End Date'] = pandas.to_datetime(datastore['End Date'])
            weekly = datastore.groupby(['End Date','School Type','Incident Type']).sum()['Total'].reset_index()
            weekly.sort_values('End Date', inplace=True)
            weekly['New'] = weekly['Total'] - weekly.groupby(['School Type','Incident Type'])['Total'].shift(1)
            weekly['New no neg'] = weekly['New'].clip(lower=0) # Remove negatives for the detailed plot
            weekly['order'] = weekly['Incident Type'].replace(
                {val: i for i, val in enumerate(['Cluster (>5 cases)', 'Cluster (2-5 cases)', 'Single Case', 'White'])}
            )
            latest = weekly[weekly['End Date']==weekly['End Date'].max()]
            p = altair.vconcat(
                altair.Chart(
                    latest
                ).mark_bar().encode(
                    x = altair.X('New:Q', axis=altair.Axis(title='Total reported', tickMinStep=1)),
                    y = altair.Y('School Type:O', sort=['Preschool','Primary','Post Primary','Special']),
                    color=altair.Color('Incident Type', sort=altair.SortField('order',order='descending')),
                    order=altair.Order(
                        'order',
                        sort='ascending'
                    ),
                ).properties(
                    height=225,
                    width=400,
                    title='NI COVID-19 School Surveillance reports for week ending %s' %datastore['End Date'].max().strftime('%-d %B %Y')
                ),
            ).properties(
                title=altair.TitleParams(
                    ['Data from Public Health Agency',
                    'Some data has been manually extracted',
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
                    weekly[~weekly['New'].isna()]
                ).mark_area().encode(
                    x = altair.X('End Date:T', axis=altair.Axis(title='Date reported')),
                    y = altair.Y('sum(New):Q', axis=altair.Axis(title='Newly reported', orient="right", tickMinStep=1)),
                    color=altair.Color('Incident Type', sort=altair.SortField('order',order='descending')),
                    order=altair.Order(
                        'order',
                        sort='ascending'
                    ),
                ).properties(
                    height=450,
                    width=800,
                    title='NI COVID-19 School Surveillance reports from %s to %s' %(datastore['End Date'].min().strftime('%-d %B %Y'), datastore['End Date'].max().strftime('%-d %B %Y'))
                ),
            ).properties(
                title=altair.TitleParams(
                    ['Data from Public Health Agency',
                    'Some data has been manually extracted',
                    'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                    baseline='bottom',
                    orient='bottom',
                    anchor='end',
                    fontWeight='normal',
                    fontSize=10,
                    dy=10
                ),
            )
            plotname = 'pha-education-time-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
            plotstore = io.BytesIO()
            p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            plotstore.seek(0)
            plots.append({'name': plotname, 'store': plotstore})
            p = altair.vconcat(
                altair.Chart(
                    weekly[~weekly['New'].isna()]
                ).mark_area().encode(
                    x = altair.X('End Date:T', axis=altair.Axis(title='Date reported')),
                    y = altair.Y('sum(New no neg):Q', axis=altair.Axis(title='Newly reported', orient="right", tickMinStep=1)),
                    color=altair.Color('Incident Type', sort=altair.SortField('order',order='descending')),
                    facet=altair.Facet('School Type:O', columns=2, title=None, spacing=0, sort=['Preschool','Primary','Post Primary','Special']),
                    order=altair.Order(
                        'order',
                        sort='ascending'
                    ),
                ).properties(
                    height=225,
                    width=450,
                    title=altair.TitleParams(
                        'NI COVID-19 School Surveillance reports from %s to %s' %(datastore['End Date'].min().strftime('%-d %B %Y'), datastore['End Date'].max().strftime('%-d %B %Y')),
                        anchor='middle',
                    ),
                ),
            ).properties(
                title=altair.TitleParams(
                    ['Data from Public Health Agency',
                    'Some data has been manually extracted, negative values have been removed',
                    'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                    baseline='bottom',
                    orient='bottom',
                    anchor='end',
                    fontWeight='normal',
                    fontSize=10,
                    dy=10
                ),
            )
            plotname = 'pha-education--school-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
            plotstore = io.BytesIO()
            p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            plotstore.seek(0)
            plots.append({'name': plotname, 'store': plotstore})
            change = event[0]
            tweet = '''School Surveillance for COVID-19 in NI, week ending {end_date}
\u2022 Preschool: {preschool:,} incidents
\u2022 Primary: {primary:,}
\u2022 Post Primary: {post_primary:,}
\u2022 Special: {special:,}
\u2022 Total: {total:,}

Source: https://www.publichealth.hscni.net/publications/coronavirus-bulletin'''.format(
                end_date=latest['End Date'].max().strftime('%-d %B %Y'),
                preschool=int(latest[latest['School Type']=='Preschool']['New'].sum()),
                primary=int(latest[latest['School Type']=='Primary']['New'].sum()),
                post_primary=int(latest[latest['School Type']=='Post Primary']['New'].sum()),
                special=int(latest[latest['School Type']=='Special']['New'].sum()),
                total=int(latest['New'].sum())
            )
            if change.get('notweet') is not True:
                api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
                upload_ids = api.upload_multiple(plots)
                if change.get('testtweet') is True:
                    if len(upload_ids) > 0:
                        resp = api.dm(secret['twitter_dmaccount'], tweet, upload_ids[0])
                        if len(upload_ids) > 1:
                            resp = api.dm(secret['twitter_dmaccount'], 'Test 1', upload_ids[1])
                            if len(upload_ids) > 2:
                                resp = api.dm(secret['twitter_dmaccount'], 'Test 2', upload_ids[2])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], tweet)
                    messages.append('Tweeted DM ID %s' %(resp.id))
                else:
                    if len(upload_ids) > 0:
                        resp = api.tweet(tweet, media_ids=upload_ids)
                    else:
                        resp = api.tweet(tweet)
                    # Download and update the index
                    status = S3_scraper_index(s3, secret['bucketname'], secret['pha-bulletin-index'])
                    index = status.get_dict()
                    for i in range(len(index)):
                        if index[i]['filedate'] == datastore['filedate'].max():
                            index[i]['tweet'] = resp.id
                            break
                    status.put_dict(index)
                    messages.append('Tweeted ID %s and updated %s' %(resp.id, secret['pha-bulletin-index']))
            else:
                print(tweet)
                messages.append('Did not tweet')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": messages,
        }),
    }
