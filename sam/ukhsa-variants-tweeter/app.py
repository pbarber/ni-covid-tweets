import json
import io
import datetime
import os
import logging

import boto3
import pandas
import numpy
import altair

from bs4 import BeautifulSoup

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver, output_plot
from data_shared import update_datastore

good_symb = '\u2193'
bad_symb = '\u2191'

def lambda_handler(event, context):
    messages = ['Failure']

    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    try:
        # Get the index
        s3 = boto3.client('s3')
        status = S3_scraper_index(s3, secret['bucketname'], secret['ukhsa-variants-index'])
        index = status.get_dict()

        tweets = []
        # Download the most recently updated Excel file
        for change in event:
            store_data = (change.get('notweet') is not True) and (change.get('testtweet') is not True)
            last_updated = datetime.datetime.strptime(change['filedate'], '%Y-%m-%d')
            obj = s3.get_object(Bucket=secret['bucketname'],Key=change['keyname'])['Body']
            stream = io.BytesIO(obj.read())
            html = BeautifulSoup(stream ,features="html.parser")
            header = html.find('h2', text='Alpha')
            if header is None:
                raise Exception('Unable to find Alpha header in %s' %change['keyname'])
            df = pandas.DataFrame()
            while header is not None:
                table = header.find_next('table')
                tabs = pandas.read_html(table.prettify())
                tabs[0]['Variant'] = header.text.strip()
                df = pandas.concat([df, tabs[0]])
                header = table.find_next('h2')
                if header is not None and header.text.strip().startswith('Is this page useful'):
                    header = None
            df.rename(columns={'Unnamed: 0': 'Region'}, inplace=True)
            df = df[df['Region'] != 'Total cases'] # Remove unnecessary sum rows
            # Write this report's dataframe to S3
            stream = io.BytesIO()
            df.to_csv(stream, index=False)
            stream.seek(0)
            s3key = '%s_extract.csv' % change['keyname'].rsplit('.',maxsplit=1)[0]
            s3.upload_fileobj(stream, secret['bucketname'], s3key)
            # Update the S3 datastore with this data, replacing any already recorded for today's date
            datastore = update_datastore(s3, secret['bucketname'], secret['ukhsa-variants-datastore'], last_updated, df, store_data)

            datastore = datastore[datastore['Region']=='Northern Ireland']
            datastore['Total confirmed and probable cases'] = pandas.to_numeric(datastore['Total confirmed and probable cases'], errors='coerce')
            datastore['Date'] = pandas.to_datetime(datastore['Date'])

            latest = datastore[datastore['Date']==datastore['Date'].max()]
            previous = datastore[datastore['Date']!=datastore['Date'].max()]
            previous = previous[previous['Date']==previous['Date'].max()]
            compare = latest.merge(previous, how='left', on='Variant')
            compare = compare.groupby('Variant').sum()[['Total confirmed and probable cases_x','Total confirmed and probable cases_y']]
            compare['Total confirmed and probable cases_y'] = compare['Total confirmed and probable cases_y'].fillna(0)
            compare['diff'] = (compare['Total confirmed and probable cases_x'] - compare['Total confirmed and probable cases_y']).fillna(0).astype(int)
            top5 = compare.nlargest(5, 'diff')

            tweet = """{total:,d} new variant analyses ({altogether:,d} total):
""".format(
                total=int(latest['Total confirmed and probable cases'].sum() - previous['Total confirmed and probable cases'].sum()),
                currdate=latest['Date'].max().strftime('%-d %B'),
                altogether=int(latest['Total confirmed and probable cases'].sum())
            )
            for variant,data in top5.to_dict('index').items():
                if data['diff'] > 0 and data['diff'] != int(data['Total confirmed and probable cases_x']):
                    tweet += f"\u2022 {variant.replace('sub-lineage ', '')}: {int(data['diff']):,d} (of {int(data['Total confirmed and probable cases_x']):,d})\n"
            others = int(compare['diff'].sum() - top5['diff'].sum())
            if others != 0:
                tweet += f"\u2022 Others: {others:,d}\n"
            tweet += '\n%s' %change['url']

            plots = []
            if change.get('notweet', False) is False:
                api = TwitterAPI(
                    secret['twitter_apikey'],
                    secret['twitter_apisecretkey'],
                    secret['twitter_accesstoken'],
                    secret['twitter_accesstokensecret']
                )
                upload_ids = api.upload_multiple(plots)
                if change.get('testtweet', False) is False:
                    if len(plots) > 0:
                        resp = api.tweet(tweet, media_ids=upload_ids)
                    else:
                        resp = api.tweet(tweet)
                    messages.append('Tweeted ID %s, ' %resp.id)

                    # Update the file index
                    for i in range(len(index)):
                        if index[i]['filedate'] == change['filedate']:
                            index[i]['tweet'] = resp.id
                            break
                    status.put_dict(index)

                    messages[-1] += ('updated %s' %secret['ukhsa-variants-index'])
                else:
                    if len(upload_ids) > 0:
                        resp = api.dm(secret['twitter_dmaccount'], tweet, upload_ids[0])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], tweet)
                    messages.append('Tweeted DM %s, ' %resp.id)
            else:
                messages.append('Did not tweet')
                print(tweet)
    except:
        logging.exception('Caught error in UKHSA variants tweeter')
        api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
        api.dm(secret['twitter_dmaccount'], 'Error in UKHSA variants tweeter')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
