import json
import io
import datetime
import logging

import boto3
import pandas
import requests
import altair
from selenium import webdriver

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver

good_symb = '\u2193'
bad_symb = '\u2191'

def lambda_handler(event, context):
    messages = []

    try:
        # Get the secret
        sm = boto3.client('secretsmanager')
        secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
        secret = json.loads(secretobj['SecretString'])

        # Get the index
        s3 = boto3.client('s3')
        status = S3_scraper_index(s3, secret['bucketname'], secret['cog-variants-index'])
        index = status.get_dict()

        # Create a copy of the file in s3
        keyname = "COG-variants/%s/%s-%s.csv" %(event['filedate'],event['modified'].replace(':','_'),event['length'])
        print('getting URL')
        with requests.get(event['url'], stream=True) as stream:
            stream.raise_for_status()
            stream.raw.decode_content = True
            s3.upload_fileobj(stream.raw, secret['bucketname'], keyname, Config=boto3.s3.transfer.TransferConfig(use_threads=False))
        print('done')

        # Download the most recently updated CSV file
        obj = s3.get_object(Bucket=secret['bucketname'],Key=keyname)['Body']
        stream = io.BytesIO(obj.read())

        # Dataframe for converting between pago lineage and WHO labels
        lineage_lookup = pandas.DataFrame([
            {'WHO label': 'Alpha', 'Pango lineage': 'B.1.1.7'},
            {'WHO label': 'Beta', 'Pango lineage': 'B.1.351'},
            {'WHO label': 'Beta', 'Pango lineage': 'B.1.351.2'},
            {'WHO label': 'Beta', 'Pango lineage': 'B.1.351.3'},
            {'WHO label': 'Gamma', 'Pango lineage': 'P.1'},
            {'WHO label': 'Gamma', 'Pango lineage': 'P.1.1'},
            {'WHO label': 'Gamma', 'Pango lineage': 'P.1.2'},
            {'WHO label': 'Delta', 'Pango lineage': 'B.1.617.2'},
            {'WHO label': 'Delta', 'Pango lineage': 'AY.1'},
            {'WHO label': 'Delta', 'Pango lineage': 'AY.2'},
            {'WHO label': 'Delta', 'Pango lineage': 'AY.3'},
            {'WHO label': 'Epsilon', 'Pango lineage': 'B.1.427'},
            {'WHO label': 'Zeta', 'Pango lineage': 'P.2'},
            {'WHO label': 'Eta', 'Pango lineage': 'B.1.525'},
            {'WHO label': 'Theta', 'Pango lineage': 'P.3'},
            {'WHO label': 'Iota', 'Pango lineage': 'B.1.526'},
            {'WHO label': 'Kappa', 'Pango lineage': 'B.1.617.1'},
            {'WHO label': 'Lambda', 'Pango lineage': 'C.37'},
        ])

        # Load variant data, aggregate and push back to S3
        df = pandas.read_csv(stream)
        df = df[df['adm1']=='UK-NIR']
        df['Sample Date'] = pandas.to_datetime(df['sample_date'])
        df['Week of sample'] = df['Sample Date'] - pandas.to_timedelta(df['Sample Date'].dt.dayofweek, unit='d')
        df = df.merge(lineage_lookup, how='left', left_on='lineage', right_on='Pango lineage')
        df['WHO label'] = df['WHO label'].fillna('Other')
        lin_by_week = df.groupby(['Week of sample','WHO label']).size().rename('count')
        lin_pc_by_week = lin_by_week/lin_by_week.groupby(level=0).sum()
        lin_by_week = pandas.DataFrame(lin_by_week).reset_index()
        lin_pc_by_week = pandas.DataFrame(lin_pc_by_week).reset_index()
        lineage = df.groupby('lineage').size().reset_index(name='count')
        stream = io.BytesIO()
        lineage.to_csv(stream, index=False)
        stream.seek(0)
        lineage_key = '%s_lineage.csv' % keyname.rsplit('.',maxsplit=1)[0]
        s3.upload_fileobj(stream, secret['bucketname'], lineage_key)
        messages.append('Wrote lineage summary to s3')

        # Update the S3 index and find the previous date
        previous = '1970-01-01'
        prev_lineagekey = None
        thisindex = None
        for i in range(len(index)):
            if index[i]['modified'] == event['modified']:
                index[i]['lineage'] = lineage_key
                index[i]['keyname'] = keyname
                thisindex = i
            elif index[i]['filedate'] != event['filedate']:
                if (index[i]['filedate'] > previous) and (index[i]['filedate'] < event['filedate']):
                    previous = index[i]['filedate']
                    prev_lineagekey = index[i].get('lineage')
        status.put_dict(index)

        # If there is a previous file, then load it and work out the differences
        if prev_lineagekey is not None:
            obj = s3.get_object(Bucket=secret['bucketname'],Key=prev_lineagekey)['Body']
            stream = io.BytesIO(obj.read())
            prev_lineage = pandas.read_csv(stream)
            lineage = lineage.merge(prev_lineage, how='left', left_on='lineage', right_on='lineage')
            lineage = lineage.merge(lineage_lookup, how='left', left_on='lineage', right_on='Pango lineage')
            lineage['WHO label'] = lineage['WHO label'].fillna('Other')
            lineage = lineage.groupby('WHO label').sum()[['count_x','count_y']]
            lineage['diff'] = (lineage['count_x'] - lineage['count_y']).fillna(0).astype(int)
            top5 = lineage.nlargest(5, 'diff')
            tweet = """{total:,d} new variant analyses reported for NI on {currdate} since {prevdate} ({altogether:,d} total):
""".format(
                total=lineage['diff'].sum(),
                prevdate=datetime.datetime.strptime(previous, '%Y-%m-%d').date().strftime('%A %-d %B %Y'),
                currdate=datetime.datetime.strptime(event['filedate'], '%Y-%m-%d').date().strftime('%A %-d %B %Y'),
                altogether=lineage['count_x'].sum()
            )
            for variant,data in top5.to_dict('index').items():
                if data['diff'] > 0:
                    tweet += f"\u2022 {variant}: {data['diff']:,d} (of {data['count_x']:,d})\n"
            others = int(lineage['diff'].sum() - top5['diff'].sum())
            if others != 0:
                tweet += f"\u2022 Others: {others:,d}\n"
            tweet += '\nSource: https://beta.microreact.org/'

            driver = get_chrome_driver()
            if driver is None:
                raise Exception('Failed to start chrome')

            p = altair.vconcat(
                altair.Chart(
                    lin_by_week[lin_by_week['Week of sample']>lin_by_week['Week of sample'].max()-pandas.to_timedelta(84, unit='d')]
                ).mark_line().encode(
                    x = altair.X('Week of sample:T', axis=altair.Axis(title='', labels=False, ticks=False)),
                    y = altair.Y('count:Q', axis=altair.Axis(title='Samples')),
                    color='WHO label'
                ).properties(
                    height=225,
                    width=800,
                    title='NI COVID-19 variants identified by COG-UK over the most recent 12 weeks'
                ),
                altair.Chart(
                    lin_pc_by_week[lin_pc_by_week['Week of sample']>lin_pc_by_week['Week of sample'].max()-pandas.to_timedelta(84, unit='d')]
                ).mark_area().encode(
                    x = 'Week of sample:T',
                    y = altair.Y('sum(count):Q', axis=altair.Axis(format='%', title='% of samples', orient="right")),
                    color='WHO label'
                ).properties(
                    height=225,
                    width=800,
                )
            ).properties(
                title=altair.TitleParams(
                    ['Variant identification can take up to 3 weeks, so recent totals are likely to be revised upwards',
                    'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                    baseline='bottom',
                    orient='bottom',
                    anchor='end',
                    fontWeight='normal',
                    fontSize=10,
                    dy=10
                ),
            )
            plotname = 'ni-variants-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
            plotstore = io.BytesIO()
            p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
            plotstore.seek(0)

            if event.get('notweet') is not True:
                api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
                resp = api.upload(plotstore, plotname)
                if event.get('testtweet') is True:
                    resp = api.dm(secret['twitter_dmaccount'], tweet, resp.media_id)
                    messages.append('Tweeted DM ID %s, ' %resp.id)
                else:
                    resp = api.tweet(tweet, media_ids=[resp.media_id])
                    messages.append('Tweeted ID %s, ' %resp.id)
                    # Update the file index
                    index[thisindex]['tweet'] = resp.id
                    status.put_dict(index)
            else:
                messages.append('Did not tweet')
                print(tweet)
        else:
            messages.append('Did not find previous lineage data')
    except:
        logging.exception('Caught exception in COG variants tweeter')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
