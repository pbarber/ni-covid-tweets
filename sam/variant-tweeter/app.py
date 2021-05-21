import json
import io
import datetime
import logging

import boto3
import pandas

from shared import S3_scraper_index
from twitter_shared import TwitterAPI

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

        # Download the most recently updated CSV file
        obj = s3.get_object(Bucket=secret['bucketname'],Key=event['keyname'])['Body']
        stream = io.BytesIO(obj.read())

        # Load variant data, aggregate and push back to S3
        df = pandas.read_csv(stream)
        df = df[df['adm1']=='UK-NIR']
        lineage = df.groupby('lineage').size().reset_index(name='count')
        stream = io.BytesIO()
        lineage.to_csv(stream, index=False)
        stream.seek(0)
        lineage_key = '%s_lineage.csv' % event['keyname'].rsplit('.',maxsplit=1)[0]
        s3.upload_fileobj(stream, secret['bucketname'], lineage_key)
        messages.append('Wrote lineage summary to s3')

        # Update the S3 index and find the previous date
        previous = '1970-01-01'
        prev_lineagekey = None
        for i in range(len(index)):
            if index[i]['keyname'] == event['keyname']:
                index[i]['lineage'] = lineage_key
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
            lineage['diff'] = (lineage['count_x'] - lineage['count_y']).fillna(0).astype(int)
            lineage.set_index('lineage', inplace=True)
            top5 = lineage['diff'].nlargest(5)
            tweet = """{total:,d} new COVID analyses reported for NI on {currdate} since {prevdate}:
""".format(
                total=lineage['diff'].sum(),
                prevdate=datetime.datetime.strptime(previous, '%Y-%m-%d').date().strftime('%A %-d %B %Y'),
                currdate=datetime.datetime.strptime(event['filedate'], '%Y-%m-%d').date().strftime('%A %-d %B %Y')
            )
            for variant,count in top5.to_dict().items():
                if count > 0:
                    tweet += f"\u2022 {variant}: {count:,d}\n"
            others = int(lineage['diff'].sum() - top5.sum())
            if others > 0:
                tweet += f"\u2022 Others: {others:,d}\n"
            if others < 0:
                tweet += f"\u2022 Corrections: {others:,d}\n"
            tweet += '\nSource: https://beta.microreact.org/'

            if event.get('notweet') is not True:
                api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
                if event.get('testtweet') is True:
                    resp = api.dm(secret['twitter_dmaccount'], tweet)
                    messages.append('Tweeted DM ID %s, ' %resp.id)
                else:
                    resp = api.tweet(tweet)
                    messages.append('Tweeted ID %s, ' %resp.id)
                    # Update the file index
                    for i in range(len(index)):
                        if index[i]['keyname'] == event['keyname']:
                            index[i]['tweet'] = resp.id
                            break
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
