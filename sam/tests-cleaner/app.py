import json
import io
import logging

import boto3
import pandas

from shared import S3_scraper_index

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    s3 = boto3.client('s3')
    if event.get('mode') == 'aggregate':
        # Get the index of all reports
        status = S3_scraper_index(s3, secret['bucketname'], secret['doh-dd-index'])
        index = status.get_dict()

        allreports = pandas.DataFrame(columns=['Date of Specimen','Reported_Date','Total Lab Tests','Individ with Lab Test','Individ with Positive Lab Test'])
        # Download the most recently updated Excel file
        for item in index:
            try:
                obj = s3.get_object(Bucket=secret['bucketname'],Key=item['keyname'])['Body']
                stream = io.BytesIO(obj.read())
                # Load test data
                daily = pandas.read_excel(stream,engine='openpyxl',sheet_name='Tests')
                # Take only the required columns
                daily = daily.groupby(['Date of Specimen']).sum()[['Total Lab Tests','Individ with Lab Test','Individ with Positive Lab Test']].reset_index()
                # Add reported date
                daily['Reported_Date'] = pandas.to_datetime(item['filedate'], format='%Y-%m-%d')
            except:
                logging.exception('Error loading %s' %item)
                raise

            # Combine with the other data reports
            allreports = pandas.concat([allreports, daily])

        # Write the output to CSV
        keyname = 'DoH-DD/all_tests.csv'
        csvbuffer = io.StringIO()
        allreports.to_csv(csvbuffer, index=False)
        s3.put_object(Bucket=secret['bucketname'], Key='DoH-DD/all_tests.csv', Body=csvbuffer.getvalue())

        message = 'Wrote %d rows to %s' %(len(allreports), keyname)
    else:
        obj = s3.get_object(Bucket=secret['bucketname'],Key='DoH-DD/all_tests.csv')['Body']
        stream = io.BytesIO(obj.read())
        # Load test data
        df = pandas.read_csv(stream)

        print(df.columns)

        message = 'Done'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
