import json
import io
import datetime
import logging

import boto3
import pandas
import altair

from shared import S3_scraper_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver

good_symb = '\u2193'
bad_symb = '\u2191'

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the history file
    s3 = boto3.client('s3')
    status = S3_scraper_index(s3, secret['bucketname'], secret['ons-infection-index'])
    index = status.get_dict()

    try:
        # Download the most recently updated Excel file
        for change in event:
            obj = s3.get_object(Bucket=secret['bucketname'],Key=change['keyname'])['Body']
            stream = io.BytesIO(obj.read())

            # Load test data and add extra fields
            df = pandas.read_excel(stream,engine='openpyxl',sheet_name='1a', header=4)
            df.dropna('columns',how='all',inplace=True)
            df.dropna('rows',subset=['95% Lower confidence/credible interval'],inplace=True)
            df['95% Lower confidence/credible interval'] = df['95% Lower confidence/credible interval']/100
            df['95% Upper confidence/credible interval'] = df['95% Upper confidence/credible interval']/100
            df['Start Date'] = df['Time period'].str.extract(r'(.+) to ')
            df['End Date'] = df['Time period'].str.extract(r'.+ to (.+)')
            df['Week Beginning'] = pandas.to_datetime(df['Start Date'], format='%d %B %Y')
            df['Week Ending'] = pandas.to_datetime(df['End Date'], format='%d %B %Y')
            latest = df.iloc[-1]
            prev = df.iloc[-2]

            df2 = pandas.read_excel(stream,engine='openpyxl',sheet_name='1b', header=4)
            df2.dropna('columns',how='all',inplace=True)
            df2.dropna('rows',subset=['95% Lower credible interval'],inplace=True)
            df2['Date'] = pandas.to_datetime(df2['Date'], format='%d %B %Y')
            df2['95% Lower credible interval'] = df2['95% Lower credible interval']/100
            df2['95% Upper credible interval'] = df2['95% Upper credible interval']/100

            tweets = []
            plots = []
            tweet = '''ONS infection survey for NI, week ending {period}

Between {lower} ({lower_pct:.1%}) and {upper} ({upper_pct:.1%}) estimated testing positive for COVID-19

Previous week was between {plower} ({plower_pct:.1%}) and {pupper} ({pupper_pct:.1%})

https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/conditionsanddiseases/datasets/covid19infectionsurveynorthernireland'''.format(
                period=latest['End Date'],
                lower=latest['95% Lower confidence/credible interval.2'],
                lower_pct=latest['95% Lower confidence/credible interval'],
                upper=latest['95% Upper confidence/credible interval.2'],
                upper_pct=latest['95% Upper confidence/credible interval'],
                plower=prev['95% Lower confidence/credible interval.2'],
                plower_pct=prev['95% Lower confidence/credible interval'],
                pupper=prev['95% Upper confidence/credible interval.2'],
                pupper_pct=prev['95% Upper confidence/credible interval'])
            tweets.append(tweet)
            try:
                driver = get_chrome_driver()
                if driver is None:
                    logging.error('Failed to start chrome')
                else:
                    toplot = df[(df['Week Ending'] > df['Week Ending'].max()-pandas.to_timedelta(84, unit='d'))]
                    p = altair.vconcat(
                        altair.hconcat(
                            altair.Chart(
                                toplot
                            ).mark_area(
                                opacity=0.7
                            ).encode(
                                x = altair.X(
                                    'Week Ending:T',
                                    axis=altair.Axis(title='Week ending')
                                ),
                                y = altair.Y(
                                    '95% Lower confidence/credible interval:Q',
                                    axis=altair.Axis(
                                        title='% of population infected',
                                        orient="right",
                                        format='%',
                                    )
                                ),
                                y2 = altair.Y2(
                                    '95% Upper confidence/credible interval:Q'
                                ),
                            ).properties(
                                height=450,
                                width=400
                            ),
                            altair.Chart(
                                toplot
                            ).mark_area(
                                opacity=0.7
                            ).encode(
                                x = altair.X(
                                    'Week Ending:T',
                                    axis=altair.Axis(title='Week ending')
                                ),
                                y = altair.Y(
                                    '95% Lower confidence/credible interval:Q',
                                    axis=altair.Axis(
                                        title='% of population infected (log scale)',
                                        orient="right",
                                        format='%',
                                    ),
                                    scale=altair.Scale(
                                        type='log'
                                    ),
                                ),
                                y2 = altair.Y2(
                                    '95% Upper confidence/credible interval:Q'
                                ),
                            ).properties(
                                height=450,
                                width=400
                            )
                        ).properties(
                            title=altair.TitleParams(
                                'Estimated percentage of population testing positive for COVID-19 in NI %s to %s' %(toplot['Week Ending'].min().strftime('%-d %B %Y'), toplot['Week Ending'].max().strftime('%-d %B %Y')),
                                anchor='middle',
                            )
                        )
                    ).properties(
                        title=altair.TitleParams(
                            ['Data from ONS',
                            'Use linear scale (left) to compare values and log scale (right) to compare rate of change',
                            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                            baseline='bottom',
                            orient='bottom',
                            anchor='end',
                            fontWeight='normal',
                            fontSize=10,
                            dy=10
                        ),
                    )
                    plotname = 'ons-infection-time-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
                    plotstore = io.BytesIO()
                    p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
                    plotstore.seek(0)
                    plots.append({'name': plotname, 'store': plotstore})
                    p = altair.vconcat(
                        altair.hconcat(
                            altair.Chart(
                                df2
                            ).mark_area(
                                opacity=0.7
                            ).encode(
                                x = altair.X(
                                    'Date:T',
                                    axis=altair.Axis(title='Date')
                                ),
                                y = altair.Y(
                                    '95% Lower credible interval:Q',
                                    axis=altair.Axis(
                                        title='% of population testing positive',
                                        orient="right",
                                        format='%',
                                    )
                                ),
                                y2 = altair.Y2(
                                    '95% Upper credible interval:Q'
                                ),
                            ).properties(
                                height=450,
                                width=400
                            ),
                            altair.Chart(
                                df2
                            ).mark_area(
                                opacity=0.7
                            ).encode(
                                x = altair.X(
                                    'Date:T',
                                    axis=altair.Axis(title='Date')
                                ),
                                y = altair.Y(
                                    '95% Lower credible interval:Q',
                                    axis=altair.Axis(
                                        title='% of population testing positive (log scale)',
                                        orient="right",
                                        format='%',
                                    ),
                                    scale=altair.Scale(
                                        type='log'
                                    ),
                                ),
                                y2 = altair.Y2(
                                    '95% Upper credible interval:Q'
                                ),
                            ).properties(
                                height=450,
                                width=400
                            )
                        ).properties(
                            title=altair.TitleParams(
                                'Modelled daily rates of percentage of NI population testing positive for COVID-19 to %s' %(df2['Date'].max().strftime('%-d %B %Y')),
                                anchor='middle',
                            )
                        )
                    ).properties(
                        title=altair.TitleParams(
                            ['Modelled daily data from ONS COVID-19 infection survey',
                            'Use linear scale (left) to compare values and log scale (right) to compare rate of change',
                            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
                            baseline='bottom',
                            orient='bottom',
                            anchor='end',
                            fontWeight='normal',
                            fontSize=10,
                            dy=10
                        ),
                    )
                    plotname = 'ons-modelled-daily-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m')
                    plotstore = io.BytesIO()
                    p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
                    plotstore.seek(0)
                    plots.append({'name': plotname, 'store': plotstore})
            except:
                logging.exception('Error creating plot')

            messages = []
            for idx in range(len(tweets)):
                tweet = tweets[idx]
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
                        messages.append('Tweeted ID %s, ' %resp.id)

                        # Update the file index
                        for i in range(len(index)):
                            if index[i]['filedate'] == change['filedate']:
                                index[i]['tweet'] = resp.id
                                break
                        status.put_dict(index)

                        messages[-1] += ('updated %s' %secret['ons-infection-index'])
                else:
                    messages.append('Did not tweet')
                    print(tweet)
    except:
        logging.exception('Caught error in ONS tweeter')
        api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
        api.dm(secret['twitter_dmaccount'], 'Error in ONS tweeter')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": messages,
        }),
    }
