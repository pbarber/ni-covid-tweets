import json
import io

import boto3
import pandas
import tweepy

donottweet = True

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Download the most recently updated Excel file
    s3 = boto3.client('s3')
    changes = sorted(event['payload'], key=lambda k: k['filedate'], reverse=True)
    obj = s3.get_object(Bucket=secret['bucketname'],Key=changes[0]['keyname'])['Body']

    df = pandas.read_excel(io.BytesIO(obj.read()),engine='openpyxl', sheet_name='Summary Tests')
    df['pos_rate'] = df['INDIVIDUALS TESTED POSITIVE']/df['ALL INDIVIDUALS TESTED']
    df['rolling_pos_rate'] = df['ROLLING 7 DAY POSITIVE TESTS']/df['ROLLING 7 DAY INDIVIDUALS TESTED']
    df['printdate']=df['Sample_Date'].dt.strftime('%A %-d %B %Y')
    latest = df.iloc[df['Sample_Date'].idxmax()]
    earlier = df[df['Sample_Date'] < latest['Sample_Date']]
    earlier['gte'] = earlier['pos_rate'] >= latest['pos_rate']
    earlier['lte'] = earlier['pos_rate'] <= latest['pos_rate']
    if earlier.iloc[earlier['Sample_Date'].idxmax()]['gte'] == True:
        est = '\u2193 lowest'
        prev = earlier.iloc[earlier[earlier['lte'] == True]['Sample_Date'].idxmax()]['printdate']
    else:
        est = '\u2191 highest'
        prev = earlier.iloc[earlier[earlier['gte'] == True]['Sample_Date'].idxmax()]['printdate']
    latest_7d = df.iloc[df[df['rolling_pos_rate'].notna()]['Sample_Date'].idxmax()]
    earlier_7d = df[df['Sample_Date'] < latest_7d['Sample_Date']]
    earlier_7d['gte_7d'] = earlier_7d['rolling_pos_rate'] >= latest_7d['rolling_pos_rate']
    earlier_7d['lte_7d'] = earlier_7d['rolling_pos_rate'] <= latest_7d['rolling_pos_rate']
    if earlier_7d.iloc[earlier_7d['Sample_Date'].idxmax()]['gte_7d'] == True:
        est_7d = '\u2193 lowest'
        prev_7d = earlier_7d.iloc[earlier_7d[earlier_7d['lte_7d'] == True]['Sample_Date'].idxmax()]['printdate']
    else:
        est_7d = '\u2191 highest'
        prev_7d = earlier_7d.iloc[earlier_7d[earlier_7d['gte_7d'] == True]['Sample_Date'].idxmax()]['printdate']
    tweet = '''{date}

{ind_positive:,} people tested positive, {ind_tested:,} people tested, {pos_rate:.2%} positivity rate
{est} rate since {prev}

{pos_rate_7d:.2%} 7-day positivity rate
{est_7d} 7-day rate since {prev_7d}'''.format(
    date=latest['printdate'],
    ind_positive=latest['INDIVIDUALS TESTED POSITIVE'],
    ind_tested=latest['ALL INDIVIDUALS TESTED'],
    pos_rate=latest['pos_rate'],
    pos_rate_7d=latest_7d['rolling_pos_rate'],
    est=est,
    prev=prev,
    est_7d=est_7d,
    prev_7d=prev_7d)
    print(tweet)

    auth = tweepy.OAuthHandler(secret['twitter_apikey'], secret['twitter_apisecretkey'])
    auth.set_access_token(secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])

    api = tweepy.API(auth)

    if not donottweet:
        resp = api.update_status(tweet)
        if resp.statusCode == 200:
            message = 'Tweeted ID %s' %resp.id_str
        else:
            message = 'ERROR: Twitter API returned %s' %resp
    else:
        message = 'Did not tweet'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
