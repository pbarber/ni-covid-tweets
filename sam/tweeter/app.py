import json
import io

import boto3
import pandas
import tweepy

from shared import S3_scraper_index

good_symb = '\u2193'
bad_symb = '\u2191'

def find_previous(df, newest, colname):
    # Find the date since which the rate was as high/low
    gte = df.iloc[df[(df[colname] >= newest[colname]) & (df['Sample_Date'] < newest['Sample_Date'])]['Sample_Date'].idxmax()]
    lt = df.iloc[df[(df[colname] < newest[colname]) & (df['Sample_Date'] < newest['Sample_Date'])]['Sample_Date'].idxmax()]
    if gte['Sample_Date'] < lt['Sample_Date']:
        est = bad_symb + ' highest'
        prev = gte['printdate']
    else:
        est = good_symb + ' lowest'
        prev = lt['printdate']
    return est, prev

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
    status = S3_scraper_index(s3, secret['bucketname'], secret['doh-dd-index'])
    index = status.get_dict()

    # Download the most recently updated Excel file
    changes = sorted(event, key=lambda k: k['filedate'], reverse=True)
    obj = s3.get_object(Bucket=secret['bucketname'],Key=changes[0]['keyname'])['Body']
    stream = io.BytesIO(obj.read())

    # Load test data and add extra fields
    df = pandas.read_excel(stream,engine='openpyxl',sheet_name='Summary Tests')
    df['pos_rate'] = df['INDIVIDUALS TESTED POSITIVE']/df['ALL INDIVIDUALS TESTED']
    df['rolling_pos_rate'] = df['ROLLING 7 DAY POSITIVE TESTS']/df['ROLLING 7 DAY INDIVIDUALS TESTED']
    df['printdate']=df['Sample_Date'].dt.strftime('%-d %B %Y')
    df['rolling_7d_change'] = (df['ROLLING 7 DAY POSITIVE TESTS'] - df['ROLLING 7 DAY POSITIVE TESTS'].shift(7)) * 7

    # Get the latest dates with values for tests and rolling
    latest = df.iloc[df['Sample_Date'].idxmax()]
    latest_7d = df.iloc[df[df['rolling_pos_rate'].notna()]['Sample_Date'].idxmax()]

    # Find the date since which the rate was as high/low
    est, prev = find_previous(df, latest, 'pos_rate')
    est_7d, prev_7d = find_previous(df, latest_7d, 'rolling_pos_rate')

    tweet = '''{ind_tested:,} people tested, {ind_positive:,} ({pos_rate:.2%}) positive on {date}
{est} rate since {prev}

{pos_rate_7d:.2%} 7-day positivity rate
{est_7d} since {prev_7d}

{pos_7d:,} positive in last 7 days
{tag_7d} {dif_7d:,} {dir_7d} than preceding 7 days ({pct_7d:.2%})
'''.format(
    date=latest['Sample_Date'].strftime('%A %-d %B %Y'),
    ind_positive=latest['INDIVIDUALS TESTED POSITIVE'],
    ind_tested=latest['ALL INDIVIDUALS TESTED'],
    pos_rate=latest['pos_rate'],
    pos_rate_7d=latest_7d['rolling_pos_rate'],
    est=est,
    prev=prev,
    est_7d=est_7d,
    prev_7d=prev_7d,
    pos_7d=int(round(latest_7d['ROLLING 7 DAY POSITIVE TESTS']*7,0)),
    tag_7d=good_symb if int(round(latest_7d['rolling_7d_change'],0))<0 else bad_symb,
    dir_7d='fewer' if int(round(latest_7d['rolling_7d_change'],0))<0 else 'more',
    dif_7d=int(abs(round(latest_7d['rolling_7d_change'],0))),
    pct_7d=latest_7d['rolling_7d_change']/(latest_7d['rolling_7d_change']+(latest_7d['ROLLING 7 DAY POSITIVE TESTS']*7)))

    auth = tweepy.OAuthHandler(secret['twitter_apikey'], secret['twitter_apisecretkey'])
    auth.set_access_token(secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])

    api = tweepy.API(auth)

    if changes[0].get('notweet') is not True:
        resp = api.update_status(tweet)
        for i in range(len(index)):
            if index[i]['filedate'] == changes[0]['filedate']:
                index[i]['tweet'] = resp.id
                break
        status.put_dict(index)
        message = 'Tweeted ID %s and updated %s' %(resp.id, secret['doh-dd-index'])
    else:
        print(tweet)
        message = 'Did not tweet'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
