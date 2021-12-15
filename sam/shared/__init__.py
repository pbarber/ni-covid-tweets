import json

import boto3

class S3_scraper_index:
    def __init__(self, client, bucketname, keyname):
        self.client = client
        self.bucketname = bucketname
        self.keyname = keyname

    def get_dict(self):
        try:
            dataobj = self.client.get_object(Bucket=self.bucketname,Key=self.keyname)
        except self.client.exceptions.NoSuchKey:
            print("The object %s does not exist in bucket %s." %(self.keyname, self.bucketname))
            return []
        return json.load(dataobj['Body'])

    def put_dict(self, data):
        self.client.put_object(Bucket=self.bucketname, Key=self.keyname, Body=json.dumps(data))

def launch_lambda_async(functionname, payload):
    lambda_client = boto3.client('lambda')
    lambda_client.invoke(
        FunctionName=functionname,
        InvocationType='Event',
        Payload=json.dumps(payload)
    )

def get_url(session, url, format, useragent=None, referer=None, upgradeinsecure=False):
    headers = {
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }
    if useragent is not None:
        headers['User-Agent'] = useragent
    if referer is not None:
        headers['Referer'] = referer
    if upgradeinsecure is True:
        headers['Upgrade-Insecure-Requests'] = '1'
        headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        headers['Accept-Language'] = 'en-US,en;q=0.5'
    resp = session.get(
        url,
        headers=headers
    )
    resp.raise_for_status()
    if upgradeinsecure is True:
        print(resp.request.headers)
    if format=='text':
        return(resp.text)
    elif format=='content':
        return(resp.content)
    else:
        return(resp.json())

def get_and_sort_index(bucketname, indexkey, s3, sortby='Last Updated'):
    status = S3_scraper_index(s3, bucketname, indexkey)
    previous = status.get_dict()
    if len(previous) > 0:
        previous = sorted(previous, key=lambda k: k[sortby], reverse=True)
    return previous, status
