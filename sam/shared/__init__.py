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

def get_url(session, url, format):
    resp = session.get(url)
    resp.raise_for_status()
    if format=='text':
        return(resp.text)
    elif format=='content':
        return(resp.content)
    else:
        return(resp.json())
