import json

class S3_scraper_status:
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
