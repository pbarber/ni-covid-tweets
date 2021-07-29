import tweepy

class TwitterAPI:
    def __init__(self, apikey, apisecretkey, accesstoken, accesstokensecret):
        self.auth = tweepy.OAuthHandler(apikey, apisecretkey)
        self.auth.set_access_token(accesstoken, accesstokensecret)
        self.api = tweepy.API(self.auth)

    def tweet(self, text, replyto=None, media_ids=[]):
        if replyto is None:
            if len(media_ids)==0:
                return(self.api.update_status(text))
            else:
                return(self.api.update_status(text, media_ids=media_ids))
        else:
            if len(media_ids)==0:
                return(self.api.update_status(text, in_reply_to_status_id=replyto))
            else:
                return(self.api.update_status(text, in_reply_to_status_id=replyto, media_ids=media_ids))

    def dm(self, accountid, text, media_id=None):
        if media_id is None:
            return(self.api.send_direct_message(accountid, text))
        else:
            return(self.api.send_direct_message(accountid, text, attachment_type='media', attachment_media_id=media_id))

    def upload(self, fp, name):
        return(self.api.media_upload(filename=name, file=fp))

    def upload_multiple(self, configs):
        ids = []
        for c in configs:
            resp = self.upload(c['store'], c['name'])
            ids.append(resp.media_id)
        return ids
