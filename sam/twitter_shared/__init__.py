import tweepy

class TwitterAPI:
    def __init__(self, apikey, apisecretkey, accesstoken, accesstokensecret):
        self.auth = tweepy.OAuthHandler(apikey, apisecretkey)
        self.auth.set_access_token(accesstoken, accesstokensecret)
        self.api = tweepy.API(self.auth)

    def tweet(self, text, replyto=None):
        if replyto is None:
            return(self.api.update_status(text))
        else:
            return(self.api.update_status(text, in_reply_to_status_id=replyto))

    def dm(self, accountid, text):
        return(self.api.send_direct_message(accountid, text))
