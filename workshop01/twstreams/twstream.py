# !/usr/bin/python  
# -*- coding: utf-8 -*-
'''
Created on Jan 31, 2014

@author:       Bo Zhao
@email:        Jakobzhao@gmail.com
@website:      http://yenching.org
@organization: The Ohio State University
'''

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from shutil import copy
import simplejson as json
import sqlite3, os, sys
import time
from settings import consumer_key, consumer_secret, access_token, access_token_secret


# looking for proxies: http://proxy-list.org/english/index.php

current_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
database = current_path + '/data/tweets.sqlite'
refresh = False


def createDB(database, refresh):
    current_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
    if os.path.exists(database):
        if refresh:
            copy(current_path + '/' + 'twitter_template.sqlite', database)
    else:
        copy(current_path + '/' + 'twitter_template.sqlite', database)


class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def on_data(self, data):
        obj = json.loads(data)
        try:
            content = self.processTweet(obj)
            print (content)
            if content != '':
                cursor.execute(content)
                conn.commit()
        except:
            pass
        return True

    def on_error(self, status):
        print (status)

    def processTweet(self, obj):
        tid = obj['id_str']
        created_at = obj['created_at']
        text = obj['text']
        source = obj['source'].split('>')[1].split('<')[0]
        lang = obj['lang']

        user_id = obj['user']['id_str']
        user_name = obj['user']['name']
        user_description = obj['user']['description']
        user_screenname = obj['user']['screen_name']

        retweet_count = str(obj['retweet_count'])
        favorite_count = str(obj['favorite_count'])
        followers_count = str(obj['user']['followers_count'])
        friends_count = str(obj['user']['friends_count'])
        statuses_count = str(obj['user']['statuses_count'])

        user_location = obj['user']['location']
        user_verified = str(obj['user']['verified'])
        placename = obj['place']['full_name']
        placetype = obj['place']['place_type']
        try:
            if obj['coordinates']['type'] == "Point":
                lng = str(obj['coordinates']['coordinates'][0])
                lat = str(obj['coordinates']['coordinates'][1])
            # country = obj['place']['country_code']
            content = 'INSERT INTO tweets (id, created_at, text, source, lang, user_id, user_name, user_screenname, user_description, retweet_count, favorite_count, followers_count, friends_count, statuses_count, user_location, user_verified, placename, placetype, lng, lat) VALUES (%s, "%s", "%s", "%s", "%s", %s, "%s", "%s", "%s", %s, %s, %s, %s, %s, "%s", "%s", "%s", "%s", %s, %s)' % (
            tid, created_at, text, source, lang, user_id, user_name, user_screenname, user_description, retweet_count,
            favorite_count, followers_count, friends_count, statuses_count, user_location, user_verified, placename,
            placetype, lng, lat)
        except:
            content = ''
        return content


if __name__ == '__main__':
    while True:
        try:
            createDB(database, refresh)
            conn = sqlite3.connect(database)
            cursor = conn.cursor()
            l = StdOutListener(database)
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            stream = Stream(auth, l)
            # stream.filter(track=['china'], async=True)
            stream.filter(track=['refugee'])
            # stream.filter(track=['beijing hotel', 'shanghai hotel','guangzhou hotel', 'beijing flight', 'shanghai flight', 'guangzhou flight', 'China tourism', 'China travel', 'beijing tourism', 'beijing travel', 'Shanghai tourism', 'shanghai travel','guangdong tourism', 'guangdong travel','great wall tourism', 'great walll travel'])
            # stream.filter( locations=[-84,39.5,-82.5,40.2], track=['gay', 'lesbian', 'transgender', 'bisexual', 'queer', 'LGBT', 'lesbo', 'les', 'faggot','fag', 'homo', 'femme', 'hasibian', 'bareback', 'boi', 'chickenhawk', 'trans', 'tranny', 'transvestite', 'trannie', 'shemale', 'drag queen', 'dragqueen'])
            conn.close()
        except:
            time.sleep(2)
