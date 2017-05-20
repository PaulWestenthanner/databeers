#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
#  To run this code, first edit config.py with your configuration

import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import argparse
import multiprocessing
import string
import config
import json
import datetime


def get_parser():
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(description="Twitter Downloader")
    parser.add_argument("-q",
                        "--query",
                        dest="query",
                        help="Query/Filter",
                        default='-')
    parser.add_argument("-d",
                        "--data-dir",
                        dest="data_dir",
                        help="Output/Data Directory")
    return parser


class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self, data_dir):
        self.outfile = "%s/stream_" % data_dir + datetime.datetime.now().strftime('%d%m%H%M') + '.json'

    def on_data(self, data):
        try:
            with open(self.outfile, 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
        return True

    def on_error(self, status):
        print(status)
        return True


def format_filename(fname):
    """Convert file name into a safe string.
    Arguments:
        fname -- the file name to convert
    Return:
        String -- converted file name
    """
    return ''.join(convert_valid(one_char) for one_char in fname)


def convert_valid(one_char):
    """Convert a character into '_' if invalid.
    Arguments:
        one_char -- the char to convert
    Return:
        Character -- converted char
    """
    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    if one_char in valid_chars:
        return one_char
    else:
        return '_'


@classmethod
def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

if __name__ == '__main__':
    # !/usr/bin/python
    # -*- coding: UTF-8 -*-
    parser = get_parser()
    args = parser.parse_args()
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)

    twitter_stream = Stream(auth, MyListener(args.data_dir))

    # stream tweets that contain beer in several different languages

    p = multiprocessing.Process(target=twitter_stream.filter, name="StreamBeer",
                                kwargs={'track': [u'beer', u'cerveza', u'bier', u'pivo', u'bière', u'øl',
                                                  u'birra', u'piwo', u'cerveja', u'öl']})
    p.start()

    # Wait 60*60 seconds to stream. Job is started by cron every hour.
    time.sleep(60*60)

    if p.is_alive():
        p.terminate()
        p.join()
