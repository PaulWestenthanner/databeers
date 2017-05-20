#!/usr/bin/python
import json
import os.path

import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from os import remove


def create_tweets_user_dfs(filename):
    tweet_dict = defaultdict(list)
    user_dict = defaultdict(list)
    with open(filename, 'r') as f:
        exception_count = 0
        for idx, line in enumerate(f):
            tweet = json.loads(line)  # load it as Python dict
            user = tweet.get('user')
            coord = tweet.get('coordinates')

            if isinstance(coord, dict):
                tweet['lat'] = coord['coordinates'][0]
                tweet['lon'] = coord['coordinates'][1]
            else:
                tweet['lat'] = None
                tweet['lon'] = None

            place = tweet.get('place')
            if isinstance(place, dict):
                tweet['place_name'] = place['name']
                tweet['country'] = place['country']
                tweet['place_id'] = place['id']
            else:
                tweet['place_name'], tweet['country'], tweet['place_id'] = None, None, None

            try:
                del tweet['user']
                del tweet['entities']
                del tweet['geo']
                del tweet['coordinates']
                del tweet['place']
                del tweet['timestamp_ms']
                tweet['user_id'] = user['id']

                for k, v in tweet.iteritems():
                    tweet_dict[k].append(v)
                for k, v in user.iteritems():
                    user_dict[k].append(v)

            except KeyError:
                # streaming did not work properly
                exception_count += 1

        n_tweets = max([len(v) for k, v in tweet_dict.iteritems()])
        n_user = max([len(v) for k, v in user_dict.iteritems()])

        del_keys_t = []
        for k in tweet_dict:
            if len(tweet_dict[k]) != n_tweets:
                del_keys_t.append(k)
        for k in del_keys_t:
            del tweet_dict[k]

        del_keys_u = []
        for k in user_dict:
            if len(user_dict[k]) != n_user:
                del_keys_u.append(k)
        for k in del_keys_u:
            del user_dict[k]
        print 'exceptions', exception_count

        return pd.DataFrame(tweet_dict), pd.DataFrame(user_dict)


def write_to_csv(df, filename):
    df.to_csv('/home/ec2-user/databeers/data/' + filename + '.csv', encoding='utf-8')
    return


def load_data(start_date, end_date, tweets=True):
    """
    load either tweets (tweets=True) or user (tweets=False) data between start and end date
    :param start_date datetime
    :param end_date datetime
    :param tweets bool
    """
    start_date = datetime(start_date.year, start_date.month, start_date.day, start_date.hour)
    if tweets:
        to_read = 'tweets'
    else:
        to_read = 'users'
    df = pd.read_csv('~/databeers/data/' + to_read + '_' + start_date.strftime('%d%m%H%M') + '.csv')
    date = start_date + timedelta(hours=1)
    while date < end_date:
        df = df.append(pd.read_csv('~/databeers/data/' + to_read + '_' + date.strftime('%d%m%H%M') + '.csv'))
        date += timedelta(hours=1)
    return df


if __name__ == '__main__':
    start = datetime.now()
    file_to_process = '/home/ec2-user/databeers/data/stream_' + (start-timedelta(hours=1)).strftime('%d%m%H%M'
                                                                                                    ) + '.json'

    if os.path.exists(file_to_process):
        df_t, df_u = create_tweets_user_dfs(file_to_process)
        write_to_csv(df_t, 'tweets_' + start.strftime('%d%m%H%M'))
        write_to_csv(df_u, 'users_' + start.strftime('%d%m%H%M'))
        remove(file_to_process)
