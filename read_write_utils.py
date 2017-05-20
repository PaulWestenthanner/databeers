import csv
from datetime import datetime, timedelta
import pandas as pd


def load_df(path):
    """
    this load function is more flexible than the pandas.from_csv
    :param path: string
    :return: pandas df
    """
    with open(path, 'rU') as csvfile:
        file_loaded = csv.reader(csvfile)
        data = []
        for j, row in enumerate(file_loaded):
            if j == 0:
                headers = row[1:]
            else:
                data.append(row[1:])

    return pd.DataFrame(data, columns=headers)


def get_daily_user_df(day, root_path='/home/paul/databeers'):
    """
    Union hourly user dataframes and do a distinct
    day is datetime.datetime
    """
    hours_list = [datetime(2000, 1, 1, h, 0).strftime('%H%M') for h in range(24)]
    daily_df = None
    root_path += '/data/users_'
    for path in [root_path + day.strftime('%d%m') + h + '.csv' for h in hours_list]:
        try:
            hour_df = load_df(path)
            if daily_df is None:
                daily_df = hour_df
            else:
                daily_df = daily_df.append(hour_df)
        except IOError:
            print path, 'does not exist'
    return daily_df.drop_duplicates()


def clean_users_df(date, root_path='/home/paul/databeers'):
    """
    create the clean users df, this is also the users df on a daily basis with selected columns.
    :param date:
    :param root_path:
    :return:
    """
    cols_to_select = [u'created_at', u'default_profile', u'description',
                      u'favourites_count', u'followers_count', u'friends_count',
                      u'geo_enabled', u'id', u'id_str', u'lang', u'statuses_count',
                      u'utc_offset', u'verified']
    agg_dict = {x: max for x in cols_to_select if x != 'id'}
    agg_dict['created_at'] = min
    raw_df = load_df(root_path + '/data/daily_users/users_' + date.strftime('%m%d') + '.csv')
    raw_df = raw_df[raw_df["id"].str.match("[0-9]+")]
    df_out = raw_df.groupby('id').aggregate(agg_dict).reset_index().dropna(subset=["id"])
    df_out.to_csv(root_path + '/data/daily_users/clean_users_' + date.strftime('%m%d') + '.csv',
                  encoding='utf-8')


def load_tweets_or_users(start_date, end_date, columns=None, tweets=True, root_path='/home/paul/databeers'):
    """
    Load tweets or user data between start_date and end_date
    default is loading tweets
    both start and end date are datetimes
    with the columns parameter you can specify a whitelist of columns to save memory
    """
    tweets_df = None
    # get rid of minutes
    start_date = datetime(start_date.year, start_date.month, start_date.day, start_date.hour)
    while start_date < end_date:
        path = root_path + '/data/'
        if tweets:
            path += 'tweets_' + start_date.strftime('%d%m%H%M')
        else:
            path += 'daily_users/clean_users_' + start_date.strftime('%m%d')
        path += '.csv'
        try:
            hour_df = load_df(path)
            hour_df['day_str'] = pd.Series([start_date.strftime('%Y-%m-%d')]*len(hour_df))
            if columns:
                columns = [x for x in columns if x in hour_df.columns]
                hour_df = hour_df[columns]
            if tweets_df is None:
                tweets_df = hour_df
            else:
                tweets_df = tweets_df.append(hour_df)
        except IOError:
            print path, 'does not exist'
        if start_date >= start_date + timedelta(hours=1):
            return
        if tweets:
            start_date = start_date + timedelta(hours=1)
        else:
            start_date = start_date + timedelta(days=1)
    return tweets_df.reset_index().drop('index', axis=1)


def load_joined_df(start_date, end_date, columns=None, root_path='/home/paul/databeers'):
    """
    load joined tweets-users dataframe
    """
    if columns:
        columns = list(set(columns + ['user_id', 'id', 'day_str']))
    tweets = load_tweets_or_users(start_date, end_date, columns=columns, tweets=True,
                                  root_path=root_path).drop('id', axis=1)
    users = load_tweets_or_users(start_date, end_date, columns=columns, tweets=False, root_path=root_path)
    joined_df = tweets.merge(users, left_on=['user_id', 'day_str'], right_on=['id', 'day_str'])
    return joined_df
