# coding:utf8
from datetime import datetime, timedelta
import pandas as pd
from read_write_utils import *


def add_local_time_col(df, col_out_name='local_time'):
    """
    tweets do not have local time, however the users have their time zone specified and hence the local time can be
    calculated from the joined df. Since the joined df always has the same column names we can hard code them.
    :param df: pandas df
    :param col_out_name: string, name of local time column
    :return: pandas df with local time column added
    """
    if 'created_at_x' not in df.columns or 'utc_offset' not in df.columns:
        raise ValueError("df has to have columns 'created_at_x' and 'utc_offset'")
    df['created_at'] = pd.to_datetime(df['created_at_x'].str.replace(' \+0000', ''), format='%c')
    offset_series = df['utc_offset'].replace('', '0').apply(lambda x: int(float(x))) / 3600
    df[col_out_name] = df['created_at'] + offset_series.apply(lambda x: pd.Timedelta(
        pd.tseries.offsets.Hour(x)))
    return df


def add_estimated_country(df, col_out_name="country_est"):
    """
    Estimate the country of a the twitter user given country information language and time zone.
    :param df:
    :param col_out_name:
    :return:
    """
    if not all([x in df.columns for x in ('country', 'utc_offset', 'lang_y')]):
        raise ValueError("The dataframe does not have country, utc_offset, lang_y column")
    # create dictionary {(language, timezone start, timezone end): Country}
    country_dict = {'en': [(-28800, -14400, 'United States'),  # sorry canadians
                           (3600, 3600, 'United Kingdom')],
                    'es': [(-10800, -10800, "Argentina"),
                           (7200, 7200, "Spain"),
                           (-25200, -18000, "Mexico"),
                           (-16200, -16200, "Venezuela"),
                           (-14400, -14400, "Columbia")],  # or chile
                    'pt': [(-25200, -10800, "Brazil"),
                           (3600, 3600, "Portugal")],
                    'fr': [(7200, 7200, "France"),
                           (-28800, -14400, 'Canada')]
                    }
    tweet_country_dict = {x: x for x in ['United States', 'Brasil', 'United Kingdom', 'Canada',
                                         'Argentina', 'Brazil', 'M\xc3\xa9xico', 'France', 'Australia',
                                         'Espa\xc3\xb1a', '\xe6\x97\xa5\xe6\x9c\xac', 'Nederland', 'Italia',
                                         'T\xc3\xbcrkiye', 'Portugal', 'Colombia', 'Mexico', 'South Africa',
                                         'Indonesia']}
    tweet_country_dict['Brasil'] = 'Brasil'
    tweet_country_dict['M\xc3\xa9xico'] = 'Mexico'
    tweet_country_dict['Espa\xc3\xb1a'] = 'Spain'
    tweet_country_dict['\xe6\x97\xa5\xe6\x9c\xac'] = 'Japan'
    tweet_country_dict['Deutschland'] = 'Germany'
    zh_country = 'China'
    lang_code_country_dict = {'fr-CA': 'Canada', 'zh-Hant': zh_country, 'zh-Hans': zh_country, 'sl': 'Slovenia',
                              'sk': 'Slovakia', 'fr': "France",
                              'zh-CN': zh_country, 'en-AU': 'Australia', 'zh-TW': zh_country, 'bg': 'Bulgaria',
                              'es-MX': "Mexico", 'ro': 'Romania', 'pt-PT': "Portugal", 'zh-tw': zh_country,
                              'uk': "United Kingdom", 'hu': "Hungary",
                              'zh-cn': zh_country, 'fi': "Finland", 'da': "Denmark", 'no': "Norway", 'ca': "Canada",
                              'ru': "Russia", 'en-GB': "United Kingdom", 'pl': "Poland", 'nl': "Netherlands",
                              'en-gb': "United Kingdom", 'it': "Italy", 'de': "Germany", 'ja': " Japan",
                              }

    # for germany and the netherlands the time zone is sufficient
    def add_country(lang_user, tz_user, country_tweet):
        if lang_user in country_dict.keys():
            if tz_user:
                if len([x[2] for x in country_dict[lang_user] if x[0] <= float(tz_user) <= x[1]]) == 1:
                    return [x[2] for x in country_dict[lang_user] if x[0] <= float(tz_user) <= x[1]][0]
                else:
                    return tweet_country_dict.get(country_tweet, '')
            else:
                return tweet_country_dict.get(country_tweet, '')
        else:
            if lang_user:
                return lang_code_country_dict.get(lang_user, '')
            else:
                return tweet_country_dict.get(country_tweet, '')

    df[col_out_name] = df.apply(lambda row: add_country(row['lang_y'], row['utc_offset'], row['country']), axis=1)
    return df


def remove_german_oil_tweets(datetime, root_path='/home/paul/databeers'):
    """
    since öl is the german word for oil and in some scandinavian language means beer we have to remove german tweets
    containing öl.
    :param datetime: datetime, precision at least hours
    :param root_path: string, path to databeers directory
    :return:
    """
    path = root_path + '/data/tweets_' + datetime.strftime('%d%m%H%M') + '.csv'
    df = load_df(path)
    print 'loaded df', datetime
    df[(~df["text"].str.contains("öl").fillna(False)) | (~(df['lang'] == 'de').fillna(False))].to_csv(
        path, encoding='utf-8')
    return


if __name__ == "__main__":
    begin_date, end_date = datetime(2016, 6, 10), datetime(2016, 8, 29)
    while begin_date < end_date:
        print begin_date
        try:
            remove_german_oil_tweets(begin_date, '/Users/westenthannerp/Documents/databeers')
        except KeyError:
            # for two days there is a  data quality problem and the tweets do not have a lang column
            print 'Error for', begin_date
        except IOError:
            print 'paths not valid/data not available for ', begin_date
        begin_date = begin_date + timedelta(hours=1)
