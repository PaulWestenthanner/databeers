import commands as cmd
import pandas as pd
cols_to_drop = ['default_profile_image', 'follow_request_sent',
                'contributors_enabled', 'following', 'is_translator',
                'notifications', 'name', 'screen_name', 'url', 'time_zone',
                'protected', 'location', 'listed_count', 'Untitled: 0']

if __name__ == '__main__':
    root_path = cmd.getoutput('pwd')[:cmd.getoutput('pwd').find('/databeers/')] + '/databeers'
    paths = cmd.getstatusoutput(
        'ls %s/data/daily_users/users_*.csv' % root_path)[1].split('\n')
    for idx, p in enumerate(paths):
        print idx, p
        user_df = pd.read_csv(p)
        cols_to_drop += [x for x in list(user_df.columns) if 'profile_' in x]
        cols_to_drop = list(set(cols_to_drop))

        print len(user_df.columns)
        for col in cols_to_drop:
            try:
                user_df = user_df.drop(col, axis=1)
            except ValueError:
                print col
        user_df.to_csv(p.replace('users_', 'clean_users_'))