import datetime as dt
import glob
import logging
import os
import pandas as pd

def read_file(file_):
    try:
        return pd.read_feather(file_)
    except OSError as e:
        logging.warning(f'Exception while reading {file_}: {e}')
        return pd.DataFrame()

def get_filename(directory, timestamp):
    # ISO 8601 week, same as pandas uses
    return f'{os.path.basename(directory)}_{timestamp.strftime("%Y-%V")}.ftr'

if __name__ == '__main__':
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s %(levelname)s %(message)s',
        handlers = [
            logging.StreamHandler(), # Prints to stdout
        ],
    )

    logging.warning('This script should only be run during non-trading hours '
                    '(weekend), so that full weeks can be concentrated')

    # ISO 8601 week, same as pandas uses
    path_source = os.path.join(os.path.expanduser('~'), 'data', f'tick_{dt.datetime.now().strftime("%Y-%V")}', '*') # aws
    path_weekly = os.path.join(os.path.expanduser('~'), 'data', 'tick_weekly') # aws
    # path_source = os.path.join(os.path.expanduser('~'), 'data', 'indy', 'prices', 'ig_streaming', 'tick_2022-28', '*') # local
    # path_weekly = os.path.join(os.path.expanduser('~'), 'data', 'indy', 'prices', 'ig_streaming', 'tick') # local
    dirs = glob.glob(path_source)

    for directory in dirs:
        logging.info(f'Concentrating {directory}')
        files = glob.glob(os.path.join(directory, '*.ftr'))

        dfs = []
        for file_ in sorted(files):
            dfs.append(read_file(file_))

        df = pd.concat(dfs)
        df = df.sort_values(by='index')

        path_dest = os.path.join(path_weekly, os.path.basename(directory))
        if not os.path.exists(path_dest):
            os.makedirs(path_dest)
        
        # W-SAT (weekly anchored on Saturday) means last day of week is Saturday
        # We take the week number from the last sample (Friday evening)
        for a, week in df.groupby(pd.Grouper(key='index',freq='W-SAT')):
            week.reset_index(drop=True).to_feather(
                os.path.join(path_dest, get_filename(directory, week.iloc[-1,0]))
            )
