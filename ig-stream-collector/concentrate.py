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
    return f'{os.path.basename(directory)}_{timestamp.strftime("%Y-%U")}.ftr'

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

    path_source = os.path.join(os.path.expanduser('~'), 'data', 'tick', '*')
    path_weekly = os.path.join(os.path.expanduser('~'), 'data', 'tick_weekly')
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
        
        weeks = [g for n, g in df.groupby(pd.Grouper(key='index',freq='W-SUN'))]
        for week in weeks:
            week.reset_index(drop=True).to_feather(
                os.path.join(path_dest, get_filename(directory, week.iloc[-1,0]))
            )