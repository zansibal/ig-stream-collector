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

if __name__ == '__main__':
    logging.warning('This script should only be run during non-trading hours '
                    '(weekend), so that full weeks can be concentrated')

    path_source = os.path.join(os.path.expanduser('~'), 'data', 'tick')
    path_dest = os.path.join(os.path.expanduser('~'), 'data', 'tick_concentrated')
    dirs = glob.glob(path_source)

    for directory in dirs:
        logging.info(f'Concentrating {directory}')
        files = glob.glob(os.path.join(directory, '*.ftr'))

        dfs = []
        for file_ in sorted(files):
            dfs.append(read_file(file_))
            
        df = pd.concat(dfs)
        os.makedirs(os.path.join(path_dest, directory))
        df.to_feather(os.path.join(path_dest, directory, f'{filename}.ftr'))