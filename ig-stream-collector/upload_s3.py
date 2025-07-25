from doctest import BLANKLINE_MARKER
import boto3
import datetime as dt
import glob
import logging
import os
import re
import sys

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

    if len(sys.argv) > 1:
        if re.fullmatch(r'\d{4}-\d{2}', sys.argv[1]) is not None:
            year_week = sys.argv[1]
        else:
            print(f'Invalid argument {sys.argv[1]}. Exiting.')
            exit()
    else:
        year_week = dt.datetime.now().strftime('%Y-%V') # ISO 8601 week, same as pandas uses

    print(f'Uploading week {year_week}')

    buckets = {
        'ig-order-book': 'book',
        # 'ig-ohlcv-1m': 'ohlcv_1m',
        # 'ig-tick': 'tick',
    }

    for bucket, path_prefix in buckets.items():
        path_source = os.path.join(os.path.expanduser('~'), 'data', f'{path_prefix}_{year_week}')
        suffix_file = f'{year_week}.ftr' 
        dirs = glob.glob(os.path.join(path_source, '*'))
        s3 = boto3.resource('s3')

        for directory in dirs:
            # if not 'AUDCAD' in directory: continue # DEBUG
            epic = os.path.basename(directory)
            filename = f'{epic}_{suffix_file}'
            filepath_source = os.path.join(path_source, epic, filename)
            filepath_dest = f'{epic}/{filename}' # Assume path exists
            logging.info(f'Uploading {filename} to s3://{bucket}/{filepath_dest}')
            if not os.path.exists(filepath_source):
                logging.warning(f'File not found: {filepath_source}')
                continue
            
            s3.Bucket(bucket).upload_file(filepath_source, filepath_dest)
