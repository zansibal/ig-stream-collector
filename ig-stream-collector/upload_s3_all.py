from doctest import BLANKLINE_MARKER
import boto3
import datetime as dt
import glob
import logging
import os

def get_filename(directory, timestamp):
    return f'{os.path.basename(directory)}_{timestamp.strftime("%Y-%W")}.ftr'

if __name__ == '__main__':
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s %(levelname)s %(message)s',
        handlers = [
            logging.StreamHandler(), # Prints to stdout
        ],
    )

    # path_source = os.path.join(os.path.expanduser('~'), 'data', 'tick_weekly') # AWS
    path_source = os.path.join(os.path.expanduser('~'), 'data', 'indy', 'prices', 'ig_streaming', 'tick') # local
    BUCKET = 'indy-tick-data'
    dirs = glob.glob(os.path.join(path_source, '*'))
    s3 = boto3.resource('s3')

    # Get list of files already on S3
    file_list = []
    for obj in s3.Bucket(BUCKET).objects.all():
        file_list.append(os.path.basename(obj.key))

    for directory in dirs:
        # if not 'AUDCAD' in directory: continue # DEBUG
        for filepath_source in glob.glob(os.path.join(directory, '*')):
            filename = os.path.basename(filepath_source)
            if not filename in file_list:
                filepath_dest = f'{os.path.basename(directory)}/{filename}' # Assume path exists
                logging.info(f'Uploading {filename} to s3://{BUCKET}/{filepath_dest}')
                if not os.path.exists(filepath_source):
                    logging.warning(f'File not found: {filepath_source}')
                    continue
                
                s3.Bucket(BUCKET).upload_file(filepath_source, filepath_dest)
                # break # DEBUG
