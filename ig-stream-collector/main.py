import boto3
import datetime as dt
import logging
import os
import pandas as pd
import pytz
import time
import threading
import watchtower
import yaml

from trading_ig import (IGService, IGStreamService)
from trading_ig.config import config
from trading_ig.lightstreamer import Subscription

from aws_config import TOPIC_ARN
MAX_PAUSE_STREAMING = 30
last_streaming_update = None


class CollectStream():
    """ Collect IG streaming data live.
    """
    DISCONNECT_TIMEOUT = 60
    TIMEFRAMES_STREAMING = ['SECOND', '1MINUTE', '5MINUTE', 'HOUR']
    MAX_REINITS = 10

    subscriptions = []
    cur_init = 0

    def __init__(self):
        """Initiate connection, and do some checks."""
        self.connect(
            config.username, 
            config.password, 
            config.api_key, 
            config.acc_type,
            acc_number=config.acc_number,
        )

        self.check_timezone()

    def connect(self, username, password, api_key, acc_type, acc_number):
        """Connect to IG broker via ig_trading package and REST API.
        
        Args:
            username (str): Username.
            password (str): Password.
            api_key (str): API key.
            acc_type (str): 'DEMO' och 'LIVE'. No reason to use a live account here.
            acc_number (str): IG account name.
        """
        self.ig_service = IGService(username, password, api_key, acc_type, acc_number=acc_number)
        self.ig_session = self.ig_service.create_session() 

        logging.info('Connecting to IG Streaming API and creating session')
        self.cur_init += 1

        self.stream_service = IGStreamService(self.ig_service)
        self.stream_session = self.stream_service.create_session()

    def reinit(self):
        """Reinitialize after failed connection or expired CST or security tokens."""
        logging.info('Reconnecting to REST and Streaming API')
        self.disconnect()
        self.connect(
            config.username, 
            config.password, 
            config.api_key, 
            config.acc_type,
            acc_number=config.acc_number,
        )

        logging.info('Re-adding subscriptions')
        for sub in self.subscriptions:
            self.stream_service.ls_client.subscribe(sub)

        logging.info('IG Labs web API connection reestablished')

    def disconnect(self):
        """Disconnect from IG broker."""
        # Unsubscribe manually, because we want that done before timeout timer starts
        self.stream_service.unsubscribe_all()
        t = threading.Thread(target=self.stream_service.disconnect)
        t.daemon = True # Kills thread when main thread ends
        t.start()
        t.join(self.DISCONNECT_TIMEOUT) # Give it some time to disconnect, before pulling the plug

        if t.is_alive():
            # How do we handle reinit in this situation? Do we really need to .logout() before creating a new connection?
            logging.warning(f'Timeout ({self.DISCONNECT_TIMEOUT} s) reached for closing urlopen-connection to Lightstreamer.'
                            'Continuing, but thread is still alive.')
        else:
            self.ig_service.logout() # This only works if disconnect works

    def check_timezone(self):
        """Check that the timezone settings for DST on the IG platform is identical
        to local machine.
        """
        if time.localtime().tm_isdst: # Daylight savings time
            if not self.ig_session['timezoneOffset'] == 2:
                logging.exception(f'Wrong timezone offset {self.ig_session["timezoneOffset"]} in IG session')
                raise ValueError(f'Wrong timezone offset {self.ig_session["timezoneOffset"]} in IG session')
        else:
            if not self.ig_session['timezoneOffset'] == 1:
                logging.exception(f'Wrong timezone offset {self.ig_session["timezoneOffset"]} in IG session')
                raise ValueError(f'Wrong timezone offset {self.ig_session["timezoneOffset"]} in IG session')

    def subscribe_candle_data_stream(self, callback, instrument, timeframe):
        """Subscribe to a stream of candle data from IG Streaming API.

        Args:
            callback (func): Callback function to receive data.
            instrument (str): Chart symbol/asset to subscribe to.
            timeframe (str): Timeframe, must be in ['SECOND', '1MINUTE', '5MINUTE', 'HOUR'].

        Returns:
            int: Subscription key. UTM converts to local time.
        """
        if timeframe not in self.TIMEFRAMES_STREAMING: raise ValueError('Not a valid timeframe for Streaming API')

        logging.info(f'Subscribing to {instrument} for candle data with {timeframe=}')

        subscription = Subscription(
            mode = 'MERGE',
            items = [f'CHART:{instrument}:{timeframe}'],
            fields = ['LTV', 'TTV', 'UTM',
                      'OFR_OPEN', 'OFR_HIGH', 'OFR_LOW', 'OFR_CLOSE',
                      'BID_OPEN', 'BID_HIGH', 'BID_LOW', 'BID_CLOSE',
                      'CONS_END', 'CONS_TICK_COUNT']
        )
        subscription.addlistener(callback)
        self.subscriptions.append(subscription)
        return self.stream_service.ls_client.subscribe(subscription)

    def subscribe_tick_data_stream(self, callback, instrument):
        """Subscribe to a stream of tick data from IG Streaming API.

        Args:
            callback (func): Callback function to receive data.
            instrument (str): Chart symbol/asset to subscribe to.

        Returns:
            int: Subscription key. UTM converts to local time.
        """
        logging.info(f'Subscribing to {instrument} for tick data')

        subscription = Subscription(
            mode = 'DISTINCT',
            items = [f'CHART:{instrument}:TICK'],
            fields = ['BID', 'OFR', 'LTP', 'LTV', 'TTV', 'UTM']
        )
        subscription.addlistener(callback)
        self.subscriptions.append(subscription)
        return self.stream_service.ls_client.subscribe(subscription)


class DataSet():
    """Stateful dataset made up of streamed data.
    
    We save data every hour to minimize RAM footprint. This makes it possible
    to use a very small low-cost online computational instance."""

    def __init__(self, instrument, path, compression):
        """Intialize.

        Args:
            instrument (str): Instrument name.
            path (str): Path to store data. For instance '~/data/tick'. Instrument path is appended.
            compression (str): Compression standard to use. One of {“zstd”, “lz4”, “uncompressed”}.
                The default of None uses LZ4 for V2 files if it is available, otherwise uncompressed.
        """
        self.dataset = []
        self.instrument = instrument
        self.path = os.path.join(path, instrument)
        self.compression = compression

        self.check_path(self.path)
        self.resume_file(self.get_filepath(dt.datetime.now()))

        self.lock = threading.Lock()

    def acquire_lock(func):
        """Decorator that acquires and releases the object threading
        lock when function is called. The main reason is that
        we can get tick updates while saving to disk.
        """
        def inner(self, *args, **kwargs):
            if kwargs.pop('internal', None): # Bypasses lock acquisition
                return func(self, *args, **kwargs)
            else:
                with self.lock:
                    return func(self, *args, **kwargs)
        return inner

    def get_filename(self, timestamp):
        """Get filename based on timestamp. Format: instrument_year-month-day-hour.ftr
        
        Args:
            timestamp (datetime64): Timestamp of one (usually last) sample in data.
        """
        return f'{self.instrument}_{timestamp.strftime("%Y-%m-%d_%H-00")}.ftr'

    def get_filepath(self, timestamp):
        """Get filepath based on timestamp. Format: instrument_year-month-day-hour.ftr
        
        Args:
            timestamp (datetime64): Timestamp of one (usually last) sample in data.
        """
        return os.path.join(self.path, self.get_filename(timestamp))

    def check_path(self, path):
        """Check if save path exists. Create otherwise.
        
        Args:
            path (str): Path to save data to.
        """
        if not os.path.exists(path):
            logging.info(f'Creating destination folder {path}... (not found)')
            os.makedirs(path)

    def resume_file(self, path):
        """Check if datafile exists. If yes, load data to resume collection.
        
        Args:
            path (str): Datafile full path.
        """
        if os.path.exists(path):
            logging.info(f'Resuming collection on {path}')
            dft = pd.read_feather(path)
            self.dataset = list(dft.itertuples(index=False, name=None))

    def dump_to_disk(self, timestamp):
        """Check if it is time to dump data from RAM to disk.
        If so, also save and empty dataframe.

        Args:
            timestamp (datetime64): Latest timestamp
        """
        try:
            prev_timestamp = self.dataset[-1][0]
        except IndexError:
            logging.debug(f'{self.instrument} has no dataset to dump')
        else:
            if not timestamp.hour == prev_timestamp.hour:
                logging.debug(f'Dumping {self.instrument} to disk')

                # Extract data to dump and clean list in RAM. We do this before to_feather()
                # because otherwise we will miss many ticks updates while writing to disk 
                # (if instance is overloaded we may miss ticks anyway)
                dump = pd.DataFrame(self.dataset, columns=['index','bid','ask'])
                self.dataset = []

                # We save data every hour to save RAM (necessary for tick data, but do the same for candles)
                self.to_feather(dump)

    def to_feather(self, df=None):
        """Write DataFrame to disk in feather format.
        
        Args:
            df (DataFrame): DataFrame to save. Default None saves self.df.
        """
        if df is None:
            df = pd.DataFrame(self.dataset, columns=['index','bid','ask'])

        try:
            timestamp = df.iat[-1,0] # Last index
        except IndexError:
            # Nothing to save
            return

        df.to_feather(self.get_filepath(timestamp), compression=self.compression)

    def callback_candle(self, update):
        """Retrieve stream of candle stick type data.

        Data is retrieved continuously (streaming), about every 1 seconds. If the candle
        has finished (consolidated), the candle is saved.

        Args:
            update (dict): Data from IG Streaming service.
        """
        global last_streaming_update

        if self._check_instrument(update):
            if self._consolidated(update):
                logging.debug(f'{self.instrument} consolidated streaming update received')

                # Ok, so we preprocess the timestamp, but that's all
                try:
                    timestamp = dt.datetime.fromtimestamp(float(update['values']['UTM'])/1000) # local time of bar start time
                except TypeError as e:
                    logging.debug(f'{self.instrument} incorrect update from IG: {e}')
                else:
                    last_streaming_update = dt.datetime.now()
                    self.df = pd.concat([self.df, pd.DataFrame(update['values'], index=[timestamp])])
                    self.dump_to_disk()

    @acquire_lock
    def callback_tick(self, update):
        """Retrieve stream of candle stick type data.

        Data is retrieved continuously (streaming), about every 1 seconds. If the candle
        has finished (consolidated), the candle is saved.

        Args:
            update (dict): Data from IG Streaming service.
        """
        global last_streaming_update

        if self._check_instrument(update):
            logging.debug(f'{self.instrument} streaming tick update received')
            timestamp, bid, ask = self._process_tick(update)

            if timestamp is not None:
                last_streaming_update = dt.datetime.now()
                self.dump_to_disk(timestamp)
                self.dataset.append((timestamp, bid, ask))

    def _check_instrument(self, update):
        """Check that update's instrument is correct.

        Args:
            update (dict): Data from IG Streaming service.

        Returns
            bool: True if correct.
        """
        instrument = update['name'].split(':')[1]
        if instrument == self.instrument:
            return True
        else:
            logging.warning(f'{self.instrument} incorrect instrument ({instrument}) in streaming data')
            return False

    def _consolidated(self, update):
        """Routine check of streaming update.

        Args:
            update (dict): Data from IG Streaming service.

        Returns
            bool: True if candle is consolidated.
        """
        try:
            consolidated = int(update['values']['CONS_END'])
        except ValueError as e:
            logging.warning(f'{self.instrument} data callback ValueError: {e}\nUpdate: {update}')
            return False
        else:
            if consolidated:
                return True
            else:
                return False

    def _process_tick(self, update):
        """Process tick update.
        
        Args:
            update (dict): Data from IG Streaming service.
            
        returns
            datetime64, float, float: Timestamp, bid, ask. None, None, None if process failed.
        """
        try:
            timestamp = dt.datetime.fromtimestamp(float(update['values']['UTM'])/1000) # localtime
        except TypeError as e:
            logging.debug(f'{self.instrument} timestamp is None: {e}')
        else:
            try:
                bid = float(update['values']['BID'])
                ask = float(update['values']['OFR'])
            except ValueError as e:
                logging.debug(f'{self.instrument} bid or ask is empty string: {e}')
            except TypeError as e:
                logging.debug(f'{self.instrument} bid or ask is None: {e}')
            else:
                return timestamp, bid, ask

        return None, None, None

def send_notification(subject, message):
    """Send Boto3 notification.

    Args:
        subject (str): Subject.
        message (str): Message.
    """
    client = boto3.client('sns')
    response = client.publish(
        TopicArn=TOPIC_ARN,
        Message=message,
        Subject=subject
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logging.info('Notification sent via Amazon SNS')

def test_localtime_is_correct_timezone(local_tz):
    """Assert that the system's local timezone is correct.

    Args:
        local_tv (str): Timezone in pytz/linux timezone format, eg. 'Europe/Stockholm'.
    """
    # Replace seconds and microseconds so we don't get values that differ
    # (can still happen at minute/hour/day transitions)
    now_local = dt.datetime.now().replace(second=0, microsecond=0)
    now_utc = dt.datetime.now(pytz.utc).replace(second=0, microsecond=0)
    assert now_local == now_utc.astimezone(pytz.timezone(local_tz)).replace(tzinfo=None)


if __name__ == '__main__':
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s %(levelname)s %(message)s',
        handlers = [
            watchtower.CloudWatchLogHandler(
                log_group_name='ig-streaming-service',
                level=logging.INFO),
            logging.StreamHandler(), # Prints to stdout
        ],
    )

    test_localtime_is_correct_timezone('Europe/Stockholm')

    with open('instruments.yaml', 'r') as f:
        instruments = yaml.load(f, Loader=yaml.FullLoader)

    collector = CollectStream()
    compression = 'lz4'

    # # Subscribe to instruments for 1 minute candles
    # datasets_1 = {}
    # for instrument in instruments:
    #     datasets_1[instrument] = DataSet(
    #         instrument, 
    #         os.path.join(os.path.expanduser('~'), 'data', '1'),
    #         compression)
    #     collector.subscribe_candle_data_stream(datasets_1[instrument].callback_candle, instrument, '1MINUTE')

    # Subscribe to instruments for tick data
    datasets_tick = {}
    for instrument in instruments:
        datasets_tick[instrument] = DataSet(
            instrument, 
            os.path.join(os.path.expanduser('~'), 'data', 'tick'),
            compression)
        collector.subscribe_tick_data_stream(datasets_tick[instrument].callback_tick, instrument)

    # Loop until market closes on Friday 23:00 local time
    try:
        now = dt.datetime.now()
        while not (now.weekday() == 4 and now.hour == 23 and now.minute >= 1):
            # Check streaming status
            if last_streaming_update is not None:
                if (now-last_streaming_update).total_seconds() > MAX_PAUSE_STREAMING:
                    logging.warning(f'Streaming of data ceased.')
                    send_notification(
                        'Streaming ceased', 
                        f'Streaming ceased. Initializing connection ({collector.cur_init+1} times).'
                    )

                    last_streaming_update = None

                    if collector.cur_init < collector.MAX_REINITS:
                        collector.reinit() # Verified manually that it works
                    else:
                        logging.warning(f'Max number of reinits reached for this week - exiting')
                        send_notification(
                            'Max reinits reached', 
                            f'Max reinits reached {collector.MAX_REINITS}. Exiting.'
                        )
                        break
            else:
                logging.warning('last_streaming_update is None')

            time.sleep(30)
            now = dt.datetime.now()

    except KeyboardInterrupt:
        # Interrupt loop with Ctrl+C
        logging.warning('Keyboard interrupt')

    logging.info('Saving data buffer to disk before disconnecting')
    # for dataset in datasets_1.values():
    #     dataset.to_feather()

    for dataset in datasets_tick.values():
        dataset.to_feather()

    collector.disconnect()