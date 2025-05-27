import boto3
import datetime as dt
import logging
import os
import pandas as pd
import pytz
import time
import tomli
import threading
import watchtower
from dataclasses import dataclass
from lightstreamer.client import SubscriptionListener, ItemUpdate

import ig

from aws_config import TOPIC_ARN
MAX_PAUSE_STREAMING = 30
MAX_REINITS = 60
last_streaming_update = None

@dataclass
class OHLC:
    open: float
    high: float
    low: float
    close: float


class StreamListener(SubscriptionListener):
    """One listener for all streams"""
        
    def __init__(self, dataset):
        """Initialize."""
        self.dataset = dataset
        
    def onItemUpdate(self, update: ItemUpdate):
        """Retrieve stream data.

        Args:
            update (ItemUpdate): Data from IG Streaming service.
        """
        global last_streaming_update
        last_streaming_update = dt.datetime.now()

        try:
            stream_name_split = update.getItemName().split(':')
            values = update.getFields()
            # print(stream_name_split, values) # For DEBUG
            
            if stream_name_split[0] == 'CHART' and stream_name_split[2] == 'TICK':
                self.dataset[stream_name_split[1]].update(values)
                
            elif stream_name_split[0] == 'CHART' and stream_name_split[2] == '1MINUTE':
                self.dataset[stream_name_split[1]].update(values)
                
            elif stream_name_split[0] == 'PRICE':
                self.dataset[stream_name_split[2]].update(values)
        
        except Exception as e:
            logging.exception(f'Exception in stream callback: {e}')


class DataSet():
    """Stateful dataset made up of streamed data.
    
    We save data every hour to minimize RAM footprint. This makes it possible
    to use a very small low-cost online computational instance.
    """
    COLS = []
    descriptor = 'Baseclass'

    def __init__(self, market_info, path, compression):
        """Intialize.

        Args:
            market_info (dict): IG market info dict.
            path (str): Path to store data. For instance '~/data/tick'. Epic path is appended.
            compression (str): Compression standard to use. One of {“zstd”, “lz4”, “uncompressed”}.
                The default of None uses LZ4 for V2 files if it is available, otherwise uncompressed.
        """
        self.dataset = []
        self.epic = market_info['instrument']['epic']
        self.lot_size = float(market_info['instrument']['lotSize'])
        self.scaling_factor = int(market_info['snapshot']['scalingFactor'])
        self.path = os.path.join(path, epic)
        self.compression = compression

        self.prev_bid = None
        self.prev_ask = None

        os.makedirs(self.path, exist_ok=True)
        self._resume_file(self._get_filepath(dt.datetime.now()))

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

    def _get_filename(self, timestamp):
        """Get filename based on timestamp. Format: epic_year-month-day-hour.ftr
        
        Args:
            timestamp (datetime64): Timestamp of one (usually last) sample in data.
        """
        return f'{self.epic}_{timestamp.strftime("%Y-%m-%d_%H-00")}.ftr'

    def _get_filepath(self, timestamp):
        """Get filepath based on timestamp. Format: epic_year-month-day-hour.ftr
        
        Args:
            timestamp (datetime64): Timestamp of one (usually last) sample in data.
        """
        return os.path.join(self.path, self._get_filename(timestamp))

    def _resume_file(self, path):
        """Check if datafile exists. If yes, load data to resume collection.
        
        Args:
            path (str): Datafile full path.
        """
        if os.path.exists(path):
            logging.info(f'Resuming collection on {path}')
            dft = pd.read_feather(path)
            self.dataset = list(dft.itertuples(index=False, name=None))

    @staticmethod
    def _from_timestamp(timestamp):
        """Convert timestamp in seconds from epoch to datetime.

        Execution time on the order of 1 us.
        
        Args:
            timestamp (int): Timestamp in seconds since epoch.

        Returns:
            datetime64: Timestamp.
        """
        return dt.datetime.fromtimestamp(float(timestamp)/1000) # local time of bar start time

    @acquire_lock
    def to_feather(self, df=None):
        """Write DataFrame to disk in feather format. Lock here to not alter the
        data while saving after a keyboard interrupt.
        
        Args:
            df (DataFrame): DataFrame to save. Default None saves self.dataset.
        """
        if df is None:
            df = pd.DataFrame(self.dataset, columns=self.COLS)

        try:
            timestamp = df.iat[-1,0] # Last index
        except IndexError as e:
            # Nothing to save
            logging.debug(f'{self.epic} {self.descriptor} dataset saving IndexError: {e}')
        else:
            df.to_feather(self._get_filepath(timestamp), compression=self.compression)

    @acquire_lock
    def append(self, timestamp, row):
        """Clean up after each received element. Update state and save
        to disk if it is time.
        
        Args:
            timestamp (datetime64): Timestamp from last received element.
            row (tuple): Tuple of row data to append to dataset.
        """
        global last_streaming_update
        last_streaming_update = dt.datetime.now()

        try:
            prev_timestamp = self.dataset[-1][0]

        except IndexError:
            logging.debug(f'{self.epic} {self.descriptor} dataset is empty')
            self.dataset.append(row)

        else:
            if not prev_timestamp.hour == timestamp.hour:
                logging.debug(f'Dumping {self.epic} {self.descriptor} to disk')

                # Extract data to dump and clean list in RAM.
                dump = pd.DataFrame(self.dataset, columns=self.COLS)
                self.dataset = []
                self.dataset.append(row)

                # We save data every hour to save RAM (necessary for tick data, but do the same for candles)
                # internal=True bypasses lock acquisiton, because this function is called via
                # the callback, that has already locked the object.
                self.to_feather(dump, internal=True)

            elif not prev_timestamp == timestamp:
                self.dataset.append(row)

            else:
                logging.warning(f'{self.epic} {self.descriptor} not adding row, row with identical timestamp already added')
            

class DataSetBook(DataSet):
    """Data set for IG order book data stream."""
    COLS = [
        'index', 'status',
        'bid_price_1', 'bid_size_1', 'ask_price_1', 'ask_size_1',
        'bid_price_2', 'bid_size_2', 'ask_price_2', 'ask_size_2',
        'bid_price_3', 'bid_size_3', 'ask_price_3', 'ask_size_3',
        'bid_price_4', 'bid_size_4', 'ask_price_4', 'ask_size_4',
        'bid_price_5', 'bid_size_5', 'ask_price_5', 'ask_size_5',
    ]
    descriptor = 'Order Book'

    def update(self, values):
        """Process tick update.
        
        Args:
            values (dict): Data from IG Streaming service.
        """
        try:
            timestamp = self._from_timestamp(values['TIMESTAMP'])
            book_entry = [timestamp, values['DLG_FLAG'].strip()]
            for k in range(1, 6):
                try:
                    bid_price = round(float(values[f'BIDPRICE{k}'])*self.scaling_factor, 2)
                    bid_size = float(values[f'BIDSIZE{k}'])/self.lot_size
                    ask_price = round(float(values[f'ASKPRICE{k}'])*self.scaling_factor, 2)
                    ask_size = float(values[f'ASKSIZE{k}'])/self.lot_size
                except (TypeError, ValueError):
                    bid_price = float('nan')
                    bid_size = float('nan')
                    ask_price = float('nan')
                    ask_size = float('nan')

                book_entry.extend([
                    bid_price,
                    bid_size,
                    ask_price,
                    ask_size,
                ])

            self.append(timestamp, tuple(book_entry))

        except Exception as e:
            logging.exception(f'{self.epic} {self.descriptor} callback error: {e}\nUpdate: {values}')            


class DataSetOHLCV(DataSet):
    """Data set for IG OHLCV candle data stream."""
    COLS = [
        'index', 'bid_open', 'bid_high', 'bid_low', 'bid_close',
        'ask_open', 'ask_high', 'ask_low', 'ask_close', 'volume',
    ]
    descriptor = 'OHLCV'

    def update(self, values):
        """Process tick update.
        
        Args:
            values (dict): Data from IG Streaming service.
        """
        try:
            if int(values['CONS_END']):
                timestamp = self._from_timestamp(values['UTM'])

                if not all([v == '' for k, v in values.items() if k.startswith('BID_')]):
                    bid = OHLC(
                        round(float(values['BID_OPEN']) *self.scaling_factor, 2),
                        round(float(values['BID_HIGH']) *self.scaling_factor, 2),
                        round(float(values['BID_LOW'])  *self.scaling_factor, 2),
                        round(float(values['BID_CLOSE'])*self.scaling_factor, 2),
                    )
                    self.prev_bid = bid
                    
                elif self.prev_bid is not None:
                    bid = self.prev_bid

                else:
                    return
                
                if not all([v == '' for k, v in values.items() if k.startswith('OFR_')]):
                    ask = OHLC(
                        round(float(values['OFR_OPEN']) *self.scaling_factor, 2),
                        round(float(values['OFR_HIGH']) *self.scaling_factor, 2),
                        round(float(values['OFR_LOW'])  *self.scaling_factor, 2),
                        round(float(values['OFR_CLOSE'])*self.scaling_factor, 2),
                    )
                    self.prev_ask = ask
                    
                elif self.prev_ask is not None:
                    ask = self.prev_ask

                else:
                    return
                
                self.append(
                    timestamp, 
                    (
                        timestamp,
                        bid.open,
                        bid.high,
                        bid.low,
                        bid.close,
                        ask.open,
                        ask.high,
                        ask.low,
                        ask.close,
                        int(values['CONS_TICK_COUNT']),
                    )
                )

        except Exception as e:
            logging.exception(f'{self.epic} {self.descriptor} callback error: {e}\nUpdate: {values}')


class DataSetTick(DataSet):
    """Data set for IG tick data stream."""
    COLS = ['index', 'bid', 'ask']
    descriptor = 'Tick'

    def update(self, values):
        """Process tick update.

        If a bid or ask price is unchanged, then that value comes in as a None.
        
        Args:
            values (dict): Data from IG Streaming service.
        """
        try:
            if values['UTM'] is not None and (values['BID'] is not None or values['OFR'] is not None):
                timestamp = self._from_timestamp(values['UTM'])

                if values['BID'] is not None:
                    bid = float(values['BID'])
                    self.prev_bid = bid
                elif self.prev_bid is not None:
                    bid = self.prev_bid
                else:
                    return
                
                if values['OFR'] is not None:
                    ask = float(values['OFR'])
                    self.prev_ask = ask
                elif self.prev_ask is not None:
                    ask = self.prev_ask
                else:
                    return
                
                self.append(timestamp, (timestamp, bid, ask))

            else:
                # This happens from time to time (all None). Initial test
                # shows these updates should not count towards the tick count.
                logging.debug(f'{self.epic} empty tick update: {values}')

        except Exception as e:
            logging.exception(f'{self.epic} {self.descriptor} callback error: {e}\nUpdate: {values}')


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

    with open('epics.toml', 'rb') as f:
        epics = tomli.load(f)['epics']

    link = ig.Link() # Log in and create session
    market_infos = dict(sorted(link.fetch_markets_by_epics(epics).items()))

    # Subscribe to epics for tick data
    datasets_book = {}
    datasets_ohlcv = {}
    datasets_tick = {}
    basedir = os.path.join(os.path.expanduser('~'), 'data')
    # Add one day, so that we can start collecting on Sunday,
    # and still get the correct week number
    dir_suffix = (dt.datetime.now()+dt.timedelta(days=1)).strftime("%Y-%V")
    compression = 'lz4'

    for epic in epics:
        datasets_book[epic] = DataSetBook(
            market_infos[epic], os.path.join(basedir, f'book_{dir_suffix}'), compression,
        )
        datasets_ohlcv[epic] = DataSetOHLCV(
            market_infos[epic], os.path.join(basedir, f'ohlcv_1m_{dir_suffix}'), compression,
        )
        datasets_tick[epic] = DataSetTick(
            market_infos[epic], os.path.join(basedir, f'tick_{dir_suffix}'), compression,
        )
        
    link.subscribe_prices(StreamListener(datasets_book), epics)
    link.subscribe_candles(StreamListener(datasets_ohlcv), epics, '1MINUTE')
    link.subscribe_ticks(StreamListener(datasets_tick), epics)

    # Loop until market closes on Friday 23:00 local time
    try:
        now = dt.datetime.now()
        while not (now.weekday() == 4 and now.hour == 23):
            # Check streaming status
            try:
                if last_streaming_update is not None:
                    if (now-last_streaming_update).total_seconds() > MAX_PAUSE_STREAMING:
                        if not (now.weekday() == 4 and now.hour == 22 and now.minute > 50):
                            logging.warning(f'Streaming of data ceased.')
                            send_notification(
                                'Streaming ceased', 
                                f'Streaming ceased. Initializing connection ({link.cur_init+1} times).'
                            )

                            if link.cur_init < MAX_REINITS:
                                link.reinit() # Verified manually that it works
                            else:
                                logging.warning(f'Max number of reinits reached for this week - exiting')
                                send_notification(
                                    'Max reinits reached', 
                                    f'Max reinits reached {MAX_REINITS}. Exiting.'
                                )
                                break
                        else:
                            logging.warning('Lost streaming connection, but do not do anything if we nearing market closing')
                else:
                    logging.warning('last_streaming_update is None')

            except Exception as e:
                logging.exception(f'Unknown exception: {e}')
                try:
                    send_notification('Unknown exception', f'{e}')
                except Exception as e:
                    logging.exception(f'Unknown exception sending notification: {e}')

            time.sleep(30)
            now = dt.datetime.now()

    except KeyboardInterrupt:
        # Interrupt loop with Ctrl+C
        logging.warning('Keyboard interrupt')

    logging.info('Saving data buffer to disk before exiting')
    for dataset in datasets_book.values():
        try:
            dataset.to_feather()
        except Exception as e:
            logging.exception(f'Saving order book dataset for epic {dataset.epic} caused exception: {e}')

    for dataset in datasets_ohlcv.values():
        try:
            dataset.to_feather()
        except Exception as e:
            logging.exception(f'Saving OHCLV dataset for epic {dataset.epic} caused exception: {e}')

    for dataset in datasets_tick.values():
        try:
            dataset.to_feather()
        except Exception as e:
            logging.exception(f'Saving tick dataset for epic {dataset.epic} caused exception: {e}')

    link.deinit() # Unsubscribe and disconnect