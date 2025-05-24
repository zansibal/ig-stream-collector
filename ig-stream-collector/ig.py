import logging
import pandas as pd
import threading
import time

from lightstreamer.client import Subscription
from trading_ig import (IGService, IGStreamService)
from trading_ig.config import config


class Link():
    """Wrapper for trading_ig lib that is used to communicate with IG Labs web API."""

    TIMEFRAMES_REST = ['1Min', '2Min', '3Min', '5Min', '10Min', '15Min', '30Min', '1H', '2H', '3H', '4H']
    TIMEFRAMES_STREAMING = ['SECOND', '1MINUTE', '5MINUTE', 'HOUR']
    DISCONNECT_TIMEOUT = 10 # In seconds
    subscriptions = []
    HISTORIC_DATA_DELAY = { # In seconds
        'LIVE': 10,
        'DEMO': 10,
    }
    cur_init = 0

    def __init__(self):
        """Initialize."""
        self.connected = False
        logging.info('Connecting to IG REST trading API and creating session')
        self.connect()
        logging.info('IG Labs web API connection established')
        print('IG Labs web API connection established')

    def if_connected(func):
        """Decorator that checks that we are connected to IG before calling function."""
        def inner(self, *args, **kwargs):
            if self.connected:
                return func(self, *args, **kwargs)
            else:
                return None
        return inner

    def deinit(self):
        """Deinitialize."""
        # Unsubscribe manually, because we want that done before timeout timer starts
        self.connected = False
        self.ig_stream_service.unsubscribe_all()
        t = threading.Thread(target=self.ig_stream_service.disconnect)
        t.daemon = True # Kills thread when main thread ends
        t.start()
        t.join(self.DISCONNECT_TIMEOUT)
        if t.is_alive():
            # How do we handle reinit in this situation? Do we really need to .logout() before creating a new connection?
            logging.warning(f'Timeout ({self.DISCONNECT_TIMEOUT}s) reached for closing urlopen-connection to Lightstreamer. Continuing, but thread is still alive.')
        else:
            self.ig_service.logout() # This only works if disconnect works

    def reinit(self):
        """
        Reinitialize after failed connection or expired CST or security tokens.
        
        Returns:
            bool: True if no exception occured.
        """
        try:
            logging.info('Reconnecting to REST and Streaming API')
            self.deinit()
            self.connect()
            logging.info('Re-adding subscriptions')
            for sub in self.subscriptions:
                self.ig_stream_service.subscribe(sub)
            logging.info('IG Labs web API connection reestablished')
            return True

        except Exception as e:
            logging.exception(f'Unknown exception reconnecting to IG: {e}')
            return False

    def connect(self):
        """Connect to REST and Streaming API"""
        print('{} trading'.format(config.acc_type.upper()))
        two_factor_pass = ''

        self.ig_service = IGService(
            config.username, 
            config.password+two_factor_pass, 
            config.api_key, 
            config.acc_type,
            acc_number=config.acc_number) # trading_ig 0.0.15

        session_data = self.ig_service.create_session() 
        assert session_data['currencyIsoCode'] == 'SEK'
        assert session_data['currencySymbol'] == 'SEK'
        # Very important for data processing and prediction:
        if time.localtime().tm_isdst: # Daylight savings time
            assert session_data['timezoneOffset'] == 2
        else:
            assert session_data['timezoneOffset'] == 1
        
        self.account_id = config.acc_number

        logging.info('Connecting to IG Streaming API and creating session')
        self.cur_init += 1
        self.ig_stream_service = IGStreamService(self.ig_service)
        self.ig_session = self.ig_stream_service.create_session()
        self.connected = True
        
    @if_connected
    def fetch_markets_by_epics(self, epics):
        """Fetch market info for all epics in list.

        Args:
            epics (list of str): List of epics.

        Returns:
            dict: {epic: market_info}.
        """
        try:
            resp = self.ig_service.fetch_markets_by_epics(','.join(epics))

        except Exception as e:
            logging.warning(f'Unknown exception fetching market info (continuing): {e}')

            # Returning an empty DataFrame will remove existing positions. Dangerous.
            return pd.DataFrame()

        else:
            if resp is not None and len(resp):
                return {r['instrument']['epic']: r.toDict() for r in resp}
            else:
                return resp

    def subscribe_account(self, callback):
        """Subscribe to a stream of changes to the account.

        Args:
            callback (func): Callback function to receive data.
        """
        subscription = Subscription(
            mode = 'MERGE',
            items = [f'ACCOUNT:{self.account_id}'],
            fields = ['DEPOSIT', 'AVAILABLE_CASH', 'PNL', 'PNL_LR', 'PNL_NLR', 'FUNDS', 'MARGIN',
                      'MARGIN_LR', 'MARGIN_NLR', 'AVAILABLE_TO_DEAL', 'EQUITY', 'EQUITY_USED'],
        )
        self._subscribe(subscription, callback)

    def subscribe_candles(self, callback, epics, timeframe):
        """Subscribe to a stream of candle data from IG Streaming API.

        Args:
            callback (func): Callback function to receive data.
            epics (list of str): Chart symbol/asset to subscribe to.
            timeframe (str): Timeframe (SECOND, 1MINUTE, 5MINUTE, HOUR).
        """
        if timeframe not in self.TIMEFRAMES_STREAMING: raise ValueError('Not a valid timeframe for Streaming API')

        subscription = Subscription(
            mode = 'MERGE',
            items = [f'CHART:{epic}:{timeframe}' for epic in epics],
            fields = ['LTV', 'TTV', 'UTM',
                      'OFR_OPEN', 'OFR_HIGH', 'OFR_LOW', 'OFR_CLOSE',
                      'BID_OPEN', 'BID_HIGH', 'BID_LOW', 'BID_CLOSE',
                      'CONS_END', 'CONS_TICK_COUNT'],
        )
        self._subscribe(subscription, callback)

    def subscribe_prices(self, callback, epics):
        """Subscribe to a stream of candle data from IG Streaming API.

        Args:
            callback (func): Callback function to receive data.
            epics (list of str): Chart symbol/asset to subscribe to.
        """
        subscription = Subscription(
            mode = 'MERGE',
            items = [f'PRICE:{self.account_id}:{epic}' for epic in epics],
            fields = ['BIDPRICE1', 'BIDSIZE1', 'ASKPRICE1', 'ASKSIZE1',
                      'BIDPRICE2', 'BIDSIZE2', 'ASKPRICE2', 'ASKSIZE2',
                      'BIDPRICE3', 'BIDSIZE3', 'ASKPRICE3', 'ASKSIZE3',
                      'BIDPRICE4', 'BIDSIZE4', 'ASKPRICE4', 'ASKSIZE4',
                      'BIDPRICE5', 'BIDSIZE5', 'ASKPRICE5', 'ASKSIZE5',
                      'TIMESTAMP', 'DLG_FLAG'],
        )
        subscription.setDataAdapter('Pricing')
        self._subscribe(subscription, callback)

    def subscribe_quotes(self, callback, epics):
        """Subscribe to a stream of ask/bid prices from IG Streaming API.

        Args:
            callback (func): Callback function to receive data.
            epics (list of str): Chart symbol/asset to subscribe to.
        """
        subscription = Subscription(
            mode = 'MERGE',
            items = [f'MARKET:{epic}' for epic in epics],
            fields = ['UPDATE_TIME', 'MARKET_DELAY', 'MARKET_STATE', 'BID', 'OFFER']
        )
        self._subscribe(subscription, callback)

    def subscribe_ticks(self, callback, epics):
        """Subscribe to a stream of ask/bid prices at tick level from IG Streaming API.

        Args:
            callback (func): Callback function to receive data.
            epics (list of str): Chart symbol/asset to subscribe to.
        """
        subscription = Subscription(
            mode = 'DISTINCT',
            items = [f'CHART:{epic}:TICK' for epic in epics],
            fields = ['BID', 'OFR', 'UTM'],
        )
        self._subscribe(subscription, callback)

    def subscribe_trades(self, callback):
        """Subscribe to a stream of changes to positions.

        Args:
            callback (func): Callback function to receive data.
        """
        subscription = Subscription(
            mode = 'DISTINCT',
            items = ['TRADE:' + self.account_id],
            fields = ['CONFIRMS', 'OPU', 'WOU'],
        )
        self._subscribe(subscription, callback)

    def unsubscribe(self, key):
        """Unsubscribe a stream.

        Args:
            key (int): Subscription key.
        """
        self.ig_stream_service.ls_client.unsubscribe(key)

    def unsubscribe_all(self):
        """Unsubscribe all streams."""
        self.ig_stream_service.unsubscribe_all()

    def _subscribe(self, subscription, callback):
        """Add subscription to trading_ig
    
        Args:
            subscription (Subscription): Subscription to add.
            callback (func): Callback function to receive data.

        Returns:
            int: Subscription key.
        """
        subscription.addListener(callback)
        self.subscriptions.append(subscription)
        self.ig_stream_service.subscribe(subscription)