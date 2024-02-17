# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 15:07:22 2018
@author: Aaron

Module for processing order books from various exchanges.
Handles data collection, processing, and storage in an efficient and thread-safe manner.
"""

import pandas as pd
import numpy as np
import threading
import time
import sched
import sys
import logging
import functools

import initialise_exchanges

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class OrderBookProcessor:
    """
    A class for processing order books from exchanges.
    """

    lock = threading.Lock()

    def __init__(self, server_number, exchanges):
        self.server_number = server_number
        self.exchanges = exchanges
    
    def log_errors(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Error in {func.__name__}: {e}")
                raise
        return wrapper

    @staticmethod
    def get_path(symbol, exchange_name, server_number, file_type):
        symbol = "_".join(symbol.split("/"))
        key = f"{symbol}_{exchange_name}"
        return f"orderbook{server_number}/{key}.{file_type}", key

    @log_execution
    def write_to_hdf(self, symbol, exchange_name, df):
        path, key = self.get_path(symbol, exchange_name, self.server_number, "h5")
        with self.lock, pd.HDFStore(path) as store:
            store.put(key, df, format="t")

    @log_execution
    def append_to_hdf(symbol, exchange_name, df):
        path, key = self.get_path(symbol, exchange_name, self.server_number, "h5")
        with self.lock, pd.HDFStore(path) as store:
            store.append(key, df, ignore_index=True)

    @log_execution
    def retrieve_hdf_data(self, symbol, exchange_name):
        path, key = self.get_path(symbol, exchange_name, self.server_number, "h5")
        with self.lock, pd.HDFStore(path) as store:
            return store[key]

    @log_execution
    def write_to_csv(self, symbol, exchange_name, df):
        path, _ = self.get_path(symbol, exchange_name, self.server_number, 'csv')
        with self.lock, open(path, 'w') as f:
            df.to_csv(f, index=False)

    @log_execution
    def append_to_csv(self, symbol, exchange_name, df):
        path, _ = self.get_path(symbol, exchange_name, self.server_number, "csv")
        with self.lock, open(path, "a") as f:
            df.to_csv(f, index=False, header=False)

    @log_execution
    def retrieve_csv_data(self, symbol, exchange_name):
        path, _ = self.get_path(symbol, exchange_name, self.server_number, "csv")
        return pd.read_csv(path)

    @log_execution
    def get_orderbook(self, symbol, exchange_object):
        """
        Retrieves the current order book for a given exchange.
        """
        exchange_name = exchange_object.id
        orderbook = self.fetch_orders_safely(symbol, exchange_object, 0)
        if not orderbook["asks"]:
            self.exchanges[exchange_name]["symbols"].remove(symbol)
            return pd.DataFrame(columns=["A"])
        precision = get_precision(exchange_object, symbol)
        orderbook_df = process_orderbook_data(orderbook, precision)
        return orderbook_df

    def fetch_orders_safely(self, symbol, exchange_object, attempt):
        if attempt < 5:
            try:
                return exchange_object.fetch_l2_order_book(symbol, 5)
            except Exception as e:
                time.sleep(5)
                return self.fetch_orders_safely(symbol, exchange_object, attempt + 1)
        return {"asks": False, "bids": False}

    def get_precision(self, exchange_object, symbol):
        """
        Determines the price precision for a given exchange
        """
        precision = exchange_object.markets[symbol].get(
            "precision", {"amount": 8, "price": 8}
        )
        if not isinstance(precision.get("amount"), int) or not isinstance(
            precision.get("price"), int
        ):
            precision = {"amount": 8, "price": 8}
        return precision

    def process_orderbook_data(self, orderbook, precision):
        timestamp = int(round(time.time() * 1000))
        orderbook_df = self.calculate_weighted_prices(orderbook, precision, timestamp)
        return self.convert_orderbook_dtypes(orderbook_df, precision)

    def calculate_weighted_prices(self, orderbook, precision, timestamp):
        bid_volume, bid_weighted_price = self.calculate_volume_and_weighted_price(
            orderbook["bids"][:3]
        )
        ask_volume, ask_weighted_price = self.calculate_volume_and_weighted_price(
            orderbook["asks"][:3]
        )
        orderbook_dict = {
            "timestamp": [timestamp],
            "bid_price": [round(bid_weighted_price, precision["price"])],
            "bid_volume": [round(bid_volume, precision["amount"])],
            "ask_price": [round(ask_weighted_price, precision["price"])],
            "ask_volume": [round(ask_volume, precision["amount"])],
        }
        return pd.DataFrame(orderbook_dict)

    @staticmethod
    def calculate_volume_and_weighted_price(orders):
        volume = sum(order[1] for order in orders)
        weighted_price = (
            sum(order[0] * order[1] for order in orders) / volume if volume else 0
        )
        return volume, weighted_price

    @staticmethod
    def convert_orderbook_dtypes(df, precision):
        price_dtype = np.float32 if precision["price"] < 7 else np.float64
        volume_dtype = np.float32 if precision["amount"] < 7 else np.float64
        return df.astype(
            {
                "bid_price": price_dtype,
                "bid_volume": volume_dtype,
                "ask_price": price_dtype,
                "ask_volume": volume_dtype,
            }
        )

    def collect_data(self, symbol, exch_object):
        """
        Collects order book data from all exchanges and processes it.

        This method orchestrates the retrieval, processing, and storage of order book data from 
        the configured exchanges.
        """
        exchange_name = str(exch_object.id)
        orderbook = self.get_orderbook(symbol, exch_object)
        if not orderbook.empty:
            try:
                self.retrieve_csv_data(symbol, exchange_name)
            except FileNotFoundError:
                self.write_to_csv(symbol, exchange_name, orderbook)
            else:
                self.append_to_csv(symbol, exchange_name, orderbook)

    @staticmethod
    def scheduled_task(exchange, exchanges):
        """
        A scheduled task that triggers data collection at regular intervals.
        """
        try:
            exch_object = exchanges[exchange]["exch_object"]
            all_symbols = exchanges[exchange]["symbols"]
            rate_limit = (
                    5.1 if exch_object.id == "quadrigacx" else exch_object.rateLimit / 1000
                )
            logging.info(f"Scheduled tasks for {exchange} executed successfully.")
        except AttributeError:
            logging.error(f"EXCHANGE {exchange} BEING REMOVED.")
        s = sched.scheduler(time.time, time.sleep)
        for symbol in all_symbols:
            s.enter(rate_limit, 0, collect_data, argument=(symbol, exch_object))
        s.run()

    def set_threads(self):
        """
        Initializes and starts threads for data collection and processing.

        This method sets up threading for the OrderBookProcessor to enable concurrent data 
        collection and processing.
        """
        try:
            threads = [
                threading.Thread(target=self.scheduled_task, args=(exchange,), daemon=True)
                for exchange in self.exchanges
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            logging.info("All threads set successfully.")
        except Exception as e:
            logging.error(f"Error setting up threads: {e}")

if __name__ == "__main__":
    server_num = int(sys.argv[1])
    exchanges = initialise_exchanges.initialise(server_num)
    processor = OrderBookProcessor(exchanges)
    processor.set_threads()