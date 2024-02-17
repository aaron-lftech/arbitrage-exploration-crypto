# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 13:10:15 2018

@author: Aaron
"""

import pandas as pd
import numpy as np
import json
import logging

import ccxt
import ccxt2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class ArbitrageCalculator:
    def __init__(self, config_path="config.json", initialize=True):
        self.exchanges = {}
        self.euro_values = {}
        self.btc_values = {}
        self.config_path = config_path
        self.load_config()
        if initialize == True:
            self.initialize_exchanges()
            self.load_market_values()
            logging.info("Arbitrage calculator initialization complete.")

    def load_config(self):
        with open(self.config_path, "r") as config_file:
            config = json.load(config_file)
            self.exchanges = config["exchanges"]

    def initialize_exchanges(self):
        for exchange_name in self.exchanges.keys():
            try:
                exchange_class = getattr(ccxt2, exchange_name)
                exchange_obj = exchange_class()
                exchange_obj.load_markets()
                self.exchanges[exchange_name] = exchange_obj
                logging.info(f"Initialized exchange: {exchange_name}")
            except Exception as e:
                logging.error(f"Failed to initialize {exchange_name}: {e}")

    def load_market_values(self):
        """
        Loads the current market values for selected currencies in EUR and BTC.       
        """
        for symbol, exchange_name in [('BTC/EUR', 'kraken'), ('LTC/EUR', 'bitstamp'), 
                                      ('USDT/EUR', 'coinmarketcap'), ('ETH/EUR', 'bitstamp')]:
            try:
                self.euro_values[symbol.split('/')[0]] = float(
                    self.exchanges[exchange_name].fetch_ticker(symbol)["last"]
                )
            except Exception as e:
                logging.error(f"Error loading market value for {symbol}: {e}")
        
        for symbol, exchange_name in [('BTC/USDT', 'bittrex'), ('ETH/BTC', 'bitstamp'), 
                                      ('LTC/BTC', 'bittrex')]:
            try:
                value = float(self.exchanges[exchange_name].fetch_ticker(symbol)["last"])
                self.btc_values[symbol.split('/')[0]] = 1 / value if 'BTC/USDT' == symbol else value
            except Exception as e:
                logging.error(f"Error loading market value for {symbol}: {e}")

    def convert_to_euro(self, initial_quote_paid, quote):
        return initial_quote_paid * self.euro_values[quote]

    def convert_to_btc(self, initial_quote_paid, quote):
        if quote == "BTC":
            return initial_quote_paid
        else:
            return initial_quote_paid * self.btc_values[quote]

    def profitability_calculator(self, df_exchange1, df_exchange2, exchange_obj1, exchange_obj2):
        """
        Calculate the profitability of arbitrage between two exchanges.

        Assuming dataframes have columns: ['timestamp', 'ask_price', 'ask_volume', 'bid_price', 'bid_volume']
        and self.exchanges have been initialized with exchange objects containing 'fees' data.
        Exchange objects in self.exchanges should ideally be instances of classes from 'ccxt2' that have
        'fees' attribute.

        This method needs access to both the ccxt and ccxt2 functionality, as well as the data loaded into
        euro_values and btc_values.
        """

        timestamp = (df_exchange1["timestamp"] + df_exchange2["timestamp"]) // 2
        symbol = df_exchange1.name.split("_")[0]
        base, quote = symbol.split("/")

        exchange_obj1 = self.exchanges[exchange_name1]
        exchange_obj2 = self.exchanges[exchange_name2]

        transaction_fee_exchange1 = exchange_obj1.fees["trading"]["taker"]
        transaction_fee_exchange2 = exchange_obj2.fees["trading"]["taker"]

        withdrawal_fee_exchange1 = self.get_withdrawal_fee(exchange_name1, base)
        withdrawal_fee_exchange2 = self.get_withdrawal_fee(exchange_name2, quote)

        volume_exchange1 = df_exchange1["ask_volume"]
        volume_exchange2 = df_exchange2["bid_volume"]
        max_volume = min(volume_exchange1, volume_exchange2)

        initial_quote_paid, quote_gained = self.calculate_trade_amounts(
            df_exchange1,
            df_exchange2,
            max_volume,
            transaction_fee_exchange1,
            transaction_fee_exchange2,
        )

        quote_gained_after_withdrawal = self.adjust_for_withdrawal(
            quote_gained, withdrawal_fee_exchange2
        )

        gross_profit, percent_gross_profit, net_profit, percent_net_profit = (
            self.calculate_profits(
                initial_quote_paid, quote_gained, quote_gained_after_withdrawal
            )
        )

        gross_profit_euro = self.convert_to_euro(gross_profit, quote)
        gross_profit_btc = self.convert_to_btc(gross_profit, quote)
        net_profit_euro = (
            self.convert_to_euro(net_profit, quote) if net_profit is not None else None
        )
        net_profit_btc = (
            self.convert_to_btc(net_profit, quote) if net_profit is not None else None
        )

        max_initial_amount_euro = self.convert_to_euro(initial_quote_paid, quote)
        max_initial_amount_btc = self.convert_to_btc(initial_quote_paid, quote)

        return pd.DataFrame(
            {
                "max_initial_amount_euro": [max_initial_amount_euro],
                "max_initial_amount_btc": [max_initial_amount_btc],
                "gross_profit_euro": [gross_profit_euro],
                "gross_profit_btc": [gross_profit_btc],
                "percent_gross_profit": [percent_gross_profit],
                "net_profit_euro": [net_profit_euro],
                "net_profit_btc": [net_profit_btc],
                "percent_net_profit": [percent_net_profit],
                "buy_exchange": [exchange_name1],
                "sell_exchange": [exchange_name2],
                "timestamp": [timestamp],
            }
        )

    def get_withdrawal_fee(self, exchange_name, currency):
        try:
            return self.exchanges[exchange_name].fees["funding"]["withdraw"][currency]
        except KeyError:
            return None

    def calculate_trade_amounts(
        self,
        df_exchange1,
        df_exchange2,
        max_volume,
        transaction_fee_exchange1,
        transaction_fee_exchange2,
    ):
        ask_price_exchange1 = df_exchange1["ask_price"]
        bid_price_exchange2 = df_exchange2["bid_price"]

        initial_quote_paid = max_volume * ask_price_exchange1
        initial_base_received = max_volume * (1 - transaction_fee_exchange1)
        quote_gained = (
            initial_base_received
            * bid_price_exchange2
            * (1 - transaction_fee_exchange2)
        )

        return initial_quote_paid, quote_gained

    def adjust_for_withdrawal(self, quote_gained, withdrawal_fee_exchange2):
        if withdrawal_fee_exchange2 is not None:
            return quote_gained - withdrawal_fee_exchange2
        return quote_gained

    def calculate_profits(self, initial_quote_paid, quote_gained, quote_gained_after_withdrawal):
        gross_profit = quote_gained - initial_quote_paid
        percent_gross_profit = (gross_profit / initial_quote_paid) * 100

        if quote_gained_after_withdrawal is not None:
            net_profit = quote_gained_after_withdrawal - initial_quote_paid
            percent_net_profit = (net_profit / initial_quote_paid) * 100
        else:
            net_profit = None
            percent_net_profit = None

        return gross_profit, percent_gross_profit, net_profit, percent_net_profit

    def compare_arbitrage_profitability(df_exchange1, df_exchange2, exchange_obj1, exchange_obj2):
        result1 = self.profitability_calculator(
            df_exchange1, df_exchange2, exchange_obj1, exchange_obj2
        )
        result2 = self.profitability_calculator(
            df_exchange2, df_exchange1, exchange_obj2, exchange_obj1
        )
        return (
            result1
            if result1["gross_profit_euro"].iloc[0]
            > result2["gross_profit_euro"].iloc[0]
            else result2
        )


if __name__ == "__main__":
    exchange_data = ArbitrageCalculator(initialize=True)