# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 15:36:24 2018

@author: Aaron
"""

import os
import pandas as pd
import time
import itertools
import threading

import profitability_calculator as pc


class SynchronizedDataSelector:
    """
    Synchronizes rows between two data frames based on closest timestamps within a tolerance.
    """
    
    def __init__(self, df1, df2, tolerance_ms=60000):
        self.df1 = df1
        self.df2 = df2
        self.tolerance = tolerance_ms
        self.index1 = 0
        self.index2 = 0
        self.finished = False  
    
    def get_next_row_pair(self):
        """
        Finds the next pair of rows from df1 and df2 with timestamps as close as possible within tolerance.
        """
        while not self.finished:
            row1_now = self.df1.iloc[self.index1]
            row2_now = self.df2.iloc[self.index2]
            
            if self._is_last_row(self.index1, self.df1) or self._is_last_row(self.index2, self.df2):
                self.finished = True
                return None, None

            time_diff = abs(row1_now['timestamp'] - row2_now['timestamp'])
            
            if time_diff < self.tolerance:
                self.index1 += 1
                self.index2 += 1
                return row1_now, row2_now
            else:
                self._advance_index_with_greater_timestamp(row1_now, row2_now)
 
    def _is_last_row(self, index, df):
        """
        Checks if the current index is the last one in the DataFrame.
        """
        return index + 1 >= len(df)

    def _advance_index_with_greater_timestamp(self, row1, row2):
        """
        Advances the index of the DataFrame with the greater timestamp.
        """
        if row1['timestamp'] > row2['timestamp']:
            self.index2 += 1
        else:
            self.index1 += 1


def load_csv_to_df(filename, directory='raw_data'):
    """
    Loads a CSV file into a DataFrame and sets its name based on the file's naming convention.
    """
    path = os.path.join(directory, filename)
    df = pd.read_csv(path)
    symbol, exchange = filename.split('.csv')[0].split('_')[0:2]
    df.name = f'{symbol}/{exchange}'
    return df

def save_df_to_csv(df, directory='refined_data'):
    """
    Saves a DataFrame to a CSV file in the specified directory, naming it based on the DataFrame's name.
    """
    filename = df.name.replace('/', '_') + '.csv'
    path = os.path.join(directory, filename)
    df.to_csv(path, index=False)
    print(f'{df.name} saved to csv at {path}.')
    
def refine_results(df1, df2, exchange_obj1, exchange_obj2, arbitrage_calculator):
    """
    Refines results from two data frames and saves the refined data to a CSV file.
    """
    selector = SynchronizedDataSelector(df1, df2)
    refined_data = pd.DataFrame()
    
    while not selector.finished:
        row1, row2 = selector.get_next_row_pair()
        if row1 is None or row2 is None:
            break
        result = arbitrage_calculator.compare_arbitrage_profitability(row1, row2, exchange_obj1, exchange_obj2)
        refined_data = pd.concat([refined_data, result], ignore_index=True)

    refined_data.name = generate_final_dataframe_name(df1, df2)  
    save_df_to_csv(refined_data)

def generate_final_dataframe_name(df1, df2):
    """
    Generates a name for the final DataFrame based on input DataFrame names.
    """
    symbol = '_'.join(df1.name.split('_')[0].split('/'))
    exchange1 = df1.name.split('_')[1]
    exchange2 = df2.name.split('_')[1]
    return '_'.join([symbol, exchange1, exchange2])
 
def get_symbols_and_filenames(directory='raw_data'):
    """
    Organizes files by symbol from a specified directory.
    """
    files = [f for f in os.listdir(directory) if not 'orderbook' in f]
    symbols_and_filenames = {}
    for file in files:
        symbol = '/'.join(file.split('_')[0:2])
        symbols_and_filenames.setdefault(symbol, []).append(file)
    return symbols_and_filenames

def get_exchange_filename_pairs(symbol, symbols_and_filenames):
    """
    Generates all combinations of exchange filenames for a given symbol.
    """
    filenames = symbols_and_filenames[symbol]
    return itertools.combinations(filenames, 2)

def process_filename_pair(filename_pair, arbitrage_calculator):
    """
    Processes a pair of filenames to refine arbitrage results.
    """
    filename1, filename2 = filename_pair
    exchange1, exchange2 = filename1.split('_')[2], filename2.split('_')[2]
    symbol1, symbol2 = '/'.join(filename1.split('_')[0:2]), '/'.join(filename2.split('_')[0:2])

    df1, df2 = load_csv_to_df(filename1), load_csv_to_df(filename2)
    df1.name, df2.name = f'{symbol1}_{exchange1}', f'{symbol2}_{exchange2}'

    exchange_obj1, exchange_obj2 = arbitrage_calculator.exchanges[exchange1], arbitrage_calculator.exchanges[exchange2]
    refine_results(df1, df2, exchange_obj1, exchange_obj2, arbitrage_calculator)

def start_iteration_for_symbol(symbol, symbols_and_filenames, arbitrage_calculator):
    """
    Starts processing for all exchange filename pairs associated with a symbol.
    """
    for filename_pair in get_exchange_filename_pairs(symbol, symbols_and_filenames):
        process_filename_pair(filename_pair, arbitrage_calculator)

def main():
    """
    Main function to orchestrate the refining of arbitrage results.
    """
    symbols_and_filenames = get_symbols_and_filenames()
    threads = []
    for symbol in symbols_and_filenames:
        thread = threading.Thread(target=start_iteration_for_symbol, args=(symbol, symbols_and_filenames, arbitrage_calculator))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    arbitrage_calculator = pc.ArbitrageCalculator(initialize=True)
    main()