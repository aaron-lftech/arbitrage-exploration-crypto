# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 12:12:25 2018

@author: Aaron
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import threading
import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataSelector:
    """
    A class for selecting data rows from a pandas DataFrame based on specific criteria.
    """

    def __init__(self, full_dataframe):
        self.full_dataframe = full_dataframe
        self.finished = False
        self.index = 0
        self.row = None

    def get_next_row(self):
        self.row = self.full_dataframe.loc[self.index]
        if self.index + 2 >= len(self.full_dataframe):
            self.finished = True
        else:
            self.index += 1

    def get_next_row_after_time(self, time_step):
        initial_time = round(self.row["timestamp"] / 1000)
        final_time = initial_time + time_step
        while self.row["timestamp"] / 1000 < final_time:
            self.get_next_row()
            if self.finished == True:
                break


def csv_to_df(filename):
    path = os.path.join("refined_data", filename)
    df = pd.read_csv(path)
    df.name = filename.split(".csv")[0]
    return df

def backtest_results(df, direction):
    """
    Calculates backtest results for trading strategy on given dataframe.

    Params:
    - df: DataFrame containing trade data.
    - direction: 0 or 1, indicating the first or second exchange respectively.

    Returns:
    - Dictionary with profit over time, time, and cumulative profit.
    """
    buy_exchange = df.name.split("_")[2 + direction]

    max_trade_volume = 1000  # How much I am willing to trade on each exchange
    waiting_time_minutes = 90  # Pause between trades in minutes
    waiting_time = waiting_time_minutes * 60  # Converted to seconds
    minimum_profit = 0

    data_row_selector = DataSelector(df)
    profits_over_time = {"profit": [], "time": [], "cumulative_profit": 0}

    data_row_selector.get_next_row()
    while not data_row_selector.finished:
        row = data_row_selector.row
        time_of_result = round(row["timestamp"] / 1000)  # Rounded to nearest second
        profits_over_time["time"].append(time_of_result)
        net_profit_euro = row["net_profit_euro"]
        max_initial_amount = row["max_initial_amount"]
        if max_initial_amount > max_trade_volume:
            max_initial_amount = max_trade_volume

        net_profit_euro = net_profit_euro * max_initial_amount

        if net_profit_euro > minimum_profit:
            profits_over_time["profit"].append(net_profit_euro)
            profits_over_time["cumulative_profit"] += net_profit_euro
            data_row_selector.get_next_row_after_time(waiting_time)
        else:
            data_row_selector.get_next_row()

    logging.info(f"Backtesting results attained for {df.name}.")
    logging.info(f"Cumulative profit = €{round(profits_over_time['cumulative_profit'],2)}")

    profits_over_time["time"] = convert_seconds_to_minutes(profits_over_time["time"])

    minutes_elapsed = profits_over_time["time"][-1]
    profits_over_time["cumulative_profit"] *= 1440 / minutes_elapsed

    return profits_over_time

def convert_time_to_minutes(time_list):
    """
    Converts a list of time values to minutes relative to the first timestamp.

    Params:
    - time_list: List of time values in seconds.

    Returns:
    - List of time values in minutes, relative to the first time and rounded to 2 decimal places.
    """
    time_array = np.array(time_list) - time_list[0]  # Make relative to first time
    return np.round(time_array / 60, 2).tolist()

def plot_profits(filename, direction):
    """
    Plots the profit over time from backtesting results.
    """
    df = csv_to_df(filename)
    results = backtest_results(df, direction)
    plt.figure()
    plt.suptitle(
        "/".join(df.name.split("_")[:2])
        + ", buying on "
        + df.name.split("_")[2 + direction]
    )
    plt.plot(results["time"], results["profit"])
    plt.ylabel("profit (€)")
    plt.xlabel("time (minutes)")
    plt.show()

def put_to_text_files(filename):
    """
    Writes backtest results to a text file if cumulative profit is greater than 10.
    """
    try:
        df = csv_to_df(filename)
        for i in range(2):
            result = backtest_results(df, i)
            if result["cumulative_profit"] > 10:
                with threading.Lock():
                    with open("backtest_results.txt", "a") as text_file:
                        text_file.write(
                            f'"{filename}":{round(result["cumulative_profit"], 2)},\n'
                        )
    except Exception as e:
        print(f"Error processing file {filename}: {e}")

def give_jobs(thread_index):
    """
    Distributes file processing jobs across threads based on the thread index.

    Params:
    - thread_index: Index of the current thread used to calculate file batch.
    """
    try:
        for index in range(4):
            filename = files[thread_index * 4 + index]
            put_to_text_files(filename)
    except Exception as e:
        logging.error(f"Error in give_jobs with thread_index {thread_index}: {e}")

def start_and_join_jobs():
    """
    Initializes and starts threads for job distribution, then waits for all jobs to complete.
    """
    threads = []
    try:
        for thread_index in range(952):
            thread = threading.Thread(
                target=give_jobs, args=(thread_index,), daemon=True
            )
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    except Exception as e:
        logging.error(f"Error in starting or joining threads: {e}")


if __name__ == "__main__":
    try:
        files = [i for i in os.listdir("refined_data")]
        start_and_join_jobs()
        logging.info("Processing complete. Done!")
    except Exception as e:
        logging.error(f"Error in main block: {e}")
