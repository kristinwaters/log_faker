import os
import re
import gzip
import shutil
import random
import argparse
import datetime

import pandas as pd

from common.log_generator import BaseLogGenerator
from utils import to_datetime, to_timestamp


class MSSQLLogGenerator(BaseLogGenerator):

    _ROOT = os.path.abspath(os.path.dirname(__file__))
    _SOURCE = open(os.path.join(_ROOT, 'samples/mssql.log'), 'r').readlines()

    def __init__(self, start=None, end=None, count=None, outdir=None, filename=None):
        """
        Initial object with time series
        """
        super().__init__()
        self.start = start
        self.end = end
        self.count = count
        self.outdir = outdir
        self.filename = filename
        self.dest = os.path.abspath(os.path.join(self.outdir, self.filename))

    def get_random_logs(self, count):
        """
        Picks up one random log from source file
        :return:
        """
        samples = random.sample(self._SOURCE, count)
        return samples

    def replace_all_dates(self, string, timestamp):
        """
        Replace all dates in log according to the format specified
        :param timestamp:
        :param string:
        :return:
        """
        date_pattern = re.compile('\D{3}\s+\d{1,2} \d{2}:\d{2}:\d{2}')
        new_date = to_datetime(timestamp, '%b %d %H:%M:%S')
        update1_ = re.sub(pattern=re.compile(date_pattern), repl=new_date, string=string)

        received_time_pattern = re.compile('"EventReceivedTime":"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"')
        new_received_time = '"EventReceivedTime":"' + to_datetime(timestamp, '%Y-%m-%d %H:%M:%S') + '"'
        updated2_ = re.sub(pattern=re.compile(received_time_pattern), repl=new_received_time, string=update1_)

        event_time_pattern = re.compile('"EventTime":\d{10}')
        new_event_time = '"EventTime":' + to_timestamp(timestamp)
        updated3_ = re.sub(pattern=re.compile(event_time_pattern), repl=new_event_time, string=updated2_)

        return updated3_

    def get_time_series(self):
        """
        Get time series for total log count
        :return:
        """
        time_series = pd.date_range(start=self.start, end=self.end, periods=self.count)
        return time_series

    def create_log(self, date):
        """
        generates actual log lines with all information in it.
        :param date:
        :return:
        """
        log = self.get_random_logs(1)[0]
        log = self.replace_all_dates(log, date)
        return log

    def generate_between_dates(self):
        """
        Generate logs
        :return:
        """
        with open(self.dest, 'wb') as log_file:
            for date in self.get_time_series():
                log = self.create_log(date)
                log_file.write(bytes(log, encoding='utf-8'))
                self.forward(log)
                print(log)

    def compress(self):
        """
        Compress .log file to .log.gz. deletes the original .log file
        :return:
        """
        with open(self.dest, 'rb') as f_in:
            with gzip.open(self.dest + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(self.dest)

    def generate_realtime(self):

        while True:
            now = datetime.datetime.now()
            log = self.create_log(now)
            self.forward(log)
            print(log)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log faker for generating mssql fake log')

    parser.add_argument('-c', '--count', type=int, help='How much logs you want, default to 1 billion', default=1000000)
    parser.add_argument('-o', '--outdir', type=str, help='Output dir for log file', default='destination')
    parser.add_argument('-n', '--filename', type=str, help='Filename for log file', default='mssql.log')
    parser.add_argument('-s', '--start', type=str, help='Start date from which logs will generate',
                        default='2011-01-01')
    parser.add_argument('-e', '--end', type=str, help='End date up to which logs will generate',
                        default='2020-01-01')

    parser.add_argument(
        '-m', '--mode', type=str, help='Generation mode whether logs will generate realtime or between given dates',
        default='live'
    )

    args = parser.parse_args()

    mssql = MSSQLLogGenerator(start=args.start, end=args.end, count=args.count, filename=args.filename,
                           outdir=args.outdir)
    if args.mode != 'live':
        mssql.generate_between_dates()
        mssql.compress()
    else:
        mssql.generate_realtime()
