import os
import re
import gzip
import shutil
import random
import argparse
import datetime

import pandas as pd

from common.log_generator import BaseLogGenerator
from utils import to_datetime, get_random_username, get_random_country, get_ip_list


class FortigateLogGenerator(BaseLogGenerator):

    _ROOT = os.path.abspath(os.path.dirname(__file__))
    _SOURCE = open(os.path.join(_ROOT, 'samples/fortigate.log'), 'r').readlines()
    _IP_STORE = get_ip_list()

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
        self.src_ip = random.sample(self._IP_STORE, 1)[0]
        self.src_country = get_random_country()
        self.username = get_random_username()

    def get_random_logs(self, count):
        """
        Picks up one random log from source file
        :return:
        """
        samples = random.sample(self._SOURCE, count)
        return samples

    def replace_all_ips(self, string):
        """
        Replace all ip address and update with random random ip
        :param string:
        :return:
        """
        dev_ip_pat = re.compile('\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}\sdate')
        new_ip = self.src_ip + ' date'
        update = re.sub(pattern=dev_ip_pat, repl=new_ip, string=string)

        ips = random.sample(self._IP_STORE, 3)

        src_ip_pat = re.compile('srcip=\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}')
        new_ip = 'srcip=' + ips[0].strip('\n')
        update = re.sub(pattern=src_ip_pat, repl=new_ip, string=update)

        dst_ip_pat = re.compile('dstip=\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}')
        new_ip = 'dstip=' + ips[1].strip('\n')
        update = re.sub(pattern=dst_ip_pat, repl=new_ip, string=update)

        tran_ip_pat = re.compile('tranip=\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}')
        new_ip = 'tranip=' + ips[2].strip('\n')
        update = re.sub(pattern=tran_ip_pat, repl=new_ip, string=update)

        return update

    def replace_all_dates(self, string, timestamp):
        """
        Replace all dates in log according to the format specified
        :param timestamp:
        :param string:
        :return:
        """

        log_date_pat = re.compile('\D{3} \d{2} \d{2}:\d{2}:\d{2}')
        new_date = to_datetime(timestamp, '%b %d %H:%M:%S')
        update1 = re.sub(pattern=log_date_pat, repl=new_date, string=string)

        date_pattern = re.compile('date=\d{4}-\d{2}-\d{2}')
        new_creation_date = 'date=' + to_datetime(timestamp, '%Y-%m-%d')
        update2 = re.sub(pattern=date_pattern, repl=new_creation_date, string=update1)

        time_pattern = re.compile('time=\d{2}:\d{2}:\d{2}')
        new_event_time = 'time=' + to_datetime(timestamp, '%H:%M:%S')
        update3 = re.sub(pattern=time_pattern, repl=new_event_time, string=update2)

        tz_pattern = re.compile('tz="\+\d{4}"')
        new_tz = 'tz="+0400"'
        update4 = re.sub(pattern=tz_pattern, repl=new_tz, string=update3)

        return update4

    def replace_other(self, string):
        """
        replace all other variables in the log
        :param string:
        :return:
        """
        src_country_pat = re.compile('srccountry="((?!Reserved).)*"')
        new_src_country = '"srccountry="' + self.src_country + '"'
        update1 = re.sub(pattern=src_country_pat, repl=new_src_country, string=string)
        return update1

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
        log = self.replace_all_ips(log)
        log = self.replace_other(log)
        log = self.replace_all_dates(log, date)
        return log.replace('\n', ' ')

    def generate_between_dates(self):
        """
        Generate logs
        :return:
        """
        with open(self.dest, 'wb') as log_file:
            for date in self.get_time_series():
                log = self.create_log(date)
                log_file.write(bytes(log, encoding='utf-8'))
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
        """
        Generates logs, with current time in it.
        :return:
        """

        while True:
            now = datetime.datetime.now()
            log = self.create_log(now)
            self.forward(log)
            print(log)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log faker for generating fake log')

    parser.add_argument('-c', '--count', type=int, help='How much logs you want, default to 1 billion', default=1000000)
    parser.add_argument('-o', '--outdir', type=str, help='Output dir for log file', default='destination')
    parser.add_argument('-n', '--filename', type=str, help='Filename for log file', default='fortigate.log')
    parser.add_argument('-s', '--start', type=str, help='Start date from which logs will generate', default='2011-01-01')
    parser.add_argument('-e', '--end', type=str, help='End date up to which logs will generate', default='2020-01-01')
    parser.add_argument(
        '-m', '--mode', type=str, help='Generation mode whether logs will generate realtime or between given dates',
        default='live'
    )

    args = parser.parse_args()

    forti = FortigateLogGenerator(start=args.start, end=args.end, count=args.count, filename=args.filename,
                           outdir=args.outdir)
    if args.mode != 'live':
        forti.generate_between_dates()
        forti.compress()
    else:
        forti.generate_realtime()
