import os
import re
import gzip
import time
import shutil
import random
import argparse
import datetime

import pandas as pd

from common.log_generator import BaseLogGenerator
from utils import get_ip_list, to_datetime, get_random_username, time_range


class SonicwallLogGenerator(BaseLogGenerator):

    _ROOT = os.path.abspath(os.path.dirname(__file__))
    _SOURCE = open(os.path.join(_ROOT, 'samples/sonicwall.log'), 'r').readlines()
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

    def get_random_logs(self, count):
        """
        Picks up one random log from source file
        :return:
        """
        samples = random.sample(self._SOURCE, count)
        return samples

    def get_time_series(self):
        """
        Get time series for total log count
        :return:
        """
        time_series = pd.date_range(start=self.start, end=self.end, periods=self.count)
        return time_series

    def replace_all_ips(self, string):
        """
        Replace all present ip address
        :param string:
        :return:
        """
        ips = random.sample(self._IP_STORE, 2)

        src_ip_pattern = re.compile('src=\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}')
        new_src_ip = 'src=' + ips[0]
        update1_ = re.sub(pattern=re.compile(src_ip_pattern), repl=new_src_ip, string=string)

        dst_ip_pattern = re.compile('dst=\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}')
        new_dst_ip = 'dst=' + ips[1]
        update2_ = re.sub(pattern=re.compile(dst_ip_pattern), repl=new_dst_ip, string=update1_)

        return update2_

    def replace_all_dates(self, string, timestamp):
        """
        Replace all timestamps present in log
        :param string:
        :param timestamp:
        :return:
        """
        date_pattern = re.compile('\D{3}\s+\d{1,2} \d{2}:\d{2}:\d{2}')
        new_date = to_datetime(timestamp, '%b %d %H:%M:%S')
        update1_ = re.sub(pattern=re.compile(date_pattern), repl=new_date, string=string)

        time = re.compile('time="\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"')
        new_time = "time=" + to_datetime(timestamp, '%Y-%m-%d %H:%M:%S')
        updated2_ = re.sub(pattern=re.compile(time), repl=new_time, string=update1_)

        vp_time = re.compile('vp_time=\"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC\"')
        new_vp_time = "vp_time=" + to_datetime(timestamp, '%Y-%m-%d %H:%M:%S')
        updated3_ = re.sub(pattern=re.compile(vp_time), repl=new_vp_time, string=updated2_)

        return updated3_

    def replace_other(self, string):
        new_user = get_random_username()

        username_pattern = re.compile('user="[A-Za-z0-9.]+"')
        new_username = 'user="' + new_user + '"'
        update = re.sub(pattern=username_pattern, repl=new_username, string=string)

        username_pattern = re.compile('usr="[A-Za-z0-9.]+"')
        new_username = 'usr="' + new_user + '"'
        update = re.sub(pattern=username_pattern, repl=new_username, string=update)

        return update

    def create_log(self, date):
        """
        generates actual log lines with all information in it.
        :param date:
        :return:
        """
        log = self.get_random_logs(1)[0]
        log = self.replace_all_ips(log)
        log = self.replace_all_dates(log, date)
        log = self.replace_other(log)
        return log.replace('\n', ' ')

    def generate_between_dates(self):
        """
        Generate logs in given two dates
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
        """
        Generates logs, with current time in it.
        :return:
        """

        while True:
            now = datetime.datetime.now()
            log = self.create_log(now)
            self.forward(log)
            period = random.sample(time_range, 1)[0]
            time.sleep(period)
            print(log)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log faker for generating mssql fake log')

    parser.add_argument('-c', '--count', type=int, help='How much logs you want, default to 1 billion, default=10000000')
    parser.add_argument('-o', '--outdir', type=str, help='Output dir for log file', default='destination')
    parser.add_argument('-n', '--filename', type=str, help='Filename for log file', default='sonicwall.log')
    parser.add_argument('-s', '--start', type=str, help='Start date from which logs will generate', default='2011-01-01')
    parser.add_argument('-e', '--end', type=str, help='End date up to which logs will generate', default='2020-01-01')
    parser.add_argument(
        '-m', '--mode', type=str, help='Generation mode whether logs will generate realtime or between given dates',
        default='live'
    )

    args = parser.parse_args()

    sonic = SonicwallLogGenerator(start=args.start, end=args.end, count=args.count, filename=args.filename,
                           outdir=args.outdir)
    if args.mode != 'live':
        sonic.generate_between_dates()
        sonic.compress()
    else:
        sonic.generate_realtime()
