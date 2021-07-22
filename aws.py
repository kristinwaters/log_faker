import os
import re
import time
import gzip
import shutil
import random
import argparse
import datetime

import pandas as pd
from geoip2.errors import AddressNotFoundError

from common.log_generator import BaseLogGenerator
from common.location_finder import LocationFinder
from utils import to_datetime, get_ip_list, get_random_username, time_range


class AWSLogsGenerator(BaseLogGenerator):

    _ROOT = os.path.abspath(os.path.dirname(__file__))
    _SOURCE = open(os.path.join(_ROOT, 'samples/aws.log'), 'r').readlines()
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
        ip_pattern = re.compile('"sourceIPAddress":"\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"')
        new_ip = '"sourceIPAddress":"' + self.src_ip.strip() + '"'
        update = re.sub(pattern=ip_pattern, repl=new_ip, string=string)
        return update

    def replace_all_dates(self, string, timestamp):
        """
        Replace all dates in log according to the format specified
        :param timestamp:
        :param string:
        :return:
        """

        new_date = to_datetime(timestamp, '%b %d %H:%M:%S')
        update = new_date + " console - " + string

        creation_date = re.compile('"creationDate":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"')
        new_creation_date = '"creationDate":"' + to_datetime(timestamp, '%Y-%m-%dT%H:%M:%SZ') + '"'
        update = re.sub(pattern=creation_date, repl=new_creation_date, string=update)

        event_time = re.compile('"eventTime":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"')
        new_event_time = '"eventTime":"' + to_datetime(timestamp, '%Y-%m-%dT%H:%M:%SZ') + '"'
        update = re.sub(pattern=event_time, repl=new_event_time, string=update)

        return update

    def replace_other(self, string):
        """
        replace all other variables in the log
        :param string:
        :return:
        """
        username_pattern = re.compile('"userName":"\w+"')
        new_username = '"userName":"' + self.username + '"'
        update1 = re.sub(pattern=username_pattern, repl=new_username, string=string)

        arn_pattern = re.compile('"arn":"arn:aws:iam::202925831767:user/\w+')
        new_arn = '"arn":"arn:aws:iam::202925831767:user/' + self.username
        update2 = re.sub(pattern=arn_pattern, repl=new_arn, string=update1)

        return update2

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
        Generate logs in given two dates
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

    def add_location(self, log: str) -> str:
        """
        A method to return ip geolocation if ip present in log
        :param log:
        :return:
        """
        ip_pat = re.compile(r'sourceIPAddress":"(?P<src_ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})"')
        match = ip_pat.search(log)
        try:
            if match:
                ip = match.group('src_ip')
                city = LocationFinder(ip).get_city()
                lat = LocationFinder(ip).get_latitude()
                long = LocationFinder(ip).get_longitude()
                location = r'''"location": {"ip_city":"%s","latitude":"%s","longitude":"%s"}''' % (city, lat, long)
                updated = log.strip('\n') + location
                return updated
            return log
        except AddressNotFoundError:
            return log

    def generate_realtime(self):
        """
        Generates logs, with current time in it.
        :return:
        """

        while True:
            now = datetime.datetime.now().strftime('%b %d %H:%M:%S')
            log = self.create_log(now)
            updated = self.add_location(log)
            self.forward(updated)
            period = random.sample(time_range, 1)[0]
            time.sleep(period)
            print(updated)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log faker for generating fake log')

    parser.add_argument('-c', '--count', type=int, help='How much logs you want, default to 1 billion', default=10000000)
    parser.add_argument('-o', '--outdir', type=str, help='Output dir for log file', default='destination')
    parser.add_argument('-n', '--filename', type=str, help='Filename for log file', default='aws.log')
    parser.add_argument('-s', '--start', type=str, help='Start date from which logs will generate', default='2011-01-01')
    parser.add_argument('-e', '--end', type=str, help='End date up to which logs will generate', default='2020-01-01')
    parser.add_argument(
        '-m', '--mode', type=str, help='Generation mode whether logs will generate realtime or between given dates',
        default='live'
    )

    args = parser.parse_args()

    aws = AWSLogsGenerator(start=args.start, end=args.end, count=args.count, filename=args.filename,
                           outdir=args.outdir)
    if args.mode != 'live':
        aws.generate_between_dates()
        aws.compress()
    else:
        aws.generate_realtime()
