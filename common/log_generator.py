import re
import socket

from common.config_reader import ConfigReader
from common.location_finder import LocationFinder
from geoip2.errors import AddressNotFoundError


class BaseLogGenerator:

    def __init__(self):
        pass

    _CONFIG = ConfigReader()
    _SOC = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def get_random_logs(self, count):
        pass

    def replace_all_ips(self, string):
        pass

    def replace_all_dates(self, string, timestamp):
        pass

    def generate_between_dates(self):
        pass

    def forward(self, data):
        """
        Forward logs to syslog daemon
        :param data:
        :return:
        """
        host = self._CONFIG.read('syslog', 'host')
        port = self._CONFIG.read('syslog', 'port')
        self._SOC.sendto(bytes(data, encoding='utf-8'), (host, int(port)))

    def compress(self):
        pass
