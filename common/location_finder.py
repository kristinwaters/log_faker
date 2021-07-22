# -*- coding: utf-8 -*-
"""
Ip address location updater using local db on given parameters
Created on 10/9/2019
@author: Anurag
"""

# imports
import os
import geoip2.database

from common.config_reader import config


class LocationFinder:
    """
    A class to implement custom location updater for easy fetching of location parameters
    """

    def __init__(self, ip_address):
        """
        Instance initialisation
        :param ip_address:
        """
        self.ip_address = ip_address
        db = os.path.abspath(config.read('geoip2-db', 'db'))
        if not os.path.isfile(db):
            raise FileNotFoundError("GeoLite database is not present")
        else:
            self._reader = geoip2.database.Reader(db)
            self._result = self._reader.city(self.ip_address)
            self._reader.close()

    def get_city(self):
        """
        Method to retrieve ip address city name
        :return: city in string
        """
        return self._result.city.name

    def get_country(self):
        """
        Method to retrieve ip address country
        :return: country in string
        """
        return self._result.country.name

    def get_latitude(self):
        """
        Method to retrieve ip address latitude
        :return: latitude in string
        """
        return self._result.location.latitude

    def get_longitude(self):
        """
        Method to retrieve ip address longitude
        :return: longitude in string
        """
        return self._result.location.longitude
