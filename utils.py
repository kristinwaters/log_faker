import os
import random
import datetime
import faker.providers
import faker.providers.python
import pandas as pd
import string
import faker

fake = faker.Faker()

def generate_random_string(length=12):
 
    # ascii_letters consist of alphabets for 'a' to 'z' and 'A' to 'Z'
    # digits consist of '0' to '9'
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))


def get_ip_list():
    """
    Picks up one random log from source file
    :return:
    """
    store_file = os.path.join(os.path.dirname(__file__) + '/resource/', 'ip_store.txt')
    ip_list = open(store_file, 'r').readlines()
    return ip_list


def get_random_country():
    index = random.randint(0, 5)
    store_file = os.path.join(os.path.dirname(__file__) + '/resource/', 'country_store.txt')
    return open(store_file, 'r').readlines()[index].strip('\n')


def to_timestamp(timestamp):
    timestamp = timestamp.timestamp()
    return str(int(timestamp))


def to_datetime(timestamp, str_format):
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime().strftime(str_format)

    if isinstance(timestamp, datetime.datetime):
        return timestamp.strftime(str_format)

    if isinstance(timestamp, str):
        return timestamp


def get_random_username():
    """ returns a random username """
    return fake.profile(fields=['username'])['username']


time_range = [1, 2, 3, 4, 5]
