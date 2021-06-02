"""This service allows to write new channels to db"""
import os
import sys
import time
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor


def parse_channel(id):
    """Parses a channel"""
    return True


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('parse_channel', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='parse_channel')
        worker.work()
