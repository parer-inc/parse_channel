"""This service allows to write new channels to db"""
import os
import sys
import time
import datetime
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor, await_job
from pyyoutube import Api
import urllib.request, urllib.error, urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidArgumentException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.utils import ChromeType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

driver = webdriver.Remote(
command_executor='chromedriver:4444/wd/hub',
desired_capabilities=DesiredCapabilities.CHROME)
YOUTUBE_URL = "https://www.youtube.com/channel/"
api = Api(api_key=os.environ['YOUTUBE_TOKEN'])


def parse_channel(id):
    """Parses a channel"""
    # GET CHANNEL DATA USING API
    channel_by_id = api.get_channel_info(channel_id=id)
    api_data = None
    if channel_by_id.items is not None:
        api_data = channel_by_id.items[0].to_dict()


    # GET ALL CHANNEL VIDEOS USING selenium
    q = Queue('create_tmp_table', connection=r)
    job = q.enqueue('create_tmp_table.create_tmp_table', id+"_tmp")
    await_job(job)
    if not job.result:
        return False  # Not sure
    driver.get(YOUTUBE_URL + id)
    time.sleep(5)
    height = driver.execute_script("return document.documentElement.scrollHeight")
    lastheight = 0
    q = Queue('write_tmp_table', connection=r)
    while True:
        if lastheight == height:
            break
        lastheight = height
        driver.execute_script("window.scrollTo(0, " + str(height) + ");")
        time.sleep(4)
        height = driver.execute_script("return document.documentElement.scrollHeight")
        user_data = driver.find_elements_by_xpath('//*[@id="video-title"]')
        for i in user_data:
            link = (i.get_attribute('href'))
            print(link)
            job = q.enqueue('write_tmp_table.write_tmp_table', link, id+"_tmp")

    return api_data


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('parse_channel', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='parse_channel')
        worker.work()
