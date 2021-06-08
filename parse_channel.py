"""This service allows to write new channels to db"""
import os
import time
from rq import Worker, Queue, Connection
from methods.connection import get_redis, await_job
from pyyoutube import Api
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

driver = webdriver.Remote(
command_executor='http://chromedriver:4444/wd/hub',
desired_capabilities=DesiredCapabilities.CHROME)

YOUTUBE_URL = "https://www.youtube.com/channel/"
api = Api(api_key=os.environ['YOUTUBE_TOKEN'])


def parse_channel(id):
    """Parses a channel"""
    # GET CHANNEL DATA USING API
    channel_by_id = api.get_channel_info(channel_id=id)
    data = None
    if channel_by_id.items is not None:
        data = channel_by_id.items[0].to_dict()
    if data is None:
        # log
        return False
    # GET ALL CHANNEL VIDEOS USING selenium
    q = Queue('create_tmp_table', connection=r)
    job = q.enqueue('create_tmp_table.create_tmp_table', id+"_tmp")
    await_job(job)
    if not job.result:
        return False
    driver.get(YOUTUBE_URL + id + "/videos")
    time.sleep(5)
    height = driver.execute_script("return document.documentElement.scrollHeight")
    q = Queue('write_tmp_table', connection=r)
    try:
        while True:
            prev_ht = driver.execute_script("return document.documentElement.scrollHeight;")
            driver.execute_script("window.scrollTo(0, " + str(height) + ");")
            time.sleep(3)
            height = driver.execute_script("return document.documentElement.scrollHeight")
            if prev_ht == height:
                break
    except Exception as e:
        print(e)  # LOG
    try:
        links = driver.find_elements_by_xpath('//*[@id="video-title"]')
        for i in links:
            link = (i.get_attribute('href'))
            link = link.split("watch?v=")[-1]
            print(link)
            q.enqueue('write_tmp_table.write_tmp_table', link, id+"_tmp")
    except Exception as e:
        print(e)  # LOG

    data = [data['id'], data['snippet']['title'], data['snippet']['description'],
            data['snippet']['customUrl'], data['snippet']['publishedAt'],
            data['snippet']['defaultLanguage'], data['statistics']['viewCount'],
            data['statistics']['subscriberCount'], data['statistics']['hiddenSubscriberCount'],
            data['statistics']['videoCount'], data['brandingSettings']['channel']['keywords'],
            data['brandingSettings']['channel']['country']]
    return data


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('parse_channel', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='parse_channel')
        worker.work()
