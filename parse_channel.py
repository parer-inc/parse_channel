"""This service allows to prase channels info"""
import os
import re
import json
import requests
from rq import Worker, Queue, Connection
from methods.connection import get_redis, await_job
from pyyoutube import Api

api = Api(api_key=os.environ['YOUTUBE_TOKEN'])
r = get_redis()


def get_init_data(url):
    resp_txt = requests.get(url).text
    API_KEY = resp_txt.split('"INNERTUBE_API_KEY":"', 1)[-1].split('"', 1)[0]
    API_VERSION = resp_txt.split(
        'client.version\\x3d', 1)[-1].split("')", 1)[0]
    token = re.search(r'"token":"(.*?)"', resp_txt).groups()[0]
    params = (
        ('key', API_KEY),
    )
    resp = resp_txt.split('var ytInitialData = ',
                          1)[-1].split(';</script>', 1)[0]
    resp = json.loads(resp)
    return API_VERSION, params, token, resp


def parse_channel(id):  # "UCXuqSBlHAE6Xw-yeJA0Tunw" "UCIIDymHgUB6wD91-h8wlZdQ"
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
    q = Queue('write_tmp_table', connection=r)
    print("Parsing")
    url = f"https://www.youtube.com/channel/{id}/videos"
    start = True
    params, resp, token = None, None, None
    continuationheaders = {"x-youtube-client-name": "1",
                           "x-youtube-client-version": None, "Accept-Language": "en-US"}
    data = {"context": {"client": {"hl": "en", "gl": "US",
                                   "clientName": "WEB", "clientVersion": None}, "originalUrl": url}}
    while True:
        if start:
            API_VERSION, params, token, resp = get_init_data(url)
            continuationheaders["x-youtube-client-version"] = API_VERSION
            data["context"]["client"]["clientVersion"] = API_VERSION
            resp = resp['contents']['twoColumnBrowseResultsRenderer']['tabs'][1]['tabRenderer']['content'][
                'sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0]['gridRenderer']['items']
            start = False
        else:
            resp = requests.post(
                "https://www.youtube.com/youtubei/v1/browse", params=params, json=data)
            resp = json.loads(resp.text)
            resp = resp['onResponseReceivedActions'][0]['appendContinuationItemsAction']['continuationItems']
        token = resp[-1]
        vid_threads = resp[:-1]
        for vid in vid_threads:
            video_id = vid['gridVideoRenderer']['videoId']
            try:
                q.enqueue('write_tmp_table.write_tmp_table', video_id, id+"_tmp")
            except Exception as e:
                print(e)  # LOG
        try:
            token = token['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
            data["continuation"] = token
        except Exception:
            break

    data = [data['id'], data['snippet']['title'], data['snippet']['description'],
            data['snippet']['customUrl'], data['snippet']['publishedAt'],
            data['snippet']['defaultLanguage'], data['statistics']['viewCount'],
            data['statistics']['subscriberCount'], data['statistics']['hiddenSubscriberCount'],
            data['statistics']['videoCount'], data['brandingSettings']['channel']['keywords'],
            data['brandingSettings']['channel']['country']]
    return data


if __name__ == '__main__':
    q = Queue('parse_channel', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='parse_channel')
        worker.work()
