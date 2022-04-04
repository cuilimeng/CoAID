"""
Script for fetching tweets using Tweepy API

This will create folder data/ in each folder dataset
"""

import json
import os
import time
from glob import glob
from typing import Union

import pandas as pd
import tweepy
from tqdm import tqdm


def read_conf(pth="./tweet_keys_file.json"):
    """Load Twitter API Key

    :param pth: path of your Twitter API conf in JSON format
    :type pth: str
    :return: twitter configuration API
    :rtype: dict
    """
    return load_json(pth)


def get_api(data, wait_on_rate_limit=True):
    """Start a new Client for twitter API

    :param data: configuration of API
    :type data: dict
    :param wait_on_rate_limit: The API should wait if error, defaults to True
    :type wait_on_rate_limit: bool, optional
    :return: Client Twitter API
    :rtype: tweepy.Client
    """
    api = tweepy.Client(
        data["oauth_token"],
        data["app_key"],
        data["app_secret"],
        wait_on_rate_limit=wait_on_rate_limit,
    )
    return api


def load_json(pth):
    """Read json file

    :param pth: path of the json file
    :type pth: str
    :return: file content
    :rtype: Any
    """
    return json.load(open(pth, "r"))


def look_up_tweets(client: tweepy.Client, ids: list, base_pth: str):
    """Use client for look_up of tweet ids

    :param client: The client to use
    :type client: tweepy.Client
    :param ids: list of tweet ids
    :type ids: list
    :param base_pth: folder where to save tweets
    :type base_pth: str
    """
    for i in tqdm(range(len(ids) // 100 + 1)):
        tweets_ids = ids[100 * i: 100 * (i + 1)]
        if len(tweets_ids) < 1:
            return
        response = client.get_tweets(
            tweets_ids,
            expansions=[
                "author_id",
                "in_reply_to_user_id",
                "referenced_tweets.id",
                "referenced_tweets.id.author_id",
            ],
            **{
                "tweet.fields": ["created_at", "public_metrics", "lang"],
                "user.fields": ["created_at", "public_metrics", "id"],
            },
        )
        try:
            tweets = response.data
            for tweet in tweets:
                json.dump(tweet.data, open(base_pth + "/" + str(tweet.id) + ".json", "w"))
        except Exception as ex:
            print(ex)


def load_pandas(pth, join=True):
    data = pd.read_csv(pth)
    columns = data.columns[1:]
    return get_tweet_ids_from_df(data, columns, join)


def get_tweet_ids_from_df(data, columns: Union[str, list], join: bool = True):
    columns = [columns] if isinstance(columns, str) else columns
    result = [list(set(data[col].values)) for col in columns]
    if join:
        result = list(set([j for i in result for j in i]))
    return result


def get_name(file_name: str):
    return file_name.split("/")[-1].split(".csv")[0]


def list_tweet_ids_files():
    files = glob("./*/*tweet*.csv")
    return files


def get_folder(pth: str):
    name = get_name(pth)
    return pth.split(name)[0]+"data/"+name


pths = list_tweet_ids_files()
for pth in pths:
    data = load_pandas(pth)
    base_pth = get_folder(pth)
    if not os.path.exists(base_pth):
        os.makedirs(base_pth)
        client = get_api(read_conf(), True)
        look_up_tweets(client, data, base_pth)
        del client
        time.sleep(1)
