"""
Youtube Scraper.
By Ahmed Abousetta.

https://github.com/ahmadabousetta/youtube-scraper
---------
A simple, yet powerful Python module to scrape youtube videos and channels.
The scraped data is imported into a Pandas dataframe.

The module is a soft wrapper for Youtube data api.
https://developers.google.com/youtube/v3

Installation instructions and sample scripts are in README.md file.

"""
# -----------------------------------------------------------------------------------------

# import required libraries.

import pandas as pd
import os
import googleapiclient.discovery
import googleapiclient.errors
# -----------------------------------------------------------------------------------------


pd.options.display.max_columns = None
pd.options.display.max_rows = None
# -----------------------------------------------------------------------------------------


api_service_name = "youtube"
api_version = "v3"

with open("key.txt", "r") as f:
    API_key = f.read()

youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=API_key)
# -----------------------------------------------------------------------------------------



def search(query=None, channel_id=None, order_by='relevance', date_start="1970-01-01T00:00:00Z", date_end=None, required_results_count=10, scope="video,channel,playlist"):

    search_results_items = []
    page_token = None

    while len(search_results_items) < required_results_count :

        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            channelType="any",
            eventType="none",
            maxResults=required_results_count,
            order=order_by,
            pageToken=page_token,
            publishedAfter=date_start,
            publishedBefore=date_end,
            q=query,
            regionCode="sg",
            relevanceLanguage="en",
            safeSearch="moderate",
            type=scope,
            videoDefinition="any",
            videoDuration="any"
            )

        response = request.execute()
        search_results_items.extend(response['items'])

        try:
            page_token=response['nextPageToken']
        except:
            break

    search_results_items = search_results_items[:required_results_count]

    df = pd.DataFrame(search_results_items)
    df['kind'] = df.id.apply(lambda x: x['kind'][8:])

    def extract_id(x):
        try:
            return x['videoId']
        except:
            try:
                return x['channelId']
            except:
                return x['playlistId']
    df['id'] = df.id.apply(extract_id)

    df['published_at'] = pd.to_datetime(df.snippet.apply(lambda x: x['publishedAt']))
    df['title'] = df.snippet.apply(lambda x: x['title'])
    df['description'] = df.snippet.apply(lambda x: x['description'])
    df['channel_id'] = df.snippet.apply(lambda x: x['channelId'])
    df['channel_title'] = df.snippet.apply(lambda x: x['channelTitle'])
    df = df.drop(columns='snippet')

    return df
# -----------------------------------------------------------------------------------------


def get_video_data(video_id):

    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()

    df = pd.DataFrame(response['items'])
    df['published_at'] = pd.to_datetime(df.snippet.apply(lambda x: x['publishedAt']))
    df['title'] = df.snippet.apply(lambda x: x['title'])
    df['url'] = 'www.youtube.com/watch?v=' + df['id']
    df['description'] = df.snippet.apply(lambda x: x['description'])
    df['duration'] = pd.to_timedelta(df.contentDetails.apply(lambda x: x['duration']))
    df['views'] = df.statistics.apply(lambda x: x['viewCount'])
    df['likes'] = df.statistics.apply(lambda x: x['likeCount'])
    df['dislikes'] = df.statistics.apply(lambda x: x['dislikeCount'])
    df['favorites'] = df.statistics.apply(lambda x: x['favoriteCount'])
    df['comments'] = df.statistics.apply(lambda x: x['commentCount'])
    df['tags'] = df.snippet.apply(lambda x: x['tags'])
    df['thumbnails'] = df.snippet.apply(lambda x: x['thumbnails'])
    df['channel_id'] = df.snippet.apply(lambda x: x['channelId'])
    df['channel_title'] = df.snippet.apply(lambda x: x['channelTitle'])
    df = df.drop(columns=['kind', 'snippet', 'contentDetails', 'statistics'])

    return df
# -----------------------------------------------------------------------------------------


def get_channel_videos(channel_id, since="1970-01-01T00:00:00Z", to_date=None, results_count=100):
    return search(query=None, channel_id=channel_id, order_by='date', date_start=since, date_end=to_date, required_results_count=results_count, scope="video")
# -----------------------------------------------------------------------------------------


def get_channel_data(channel_id):

    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()

    df = pd.DataFrame(response['items'])
    df['published_at'] = pd.to_datetime(df.snippet.apply(lambda x: x['publishedAt']))
    df['title'] = df.snippet.apply(lambda x: x['title'])
    df['url'] = 'www.youtube.com/channel/' + df['id']
    df['description'] = df.snippet.apply(lambda x: x['description'])
    df['views'] = df.statistics.apply(lambda x: x['viewCount'])
    df['subscribers'] = df.statistics.apply(lambda x: x['subscriberCount'])
    df['video_count'] = df.statistics.apply(lambda x: x['videoCount'])
    df['thumbnails'] = df.snippet.apply(lambda x: x['thumbnails'])
    df['custom_url'] = df.snippet.apply(lambda x: x['customUrl'])


    df = df.drop(columns=['kind', 'snippet', 'statistics'])

    return df
# -----------------------------------------------------------------------------------------


def get_video_top_level_comments(video_id, order_by='time', results_count=100):

        request = youtube.commentThreads().list(
        part="snippet,replies",
        maxResults=results_count,
        order=order_by,
        videoId=video_id
        )
        response = request.execute()

        df = pd.DataFrame(response['items'])
        df['id'] = df.snippet.apply(lambda x: x['topLevelComment']['id'])
        df['comment'] = df.snippet.apply(lambda x: x['topLevelComment']['snippet'])
        df['published_at'] = df.comment.apply(lambda x: x['publishedAt'])
        df['updated_at'] = df.comment.apply(lambda x: x['updatedAt'])
        df['text'] = df.comment.apply(lambda x: x['textDisplay'])
        df['author'] = df.comment.apply(lambda x: x['authorDisplayName'])
        df['likes'] = df.comment.apply(lambda x: x['likeCount'])
        df['author_image'] = df.comment.apply(lambda x: x['authorProfileImageUrl'])
        df['author_channel_id'] = df.comment.apply(lambda x: x['authorChannelId']['value'])
        df['author_channel_url'] = df.comment.apply(lambda x: x['authorChannelUrl'])

        df = df.drop(columns=['kind', 'snippet', 'comment'])

        return df
# -----------------------------------------------------------------------------------------
