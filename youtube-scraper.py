"""
Youtube Scraper.
By Ahmed Abousetta.

https://github.com/ahmadabousetta/youtube-scraper
---------
A simple, yet powerful Python module to scrape youtube videos and channels.
The scraped data is imported into a Pandas dataframe.

The module is a soft wrapper for Youtube data api.
https://developers.google.com/youtube/v3

Installation instructions are in README.md file.

"""
# -----------------------------------------------------------------------------------------

# Import required libraries.

import pandas as pd
import os
import googleapiclient.discovery
import googleapiclient.errors
# -----------------------------------------------------------------------------------------

# Adjust Pandas display options.

pd.options.display.max_columns = None
pd.options.display.max_rows = None
# -----------------------------------------------------------------------------------------

# Prepare API connection using API_key
# The module doesn't use OAuth

api_service_name = "youtube"
api_version = "v3"

with open("API_key.txt", "r") as f:
    API_key = f.read()

youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=API_key)
# -----------------------------------------------------------------------------------------

def search(query=None, channel_id=None, order_by='relevance', date_start="1970-01-01T00:00:00Z",
           date_end=None, required_results_count=10, scope="video,channel,playlist",
           region_code=None, language=None, safe_search='moderate'):

    """
    Perform a Youtube search and return the search results in an organized Pandas DataFrame.

    The search offer the advanced search options available at Youtube.
    https://developers.google.com/youtube/v3/docs/search/list

    Parameters
    ----------%s
    query : string
        Search query. The text you put in the search toolbar.
    channel_id : string, default None
        Unique Youtube channel id in case you want to filter your search to a certain channel.
        Channel id is the last part of youtube channel url address.
        www.youtube.com/channel/UCcIgGcUE-nb1tYKya3Qtp0Q channel id is 'UCcIgGcUE-nb1tYKya3Qtp0Q'
    order_by :  {'date', 'rating', 'title', 'viewCount', 'relevance'}, default 'relevance'
        How the search results are ordered in output.
    date_start : timedate
        Earliest search result, default 1970-01-01
    date_end : timedate
        Latest search result, default current datetime
    required_results_count : int
        Count of search results, default 10.
        Use this feature wisely to avoid reaching daily quota limits.
    scope : str
        Searching scope. 'video', 'channel' or 'playlist', default "video,channel,playlist".
    region_code : str
        ISO 3166-1 alpha-2 country code.
        Search from a certain region, default None .
        use code like 'eg' for Egypt and 'sg' for Singapore.
    language: str
        ISO 639-1 two-letter language code. However, you should use the values zh-Hans for simplified Chinese and zh-Hant for traditional Chinese.
        preferred results language, default None.
        Use code like 'en' for English.
    safe_search: {'moderate', 'none', 'strict'}, default 'moderate'
        Safe search filter.

    Returns: Pandas DataFrame with search results.
    """

    # the api returns only 50 search results. so we fill this list 50 by 50.
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
            regionCode=region_code,
            relevanceLanguage=language,
            safeSearch=safe_search,
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

    '''
    A function that returns all meta data of a specific video.

    Parameters
    ----------%s
    video_id : int
        A unique identifier of every video on youtube.
        It's the last part of the video url.

    Returns : Pandas DataFrame containing video meta data.
    '''

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


def get_channel_videos(channel_id, since="1970-01-01T00:00:00Z", to_date=None, order_by='date', results_count=100):

    '''
    A function that returns a list of videos in a specific channel id.

    Parameters
    ----------%s
    channel_id : int
        The channel we want to scrape.
    since: datetime
        Earliest search result.
    to_date: datetime
        Latest search result.
    order_by :  {'date', 'rating', 'title', 'viewCount', 'relevance'}, default 'date'
        How the search results are ordered in output.
    results_count: int, default: 100
        required number of videos.

    Returns : Pandas DataFrame containing a list of videos in channel.
    '''
    return search(query=None, channel_id=channel_id, order_by=order_by, date_start=since, date_end=to_date, required_results_count=results_count, scope="video")
# -----------------------------------------------------------------------------------------


def get_channel_data(channel_id):

    '''
    A function that returns a meta data of a specific channel id.

    Parameters
    ----------%s
    channel_id : int
        The channel we want to get meta data about.

    Returns : Pandas DataFrame containing channel meta data.
    '''

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
    df['custom_url'] = df.snippet.apply(lambda x: x.get('customUrl'))


    df = df.drop(columns=['kind', 'snippet', 'statistics'])

    return df
# -----------------------------------------------------------------------------------------


def get_video_top_level_comments(video_id, order_by='time', results_count=100):

    '''
    A function that returns a list of top level comments of a specific video id.

    Parameters
    ----------%s
    video_id : int
        The video we want to scrape comments from.
    order_by :  {'time', 'relevance'}, default 'relevance'
        How the comments are listed.
    results_count: int, default: 100 (youtube allow max 100)
        required number of top level comments.

    Returns : Pandas DataFrame containing a list of top level comments of a video.
    '''
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
