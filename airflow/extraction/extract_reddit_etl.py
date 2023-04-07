import pandas as pd
import praw
import sys
import utils
# from psaw import PushshiftAPI
# from datetime import timedelta

"""
Part of Airflow DAG. Takes in one command line argument of format YYYYMMDD. 
Script will connect to Reddit API and extract top posts from past day
with no limit. For a small subreddit like Data Engineering, this should extract all posts
from the past 24 hours.
"""

# Fields that will be extracted from Reddit.
# Check PRAW documentation for additional fields.
# NOTE: if you change these, you'll need to update the create table
# sql query in the upload_aws_redshift.py file
FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)

def subreddit_posts(conn, conf, date):
    subreddit = conn.subreddit(conf["subreddit"])
    posts = subreddit.top(time_filter=conf.get("time_filter", "day"),
                          limit=conf.get("limit"))
    return posts

def extract_data(posts):
    list_of_items = []
    for submission in posts:
        to_dict = vars(submission)
        sub_dict = {field: to_dict[field] for field in FIELDS}
        list_of_items.append(sub_dict)
    return pd.DataFrame(list_of_items)

def transform(df):
    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s")
    df["edited"] = df["edited"].astype(str) == "True"
    return df

if __name__ == "__main__":
    # arg = "20230130"
    # file = "./configuration.conf"
    file = __file__
    arg = sys.argv[1]

    date = utils.parse_date_input(arg)
    conf = dict(utils.read_config(file).items("reddit_config"))

    conn = praw.Reddit(
        client_id=conf["client_id"],
        client_secret=conf["secret"],
        username=conf["username"],
        user_agent="My User Agent",
    )

    posts = subreddit_posts(conn, conf, date)
    df = extract_data(posts)
    df = transform(df)
    out_file = "/tmp/reddit-{}-{}.csv".format(conf["subreddit"], arg)
    df.to_csv(out_file, index=False)
