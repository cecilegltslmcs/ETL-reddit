import praw
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

PERSONAL_SCRIPT = os.getenv("personal_script")
CLIENT_SECRET = os.getenv("secret")
USERNAME = os.getenv("username")
PASSWORD = os.getenv("password")
USER_AGENT = os.getenv("user_agent")

STRING_CONNECTION = os.getenv("string_connection_docker")

def extract(subreddit_name, limit_post):
    print("Name of the subreddit: ", subreddit_name)
    print("Extraction...")
    client = praw.Reddit(
        client_id = PERSONAL_SCRIPT,
        client_secret = CLIENT_SECRET,
        username = USERNAME,
        password = PASSWORD,
        user_agent = USER_AGENT
    )

    subreddit = client.subreddit(subreddit_name)
    reddit_post = subreddit.hot(limit=limit_post)
    data = []

    for submission in reddit_post:
        yield {
            'id': submission.id,
            'subreddit_name': subreddit_name,
            'title': submission.title,
            'num_comments': submission.num_comments,
            'score': submission.score,
            'text': submission.selftext,
            'url': submission.url,
        }
    
    return data

def transform(data):
    print("Transformation...")
    data = pd.DataFrame(data)
    data['id'] = data['id'].values.astype(str)
    return data

def load(data):
    print("Loading on database...")
    engine = create_engine(STRING_CONNECTION)
    data.to_sql("reddit_extraction", engine, if_exists='append')
    return print('ETL FINISHED! ')

def main(subreddit_name):
    data = extract(subreddit_name, 2500)
    transformed_data = transform(data)
    load(transformed_data)

if __name__ == '__main__':
    subreddit = ["Autism", "AutismInWomen"]
    for i in subreddit:
        main(i)