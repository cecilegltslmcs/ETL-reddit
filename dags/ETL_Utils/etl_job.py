import praw
import authentification_token as auth
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

PERSONAL_SCRIPT = auth.personal_script
CLIENT_SECRET = auth.secret
USERNAME = auth.username
PASSWORD = auth.password
USER_AGENT = auth.user_agent

STRING_CONNECTION = auth.string_connection

def extract_data():
    # print("Name of the subreddit: ", subreddit_name)
    print("Extraction...")
    client = praw.Reddit(
        client_id = PERSONAL_SCRIPT,
        client_secret = CLIENT_SECRET,
        username = USERNAME,
        password = PASSWORD,
        user_agent = USER_AGENT
    )

    subreddit = client.subreddit("Autism")
    reddit_post = subreddit.hot(limit=2500)
    data = []

    for submission in reddit_post:
        data.append(
            {   'id' : submission.id,
                'subreddit_name' : subreddit,
                'title': submission.title,
                'num_comments' : submission.num_comments,
                'score' : submission.score,
                'text' : submission.selftext,
                'url': submission.url,             
            }
        )
    return data

def transform_data(data):
    print("Transformation...")
    data = pd.DataFrame(data)
    data['id'] = data['id'].values.astype(str)
    return data
 
def load_data(data):
    print("Loading on database...")
    engine = create_engine(STRING_CONNECTION)
    data.to_sql("reddit_extraction", engine, if_exists='append')
    return print('ETL FINISHED! ')

def etl_job_complete():
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
