import praw
import utils.authentification_token as auth
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

PERSONAL_SCRIPT = auth.personal_script
CLIENT_SECRET = auth.secret
USERNAME = auth.username
PASSWORD = auth.password
USER_AGENT = auth.user_agent

STRING_CONNECTION = auth.string_connection

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
        data.append(
            {   'id' : submission.id,
                'subreddit_name' : subreddit_name,
                'title': submission.title,
                'num_comments' : submission.num_comments,
                'score' : submission.score,
                'text' : submission.selftext,
                'url': submission.url,             
            }
        )
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