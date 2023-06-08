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

# USERNAME_DB = auth.user_db
# PASSWORD_DB = auth.password_db
# HOST_DB = auth.host
# NAME_DB = auth.database
# PORT_DB = auth.port
STRING_CONNECTION = auth.string_connection

def extract(subreddit_name, limit_post):
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
                'created' : submission.created,
                'num_comments' : submission.num_comments,
                'score' : submission.score,
                'text' : submission.selftext,
                'url': submission.url,             
            }
        )
    return data

def transform(data):
    data = pd.DataFrame(data)
    data['created'] = pd.to_datetime(data['created'])
    return data

def load(data):
    engine = create_engine(STRING_CONNECTION)
    conn = engine.connect()
    
    data.to_sql('reddit_extraction', con=conn, if_exists='replace', index=False)
    """conn = psycopg2.connect(conn_string)"""
    
    conn.autocommit = True
    conn.close()

def main():
    print("Extraction...")
    data = extract('Autism', 2500)
    print("Transformation...")
    transformed_data = transform(data)
    print("Loading...")
    load(transformed_data)
    print("End!")

if __name__ == '__main__':
    main()