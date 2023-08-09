import praw
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime

def connection_to_reddit(personal_script: str, 
                         client_secret: str, 
                         username: str,
                         password: str,
                         user_agent: str) -> praw.reddit.Reddit:
    """Takes the different credentials generate by Reddit app (https://www.reddit.com/prefs/apps) 
    and creates a client.

    Args:
        personal_script (str): personal script generates by Reddit app.
        client_secret (str): client secret generates by Reddit app.
        username (str): username used associated to Reddit account.
        password (str): password used associated to Reddit account.
        user_agent (str): user agent generates by Reddit app.

    Returns:
        praw.reddit.Reddit: Reddit client from Praw library
    """
    try:
        client = praw.Reddit(
        client_id = personal_script,
        client_secret = client_secret,
        username = username,
        password = password,
        user_agent = user_agent
    )
    except:
        print("Failed to connect to reddit! ")
        
    return client

def extract(subreddit_name: str, 
            limit_post: int,
            client):
    """Takes the name of the subreddit name to extract data,
    the number of posts to extract and the client creates 
    by the function connection_to_reddit

    Args:
        subreddit_name (str): name of the subreddit to explore
        limit_post (int): number of reddit posts to extract
        client (_type_): client create with connection_to_reddit function

    Yields:
        Iterator[list]: list of Reddit posts extracted
    """
    print("Name of the subreddit: ", subreddit_name)
    print("Extraction...")
    subreddit = client.subreddit(subreddit_name)
    reddit_post = subreddit.hot(limit=limit_post)

    for submission in reddit_post:
        yield {
            'id': submission.id,
            'subreddit_name': subreddit_name,
            'title': submission.title,
            'num_comments': submission.num_comments,
            'date': submission.created,
            'score': submission.score,
            'text': submission.selftext,
            'url': submission.url,
        }
    
def transform(data: list) -> pd.DataFrame:
    """Takes the list of data in order to transform data
    before storage in database

    Args:
        data (list): list of data extract from Reddit

    Returns:
        pd.DataFrame: DataFrame with transformed data
    """
    print("Transformation...")
    data = pd.DataFrame(data)
    data['id'] = data['id'].values.astype(str)
    data['date'] = pd.to_datetime(data['date'], unit='s')
    data.set_index('id')
    return data

def connection_to_database(string_connection: str) -> sqlalchemy.engine.base.Engine:
    """Takes the string connection to database
    and return an engine

    Args:
        string_connection (str): uri connection to the database in form of
        postgresql://username:password@localhost:5432/db

    Returns:
        sqlalchemy.engine.base.Engine: SQLAlchemy engine related to the uri
    """
    try:
        engine = create_engine(string_connection)
    except:
        print("Failed to connect to the database! ")
    return engine

def load(data: pd.DataFrame,
         engine: sqlalchemy.engine.base.Engine) -> str:
    """Takes the transformed data and
    the engine in order to load data in database

    Args:
        data (pd.DataFrame): transformed dataframe from Reddit extraction
        engine (sqlalchemy.engine.base.Engine): engine for connection to database

    Returns:
        str: return statement related to the loading of the data
    """
    print("Loading on database...")
    try:
        data.to_sql("reddit_extraction", engine, if_exists='append')
        return 'Data loaded into database!'
    except:
        return 'Error when loading data in database! '

def main(subreddit_name: str,
         client: praw.reddit.Reddit,
         engine: sqlalchemy.engine.base.Engine) -> str:
    """Takes the name of the subreddit, the praw client and the SQL engine
    in order to realise the ETL step

    Args:
        subreddit_name (str): name of the subreddit to extract
        client (praw.reddit.Reddit): praw client to realise connection to Reddit
        engine (sqlalchemy.engine.base.Engine): Engine to realise connection to the database

    Returns:
        str: return statement related to the state of the ETL
    """
    try:
        data = extract(subreddit_name, 2500, client)
        transformed_data = transform(data)
        load(transformed_data, engine)
        return 'ETL FINISHED!'
    except:
        return 'Error during ETL realisation'

if __name__ == '__main__':
    load_dotenv()
    PERSONAL_SCRIPT = os.getenv("personal_script")
    CLIENT_SECRET = os.getenv("secret")
    USERNAME = os.getenv("username")
    PASSWORD = os.getenv("password")
    USER_AGENT = os.getenv("user_agent")
    STRING_CONNECTION = os.getenv("string_connection_docker")
    
    subreddit = ["Autism", "AutismInWomen"]
    
    client = connection_to_reddit(PERSONAL_SCRIPT, 
                                    CLIENT_SECRET, 
                                    USERNAME, 
                                    PASSWORD,
                                    USER_AGENT)
    
    engine = connection_to_database(STRING_CONNECTION)
    
    for i in subreddit:
        main(i, client, engine)